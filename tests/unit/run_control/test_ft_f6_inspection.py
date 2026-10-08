"""FT-F6: inspection enrichment (lane, sessions, mailbox, delivery reports, cursor, chain,
subscriptions, stop fence) from authority Mission Control holds; no provider is called.

Deterministic: the real reducer, receipt ledger, mailbox and stop fence stores, the C1/C3
transcript over fixture provider frames, and in-memory chain and subscription reads.
"""

from __future__ import annotations

import json
from typing import Any, Literal
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from fastmcp import Client
from pydantic import BaseModel, ConfigDict

from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.harness.describe import DECLARED_LANE_MATRICES
from mission_control.application.execution.stop_fence import InMemoryStopFenceRepository
from mission_control.application.frames.transcript import TranscriptService
from mission_control.application.missions.inspection import InspectionSources
from mission_control.application.missions.service import MissionControlService
from mission_control.contracts.contracts import (
    ChainLinkView,
    ChainMembership,
    MissionCommandRequest,
    MissionInspection,
)
from mission_control.domain.frames.transcript import TranscriptQuery
from mission_control.domain.policies.contracts import RunProjection
from mission_control.interfaces.cli.main import inspection_wait_state, main
from mission_control.interfaces.mcp.coordinator_server import create_coordinator_server
from mission_control.interfaces.mcp.run_control_tools import RUN_INSPECT_TOOL, ScopedRunControl
from tests.fixtures.provider_frames import FIXTURE_RUN
from tests.fixtures.transcripts import (
    StaticFrames,
    transcript_events,
    transcript_frames,
)
from tests.unit.coordinator.test_coordinator_mcp_read_surface import FakeFacade
from tests.unit.run_control.test_ft_f1_command_mailbox import (
    SCOPE,
    Resolver,
    World,
    _app,
    operator,
)


class PreF6Inspection(BaseModel):
    """`mc.inspection.v1` exactly as a client written before SPEC-06 parses it."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["mc.inspection.v1"]
    run_id: str
    version: int
    execution_generation: int
    lifecycle: str
    phase: str
    execution_outcome: str | None
    projection: RunProjection


class AnyRunEvents:
    """The fixture transcript's mission events, served for whichever run is inspected."""

    def __init__(self) -> None:
        self._events = transcript_events(transcript_frames())

    async def run_identity(self, request_scope: str, run_key: str) -> UUID | None:
        del request_scope, run_key
        return FIXTURE_RUN

    async def events_for_run(
        self, request_scope: str, run_key: str, *, limit: int = 20_000
    ) -> tuple[Any, ...]:
        del request_scope, run_key
        return tuple(self._events[:limit])


class Sections:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def chain_membership(self, request_scope: str, run_id: str) -> ChainMembership | None:
        self.calls.append(run_id)
        return ChainMembership(
            chain_id="chain-1",
            chain_key="research-then-ingest",
            lifecycle="running",
            links=(
                ChainLinkView(
                    link_key="research-supplies-ingestion",
                    from_mission_key="research",
                    to_mission_key="ingestion",
                    kind="supplies",
                    state="armed",
                ),
            ),
        )

    async def active_subscriptions(self, request_scope: str, run_id: str) -> int:
        return 2


def transcripts() -> TranscriptService:
    return TranscriptService(
        AnyRunEvents(),  # type: ignore[arg-type]
        StaticFrames(transcript_frames()),  # type: ignore[arg-type]
        request_scope=SCOPE,
    )


async def enriched_world() -> tuple[World, MissionControlService, InMemoryStopFenceRepository]:
    world = await World().start()
    fences = InMemoryStopFenceRepository()
    world.facade = MissionControlService(
        world.authority,
        BoundaryInterventionService(world.authority),
        request_scope=SCOPE,
        mailbox=world.mailbox,
        stop_fences=fences,
        inspection=InspectionSources(transcripts=transcripts(), sections=Sections()),
    )
    return world, world.facade, fences


def test_existing_clients_keep_parsing_the_inspection_body() -> None:
    fields = set(MissionInspection.model_fields)
    added = {
        "lineage",
        "lane",
        "sessions",
        "mailbox",
        "delivery_reports",
        "frames_cursor",
        "chain",
        "subscriptions",
        "stop_fence",
    }
    assert set(PreF6Inspection.model_fields) == fields - added
    for name in added:
        assert not MissionInspection.model_fields[name].is_required()


@pytest.mark.asyncio
async def test_an_unenriched_inspection_has_exactly_the_pre_f6_body() -> None:
    world = await World().start()
    plain = MissionControlService(
        world.authority, BoundaryInterventionService(world.authority), request_scope=SCOPE
    )
    body = (await plain.inspect(world.run_id, operator())).model_dump(mode="json")
    assert PreF6Inspection.model_validate(body)
    assert set(body) == set(PreF6Inspection.model_fields)


@pytest.mark.asyncio
async def test_inspection_sections_come_from_frames_and_the_ledger() -> None:
    world, facade, _fences = await enriched_world()
    await world.queue("Prefer the 2024 meta-analysis.")
    await world.queue("delivered note")
    await world.mailbox.deliver(
        SCOPE,
        world.run_id,
        delivery_key="key-1",
        family="GoalDirected",
        node_key="goal/executor",
        iteration_start=True,
        lane_profile="deep_agents",
    )

    inspection = await facade.inspect(world.run_id, operator())

    describe = DECLARED_LANE_MATRICES["deep_agents"]
    assert inspection.lane is not None
    assert inspection.lane.lane_profile == "deep_agents"
    assert inspection.lane.describe_digest == describe.digest
    assert inspection.lane.native_session_refs
    (session,) = inspection.sessions or ()
    assert (session.turn_count, session.tool_calls) == (1, 1)
    assert session.last_turn_status == "finished"
    assert session.usage_disposition is not None
    # The transcript cursor opens nothing newer: it is where the transcript stands.
    assert inspection.frames_cursor is not None
    assert inspection.frames_cursor.frame_count == len(transcript_frames())
    assert inspection.frames_cursor.last_arrival_ordinal == 8
    page = await transcripts().materialize(
        world.run_id,
        actor=operator(),
        query=TranscriptQuery(since=inspection.frames_cursor.transcript_cursor),
    )
    assert page.entries == ()
    # The mailbox is reference-only.
    mailbox = inspection.mailbox or ()
    assert [(item.state, item.admission_sequence) for item in mailbox] == [
        ("delivered", 1),
        ("delivered", 2),
    ]
    dumped = inspection.model_dump_json()
    assert "Prefer the 2024 meta-analysis" not in dumped
    # Every Command carries requested and delivered semantics and its outcome.
    reports = {item.command_id: item for item in inspection.delivery_reports or ()}
    assert len(reports) == 2
    for item in reports.values():
        assert (item.kind, item.lifecycle) == ("queue_instruction", "delivered")
        assert item.requested_semantics == item.delivered_semantics == "turn_boundary_guaranteed"
        assert item.observed_outcome == "delivered"
        assert "key-1" in item.native_refs
        assert item.outcome is None
    assert inspection.chain is not None and inspection.chain.chain_key == "research-then-ingest"
    assert inspection.subscriptions is not None and inspection.subscriptions.active == 2
    assert inspection.stop_fence is None  # no immediate cancel: the key is absent
    assert "stop_fence" not in inspection.model_dump(mode="json")


@pytest.mark.asyncio
async def test_delivery_reports_cover_non_mailbox_commands_and_the_stop_fence_report() -> None:
    world, facade, _fences = await enriched_world()
    version = (await world.projection()).version
    cancel = MissionCommandRequest.model_validate(
        {
            "request_id": str(uuid4()),
            "expected_version": version,
            "expected_generation": 1,
            "target": {"kind": "run", "id": world.run_id},
            "kind": "cancel",
            "payload": {"urgency": "immediate"},
            "reason": "wrong repo",
        }
    )
    admin = operator().model_copy(
        update={"permissions": operator().permissions | {"workflow_run.admin"}}
    )
    await facade.command(world.run_id, cancel, admin)
    inspection = await facade.inspect(world.run_id, admin)
    (report,) = inspection.delivery_reports or ()
    assert (report.kind, report.lifecycle, report.outcome) == ("cancel", "accepted", None)
    # The lane declares its cancel semantics; nothing was delivered yet.
    assert report.requested_semantics == "turn_boundary_guaranteed"
    assert report.delivered_semantics is None
    assert inspection.stop_fence is not None
    assert inspection.stop_fence.command_id == str(cancel.request_id)
    assert inspection.stop_fence.state == "fence_persisted"


@pytest.mark.asyncio
async def test_http_and_mcp_return_the_same_enriched_inspection() -> None:
    world, facade, _fences = await enriched_world()
    await world.queue("visible only as a digest")
    app, _scope = _app(world)
    http = TestClient(app).get(f"/v1/applications/biotech/runs/{world.run_id}/inspection")
    assert http.status_code == 200
    body = {key: value for key, value in http.json().items() if key != "application_id"}
    assert body["mailbox"][0]["state"] == "queued"
    assert "visible only as a digest" not in json.dumps(body)
    server = create_coordinator_server(
        FakeFacade(),
        Resolver(operator().permissions),
        run_control=ScopedRunControl({SCOPE: facade}),
    )
    async with Client(server) as mcp:
        result = await mcp.call_tool(RUN_INSPECT_TOOL, {"run_id": world.run_id})
    envelope = result.structured_content
    assert envelope is not None and envelope["ok"] is True
    assert envelope["data"] == body


def test_inspect_wait_returns_within_a_second_of_a_phase_change(monkeypatch, capsys) -> None:
    """`run inspect --wait 30` polls under a second apart and returns on the change."""

    clock = {"now": 0.0}
    phases = iter(["active", "active", "active", "paused"])
    sleeps: list[float] = []

    def receive(request: httpx.Request) -> httpx.Response:
        phase = next(phases)
        return httpx.Response(
            200,
            json={
                "lifecycle": "paused" if phase == "paused" else "running",
                "phase": phase,
                "execution_outcome": None,
            },
        )

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds

    original = httpx.Client
    monkeypatch.setattr(
        httpx, "Client", lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(receive))
    )
    monkeypatch.setattr("mission_control.interfaces.cli.main.time.sleep", sleep)
    monkeypatch.setattr("mission_control.interfaces.cli.main.time.monotonic", lambda: clock["now"])
    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "token")
    code = main(
        [
            "run",
            "inspect",
            "run-1",
            "--wait",
            "30",
            "--application",
            "biotech",
            "--url",
            "http://127.0.0.1:8000",
        ]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out)["phase"] == "paused"
    assert sleeps and max(sleeps) < 1  # the change is seen within one poll (< 1 second)
    assert clock["now"] < 2
    assert inspection_wait_state({"lifecycle": "running", "phase": "active"}) == (
        "running",
        "active",
        None,
    )
