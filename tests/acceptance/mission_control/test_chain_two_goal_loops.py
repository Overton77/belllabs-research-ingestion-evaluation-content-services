"""FT-D3 acceptance: two linked Goal Loops transfer state on the real local stack.

Mission 2 (`missions/02-research-ingestion-cursor-cloud-chain.yml`) is compiled against the
seeded catalog fixture with a test-only lane override (both missions on `deep_agents`; the
manifest file is unchanged), submitted through the FT-E3 submit and started through the
governed launch. The stack is the production composition on a disposable common-component
PostgreSQL with the local Temporal dev server and deterministic local cognition
(``ChainModel``, no provider, no paid effect). The chain reducer runs as the canonical
ledger's post-append hook (as the API and worker compose it) and a chain relay loop delivers
its outbox intents through ``LaunchServiceChainStarter``.

Scenario 1: `research` accepts `evidence_map` at iteration 2 and completes; both links
release, `ingestion` is admitted with a Context Packet carrying the materialized evidence map,
the relay starts it once, its executor reads the packet from `.mission/context.md` and
`.mission/inputs.json`, it accepts, and `chain.completed{accepted}` lands in both missions'
streams with one event id. Scenario 2: `research` never accepts and is cancelled during
iteration 2; the armed links are cancelled, no `ingestion` run is admitted and the chain
completes `cancelled`. `missionctl chain inspect` output of both is the proof under
`.scratch/fast-track-2026-10-07/D3/` (written when ``MISSION_CONTROL_RECORD_PROOF=1``).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from fastapi import FastAPI
from temporalio.client import WorkflowExecutionStatus
from tests.fixtures.manifest_runtime import (
    AUTHOR,
    ChainScript,
    GoalScript,
    StagedLaunchInputs,
    chain_components,
    compose_lifecycle,
)
from tests.fixtures.mission_control_common_db import INSTALLATIONS
from tests.fixtures.mission_control_production_stack import open_postgres_production_stack
from tests.fixtures.rrm009_production_harness import (
    ProductionStack,
    _command,
    _diagnose,
    _run,
    _send,
    _terminal,
    _until,
)
from tests.fixtures.rrm009_production_stack import SCOPE, technical_binding

from mission_control.adapters.postgres.chains.store import (
    HOOK_NAME,
    ChainReleaseHook,
    PostgresChainIntentStore,
    PostgresChainReader,
)
from mission_control.adapters.postgres.run_control.canonical import register_post_append_hook
from mission_control.application.authoring.manifest_submit import (
    ManifestSubmissionReceipt,
    SubmitRequest,
)
from mission_control.application.chains.relay import (
    ChainIntent,
    ChainIntentRelay,
    ChainRelayReport,
    LaunchServiceChainStarter,
)
from mission_control.application.chains.service import ChainInspectionService
from mission_control.application.execution.run_launch import RunLaunchService
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.bootstrap.technical_api import api
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.manifest import canonical_manifest_bytes, load_manifest_yaml
from mission_control.domain.policies.contracts import ActorContext, CancelAction
from mission_control.interfaces.cli import main as cli
from mission_control.interfaces.http.chains import router as chains_router
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
MISSION_2 = (
    ROOT / "docs/specs/fast-track-2026-10/missions/02-research-ingestion-cursor-cloud-chain.yml"
)
PROOF = ROOT / ".scratch/fast-track-2026-10-07/D3"
KEY = parse_request_scope(SCOPE)
OWNER_KEY = (KEY.installation_id, KEY.application_id, KEY.tenant_id)


def deep_agents_override(manifest_yaml: str) -> str:
    """Test fixture lane override: every mission on `deep_agents` (the file is unchanged).

    The Cursor Cloud mission drops what only a Cursor lane consumes (the repository workspace,
    `require: [cursor_cloud]` host filters) and gains the sandbox a Deep Agents mission needs.
    """

    document = load_manifest_yaml(manifest_yaml)
    for mission in document["missions"]:
        environment = mission.setdefault("environment", {})
        if environment.get("lane") == "cursor_cloud":
            environment["lane"] = "deep_agents"
            environment["model"] = {"profile": "frontier.default"}
            environment["sandbox"] = {
                "profile": "research.standard",
                "egress": ["pubmed", "tavily"],
            }
            environment.get("workspace", {}).pop("repo", None)
            for capability in environment.get("capabilities", []):
                capability.pop("require", None)
    return canonical_manifest_bytes(document).decode("utf-8")


# --- Stack ------------------------------------------------------------------------------------


@dataclass
class ChainStack:
    stack: ProductionStack
    script: ChainScript
    model_log: list[dict[str, Any]] = field(default_factory=list)


@pytest.fixture
async def chain_stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[ChainStack]:
    script = ChainScript(request_scope=SCOPE)
    model_log: list[dict[str, Any]] = []
    technical = technical_binding()
    async with open_postgres_production_stack(
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical_override=technical,
        components=chain_components(technical, script, model_log),
        model_log=model_log,
    ) as production:
        # The API and worker processes install the chain reducer at composition
        # (`install_chain_release_hook`); this in-process stack does it here.
        unregister = register_post_append_hook(HOOK_NAME, ChainReleaseHook())
        try:
            yield ChainStack(production, script, model_log)
        finally:
            unregister()


class IntentInputs:
    """The relay's launch input port over the test semantic binding author."""

    def __init__(self, inputs: StagedLaunchInputs) -> None:
        self._inputs = inputs
        self.intents: list[ChainIntent] = []

    async def family_input(self, intent: ChainIntent) -> dict[str, Any]:
        self.intents.append(intent)
        assert intent.family is not None
        return await self._inputs.family_input(
            request_scope=intent.request_scope,
            run_id=intent.run_key,
            family=intent.family,
            initial_goal=intent.initial_goal,
        )


class RelayLoop:
    """The chain relay as a deployment loop: lease, deliver, acknowledge, every 0.5 s."""

    def __init__(self, stack: ProductionStack, inputs: StagedLaunchInputs) -> None:
        self.inputs = IntentInputs(inputs)
        self.relay = ChainIntentRelay(
            store=PostgresChainIntentStore(stack.worker_pool),
            starter=LaunchServiceChainStarter(
                inputs=self.inputs, launches=cast(RunLaunchService, api.state.run_launch_service)
            ),
            lease_owner="ft-d3-relay",
            base_backoff_seconds=0.25,
            max_backoff_seconds=2.0,
        )
        self.reports: list[ChainRelayReport] = []
        self._task: asyncio.Task[None] | None = None

    async def _loop(self) -> None:
        while True:
            report = await self.relay.relay_once(SCOPE, now=datetime.now(UTC))
            if report.delivered or report.failed or report.dead:
                self.reports.append(report)
            await asyncio.sleep(0.5)

    @contextlib.asynccontextmanager
    async def running(self) -> AsyncIterator[RelayLoop]:
        self._task = asyncio.create_task(self._loop())
        try:
            yield self
        finally:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def delivered(self) -> list[str]:
        return [key for report in self.reports for key in report.delivered]

    def failed(self) -> list[str]:
        return [key for report in self.reports for key in (*report.failed, *report.dead)]


# --- `missionctl chain inspect` through the HTTP router -----------------------------------------


def chain_app(stack: ProductionStack) -> FastAPI:
    installation_id, project_ref = INSTALLATIONS[KEY.application_id]
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id=KEY.application_id,
                installation_id=installation_id,
                binding_version="1",
                supabase_project_ref=project_ref,
                database_secret_ref="TEST_DATABASE_URL",
                accepted_issuers={"https://issuer.invalid"},
                accepted_audiences={"authenticated"},
                required_component_version="1",
            ),
        )
    )
    registry.observe(
        KEY.application_id,
        InstallationObservation(installation_id, KEY.application_id, project_ref, frozenset({"1"})),
    )
    app = FastAPI()
    app.include_router(chains_router)
    app.state.mission_control_registry = registry
    app.state.mission_control_chain_services = {
        OWNER_KEY: ChainInspectionService(
            PostgresChainReader(stack.worker_pool), request_scope=SCOPE
        )
    }
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=installation_id,
        application_id=KEY.application_id,
        tenant_id=KEY.tenant_id,
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=ActorContext(actor_id="operator", permissions=frozenset({"workflow_run.read"})),
    )
    return app


class _LoopBridge(httpx.BaseTransport):
    """The sync CLI client (in a worker thread) served by the ASGI app on the test loop."""

    def __init__(self, app: FastAPI, loop: asyncio.AbstractEventLoop) -> None:
        self._app = app
        self._loop = loop

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        async def send() -> tuple[int, httpx.Headers, bytes]:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=self._app), base_url="http://mission-control"
            ) as client:
                response = await client.request(
                    request.method,
                    str(request.url),
                    headers=dict(request.headers),
                    content=request.content,
                )
                return response.status_code, response.headers, response.content

        status, headers, content = asyncio.run_coroutine_threadsafe(send(), self._loop).result(60)
        return httpx.Response(status, headers=headers, content=content)


async def chain_inspect_cli(
    stack: ProductionStack,
    chain_id: UUID,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> dict[str, Any]:
    app = chain_app(stack)
    loop = asyncio.get_running_loop()
    real_client = httpx.Client

    def factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = _LoopBridge(app, loop)
        return real_client(*args, **kwargs)

    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "token")
    monkeypatch.setenv("MISSION_CONTROL_APPLICATION_ID", KEY.application_id)
    monkeypatch.setattr(httpx, "Client", factory)
    capsys.readouterr()
    code = await asyncio.to_thread(cli.main, ["chain", "inspect", str(chain_id), "--json"])
    printed = capsys.readouterr().out
    assert code == 0, printed
    return cast(dict[str, Any], json.loads(printed))


def record(name: str, value: Any) -> None:
    if os.environ.get("MISSION_CONTROL_RECORD_PROOF") != "1":
        return
    PROOF.mkdir(parents=True, exist_ok=True)
    (PROOF / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


# --- Canonical reads (owner connection, explicit scope) -----------------------------------------


async def fetch(stack: ProductionStack, query: str, *args: Any) -> list[asyncpg.Record]:
    async with stack.owner_pool.acquire() as connection:
        return list(await connection.fetch(query, *OWNER_KEY, *args))


SCOPED = "installation_id = $1 AND application_id = $2 AND tenant_id = $3"


async def links(stack: ProductionStack, chain_id: UUID) -> list[asyncpg.Record]:
    return await fetch(
        stack,
        f"SELECT kind, state, released_run_id, packet_digest FROM mission_control.chain_link "
        f"WHERE {SCOPED} AND chain_id = $4 ORDER BY kind",
        chain_id,
    )


async def chain_row(stack: ProductionStack, chain_id: UUID) -> asyncpg.Record:
    (row,) = await fetch(
        stack,
        f"SELECT lifecycle, terminal_outcome FROM mission_control.mission_chain "
        f"WHERE {SCOPED} AND chain_id = $4",
        chain_id,
    )
    return row


async def mission_runs(stack: ProductionStack, mission_id: UUID) -> list[asyncpg.Record]:
    return await fetch(
        stack,
        f"SELECT run_id, run_key, lifecycle, terminal_outcome, created_by_actor_ref "
        f"FROM mission_control.mission_run WHERE {SCOPED} AND mission_id = $4",
        mission_id,
    )


async def chain_events(
    stack: ProductionStack, event_type: str, mission_ids: list[UUID]
) -> list[asyncpg.Record]:
    return await fetch(
        stack,
        f"SELECT mission_id, seq, payload FROM mission_control.mission_event "
        f"WHERE {SCOPED} AND event_type = $4 AND mission_id = ANY($5::uuid[]) "
        f"ORDER BY mission_id, seq",
        event_type,
        mission_ids,
    )


async def start_intents(stack: ProductionStack) -> list[asyncpg.Record]:
    return await fetch(
        stack,
        f"SELECT delivery_key, delivery_state, attempts, payload->>'chain_id' AS chain_id "
        f"FROM mission_control.outbox WHERE {SCOPED} AND destination_kind = $4",
        "mc.chain.start_run",
    )


async def wait(
    stack: ProductionStack,
    predicate: Callable[[], Awaitable[bool]],
    seconds: float,
    run_keys: list[str],
) -> None:
    try:
        await _until(predicate, seconds)
    except (TimeoutError, AssertionError) as error:
        diagnosis = [await _diagnose(stack, key) for key in run_keys]
        raise AssertionError(f"{error}: {' || '.join(diagnosis)}") from error


async def workflow_failed(stack: ProductionStack, run_key: str) -> bool:
    """The run's root workflow closed as failed (the run projection may not say so)."""

    description = await stack.client.get_workflow_handle(f"belllabs-run/{run_key}").describe()
    return description.status == WorkflowExecutionStatus.FAILED


async def submit(stack: ProductionStack) -> tuple[Any, ManifestSubmissionReceipt, Any]:
    service, inputs = await compose_lifecycle(stack, SCOPE)
    receipt, replayed = await service.submit(
        SubmitRequest(
            manifest_yaml=deep_agents_override(MISSION_2.read_text(encoding="utf-8")),
            request_id=uuid4(),
            actor=AUTHOR,
            sponsorship_refs=frozenset({"sponsorship:test"}),
        )
    )
    assert not replayed and receipt.chain_id is not None
    return service, receipt, inputs


def member(receipt: ManifestSubmissionReceipt, key: str) -> Any:
    return next(item for item in receipt.missions if item.mission_key == key)


def outbox_keys(chain_id: UUID, rows: list[asyncpg.Record]) -> list[str]:
    return [row["delivery_key"] for row in rows if row["chain_id"] == str(chain_id)]


# --- Scenario 1: research accepts, ingestion released with the packet, chain accepted ----------


@pytest.mark.asyncio
async def test_research_acceptance_releases_ingestion_with_its_packet_and_completes_the_chain(
    chain_stack: ChainStack,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    stack, script = chain_stack.stack, chain_stack.script
    service, receipt, inputs = await submit(stack)
    chain_id = cast(UUID, receipt.chain_id)
    research, ingestion = member(receipt, "research"), member(receipt, "ingestion")
    assert research.family == ingestion.family == "GoalDirected"
    assert research.run_id is not None and ingestion.run_id is None
    script.by_configuration[research.effective_configuration_digest] = GoalScript(
        obligation="evidence_map",
        output_contract="output:evidence_map",
        output_name="evidence_map",
        accept_at=2,
    )
    script.by_configuration[ingestion.effective_configuration_digest] = GoalScript(
        obligation="ingested",
        output_contract="output:ingestion_receipt",
        output_name="ingestion_receipt",
        accept_at=1,
    )
    submitted = await chain_inspect_cli(stack, chain_id, monkeypatch, capsys)
    assert {link["state"] for link in submitted["chain"]["links"]} == {"armed"}
    record("accepted-1-submitted.chain-inspect.json", submitted)

    relay = RelayLoop(stack, inputs)
    async with relay.running():
        started = await service.start(research.run_id, AUTHOR)
        assert started.family == "GoalDirected"

        async def chain_completed_or_research_failed() -> bool:
            if (await chain_row(stack, chain_id))["lifecycle"] == "completed":
                return True
            run = await _run(stack, research.run_id)
            if run["phase"] == "terminal" and run["terminal_outcome"] != "completed":
                return True
            return await workflow_failed(stack, research.run_id)

        await wait(stack, chain_completed_or_research_failed, 420, [research.run_id])
        released = await chain_inspect_cli(stack, chain_id, monkeypatch, capsys)

    # research: accepted evidence_map at iteration 2, completed, one accepted output.
    research_run = await _run(stack, research.run_id)
    if research_run["terminal_outcome"] != "completed":
        raise AssertionError(await _diagnose(stack, research.run_id))
    (accepted_output,) = [item["output_ref"] for item in research_run["accepted_output_evidence"]]
    assert accepted_output.startswith("workspace-candidate://"), accepted_output
    descriptor = await fetch(
        stack,
        f"SELECT content_digest, size_bytes, logical_path FROM "
        f"mission_control.workspace_candidate_descriptor WHERE {SCOPED} AND candidate_key = $4",
        accepted_output.removeprefix("workspace-candidate://"),
    )
    assert len(descriptor) == 1, accepted_output
    registered_digest = descriptor[0]["content_digest"]

    # Both links released; ingestion has exactly one run, admitted by the chain.
    link_rows = await links(stack, chain_id)
    assert [row["state"] for row in link_rows] == ["released", "released"], link_rows
    (ingestion_run,) = await mission_runs(stack, ingestion.mission_id)
    assert ingestion_run["created_by_actor_ref"] == f"chain:{chain_id}"
    ingestion_key = ingestion_run["run_key"]
    assert {row["released_run_id"] for row in link_rows} == {ingestion_run["run_id"]}

    # The start intent: one outbox row, delivered once, never failed.
    (intent,) = outbox_keys(chain_id, await start_intents(stack))
    assert relay.delivered().count(intent) == 1 and not relay.failed(), relay.reports
    assert [item.run_key for item in relay.inputs.intents] == [ingestion_key]
    states = {row["delivery_key"]: row["delivery_state"] for row in await start_intents(stack)}
    assert states[intent] == "delivered", states

    # The release packet: materialized evidence map, disposition, checkpoint, journal digest.
    (packet_row,) = await fetch(
        stack,
        f"SELECT packet, packet_digest FROM mission_control.context_selection "
        f"WHERE {SCOPED} AND run_key = $4 AND purpose = 'chain_link'",
        ingestion_key,
    )
    packet = json.loads(packet_row["packet"])
    text = json.dumps(packet)
    assert "/inputs/research.evidence_map/" in text
    assert registered_digest in text
    assert "accepted" in text and "missionctl run transcript" in text
    assert {row["packet_digest"] for row in link_rows} == {packet_row["packet_digest"]}

    # The consumer's first executor read the packet from its workspace.
    first = [
        key
        for key in script.reads
        if key[0] == ingestion_key and "goal-iteration/1/" in key[1] and "executor" in key[1]
    ]
    assert first, list(script.reads)
    context_md, inputs_json = script.reads[first[0]]
    assert "/inputs/research.evidence_map/" in context_md, context_md[:4000]
    assert "research.evidence_map" in inputs_json and registered_digest in inputs_json, inputs_json[
        :4000
    ]
    assert "missionctl run transcript" in context_md

    # ingestion accepted; chain.completed{accepted} in both streams with one event id.
    ingestion_state = await _run(stack, ingestion_key)
    assert ingestion_state["terminal_outcome"] == "completed", ingestion_state
    assert (await chain_row(stack, chain_id))["terminal_outcome"] == "accepted"
    completed = await chain_events(
        stack, "chain.completed", [research.mission_id, ingestion.mission_id]
    )
    assert {row["mission_id"] for row in completed} == {research.mission_id, ingestion.mission_id}
    payloads = [json.loads(row["payload"]) for row in completed]
    assert len({item["event_id"] for item in payloads}) == 1, payloads
    assert {item["payload"]["terminal_outcome"] for item in payloads} == {"accepted"}

    assert released["chain"]["lifecycle"] == "completed"
    assert {link["state"] for link in released["chain"]["links"]} == {"released"}
    record("accepted-2-completed.chain-inspect.json", released)
    record(
        "accepted-evidence.json",
        {
            "chain_id": str(chain_id),
            "research_run": research.run_id,
            "ingestion_run": ingestion_key,
            "accepted_output": accepted_output,
            "registered_digest": registered_digest,
            "packet_digest": packet_row["packet_digest"],
            "start_intent": intent,
            "relay_delivered": relay.delivered(),
            "chain_completed_event_id": payloads[0]["event_id"],
            "ingestion_context_md": context_md,
            "ingestion_inputs_json": inputs_json,
        },
    )


# --- Scenario 2: research cancelled during iteration 2 -----------------------------------------


@pytest.mark.asyncio
async def test_cancelling_research_mid_loop_cancels_the_downstream_link(
    chain_stack: ChainStack,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    stack, script = chain_stack.stack, chain_stack.script
    service, receipt, inputs = await submit(stack)
    chain_id = cast(UUID, receipt.chain_id)
    research, ingestion = member(receipt, "research"), member(receipt, "ingestion")
    assert research.run_id is not None
    script.by_configuration[research.effective_configuration_digest] = GoalScript(
        obligation="evidence_map",
        output_contract="output:evidence_map",
        output_name="evidence_map",
        accept_at=None,
    )
    relay = RelayLoop(stack, inputs)
    async with relay.running():
        await service.start(research.run_id, AUTHOR)

        async def in_iteration_2() -> bool:
            return any(
                item["run_id"] == research.run_id and "goal-iteration/2/" in item["operation"]
                for item in chain_stack.model_log
            )

        await wait(stack, in_iteration_2, 300, [research.run_id])
        decision: dict[str, Any] = {}
        for attempt in range(5):
            run = await _run(stack, research.run_id)
            decision = await _send(
                stack,
                research.run_id,
                _command(
                    research.run_id,
                    run["version"],
                    f"cancel:{research.run_id}:{attempt}",
                    CancelAction(),
                    "workflow_run.cancel",
                ),
            )
            if decision["reason_code"] == "accepted":
                break
        assert decision["reason_code"] == "accepted", decision

        await wait(stack, lambda: _terminal(stack, research.run_id), 240, [research.run_id])

        async def chain_completed() -> bool:
            return (await chain_row(stack, chain_id))["lifecycle"] == "completed"

        await wait(stack, chain_completed, 60, [research.run_id])
        inspected = await chain_inspect_cli(stack, chain_id, monkeypatch, capsys)

    research_run = await _run(stack, research.run_id)
    assert research_run["terminal_outcome"] == "cancelled", research_run
    link_rows = await links(stack, chain_id)
    assert {row["state"] for row in link_rows} <= {"cancelled", "blocked"}, link_rows
    assert "cancelled" in {row["state"] for row in link_rows}
    assert await mission_runs(stack, ingestion.mission_id) == []
    assert outbox_keys(chain_id, await start_intents(stack)) == []
    assert not relay.reports, relay.reports
    assert (await chain_row(stack, chain_id))["terminal_outcome"] == "cancelled"
    cancelled_links = await chain_events(
        stack, "chain_link.cancelled", [research.mission_id, ingestion.mission_id]
    )
    assert cancelled_links, "chain_link.cancelled was not appended"
    completed = await chain_events(
        stack, "chain.completed", [research.mission_id, ingestion.mission_id]
    )
    payloads = [json.loads(row["payload"]) for row in completed]
    assert len({item["event_id"] for item in payloads}) == 1, payloads
    assert {item["payload"]["terminal_outcome"] for item in payloads} == {"cancelled"}
    assert inspected["chain"]["lifecycle"] == "completed"
    assert inspected["chain"]["terminal_outcome"] == "cancelled"
    record("cancelled.chain-inspect.json", inspected)
    record(
        "cancelled-evidence.json",
        {
            "chain_id": str(chain_id),
            "research_run": research.run_id,
            "cancel_decision": decision,
            "links": [dict(row) for row in link_rows],
            "chain_completed_event_id": payloads[0]["event_id"],
        },
    )
