"""RRM-005: scoped, non-mutating inspection reads (REQ-CP-RUN-011/012, EXEC-007/015).

Behavioral tests over the in-memory run-control and lineage adapters, through the public
`/run-control/v1/inspection` facade. The checkpoint history reads use the production
`LangGraphCheckpointHistoryReader` over a real LangGraph `InMemorySaver`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import Any

import pytest
from fastapi.testclient import TestClient
from langgraph.checkpoint.memory import InMemorySaver

from app.api.control_plane import ControlPlanePrincipal, get_control_plane_principal
from app.api.runtime_inspection import get_runtime_inspection_service
from app.application.operations.checkpoint_lineage import InMemoryCheckpointLineageRepository
from app.application.run_control.inspection import (
    InMemoryInspectionReadRepository,
    InspectionCursorCodec,
    RuntimeInspectionService,
    RuntimeSourceUnavailable,
)
from app.application.run_control.run_control_repository import InMemoryRunControlRepository
from app.application.run_control.service import RunControlService
from app.domain.graph_runtime.identities import QualifiedCheckpointKey, RuntimeUnitIdentity
from app.domain.operation_execution.checkpoint_lineage import (
    STAMP_STATE_SCHEMA_DIGEST,
    cognitive_session_namespace,
)
from app.domain.operation_execution.contracts import OperationWorkflowRequest
from app.domain.orchestration.contracts import (
    BellLabsRunInput,
    GoalDirectedRunInput,
    StageGraphRunInput,
)
from app.domain.orchestration.search_attributes import (
    SearchAttributePolicyError,
    operation_search_attributes,
    require_production_search_attribute_policy,
    run_search_attributes,
    search_attribute_scope_hash,
    visibility_run_query,
)
from app.domain.run_control.inspection import (
    AsyncChildInspection,
    CheckpointObservation,
    TemporalExecution,
    summarize_channel_values,
)
from app.integrations.agents.deep_agents.checkpoint_history import (
    LangGraphCheckpointHistoryReader,
)
from app.server import api
from tests.fixtures.checkpoint_lineage import (
    BINDING,
    CHECKPOINTER,
    LINEAGE_NOW,
    OTHER_SCHEMA,
    SCHEMA,
    activity_attempt,
    namespace_claim,
    stage_unit,
    transition,
    unit_result,
)
from tests.fixtures.runtime_inspection import (
    PROMPT_TEXT,
    READ_AT,
    SECRET_TEXT,
    TOOL_ARGUMENT,
    MutableClock,
    put_checkpoint,
    seed_inspection_world,
    sensitive_state,
    stamps_for,
)
from tests.unit.run_control.test_run_control import request, service

# --- Fixture world -----------------------------------------------------------------------


@dataclass
class World:
    run_service: RunControlService
    runs: InMemoryRunControlRepository
    lineage: InMemoryCheckpointLineageRepository
    saver: InMemorySaver
    clock: MutableClock
    active_run: str
    terminal_run: str
    other_scope_run: str
    settled: RuntimeUnitIdentity
    in_doubt: RuntimeUnitIdentity
    extra_runs: list[str] = field(default_factory=list)

    def service(self, **sources: Any) -> RuntimeInspectionService:
        repository = InMemoryInspectionReadRepository(
            self.runs,
            self.lineage,
            async_children={
                self.active_run: (
                    AsyncChildInspection(
                        child_execution_id="async-child:1",
                        # The authority row names the spawning binding (no detail needed).
                        parent_operation_id=f"binding:{self.in_doubt.unit_key}:1",
                        link_id="link:1",
                        contract_id="contract:research-helper",
                        binding_digest=BINDING,
                        execution_generation=1,
                        dependency_class="required_blocking",
                        # A lifecycle value newer than this reader stays visible as recorded.
                        lifecycle="in_doubt",
                    ),
                )
            },
            clock=self.clock,
        )
        sources.setdefault(
            "checkpoints", LangGraphCheckpointHistoryReader({CHECKPOINTER: self.saver})
        )
        return RuntimeInspectionService(
            repository,
            cursors=InspectionCursorCodec(b"k" * 32, clock=self.clock),
            clock=self.clock,
            **sources,
        )


async def build_world() -> World:
    run_service, runs = service()
    lineage = InMemoryCheckpointLineageRepository()
    saver = InMemorySaver()
    seeded = await seed_inspection_world(run_service, lineage, saver)
    return World(
        run_service=run_service,
        runs=runs,
        lineage=lineage,
        saver=saver,
        clock=MutableClock(READ_AT),
        active_run=seeded.active_run,
        terminal_run=seeded.terminal_run,
        other_scope_run=seeded.other_scope_run,
        settled=seeded.settled,
        in_doubt=seeded.in_doubt,
    )


@pytest.fixture
async def world() -> World:
    return await build_world()


def principal(*roles: str, scopes: tuple[str, ...] = ("tenant-1",)) -> ControlPlanePrincipal:
    return ControlPlanePrincipal(
        actor_id="operator", roles=frozenset(roles), tenant_scopes=frozenset(scopes)
    )


@pytest.fixture
def client_for(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    async def noop(_application: object) -> None:
        return None

    monkeypatch.setattr("app.server.initialize_run_control_resources", noop)

    def make(inspection: RuntimeInspectionService, who: ControlPlanePrincipal) -> TestClient:
        api.dependency_overrides[get_runtime_inspection_service] = lambda: inspection
        api.dependency_overrides[get_control_plane_principal] = lambda: who
        return TestClient(api)

    yield make
    api.dependency_overrides.pop(get_runtime_inspection_service, None)
    api.dependency_overrides.pop(get_control_plane_principal, None)


def _state(world: World) -> tuple[Any, ...]:
    runs = world.runs
    lineage = world.lineage
    return deepcopy(
        (
            {name: value for name, value in vars(runs).items() if name != "_lock"},
            {name: value for name, value in vars(lineage).items() if name != "_lock"},
            [(item.config, item.checkpoint, item.metadata) for item in world.saver.list(None)],
        )
    )


# --- Run list and cursors ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_list_is_scoped_and_paginated_with_bound_opaque_cursors(
    world: World, client_for: Any
) -> None:
    for index in range(2):
        admitted = await world.run_service.admit(request(request_id=f"inspection-extra-{index}"))
        assert admitted.run_id is not None
        world.extra_runs.append(admitted.run_id)
    inspection = world.service()
    with client_for(inspection, principal("auditor", scopes=("tenant-1", "tenant-2"))) as client:
        first = client.get(
            "/run-control/v1/inspection/runs", params={"request_scope": "tenant-1", "limit": 3}
        )
        assert first.status_code == 200
        body = first.json()
        assert body["schema_version"] == "belllabs.inspection-read.v1"
        assert body["sections"]["runs"]["source"] == "postgres_authority"
        assert body["sections"]["runs"]["freshness"] == "current"
        page_one = [item["run_id"] for item in body["data"]["items"]]
        cursor = body["data"]["next_cursor"]
        assert len(page_one) == 3 and cursor
        second = client.get(
            "/run-control/v1/inspection/runs",
            params={"request_scope": "tenant-1", "limit": 3, "cursor": cursor},
        ).json()
        page_two = [item["run_id"] for item in second["data"]["items"]]
        assert second["data"]["next_cursor"] is None
        expected = sorted([world.active_run, world.terminal_run, *world.extra_runs])
        assert page_one + page_two == expected
        assert world.other_scope_run not in page_one + page_two
        assert {item["request_scope"] for item in body["data"]["items"]} == {"tenant-1"}

        # The active run reports its operator-required reconciliation in the list.
        states = {
            item["run_id"]: item["reconciliation_state"]
            for item in (*body["data"]["items"], *second["data"]["items"])
        }
        assert states[world.active_run] == "operator_required"
        assert states[world.terminal_run] == "none"

        # The cursor is bound to scope and filter; tampering and expiry are typed errors.
        other_scope = client.get(
            "/run-control/v1/inspection/runs",
            params={"request_scope": "tenant-2", "cursor": cursor, "limit": 3},
        )
        assert other_scope.status_code == 400
        assert other_scope.json()["code"] == "invalid_cursor"
        other_filter = client.get(
            "/run-control/v1/inspection/runs",
            params={"request_scope": "tenant-1", "cursor": cursor, "phase": "terminal"},
        )
        assert other_filter.json()["code"] == "invalid_cursor"
        tampered = cursor[:-2] + ("AA" if not cursor.endswith("AA") else "BB")
        for bad in (tampered, "not-a-cursor", "a.b.c", ""):
            response = client.get(
                "/run-control/v1/inspection/runs",
                params={"request_scope": "tenant-1", "cursor": bad},
            )
            assert response.status_code == 400, bad
            assert response.json()["code"] == "invalid_cursor"
        world.clock.now = READ_AT + timedelta(minutes=16)
        expired = client.get(
            "/run-control/v1/inspection/runs",
            params={"request_scope": "tenant-1", "cursor": cursor, "limit": 3},
        )
        assert expired.status_code == 410
        assert expired.json()["code"] == "cursor_expired"

        terminal_only = client.get(
            "/run-control/v1/inspection/runs",
            params={"request_scope": "tenant-1", "phase": "terminal"},
        ).json()
        assert [item["run_id"] for item in terminal_only["data"]["items"]] == [world.terminal_run]
        assert terminal_only["data"]["items"][0]["terminal_outcome"] == "failed"


# --- Authorization and scope -------------------------------------------------------------


@pytest.mark.asyncio
async def test_cross_scope_access_is_denied_as_absent(world: World, client_for: Any) -> None:
    inspection = world.service()
    unit_path = f"/run-control/v1/inspection/runs/{world.active_run}/units/{world.settled.unit_key}"
    with client_for(inspection, principal("auditor", scopes=("tenant-2",))) as client:
        # The principal does not hold tenant-1: 404, never a hint that the run exists.
        for path in (
            f"/run-control/v1/inspection/runs/{world.active_run}",
            unit_path,
            f"{unit_path}/checkpoints",
        ):
            response = client.get(path, params={"request_scope": "tenant-1"})
            assert response.status_code == 404, path
        # Asking for tenant-1's run under the scope it does hold is equally absent.
        response = client.get(
            f"/run-control/v1/inspection/runs/{world.active_run}",
            params={"request_scope": "tenant-2"},
        )
        assert response.status_code == 404
        assert response.json()["code"] == "inspection_not_found"
        response = client.get(unit_path, params={"request_scope": "tenant-2"})
        assert response.status_code == 404
    with client_for(inspection, principal("auditor")) as client:
        # A unit of another run is not reachable through this run.
        response = client.get(
            f"/run-control/v1/inspection/runs/{world.terminal_run}/units/{world.settled.unit_key}",
            params={"request_scope": "tenant-1"},
        )
        assert response.status_code == 404
    with client_for(inspection, principal("relay")) as client:
        response = client.get(
            f"/run-control/v1/inspection/runs/{world.active_run}",
            params={"request_scope": "tenant-1"},
        )
        assert response.status_code == 403


# --- Active and terminal runs; reconciliation state -------------------------------------


@pytest.mark.asyncio
async def test_active_and_terminal_runs_expose_identity_lineage_and_reconciliation(
    world: World, client_for: Any
) -> None:
    inspection = world.service()
    with client_for(inspection, principal("auditor")) as client:
        terminal = client.get(
            f"/run-control/v1/inspection/runs/{world.terminal_run}",
            params={"request_scope": "tenant-1"},
        ).json()
        active = client.get(
            f"/run-control/v1/inspection/runs/{world.active_run}",
            params={"request_scope": "tenant-1"},
        ).json()
        settled = client.get(
            f"/run-control/v1/inspection/runs/{world.active_run}/units/{world.settled.unit_key}",
            params={"request_scope": "tenant-1"},
        ).json()
        in_doubt = client.get(
            f"/run-control/v1/inspection/runs/{world.active_run}/units/{world.in_doubt.unit_key}",
            params={"request_scope": "tenant-1"},
        ).json()

    assert terminal["data"]["projection"]["phase"] == "terminal"
    assert terminal["data"]["projection"]["terminal_outcome"] == "failed"
    assert terminal["data"]["reconciliation_state"] == "none"
    assert terminal["data"]["units"] == []

    run = active["data"]
    assert run["projection"]["phase"] == "active", "an in_doubt unit keeps the run phase"
    assert run["reconciliation_state"] == "operator_required"
    assert [wait["kind"] for wait in run["operator_reconciliation_waits"]] == [
        "operator_reconciliation"
    ]
    units = {item["unit_key"]: item for item in run["units"]}
    assert units[world.settled.unit_key]["status"] == "active"  # observed; journal is PG-only
    assert units[world.in_doubt.unit_key]["status"] == "in_doubt"
    assert units[world.in_doubt.unit_key]["reconciliation_state"] == "operator_required"
    assert run["effects"][0]["ambiguous"] is True
    assert run["effects"][0]["operation_ref"] == f"binding:{world.in_doubt.unit_key}:1"
    assert run["budget"]["reserved"]["tokens.total"] >= 5
    assert run["async_children"][0]["lifecycle"] == "in_doubt"
    sections = active["sections"]
    assert sections["run"]["source"] == "postgres_authority"
    assert sections["run"]["projection_version"] == run["projection"]["version"]
    assert sections["run"]["reconciliation_state"] == "operator_required"
    assert sections["effects"]["reconciliation_state"] == "in_doubt"
    assert sections["run"]["observed_at"] is not None

    # Unit read: structured identity, Temporal IDs and attempts, binding, namespace,
    # checkpoint ancestry, result refs, lease state.
    unit = settled["data"]
    assert unit["unit"]["location"]["stage_id"] == "fixture-stage"
    assert unit["unit_key"] == world.settled.unit_key
    generation = unit["generations"][0]
    assert generation["binding_id"] == f"binding:{world.settled.unit_key}:1"
    assert generation["binding_digest"] == BINDING
    assert generation["cognitive_namespace"] == cognitive_session_namespace(world.settled, 1)
    assert generation["lease_state"] == "expired"
    attempt = generation["attempts"][0]
    assert attempt["attempt"]["workflow_id"] == f"operation/{world.settled.semantic_operation_id}"
    assert attempt["attempt"]["attempt"] == 1
    assert attempt["claim_fence"] == 1
    assert generation["transition"]["result_key"]["checkpoint_id"] == "c3"
    assert generation["result"]["result_manifest_ref"].startswith("manifest:")
    assert generation["namespace_head"]["checkpoint_id"] == "c3"

    doubt = in_doubt["data"]
    assert doubt["status"] == "in_doubt"
    assert doubt["reconciliation_state"] == "operator_required"
    assert doubt["run_phase"] == "active"
    assert doubt["generations"][0]["lease_state"] == "held"
    assert doubt["generations"][0]["namespace_in_flight"] is True
    assert doubt["generations"][0]["incidents"][0]["reason"] == "multiple_stamped_leaves"
    assert [effect["effect_id"] for effect in doubt["effects"]] == ["tool-effect:send"]
    assert doubt["operator_reconciliation_waits"][0]["kind"] == "operator_reconciliation"
    assert doubt["async_children"][0]["child_execution_id"] == "async-child:1"
    assert in_doubt["sections"]["reconciliation"]["reconciliation_state"] == "operator_required"


@pytest.mark.asyncio
async def test_reads_never_mutate_state_or_settle(world: World, client_for: Any) -> None:
    """REQ-CP-RUN-011: every read (and every failed read) leaves all state unchanged."""

    before = _state(world)
    inspection = world.service()
    base = f"/run-control/v1/inspection/runs/{world.active_run}"
    paths = [
        "/run-control/v1/inspection/runs",
        base,
        f"{base}/units/{world.settled.unit_key}",
        f"{base}/units/{world.in_doubt.unit_key}",
        f"{base}/units/{world.settled.unit_key}/checkpoints",
        f"{base}/units/{world.settled.unit_key}/checkpoints/c3/summary",
        f"{base}/units/{world.settled.unit_key}/checkpoints/drifted/summary",
        f"{base}/units/{world.in_doubt.unit_key}/checkpoints",
        f"{base}/units/bl-unit-v1:{'0' * 64}",
    ]
    with client_for(inspection, principal("state_inspector")) as client:
        statuses = [
            client.get(path, params={"request_scope": "tenant-1"}).status_code for path in paths
        ]
    assert statuses == [200, 200, 200, 200, 200, 200, 404, 200, 404]
    assert _state(world) == before
    # The in-doubt unit is still unreconciled and the run still waits on the operator.
    projection = await world.run_service.get_run("tenant-1", world.active_run)
    assert projection.unit_reconciliations == ()
    assert [wait.kind for wait in projection.active_waits] == ["operator_reconciliation"]


# --- Freshness and missing live providers ------------------------------------------------


class FailingVisibility:
    async def list_run_executions(self, request_scope: str, run_id: str) -> Any:
        raise RuntimeSourceUnavailable("Temporal Visibility is unavailable")


class StaticVisibility:
    def __init__(self, executions: tuple[TemporalExecution, ...]) -> None:
        self.executions = executions
        self.calls: list[tuple[str, str]] = []

    async def list_run_executions(self, request_scope: str, run_id: str) -> Any:
        self.calls.append((request_scope, run_id))
        return self.executions


class FailingCheckpoints:
    def supports(self, checkpointer_ref_digest: str) -> bool:
        return True

    async def list_root_checkpoints(self, digest: str, namespace: str) -> Any:
        raise RuntimeSourceUnavailable("saver down")

    async def read_redacted_state(self, key: QualifiedCheckpointKey) -> Any:
        raise RuntimeSourceUnavailable("saver down")


class FailingDetails:
    async def get_execution(self, request_scope: str, child_execution_id: str) -> Any:
        raise ConnectionError("detail store down")


class Lifecycle(StrEnum):
    RUNNING = "running"


@dataclass(frozen=True)
class Detail:
    provider_thread_id: str
    provider_run_id: str
    lifecycle: Lifecycle
    unrelated_new_field: str = "ignored"


class StaticDetails:
    async def get_execution(self, request_scope: str, child_execution_id: str) -> Any:
        return Detail("provider-thread:1", "provider-run:1", Lifecycle.RUNNING)


@pytest.mark.asyncio
async def test_missing_live_providers_degrade_sections_and_persisted_reads_succeed(
    world: World, client_for: Any
) -> None:
    base = f"/run-control/v1/inspection/runs/{world.active_run}"
    params = {"request_scope": "tenant-1"}
    # No live source configured at all.
    unconfigured = world.service(checkpoints=None)
    with client_for(unconfigured, principal("auditor")) as client:
        run = client.get(base, params=params)
        history = client.get(f"{base}/units/{world.settled.unit_key}/checkpoints", params=params)
    assert run.status_code == 200 and history.status_code == 200
    sections = run.json()["sections"]
    assert sections["temporal"]["freshness"] == "unavailable"
    assert sections["temporal"]["reason"] == "visibility_reader_not_configured"
    assert sections["async_children_detail"]["freshness"] == "unavailable"
    assert sections["run"]["freshness"] == "current"
    page = history.json()
    assert page["sections"]["checkpoints"]["reason"] == "checkpointer_not_registered"
    assert page["data"]["entries"] == []
    # The recorded qualified keys are still served from PostgreSQL authority.
    assert {key["checkpoint_id"] for key in page["data"]["recorded_keys"]} == {"c3"}

    # Live sources configured but down.
    failing = world.service(
        visibility=FailingVisibility(),
        checkpoints=FailingCheckpoints(),
        async_details=FailingDetails(),
    )
    with client_for(failing, principal("state_inspector")) as client:
        run = client.get(base, params=params)
        unit = client.get(f"{base}/units/{world.settled.unit_key}", params=params)
        history = client.get(f"{base}/units/{world.settled.unit_key}/checkpoints", params=params)
        summary = client.get(
            f"{base}/units/{world.settled.unit_key}/checkpoints/c3/summary", params=params
        )
    assert run.status_code == unit.status_code == history.status_code == 200
    assert run.json()["sections"]["temporal"]["reason"] == "temporal_visibility_unavailable"
    assert run.json()["sections"]["async_children_detail"]["reason"] == "detail_read_failed"
    assert run.json()["data"]["async_children"][0]["provider_thread_id"] is None
    assert history.json()["sections"]["checkpoints"]["reason"] == "checkpointer_unavailable"
    assert summary.status_code == 503
    assert summary.json()["code"] == "inspection_source_unavailable"

    # Live sources up: Visibility joins by unit key; terminal authority vs a running
    # execution is reported stale, never "fixed" by the read.
    visibility = StaticVisibility(
        (
            TemporalExecution(
                workflow_id=f"belllabs-run/{world.active_run}",
                temporal_run_id="root-run",
                workflow_type="belllabs.run.v1",
                status="RUNNING",
                workflow_kind="root",
            ),
            TemporalExecution(
                workflow_id=f"operation/{world.in_doubt.semantic_operation_id}",
                temporal_run_id="op-run",
                workflow_type="belllabs.operation.v2",
                status="RUNNING",
                workflow_kind="operation",
                unit_key=world.in_doubt.unit_key,
                execution_generation=1,
            ),
        )
    )
    live = world.service(visibility=visibility, async_details=StaticDetails())
    with client_for(live, principal("auditor")) as client:
        run = client.get(base, params=params).json()
        unit = client.get(f"{base}/units/{world.in_doubt.unit_key}", params=params).json()
        terminal = client.get(
            f"/run-control/v1/inspection/runs/{world.terminal_run}", params=params
        ).json()
    assert run["sections"]["temporal"]["freshness"] == "current"
    assert run["sections"]["temporal"]["source"] == "temporal_visibility"
    assert len(run["data"]["temporal_executions"]) == 2
    assert [item["workflow_kind"] for item in unit["data"]["temporal_executions"]] == ["operation"]
    child = run["data"]["async_children"][0]
    assert (child["provider_thread_id"], child["provider_run_id"]) == (
        "provider-thread:1",
        "provider-run:1",
    )
    assert child["detail_lifecycle"] == "running"
    assert run["sections"]["async_children_detail"]["freshness"] == "current"
    assert terminal["sections"]["temporal"]["freshness"] == "stale"
    assert terminal["sections"]["temporal"]["reason"] == "visibility_lags_authority"
    assert visibility.calls[0] == ("tenant-1", world.active_run)


# --- Historical checkpoint reads and redaction -------------------------------------------


@pytest.mark.asyncio
async def test_checkpoint_history_lists_only_the_units_recorded_lineage(
    world: World, client_for: Any
) -> None:
    inspection = world.service()
    path = (
        f"/run-control/v1/inspection/runs/{world.active_run}/units/"
        f"{world.settled.unit_key}/checkpoints"
    )
    with client_for(inspection, principal("auditor")) as client:
        first = client.get(path, params={"request_scope": "tenant-1", "limit": 2}).json()
        cursor = first["data"]["next_cursor"]
        second = client.get(
            path, params={"request_scope": "tenant-1", "limit": 2, "cursor": cursor}
        ).json()
        wrong_unit_cursor = client.get(
            f"/run-control/v1/inspection/runs/{world.active_run}/units/"
            f"{world.in_doubt.unit_key}/checkpoints",
            params={"request_scope": "tenant-1", "cursor": cursor},
        )
    entries = first["data"]["entries"] + second["data"]["entries"]
    # Lineage = result chain back to the (empty) source: the foreign branch and the
    # drifted descendant after the recorded result are not part of the unit's lineage.
    assert [entry["key"]["checkpoint_id"] for entry in entries] == ["c1", "c3-parent", "c3"]
    assert second["data"]["next_cursor"] is None
    assert entries[-1]["roles"] == ["result", "namespace_head"]
    assert entries[1]["pending_task_names"] == ["tools"]
    assert all(entry["stamped"] and entry["binding_compatible"] for entry in entries)
    assert first["sections"]["checkpoints"]["source"] == "checkpointer"
    assert first["sections"]["checkpoints"]["freshness"] == "current"
    assert first["sections"]["checkpoints"]["redaction"]["withheld_field_count"] > 0
    assert first["data"]["checkpointer_ref_digest"] == CHECKPOINTER
    assert wrong_unit_cursor.status_code == 400

    # The in-doubt unit has no transition: its incident candidates are recorded keys.
    in_doubt = await inspection.get_checkpoint_history(
        "tenant-1", world.active_run, world.in_doubt.unit_key
    )
    assert {key.checkpoint_id for key in in_doubt.data.recorded_keys} == {"leaf-a", "leaf-b"}


@pytest.mark.asyncio
async def test_redacted_summary_is_separately_authorized_and_excludes_content(
    world: World, client_for: Any
) -> None:
    inspection = world.service()
    base = f"/run-control/v1/inspection/runs/{world.active_run}/units/{world.settled.unit_key}"
    params = {"request_scope": "tenant-1"}
    with client_for(inspection, principal("operator", "auditor")) as client:
        denied = client.get(f"{base}/checkpoints/c3-parent/summary", params=params)
        default_reads = [
            client.get(f"/run-control/v1/inspection/runs/{world.active_run}", params=params).text,
            client.get(base, params=params).text,
            client.get(f"{base}/checkpoints", params=params).text,
        ]
    assert denied.status_code == 403
    with client_for(inspection, principal("state_inspector")) as client:
        summary = client.get(f"{base}/checkpoints/c3-parent/summary", params=params)
    assert summary.status_code == 200
    body = summary.json()
    facts = body["data"]["facts"]
    assert facts["channel_names"] == [
        "artifact_index",
        "files",
        "messages",
        "structured_response",
        "todos",
    ]
    assert facts["message_count"] == 3
    assert facts["todo_count"] == 1
    assert facts["artifact_index_keys"] == ["report"]
    assert facts["has_structured_response"] is True
    # Five state channels plus the pending trigger channel: every value is withheld.
    assert facts["withheld_value_count"] == 6
    assert body["data"]["pending_task_names"] == ["tools"]
    assert body["data"]["stamped_digests"][STAMP_STATE_SCHEMA_DIGEST] == SCHEMA
    assert "belllabs_attempt_ref" not in body["data"]["stamped_digests"]
    assert body["sections"]["checkpoint"]["redaction"]["policy_ref"] == (
        "belllabs.inspection-redaction.v1"
    )
    assert body["sections"]["checkpoint"]["redaction"]["withheld_field_count"] >= 5
    for text in (summary.text, *default_reads):
        for sensitive in (SECRET_TEXT, PROMPT_TEXT, TOOL_ARGUMENT, "/workspace/notes.md"):
            assert sensitive not in text


@pytest.mark.asyncio
async def test_incompatible_or_foreign_checkpoints_are_typed_errors(
    world: World, client_for: Any
) -> None:
    inspection = world.service()
    # A unit whose recorded result checkpoint carries a drifted state-schema stamp: the
    # checkpoint is on its lineage but incompatible with its binding.
    drift_unit = stage_unit(
        request_scope="tenant-1", run_id=world.active_run, operation_id="op-drift"
    )
    await world.lineage.record_attempt(
        unit=drift_unit,
        execution_generation=1,
        attempt=activity_attempt(1),
        binding_id=f"binding:{drift_unit.unit_key}:1",
        binding_digest=BINDING,
        namespace=namespace_claim(drift_unit),
        dispatching=True,
        observed_at=LINEAGE_NOW,
    )
    observed = transition(drift_unit, source=None, result_id="d2")
    await world.lineage.record_result(
        unit_result(drift_unit, fence=1, transition_id=observed.transition_id),
        transition=observed,
    )
    namespace = cognitive_session_namespace(drift_unit, 1)
    await put_checkpoint(
        world.saver, namespace, "d2-parent", None, step=0, stamps=stamps_for(drift_unit), values={}
    )
    await put_checkpoint(
        world.saver,
        namespace,
        "d2",
        "d2-parent",
        step=1,
        stamps=stamps_for(drift_unit, schema=OTHER_SCHEMA),
        values={},
    )
    base = f"/run-control/v1/inspection/runs/{world.active_run}/units"
    params = {"request_scope": "tenant-1"}
    with client_for(inspection, principal("state_inspector")) as client:
        incompatible = client.get(
            f"{base}/{drift_unit.unit_key}/checkpoints/d2/summary", params=params
        )
        history = client.get(f"{base}/{drift_unit.unit_key}/checkpoints", params=params).json()
        foreign = client.get(
            f"{base}/{world.settled.unit_key}/checkpoints/foreign/summary", params=params
        )
        missing = client.get(
            f"{base}/{world.settled.unit_key}/checkpoints/never-written/summary", params=params
        )
        other_unit = client.get(
            f"{base}/{drift_unit.unit_key}/checkpoints/c3/summary", params=params
        )
    assert incompatible.status_code == 409
    assert incompatible.json()["code"] == "incompatible_checkpoint"
    assert [
        (entry["key"]["checkpoint_id"], entry["state_schema_compatible"])
        for entry in history["data"]["entries"]
    ] == [("d2-parent", True), ("d2", False)]
    for response in (foreign, missing, other_unit):
        assert response.status_code == 404
        assert response.json()["code"] == "checkpoint_not_in_unit_lineage"


def test_redaction_allowlist_keeps_counts_and_names_only() -> None:
    facts = summarize_channel_values(sensitive_state())
    serialized = json.dumps(facts.model_dump(mode="json"))
    assert SECRET_TEXT not in serialized and PROMPT_TEXT not in serialized
    assert facts.message_count == 3
    assert summarize_channel_values({}).channel_names == ()


# --- Search Attribute policy (REQ-CP-EXEC-015) -------------------------------------------


def test_search_attribute_policy_defaults_to_disabled_and_production_requires_it() -> None:
    from tests.unit.operations.test_operation_execution import operation_request

    root = BellLabsRunInput(
        schema_version="belllabs.temporal-root.v1",
        run_id="run-1",
        request_scope="tenant-1",
        effective_configuration_digest=BINDING,
        workflow_type_digest=BINDING,
        family="StageGraph",
        family_input={},
        family_task_queue="queue",
    )
    assert root.search_attribute_policy == "disabled"
    assert StageGraphRunInput.__dataclass_fields__["search_attribute_policy"].default == "disabled"
    assert (
        GoalDirectedRunInput.__dataclass_fields__["search_attribute_policy"].default == "disabled"
    )
    operation = operation_request()
    workflow_request = OperationWorkflowRequest(
        semantic_attempt_id=operation.identity.semantic_key,
        operation_kind="bound_operation",
        operation=operation,
    )
    # A payload recorded before the field existed decodes as `disabled`.
    legacy = workflow_request.model_dump(mode="json")
    legacy.pop("search_attribute_policy")
    assert OperationWorkflowRequest.model_validate(legacy).search_attribute_policy == "disabled"

    assert require_production_search_attribute_policy("required") == "required"
    with pytest.raises(SearchAttributePolicyError):
        require_production_search_attribute_policy("disabled")

    values = operation_search_attributes(
        run_id="run-1",
        request_scope="tenant-1",
        execution_generation=2,
        family="StageGraph",
        execution_epoch=1,
        unit_key="bl-unit-v1:" + "a" * 64,
        unit_kind="stage_operation",
    ).as_mapping()
    assert values == {
        "BellLabsRunId": "run-1",
        "BellLabsScopeHash": search_attribute_scope_hash("tenant-1"),
        "BellLabsWorkflowKind": "operation",
        "BellLabsFamily": "stage_graph",
        "BellLabsExecutionEpoch": 1,
        "BellLabsUnitKey": "bl-unit-v1:" + "a" * 64,
        "BellLabsUnitKind": "stage_operation",
        "BellLabsExecutionGeneration": 2,
    }
    assert "tenant-1" not in json.dumps(values), "the raw scope is never indexed"
    root_values = run_search_attributes(
        workflow_kind="root",
        run_id="run-1",
        request_scope="tenant-1",
        family="GoalDirected",
        execution_epoch=1,
    ).as_mapping()
    assert set(root_values) == {
        "BellLabsRunId",
        "BellLabsScopeHash",
        "BellLabsWorkflowKind",
        "BellLabsFamily",
        "BellLabsExecutionEpoch",
    }
    scope_hash = search_attribute_scope_hash("tenant-1")
    assert visibility_run_query("run-1", "tenant-1") == (
        f"BellLabsRunId = 'run-1' AND BellLabsScopeHash = '{scope_hash}'"
    )
    with pytest.raises(ValueError):
        visibility_run_query("run-1' OR 'a'='a", "tenant-1")


@pytest.mark.asyncio
async def test_production_submitter_rejects_disabled_search_attributes() -> None:
    from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter

    with pytest.raises(SearchAttributePolicyError):
        TemporalWorkflowSubmitter.for_production(
            object(),  # type: ignore[arg-type]
            stagegraph_task_queue="stagegraph",
            goal_directed_task_queue="goal",
            search_attribute_policy="disabled",
        )


def test_application_code_never_addresses_temporal_persistence() -> None:
    """REQ-CP-EXEC-015 static check: no Temporal persistence DSN or table access in app/."""

    from app.config import PROJECT_ROOT

    forbidden = (
        "executions_visibility",
        "TEMPORAL_POSTGRES",
        "temporal-postgres",
        "history_node",
        "current_executions",
    )
    offenders = [
        f"{path.relative_to(PROJECT_ROOT)}:{needle}"
        for path in (PROJECT_ROOT / "app").rglob("*")
        if path.suffix in {".py", ".sql"}
        for needle in forbidden
        if needle in path.read_text(encoding="utf-8")
    ]
    assert offenders == []


@pytest.mark.asyncio
async def test_delta_channel_counts_are_folded_from_the_saver_history_without_content() -> None:
    """LangGraph 1.2 delta channels (`messages`) are counted from the saver's history walk."""

    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    from langgraph.checkpoint.base import empty_checkpoint

    from app.domain.operation_execution.checkpoint_lineage import ROOT_CHECKPOINT_NS

    saver = InMemorySaver()
    namespace = "belllabs/stage/delta-fixture/gen/1"

    def config(checkpoint_id: str | None) -> Any:
        configurable: dict[str, Any] = {"thread_id": namespace, "checkpoint_ns": ""}
        if checkpoint_id is not None:
            configurable["checkpoint_id"] = checkpoint_id
        return {"configurable": configurable}

    async def put(checkpoint_id: str, parent: str | None, versions: dict[str, str]) -> None:
        checkpoint = empty_checkpoint()
        checkpoint["id"] = checkpoint_id
        checkpoint["channel_versions"] = versions
        await saver.aput(config(parent), checkpoint, {"step": len(versions)}, versions)

    await put("d1", None, {})
    await saver.aput_writes(
        config("d1"),
        [
            ("messages", [HumanMessage(content=PROMPT_TEXT, id="m1")]),
            ("files", {"/workspace/a.md": SECRET_TEXT}),
        ],
        task_id="task-1",
    )
    await put("d2", "d1", {"messages": "1", "files": "1"})
    await saver.aput_writes(
        config("d2"),
        [
            ("messages", [AIMessage(content=SECRET_TEXT, id="m2")]),
            ("messages", [ToolMessage(content=TOOL_ARGUMENT, tool_call_id="c", id="m3")]),
            ("messages", [AIMessage(content="replaced", id="m2")]),
        ],
        task_id="task-2",
    )
    await put("d3", "d2", {"messages": "2", "files": "1"})

    reader = LangGraphCheckpointHistoryReader({CHECKPOINTER: saver})
    read = await reader.read_redacted_state(
        QualifiedCheckpointKey(
            checkpointer_ref_digest=CHECKPOINTER,
            thread_id=namespace,
            checkpoint_ns=ROOT_CHECKPOINT_NS,
            checkpoint_id="d3",
            parent_checkpoint_id="d2",
        )
    )
    assert read is not None
    _observation, facts = read
    assert facts.channel_names == ("files", "messages")
    assert facts.message_count == 3, "same-ID writes replace; distinct IDs accumulate"
    serialized = facts.model_dump_json()
    for sensitive in (SECRET_TEXT, PROMPT_TEXT, TOOL_ARGUMENT, "/workspace/a.md"):
        assert sensitive not in serialized


# --- Review follow-ups: bounded lineage walks and exact child attribution ----------------


class CyclicCheckpoints:
    """A checkpointer whose parent links loop (corrupt or adversarial data)."""

    def __init__(self, cycles: dict[str, tuple[RuntimeUnitIdentity, tuple[str, str]]]) -> None:
        self._cycles = cycles

    def supports(self, checkpointer_ref_digest: str) -> bool:
        return checkpointer_ref_digest == CHECKPOINTER

    async def list_root_checkpoints(self, digest: str, namespace: str) -> Any:
        unit, (first, second) = self._cycles[namespace]
        return tuple(
            CheckpointObservation(
                key=QualifiedCheckpointKey(
                    checkpointer_ref_digest=digest,
                    thread_id=namespace,
                    checkpoint_id=checkpoint_id,
                    parent_checkpoint_id=parent,
                ),
                step=step,
                stamps=stamps_for(unit),
            )
            for step, (checkpoint_id, parent) in enumerate(((first, second), (second, first)))
        )

    async def read_redacted_state(self, key: QualifiedCheckpointKey) -> Any:
        raise AssertionError("a cyclic lineage is never summarized")


@pytest.mark.asyncio
async def test_cyclic_checkpoint_parents_are_reported_not_walked_forever(
    world: World, client_for: Any
) -> None:
    settled_ns = cognitive_session_namespace(world.settled, 1)
    doubt_ns = cognitive_session_namespace(world.in_doubt, 1)
    inspection = world.service(
        checkpoints=CyclicCheckpoints(
            {
                # With a recorded result: the chain from `c3` loops back to itself.
                settled_ns: (world.settled, ("c3", "c3-parent")),
                # Without a transition: every stamped checkpoint's chain loops.
                doubt_ns: (world.in_doubt, ("loop-a", "loop-b")),
            }
        )
    )
    base = f"/run-control/v1/inspection/runs/{world.active_run}/units"
    params = {"request_scope": "tenant-1"}
    with client_for(inspection, principal("state_inspector")) as client:
        for unit in (world.settled, world.in_doubt):
            history = client.get(f"{base}/{unit.unit_key}/checkpoints", params=params)
            assert history.status_code == 200
            section = history.json()["sections"]["checkpoints"]
            assert (section["freshness"], section["reason"]) == (
                "unavailable",
                "checkpoint_lineage_cycle",
            )
            assert history.json()["data"]["entries"] == []
        summary = client.get(
            f"{base}/{world.settled.unit_key}/checkpoints/c3/summary", params=params
        )
    assert summary.status_code == 503
    assert summary.json()["code"] == "inspection_source_unavailable"


@pytest.mark.asyncio
async def test_namespace_history_above_the_bound_is_unavailable_not_truncated(
    world: World,
) -> None:
    bounded = world.service(
        checkpoints=LangGraphCheckpointHistoryReader({CHECKPOINTER: world.saver}, max_checkpoints=2)
    )
    page = await bounded.get_checkpoint_history(
        "tenant-1", world.active_run, world.settled.unit_key
    )
    assert page.sections["checkpoints"].freshness == "unavailable"
    assert page.sections["checkpoints"].reason == "namespace_history_exceeds_bound"
    assert page.data.entries == ()
    within = world.service(
        checkpoints=LangGraphCheckpointHistoryReader({CHECKPOINTER: world.saver}, max_checkpoints=5)
    )
    full = await within.get_checkpoint_history("tenant-1", world.active_run, world.settled.unit_key)
    assert [entry.key.checkpoint_id for entry in full.data.entries] == ["c1", "c3-parent", "c3"]


@dataclass(frozen=True)
class ChildDetail:
    parent_binding_id: str | None


class ChildDetails:
    def __init__(self, details: dict[str, ChildDetail]) -> None:
        self._details = details

    async def get_execution(self, request_scope: str, child_execution_id: str) -> Any:
        return self._details[child_execution_id]


def _child(child_id: str, parent_operation_id: str, generation: int = 1) -> AsyncChildInspection:
    return AsyncChildInspection(
        child_execution_id=child_id,
        parent_operation_id=parent_operation_id,
        link_id=f"link:{child_id}",
        contract_id="contract:helper",
        binding_digest=BINDING,
        execution_generation=generation,
        dependency_class="nonblocking",
    )


@pytest.mark.asyncio
async def test_async_children_are_attributed_by_exact_binding_not_semantic_operation(
    world: World,
) -> None:
    """Two units of one run share a semantic operation ID; each sees only its own child."""

    first = stage_unit(request_scope="tenant-1", run_id=world.active_run, operation_id="op-shared")
    second = stage_unit(
        request_scope="tenant-1",
        run_id=world.active_run,
        operation_id="op-shared",
        semantic_attempt=2,
    )
    assert first.unit_key != second.unit_key
    for unit in (first, second):
        await world.lineage.record_attempt(
            unit=unit,
            execution_generation=1,
            attempt=activity_attempt(1, workflow_id=f"operation/{unit.unit_key}"),
            binding_id=f"binding:{unit.unit_key}:1",
            binding_digest=BINDING,
            namespace=namespace_claim(unit),
            dispatching=False,
            observed_at=LINEAGE_NOW,
        )
    children = (
        _child("by-detail-binding", "op-shared"),
        _child("by-semantic-operation-only", "op-shared"),
        _child("by-authority-binding", f"binding:{second.unit_key}:1"),
        _child("other-generation", "op-shared", generation=2),
    )
    details = ChildDetails(
        {
            "by-detail-binding": ChildDetail(f"binding:{first.unit_key}:1"),
            "by-semantic-operation-only": ChildDetail(None),
            "by-authority-binding": ChildDetail(None),
            "other-generation": ChildDetail(f"binding:{first.unit_key}:1"),
        }
    )

    def inspection(**sources: Any) -> RuntimeInspectionService:
        return RuntimeInspectionService(
            InMemoryInspectionReadRepository(
                world.runs,
                world.lineage,
                async_children={world.active_run: children},
                clock=world.clock,
            ),
            clock=world.clock,
            **sources,
        )

    with_detail = inspection(async_details=details)
    run = await with_detail.get_run("tenant-1", world.active_run)
    assert [item.child_execution_id for item in run.data.async_children] == [
        "by-detail-binding",
        "by-semantic-operation-only",
        "by-authority-binding",
        "other-generation",
    ]
    owned: dict[str, list[str]] = {}
    for unit in (first, second):
        read = await with_detail.get_unit("tenant-1", world.active_run, unit.unit_key)
        owned[unit.unit_key] = [item.child_execution_id for item in read.data.async_children]
    assert owned == {
        first.unit_key: ["by-detail-binding"],
        second.unit_key: ["by-authority-binding"],
    }
    # Without the detail document only a binding-valued parent reference attributes a child.
    without_detail = inspection()
    for unit, expected in ((first, []), (second, ["by-authority-binding"])):
        read = await without_detail.get_unit("tenant-1", world.active_run, unit.unit_key)
        assert [item.child_execution_id for item in read.data.async_children] == expected
