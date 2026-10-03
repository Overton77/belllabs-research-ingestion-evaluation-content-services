"""RRM-005 real-namespace Search Attribute qualification (REQ-CP-EXEC-015).

A real Temporal dev server (`WorkflowEnvironment.start_local`) registers the BellLabs
Search Attributes. Root, family, and `OperationWorkflow` executions run under the
`required` policy carried in their inputs; Visibility lists them by `BellLabsRunId` and
`BellLabsUnitKey`; inspection joins the persisted units to those executions through the
Visibility API only, and still serves the persisted read when Temporal is unreachable.
Captured histories replay. No Temporal persistence database is read.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from google.protobuf.duration_pb2 import Duration
from temporalio import activity
from temporalio.api.enums.v1 import IndexedValueType
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest
from temporalio.api.workflowservice.v1 import RegisterNamespaceRequest
from temporalio.client import Client, WorkflowHistory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from mission_control.adapters.temporal.search_attributes import (
    BELLLABS_SEARCH_ATTRIBUTE_KEYS,
    SearchAttributeRegistrationError,
    register_belllabs_search_attributes,
    verify_belllabs_search_attributes,
)
from mission_control.adapters.temporal.submission import TemporalWorkflowSubmitter
from mission_control.adapters.temporal.visibility import TemporalVisibilityInspectionReader
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from mission_control.adapters.temporal.workflows.goal_directed import GoalDirectedWorkflow
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.adapters.temporal.workflows.stagegraph import StageGraphWorkflow
from mission_control.application.execution.inspection import (
    InMemoryInspectionReadRepository,
    RuntimeInspectionService,
)
from mission_control.application.execution.operations.checkpoint_lineage import (
    InMemoryCheckpointLineageRepository,
)
from mission_control.domain.authoring.contracts import GoalDirectedBlueprint
from mission_control.domain.authoring.fixtures import GENERIC_GOAL_DIRECTED
from mission_control.domain.coordinator.launch import BlueprintFamily
from mission_control.domain.execution.checkpoint_lineage import OperationActivityAttempt
from mission_control.domain.execution.contracts import OperationWorkflowRequest
from mission_control.domain.graph_runtime.identities import RuntimeUnitIdentity
from mission_control.domain.programs.contracts import (
    StageGraphAdmissionActivityRequest,
    StageGraphAdmissionActivityResult,
)
from mission_control.domain.programs.runtime_units import stage_runtime_unit
from mission_control.domain.programs.search_attributes import (
    BELLLABS_SEARCH_ATTRIBUTES,
    search_attribute_scope_hash,
    visibility_run_query,
)
from tests.fixtures.checkpoint_lineage import BINDING
from tests.integration.temporal.test_wp_bp_010_temporal import (
    QUEUE,
    FakeStageGraphActivities,
    _blueprint,
)
from tests.integration.temporal.test_wp_bp_010_temporal import _run_input as stage_run_input
from tests.integration.temporal.test_wp_bp_020_temporal import FakeGoalDirectedActivities
from tests.integration.temporal.test_wp_bp_020_temporal import _run_input as goal_run_input
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service

GOAL_QUEUE = "wp-bp-020-temporal"
NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


async def _start_local() -> WorkflowEnvironment:
    try:
        return await WorkflowEnvironment.start_local(
            search_attributes=BELLLABS_SEARCH_ATTRIBUTE_KEYS, dev_server_log_level="error"
        )
    except RuntimeError as error:
        pytest.skip(f"Temporal dev server is unavailable: {error}")


class UnitBoundStageGraphActivities(FakeStageGraphActivities):
    """The BP-010 fixture with each operation bound to its `CON-CP-RUNTIME-UNIT-V1`."""

    def __init__(self) -> None:
        super().__init__()
        self.operations: list[tuple[RuntimeUnitIdentity, OperationWorkflowRequest]] = []

    @activity.defn(name="stagegraph.admit_operation")
    async def admit(
        self, request: StageGraphAdmissionActivityRequest
    ) -> StageGraphAdmissionActivityResult:
        admitted = await FakeStageGraphActivities.admit(self, request)
        assert admitted.operation is not None
        unit = stage_runtime_unit(request.request_scope, request.proposal.identity)
        bound = OperationWorkflowRequest.model_validate(
            {
                **admitted.operation.model_dump(mode="python"),
                "operation": {
                    **admitted.operation.operation.model_dump(mode="python"),
                    "runtime_unit": unit,
                },
            }
        )
        self.operations.append((unit, bound))
        return replace(admitted, operation=bound)

    @property
    def functions(self) -> list[object]:
        return [
            self.initialize,
            self.admit,
            self.decide,
            self.apply_cycle,
            self.complete,
            self.execute_operation,
        ]


async def _eventually(predicate: Any) -> Any:
    async with asyncio.timeout(30):
        while True:
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.25)


def _started_attributes(history: WorkflowHistory) -> set[str]:
    started = history.events[0].workflow_execution_started_event_attributes
    return set(started.search_attributes.indexed_fields)


def _upserts(history: WorkflowHistory) -> int:
    """Upserts of BellLabs attributes. A `workflow.patched` marker also upserts the SDK's
    `TemporalChangeVersion` attribute, which is not a BellLabs attribute (RRM-008)."""

    count = 0
    for event in history.events:
        if not event.HasField("upsert_workflow_search_attributes_event_attributes"):
            continue
        upsert = event.upsert_workflow_search_attributes_event_attributes
        if any(name.startswith("BellLabs") for name in upsert.search_attributes.indexed_fields):
            count += 1
    return count


async def _replay(histories: list[WorkflowHistory]) -> None:
    replayer = Replayer(
        workflows=[
            BellLabsRunWorkflow,
            StageGraphWorkflow,
            GoalDirectedWorkflow,
            OperationWorkflow,
        ],
        workflow_runner=coordinator_workflow_runner(),
    )
    for history in histories:
        await replayer.replay_workflow(history)


@pytest.mark.asyncio
async def test_registration_is_idempotent_read_only_at_readiness_and_fails_on_conflict() -> None:
    env = await _start_local()
    async with env:
        client = env.client
        # The fixture namespace registered the attributes through `start_local`.
        await verify_belllabs_search_attributes(client, "default")
        assert await register_belllabs_search_attributes(client, "default") == ()

        for namespace in ("rrm005-admin", "rrm005-conflict"):
            await client.workflow_service.register_namespace(
                RegisterNamespaceRequest(
                    namespace=namespace,
                    workflow_execution_retention_period=Duration(seconds=86_400),
                )
            )
        # Readiness never mutates: a namespace without the attributes is refused.
        with pytest.raises(SearchAttributeRegistrationError):
            await verify_belllabs_search_attributes(client, "rrm005-admin")
        added = await register_belllabs_search_attributes(client, "rrm005-admin")
        assert added == tuple(sorted(BELLLABS_SEARCH_ATTRIBUTES))
        await verify_belllabs_search_attributes(client, "rrm005-admin")
        assert await register_belllabs_search_attributes(client, "rrm005-admin") == ()

        await client.operator_service.add_search_attributes(
            AddSearchAttributesRequest(
                namespace="rrm005-conflict",
                search_attributes={
                    "BellLabsRunId": IndexedValueType.INDEXED_VALUE_TYPE_INT  # type: ignore[dict-item]
                },
            )
        )
        with pytest.raises(SearchAttributeRegistrationError, match="conflicting type"):
            await register_belllabs_search_attributes(client, "rrm005-conflict")


@pytest.mark.asyncio
async def test_root_family_and_operations_join_inspection_through_visibility() -> None:
    run_service, runs = run_control_service()
    admitted = await run_service.admit(run_request(request_id="rrm-005-search-attributes"))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    activities = UnitBoundStageGraphActivities()
    activities.slow_release.set()
    env = await _start_local()
    async with env:
        client = env.client
        async with Worker(
            client,
            task_queue=QUEUE,
            workflows=[BellLabsRunWorkflow, StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            submitter = TemporalWorkflowSubmitter.for_production(
                client,
                stagegraph_task_queue=QUEUE,
                goal_directed_task_queue="rrm005-goal-unused",
                search_attribute_policy="required",
            )
            submission = await submitter.submit(
                replace(stage_run_input(_blueprint()), run_id=run_id),
                workflow_id="ignored",
                blueprint_family=BlueprintFamily.STAGE_GRAPH,
            )
            root = client.get_workflow_handle(submission.workflow_id)
            await asyncio.wait_for(root.result(), timeout=120)

            # A required operation started without attributes upserts exactly its own.
            unit, bound = activities.operations[0]
            direct_unit = unit.model_copy(
                update={"belllabs_run_id": f"{run_id}-direct", "semantic_attempt": 2}
            )
            operation = bound.operation
            direct = OperationWorkflowRequest.model_validate(
                {
                    **bound.model_dump(mode="python"),
                    "semantic_attempt_id": operation.identity.model_copy(
                        update={"run_id": f"{run_id}-direct", "operation_attempt": 2}
                    ).semantic_key,
                    "search_attribute_policy": "required",
                    "operation": {
                        **operation.model_dump(mode="python"),
                        "identity": operation.identity.model_copy(
                            update={"run_id": f"{run_id}-direct", "operation_attempt": 2}
                        ),
                        "runtime_unit": direct_unit,
                    },
                }
            )
            direct_handle = await client.start_workflow(
                OperationWorkflow.run, direct, id=direct.workflow_id, task_queue=QUEUE
            )
            await asyncio.wait_for(direct_handle.result(), timeout=60)

        query = visibility_run_query(run_id, "tenant-1")
        assert await _eventually(lambda: _count_is(client, query, 5)) == 5
        listed = [item async for item in client.list_workflows(query)]
        kinds = sorted(
            str(item.typed_search_attributes.get(_key("BellLabsWorkflowKind"))) for item in listed
        )
        assert kinds == ["family", "operation", "operation", "operation", "root"]
        for item in listed:
            assert item.typed_search_attributes.get(_key("BellLabsFamily")) == "stage_graph"
            assert item.typed_search_attributes.get(_key("BellLabsExecutionEpoch")) == 1
            assert item.typed_search_attributes.get(
                _key("BellLabsScopeHash")
            ) == search_attribute_scope_hash("tenant-1")
        # Every operation is found by its unit key, with kind and generation.
        for unit, bound in activities.operations:
            by_unit = [
                item async for item in client.list_workflows(f"BellLabsUnitKey = '{unit.unit_key}'")
            ]
            assert [item.id for item in by_unit] == [bound.workflow_id]
            attributes = by_unit[0].typed_search_attributes
            assert attributes.get(_key("BellLabsUnitKind")) == "stage_operation"
            assert attributes.get(_key("BellLabsExecutionGeneration")) == 1
            assert attributes.get(_key("BellLabsRunId")) == run_id
        direct_count = await client.count_workflows(f"BellLabsUnitKey = '{direct_unit.unit_key}'")
        assert direct_count.count == 1

        # Started with their attributes: no execution of the run upserts; the directly
        # started required operation upserts once.
        histories = [await root.fetch_history()]
        histories.append(await client.get_workflow_handle(f"family/{run_id}/1").fetch_history())
        histories.extend(
            [
                await client.get_workflow_handle(bound.workflow_id).fetch_history()
                for _unit, bound in activities.operations
            ]
        )
        expected = {"BellLabsRunId", "BellLabsScopeHash", "BellLabsWorkflowKind"}
        for history in histories:
            assert expected <= _started_attributes(history)
            assert _upserts(history) == 0
        assert "BellLabsUnitKey" in _started_attributes(histories[2])
        direct_history = await direct_handle.fetch_history()
        assert _started_attributes(direct_history) == set()
        assert _upserts(direct_history) == 1
        await _replay([*histories, direct_history])

        # Runtime join: persisted units joined to their executions by `BellLabsUnitKey`.
        lineage = InMemoryCheckpointLineageRepository()
        for unit, bound in activities.operations:
            await lineage.record_attempt(
                unit=unit,
                execution_generation=1,
                attempt=OperationActivityAttempt(
                    workflow_id=bound.workflow_id,
                    workflow_run_id="recorded-by-the-activity",
                    activity_id="1",
                    attempt=1,
                    worker_identity="rrm-005-qualification",
                ),
                binding_id=f"binding:{unit.unit_key}",
                binding_digest=BINDING,
                namespace=None,
                dispatching=True,
                observed_at=NOW,
            )
        inspection = RuntimeInspectionService(
            InMemoryInspectionReadRepository(runs, lineage),
            visibility=TemporalVisibilityInspectionReader(client),
        )
        detail = await inspection.get_run("tenant-1", run_id)
        assert detail.sections["temporal"].source == "temporal_visibility"
        assert detail.sections["temporal"].freshness == "current"
        assert len(detail.data.temporal_executions) == 5
        assert {item.status for item in detail.data.temporal_executions} == {"COMPLETED"}
        for unit, bound in activities.operations:
            unit_read = await inspection.get_unit("tenant-1", run_id, unit.unit_key)
            assert [item.workflow_id for item in unit_read.data.temporal_executions] == [
                bound.workflow_id
            ]
            assert unit_read.data.generations[0].attempts[0].attempt.workflow_id == (
                bound.workflow_id
            )
        # Visibility is scope-bound: another scope's hash lists nothing for this run.
        assert (
            await TemporalVisibilityInspectionReader(client).list_run_executions("tenant-2", run_id)
            == ()
        )

    # Temporal is gone: the persisted read still succeeds, the section is unavailable.
    unreachable = RuntimeInspectionService(
        InMemoryInspectionReadRepository(runs, lineage),
        visibility=TemporalVisibilityInspectionReader(client, rpc_timeout=timedelta(seconds=2)),
    )
    offline = await asyncio.wait_for(unreachable.get_run("tenant-1", run_id), timeout=60)
    assert offline.sections["temporal"].freshness == "unavailable"
    assert offline.sections["temporal"].reason == "temporal_visibility_unavailable"
    assert offline.sections["run"].freshness == "current"
    assert len(offline.data.units) == 3


@pytest.mark.asyncio
async def test_goal_directed_policy_is_carried_through_continue_as_new() -> None:
    blueprint = GoalDirectedBlueprint.model_validate(
        {**GENERIC_GOAL_DIRECTED.model_dump(mode="python"), "max_iterations": 21}
    )
    activities = FakeGoalDirectedActivities(complete_at_iteration=21)
    run_id = "run-rrm-005-goal-continuation"
    env = await _start_local()
    async with env:
        client = env.client
        async with Worker(
            client,
            task_queue=GOAL_QUEUE,
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            family_input = replace(
                goal_run_input(blueprint=blueprint, run_id=run_id),
                search_attribute_policy="required",
            )
            handle = await client.start_workflow(
                GoalDirectedWorkflow.run,
                family_input,
                id=f"family/{run_id}/1",
                task_queue=GOAL_QUEUE,
            )
            result = await asyncio.wait_for(handle.result(), timeout=180)
        assert result.goal_iterations == 21

        query = visibility_run_query(run_id, "tenant-1")
        await _eventually(lambda: _count_is(client, query, 44))
        families = [
            item
            async for item in client.list_workflows(f"{query} AND BellLabsWorkflowKind = 'family'")
        ]
        # Two runs of one family workflow: the first continued as new; both carry the
        # family attributes, so the policy survived Continue-As-New.
        assert sorted(item.status.name for item in families if item.status) == [
            "COMPLETED",
            "CONTINUED_AS_NEW",
        ]
        assert {item.typed_search_attributes.get(_key("BellLabsFamily")) for item in families} == {
            "goal_directed"
        }
        operations = await client.count_workflows(f"{query} AND BellLabsWorkflowKind = 'operation'")
        assert operations.count == 42
        histories: list[WorkflowHistory] = []
        async for page in client.list_workflows(f"{query} AND BellLabsWorkflowKind = 'family'"):
            histories.append(
                await client.get_workflow_handle(page.id, run_id=page.run_id).fetch_history()
            )
        await _replay(histories)


async def _count_is(client: Client, query: str, expected: int) -> int | None:
    count = (await client.count_workflows(query)).count
    return count if count == expected else None


def _key(name: str) -> Any:
    return next(key for key in BELLLABS_SEARCH_ATTRIBUTE_KEYS if key.name == name)
