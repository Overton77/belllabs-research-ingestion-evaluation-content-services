"""RRM-006 demonstration: API-to-Temporal semantic forks of both families.

Each family runs a tiny technical run admitted through `POST /run-control/v1/run-requests`
and started on a real Temporal dev server (`WorkflowEnvironment.start_local`, BellLabs Search
Attributes registered, policy `required`) through `TemporalWorkflowSubmitter`:
`BellLabsRunWorkflow` -> family -> `OperationWorkflow` -> `operation.execute` -> a real
`create_deep_agent` graph with a deterministic technical model over the real
`AsyncPostgresSaver`. Run control, the operation journal, checkpoint lineage, snapshots and fork
authority are application PostgreSQL; the operation binding store is MongoDB.

At a safe boundary the facade takes a `RunSnapshotManifest`
(`POST /runs/{run}/snapshots`) and forks (`POST /runs/{run}/forks`) with a typed patch. The
derived run is admitted independently at epoch 1 and started with `BellLabsParentRunId`.

* StageGraph (`draft` -> wait `release-review` -> `review`): the source is snapshotted while
  it waits after `draft` settled. The patch changes the `review` objective, so the derived
  run reuses `draft` by immutable ref (no model call) and runs `review` fresh. Both runs then
  complete with distinct result manifests and a distinct `review` artifact; the source's
  authority and artifacts are unchanged by the fork.
* GoalDirected (one iteration, executor then verifier): the source is snapshotted after its
  verifier settled. The patch changes the goal objective; GoalDirected units are never
  reused (their revision identity is run-bound), so the derived run starts fresh and produces
  distinct artifacts.

Captured histories replay. Opt-in through `TEST_APPLICATION_POSTGRES_DSN` and
`TEST_MONGODB_URI` (disposable stack). No company research and no live model.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import asyncpg
import httpx
import pytest
from pymongo import AsyncMongoClient
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from app.api.control_plane import ControlPlanePrincipal, get_control_plane_principal
from app.api.run_control import (
    get_boundary_intervention_service,
    get_run_control_service,
)
from app.api.run_forks import get_run_fork_services
from app.application.orchestration.goal_directed import (
    GoalDirectedOperationPreparationService,
    GoalDirectedOperationResultService,
    RunControlGoalOperationSettlements,
    configure_goal_directed_family_admissions,
)
from app.application.orchestration.service import (
    RunControlLifecycleGateway,
    StageGraphDecisionService,
    StageGraphOperationPreparationService,
    orchestration_lifecycle_actor,
    register_stagegraph_family_mutations,
)
from app.application.run_control.boundary_interventions import (
    BoundaryCommandApplicationService,
    BoundaryCommandDeliveryService,
    BoundaryInterventionService,
)
from app.application.run_control.service import FamilyAdmissionRegistry
from app.application.runtime.run_forks import ForkPatchPolicyRegistry
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AllowedOperationVariant,
    DefinitionKind,
    ExactDefinitionRef,
    GoalDirectedBlueprint,
    LateResultPolicy,
    LateResultRule,
    SlowSiblingPolicy,
    StageDependency,
    StageGraphBlueprint,
    StageGraphWait,
    StageInputSlot,
    StageJoin,
    StageNode,
    StageOperationSlot,
    StageOutputSlot,
)
from app.domain.control_plane.fixtures import GENERIC_GOAL_DIRECTED
from app.domain.coordinator.launch import BlueprintFamily
from app.domain.operation_execution.contracts import (
    DeepAgentExecutionBinding,
    OperationExecutionRequest,
    PromptSegment,
    StructuredOutputBinding,
)
from app.domain.orchestration.contracts import (
    GoalDirectedRunInput,
    GoalRevision,
    StageGraphDecisionMutation,
    StageGraphRunInput,
)
from app.domain.run_control.contracts import (
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    LifecycleCommand,
    RunOutcome,
    RunPhase,
    RunRequest,
    SatisfyWaitAction,
)
from app.domain.run_control.forks import (
    ForkPatchPolicy,
    PatchablePath,
    stage_objective_path,
)
from app.integrations.temporal_boundary_commands import TemporalBoundaryCommandTransport
from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter
from app.server import api
from app.temporal.activities.goal_directed import GoalDirectedActivities
from app.temporal.operation_activities import OperationExecutionActivities
from app.temporal.orchestration_activities import StageGraphActivities
from app.temporal.search_attributes import BELLLABS_SEARCH_ATTRIBUTE_KEYS
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.operation import OperationWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow, wait_condition_id
from tests.acceptance.control_plane.test_wp_bp_020_sandbox_rollover import Documents, Templates
from tests.fixtures.checkpoint_recovery import governed_workspace
from tests.fixtures.rrm006_fork_stack import (
    SAVER_SCHEMA,
    ForkStack,
    open_fork_stack,
)
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)
from tests.unit.operations.test_operation_execution import operation_request
from tests.unit.run_control.test_run_control import WORKFLOW_DIGEST
from tests.unit.run_control.test_run_control import request as run_request

SCOPE = "tenant-1"
STAGE_QUEUE = "rrm006-stagegraph"
GOAL_QUEUE = "rrm006-goal-directed"
WAIT_ID = "release-review"
OBLIGATION = "fixture-obligation"
DIGEST = "sha256:" + "a" * 64
STAGE_OBJECTIVE = "Review the draft against the stricter technical checklist."
GOAL_OBJECTIVE = "Produce one verified technical record."
PATCHED_GOAL_OBJECTIVE = "Produce one verified technical record with a second field."
PRINCIPAL = ControlPlanePrincipal(
    actor_id="operator",
    roles=frozenset({"operator", "fork_operator"}),
    tenant_scopes=frozenset({SCOPE}),
    authority_refs=frozenset({"authority:lifecycle"}),
    sponsorship_refs=frozenset({"sponsorship:test"}),
    approval_refs=frozenset({"approval:test"}),
)


@pytest.fixture
async def mongo_database(test_mongodb_uri: str) -> AsyncIterator[str]:
    name = f"rrm006_forks_{uuid4().hex[:12]}"
    yield name
    client: AsyncMongoClient[Any] = AsyncMongoClient(test_mongodb_uri)
    try:
        await client.drop_database(name)
    finally:
        await client.close()


async def _reset(dsn: str) -> None:
    owner = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    try:
        await reset_application_schema(owner)
        async with owner.acquire() as connection:
            await connection.execute(f"DROP SCHEMA IF EXISTS {SAVER_SCHEMA} CASCADE")
            await connection.execute(f"CREATE SCHEMA {SAVER_SCHEMA}")
    finally:
        await owner.close()


async def _start_local() -> WorkflowEnvironment:
    try:
        return await WorkflowEnvironment.start_local(
            search_attributes=BELLLABS_SEARCH_ATTRIBUTE_KEYS, dev_server_log_level="error"
        )
    except RuntimeError as error:
        pytest.skip(f"Temporal dev server is unavailable: {error}")


def _bounded_request(request_id: str, bounded: dict[str, int]) -> RunRequest:
    base = run_request(request_id=request_id)
    dimensions = tuple(
        BudgetDimensionLimit(
            dimension=item.dimension,
            applicability=BudgetApplicability.BOUNDED,
            hard_cap=bounded[item.dimension],
        )
        if item.dimension in bounded
        else item
        for item in base.budget_envelope.dimensions
    )
    return base.model_copy(update={"budget_envelope": BudgetEnvelope(dimensions=dimensions)})


class Facade:
    """The governed API, with the demonstration's services composed on it."""

    def __init__(self, stack: ForkStack) -> None:
        self._stack = stack

    async def __aenter__(self) -> Facade:
        api.dependency_overrides[get_control_plane_principal] = lambda: PRINCIPAL
        api.dependency_overrides[get_run_control_service] = lambda: self._stack.run_control
        api.dependency_overrides[get_run_fork_services] = lambda: self._stack.forks
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://belllabs"
        )
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.client.aclose()
        for dependency in (
            get_control_plane_principal,
            get_run_control_service,
            get_run_fork_services,
            get_boundary_intervention_service,
        ):
            api.dependency_overrides.pop(dependency, None)

    def deliver_through(self, temporal: Client) -> None:
        """RRM-007: accepted boundary commands reach Temporal only as recorded deliveries."""

        run_control = self._stack.run_control
        interventions = BoundaryInterventionService(
            run_control,
            BoundaryCommandDeliveryService(run_control, TemporalBoundaryCommandTransport(temporal)),
        )
        api.dependency_overrides[get_boundary_intervention_service] = lambda: interventions

    async def release_wait(self, run_id: str, wait_id: str) -> None:
        """Release a declared StageGraph wait through the governed command route and
        await its `applied` receipt (a raw signal is no release path)."""

        condition_id = wait_condition_id(wait_id)
        await _until(lambda: _holds_wait(self._stack, run_id, condition_id))
        run = await self._stack.run_control.get_run(SCOPE, run_id)
        command_id = f"release:{run_id[:8]}:{wait_id}"
        command = LifecycleCommand(
            command_id=command_id,
            idempotency_issuer=PRINCIPAL.actor_id,
            request_scope=SCOPE,
            run_id=run_id,
            expected_run_version=run.version,
            actor=ActorContext(
                actor_id=PRINCIPAL.actor_id,
                permissions=frozenset({"workflow_run.observe_wait"}),
            ),
            action=SatisfyWaitAction(
                condition_id=condition_id, verification_evidence_ref="evidence:rrm006-review"
            ),
            reason="RRM-006 demonstration releases the review wait",
            occurred_at=datetime.now(UTC),
            correlation_id=f"rrm006:{run_id}",
        )
        response = await self.client.post(
            f"/run-control/v1/runs/{run_id}/commands", json=command.model_dump(mode="json")
        )
        assert response.status_code == 200, response.text
        assert response.json()["reason_code"] == "accepted_pending_application", response.text

        async def applied() -> bool:
            status = await self._stack.run_control.get_boundary_command(
                SCOPE, run_id, PRINCIPAL.actor_id, command_id
            )
            return status is not None and status.state.value == "applied"

        await _until(applied)

    async def admit(self, request: RunRequest) -> str:
        response = await self.client.post(
            "/run-control/v1/run-requests", json=request.model_dump(mode="json")
        )
        assert response.status_code == 201, response.text
        return cast(str, response.json()["run_id"])

    async def snapshot(self, run_id: str, *, until_safe: bool) -> dict[str, Any]:
        """Take the snapshot; while the run is not yet at a safe boundary, the facade
        rejects it with a typed reason and the caller retries."""

        rejections: list[str] = []
        last: dict[str, Any] = {}
        try:
            async with asyncio.timeout(120):
                return await self._snapshot_until(run_id, until_safe, rejections, last)
        except TimeoutError as error:
            raise AssertionError(f"no safe boundary: last rejection {last}") from error

    async def _snapshot_until(
        self,
        run_id: str,
        until_safe: bool,
        rejections: list[str],
        last: dict[str, Any],
    ) -> dict[str, Any]:
        if True:
            while True:
                response = await self.client.post(
                    f"/run-control/v1/runs/{run_id}/snapshots", json={"request_scope": SCOPE}
                )
                if response.status_code == 201:
                    snapshot = cast(dict[str, Any], response.json())
                    snapshot["_rejections_before"] = sorted(set(rejections))
                    return snapshot
                assert until_safe and response.status_code == 409, response.text
                last.update(response.json()["detail"])
                rejections.append(response.json()["detail"]["code"])
                await asyncio.sleep(0.25)

    async def fork(self, run_id: str, snapshot: dict[str, Any], **body: Any) -> dict[str, Any]:
        payload = {
            "request_scope": SCOPE,
            "request_id": f"fork-{run_id[:8]}",
            "idempotency_key": f"fork-{run_id[:8]}",
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot_digest": snapshot["snapshot_digest"],
            "baseline_reservations": {},
            "sponsorship_ref": "sponsorship:test",
            "approval_refs": ["approval:test"],
            "reason": "RRM-006 technical fork",
            **body,
        }
        response = await self.client.post(f"/run-control/v1/runs/{run_id}/forks", json=payload)
        assert response.status_code == 201, response.text
        replay = await self.client.post(f"/run-control/v1/runs/{run_id}/forks", json=payload)
        assert replay.status_code == 201 and replay.json() == response.json()
        view = await self.client.get(
            f"/run-control/v1/forks/{payload['request_id']}", params={"request_scope": SCOPE}
        )
        assert view.status_code == 200
        return cast(dict[str, Any], view.json())


async def _until(predicate: Callable[[], Awaitable[bool]], seconds: float = 120) -> None:
    async with asyncio.timeout(seconds):
        for _ in range(int(seconds * 4)):
            if await predicate():
                return
            await asyncio.sleep(0.25)
    raise AssertionError("condition not reached")


async def _holds_wait(stack: ForkStack, run_id: str, condition_id: str) -> bool:
    run = await stack.run_control.get_run(SCOPE, run_id)
    return any(item.condition_id == condition_id for item in run.active_waits)


async def _visible(client: Client, query: str, expected: int) -> None:
    """Visibility is eventually consistent: poll until the started execution is listed."""

    async with asyncio.timeout(30):
        for _ in range(120):
            if (await client.count_workflows(query)).count == expected:
                return
            await asyncio.sleep(0.25)
    raise AssertionError(f"Visibility did not list {expected} execution(s) for {query}")


async def _source_digest(pool: asyncpg.Pool, run_id: str) -> dict[str, str]:
    """A content digest of every authority row of one run (owner reads bypass RLS)."""

    queries = {
        "workflow_runs": "SELECT * FROM belllabs_control.workflow_runs WHERE run_id = $1",
        "budget_accounts": "SELECT * FROM belllabs_control.budget_accounts WHERE run_id = $1",
        "effect_ledgers": "SELECT * FROM belllabs_control.effect_ledgers WHERE run_id = $1",
        "family_admission_heads": (
            "SELECT * FROM belllabs_control.family_admission_heads WHERE run_id = $1"
        ),
        "runtime_units": (
            "SELECT * FROM belllabs_control.runtime_units WHERE belllabs_run_id = $1"
        ),
        "runtime_unit_result_observations": (
            "SELECT r.* FROM belllabs_control.runtime_unit_result_observations r"
            " JOIN belllabs_control.runtime_units u USING (request_scope, unit_key)"
            " WHERE u.belllabs_run_id = $1"
        ),
        "runtime_checkpoint_transitions": (
            "SELECT t.* FROM belllabs_control.runtime_checkpoint_transitions t"
            " JOIN belllabs_control.runtime_units u USING (request_scope, unit_key)"
            " WHERE u.belllabs_run_id = $1"
        ),
        "operation_effect_claims": (
            "SELECT * FROM belllabs_control.operation_effect_claims WHERE belllabs_run_id = $1"
        ),
        "runtime_cognitive_namespaces": (
            "SELECT n.* FROM belllabs_control.runtime_cognitive_namespaces n"
            " WHERE n.cognitive_namespace IN ("
            "  SELECT g.cognitive_namespace FROM belllabs_control.runtime_unit_generations g"
            "  JOIN belllabs_control.runtime_units u USING (request_scope, unit_key)"
            "  WHERE u.belllabs_run_id = $1)"
        ),
        "saver_checkpoints": (
            f"SELECT c.thread_id, c.checkpoint_id, c.metadata FROM {SAVER_SCHEMA}.checkpoints c"
            " WHERE c.thread_id IN ("
            "  SELECT g.cognitive_namespace FROM belllabs_control.runtime_unit_generations g"
            "  JOIN belllabs_control.runtime_units u USING (request_scope, unit_key)"
            "  WHERE u.belllabs_run_id = $1)"
        ),
    }
    async with pool.acquire() as connection:
        return {
            name: await connection.fetchval(
                f"SELECT md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), ''))"
                f" FROM ({query}) t",
                run_id,
            )
            for name, query in queries.items()
        }


def _model_calls(stack: ForkStack, run_id: str) -> dict[str, int]:
    calls: dict[str, int] = {}
    for item in stack.model_log:
        if item["run_id"] == run_id:
            calls[item["operation"]] = calls.get(item["operation"], 0) + 1
    return calls


async def _diagnose(client: Client, run_id: str) -> str:
    """Compact history tails of a run's root and family (diagnostics on failure only)."""

    lines: list[str] = []
    for workflow_id in (f"belllabs-run/{run_id}", f"family/{run_id}/1"):
        try:
            history = await client.get_workflow_handle(workflow_id).fetch_history()
        except Exception as error:  # noqa: BLE001 - diagnostics only
            lines.append(f"{workflow_id}: {type(error).__name__}")
            continue
        lines.append(
            f"{workflow_id}: " + ", ".join(str(event.event_type) for event in history.events[-8:])
        )
        lines.extend(
            str(event)[:600]
            for event in history.events
            if event.event_type in {3, 4, 9, 13, 14, 24, 26}
        )
        completed = [
            event.activity_task_completed_event_attributes.result.payloads[0].data[:1200]
            for event in history.events
            if event.event_type == 12
        ]
        lines.extend(item.decode("utf-8", "replace") for item in completed[-1:])
    return " | ".join(lines)


async def _replay(client: Client, workflow_ids: list[str]) -> int:
    replayer = Replayer(
        workflows=[
            BellLabsRunWorkflow,
            StageGraphWorkflow,
            GoalDirectedWorkflow,
            OperationWorkflow,
        ],
        workflow_runner=coordinator_workflow_runner(),
    )
    events = 0
    for workflow_id in workflow_ids:
        history = await client.get_workflow_handle(workflow_id).fetch_history()
        await replayer.replay_workflow(history)
        events += len(history.events)
    return events


# --- StageGraph ----------------------------------------------------------------------------


def _exact(kind: DefinitionKind, logical_id: str) -> ExactDefinitionRef:
    return ExactDefinitionRef(kind=kind, logical_id=logical_id, revision=1, digest=DIGEST)


def _stage_slot() -> StageOperationSlot:
    return StageOperationSlot(
        operation_slot_id="execute",
        reservation={"tokens.total": 10},
        allowed_variants=(
            AllowedOperationVariant(
                operation_variant_id="default",
                operation_contract_ref="operation:rrm006-technical@1",
            ),
        ),
    )


def _stage_blueprint() -> StageGraphBlueprint:
    dependency = StageDependency(
        dependency_id="draft-to-review",
        consumer_stage_id="review",
        join_id="draft-ready",
        producer_stage_id="draft",
        producer_output_slot_id="result",
        consumer_input_slot_id="draft-input",
        dependency_class="required",
    )
    return StageGraphBlueprint(
        logical_id="rrm006-technical-stagegraph",
        title="RRM-006 technical StageGraph",
        description="Draft, a declared wait, then review; technical inputs only.",
        stages=(
            StageNode(
                stage_id="draft",
                output_slots=(
                    StageOutputSlot(output_slot_id="result", output_contract_ref="output:draft@1"),
                ),
                operation_slots=(_stage_slot(),),
            ),
            StageNode(
                stage_id="review",
                input_slots=(StageInputSlot(input_slot_id="draft-input"),),
                output_slots=(
                    StageOutputSlot(output_slot_id="result", output_contract_ref="output:review@1"),
                ),
                operation_slots=(_stage_slot(),),
            ),
        ),
        joins=(
            StageJoin(
                consumer_stage_id="review",
                join_id="draft-ready",
                kind="all",
                dependency_ids=(dependency.dependency_id,),
                slow_sibling_policy=SlowSiblingPolicy(
                    triggers=("join_released",),
                    execution_action="continue",
                    arrival_route="evaluate_late_result",
                ),
            ),
        ),
        dependencies=(dependency,),
        late_result_policy=LateResultPolicy(
            rules=(
                LateResultRule(
                    rule_id="admit-late",
                    trigger="consumer_already_admitted",
                    decision="admit",
                ),
            )
        ),
        waits=(StageGraphWait(scope_kind="stage", scope_id="review", wait_id=WAIT_ID),),
    )


class StageTemplates:
    """Templates per semantic input binding; a fork binding applies its stage objectives."""

    def __init__(self, stack: ForkStack) -> None:
        self._stack = stack
        self.objectives: dict[str, dict[str, str]] = {}

    async def get_template(
        self,
        *,
        semantic_input_binding_ref: str,
        operation_request_key: str,
        request_scope: str,
        run_id: str,
    ) -> OperationExecutionRequest:
        del request_scope, run_id
        stage = operation_request_key.split("/", 1)[0]
        base = operation_request(prompt=f"RRM-006 technical stage {stage}.")
        workspace = governed_workspace(self._stack.binding.workspace)
        segments = base.prompt_segments
        objective = self.objectives.get(semantic_input_binding_ref, {}).get(stage)
        if objective is not None:
            segments = (
                *segments,
                PromptSegment(
                    source_ref=f"fork-objective:{stage}",
                    source_revision=1,
                    trust_class="admitted_input",
                    content=objective,
                    rendered_digest=sha256_digest(objective),
                ),
            )
        return OperationExecutionRequest.model_validate(
            {
                **base.model_dump(mode="python"),
                "prompt_segments": segments,
                "workspace": workspace,
                "execution_runtime": "deep_agent",
                "native_placement": None,
                "deep_agent_binding": DeepAgentExecutionBinding.create(
                    **{
                        **self._stack.binding.model_dump(mode="python", exclude={"binding_digest"}),
                        "workspace": workspace,
                    }
                ),
            }
        )


def _stage_input(run_id: str, binding_ref: str, version: int) -> StageGraphRunInput:
    graph = _stage_blueprint()
    return StageGraphRunInput(
        run_id=run_id,
        request_scope=SCOPE,
        effective_configuration_digest=DIGEST,
        workflow_type_digest=WORKFLOW_DIGEST,
        blueprint_digest=sha256_digest(graph),
        blueprint=graph.model_dump(mode="json"),
        initial_run_version=version,
        max_concurrency=1,
        task_timeout_seconds=120,
        semantic_input_binding_ref=binding_ref,
        correlation_id=f"rrm006:{run_id}",
    )


@pytest.mark.asyncio
async def test_stagegraph_fork_reuses_the_settled_stage_and_reruns_the_patched_stage(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,
    tmp_path: Path,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    await _reset(test_application_postgres_dsn)
    families = FamilyAdmissionRegistry()
    register_stagegraph_family_mutations(families)
    policies = ForkPatchPolicyRegistry()
    policies.register(
        WORKFLOW_DIGEST,
        ForkPatchPolicy(
            policy_id="fork-patch-policy:rrm006-technical-stagegraph",
            family="stage_graph",
            patchable=(
                PatchablePath(path=stage_objective_path("review"), invalidates=("review",)),
            ),
        ),
    )
    async with (
        open_fork_stack(
            test_application_postgres_dsn,
            tmp_path,
            mongo_uri=test_mongodb_uri,
            mongo_database=mongo_database,
            families=families,
            policies=policies,
        ) as stack,
        Facade(stack) as facade,
    ):
        templates = StageTemplates(stack)
        source_run = await facade.admit(
            _bounded_request(
                "rrm006-stagegraph-source",
                {"tokens.total": 100, "operation.attempts": 6, "concurrency.slots": 2},
            )
        )
        stage_activities = StageGraphActivities(
            decision_service=StageGraphDecisionService(stack.run_control, stack.repository),
            operation_materializer=StageGraphOperationPreparationService(
                templates=templates, operation_bindings=stack.bindings
            ),
            lifecycle_gateway=cast(Any, None),
            boundary=BoundaryCommandApplicationService(
                stack.run_control, orchestration_lifecycle_actor()
            ),
        )
        env = await _start_local()
        async with env:
            client = env.client
            submitter = TemporalWorkflowSubmitter.for_production(
                client,
                stagegraph_task_queue=STAGE_QUEUE,
                goal_directed_task_queue=GOAL_QUEUE,
                search_attribute_policy="required",
            )
            activities = OperationExecutionActivities(
                stack.service, worker_identity=f"rrm006-worker:{os.getpid()}"
            )
            async with (
                Worker(
                    client,
                    task_queue=STAGE_QUEUE,
                    workflows=[BellLabsRunWorkflow, StageGraphWorkflow, OperationWorkflow],
                    workflow_runner=coordinator_workflow_runner(),
                    activities=[
                        stage_activities.initialize,
                        stage_activities.admit_operation,
                        stage_activities.decide_result,
                        stage_activities.apply_cycle,
                        stage_activities.complete_stagegraph,
                        stage_activities.apply_boundary_command,
                    ],
                ),
                Worker(
                    client, task_queue=stack.binding.task_queue, activities=[activities.execute]
                ),
            ):
                await submitter.submit(
                    _stage_input(source_run, "semantic-input:rrm006:source", 1),
                    workflow_id="ignored",
                    blueprint_family=BlueprintFamily.STAGE_GRAPH,
                )
                facade.deliver_through(client)
                # The source waits on `release-review` after `draft` settles: a safe boundary.
                # The wait is declared to run control (RRM-007) before the snapshot is taken,
                # so the snapshot's run version is the one the fork admits against.
                await _until(lambda: _holds_wait(stack, source_run, wait_condition_id(WAIT_ID)))
                try:
                    snapshot = await facade.snapshot(source_run, until_safe=True)
                except AssertionError as error:
                    raise AssertionError(
                        f"{error}; {await _diagnose(client, source_run)}"
                    ) from error
                assert snapshot["boundary_kind"] == "stage_settled"
                assert snapshot["run_phase"] == "waiting"
                assert snapshot["pending_commands"] == []
                assert snapshot["family_position"]["accepted_stage_ids"] == ["draft"]
                [candidate] = snapshot["reuse_candidates"]
                assert candidate["unit"]["location"]["stage_id"] == "draft"
                source_before = await _source_digest(stack.pool, source_run)
                source_draft_manifest = (
                    (tmp_path / "results")
                    .joinpath(candidate["result_manifest_ref"].removeprefix("file-artifacts://"))
                    .read_bytes()
                )

                fork = await facade.fork(
                    source_run,
                    snapshot,
                    changes=[{"path": stage_objective_path("review"), "value": STAGE_OBJECTIVE}],
                    invalidation_frontier=["review"],
                )
                receipt = fork["receipt"]
                derived_run = receipt["target_run_id"]
                # The fork changed nothing of the source (authority, units, checkpoints).
                assert await _source_digest(stack.pool, source_run) == source_before
                derived = await stack.run_control.get_run(SCOPE, derived_run)
                assert (derived.phase, derived.version) == (RunPhase.PENDING, 1)

                # The governed launch applies the patch to the derived run's semantic input.
                request = await stack.forks.receipts.get_request(SCOPE, receipt["request_id"])
                assert request is not None
                derived_binding = f"semantic-input:fork:{receipt['request_id']}"
                templates.objectives[derived_binding] = {
                    change.path.removeprefix("stage_objectives."): str(change.value)
                    for change in request.patch.changes
                }
                await submitter.submit(
                    _stage_input(derived_run, derived_binding, derived.version),
                    workflow_id="ignored",
                    blueprint_family=BlueprintFamily.STAGE_GRAPH,
                    parent_run_id=source_run,
                )
                await _visible(client, f"BellLabsParentRunId = '{source_run}'", 1)
                await _await_wait(stack, derived_run)
                await facade.release_wait(derived_run, WAIT_ID)
                await asyncio.wait_for(
                    client.get_workflow_handle(f"belllabs-run/{derived_run}").result(), 240
                )
                # Only now does the source continue past its wait.
                await facade.release_wait(source_run, WAIT_ID)
                await asyncio.wait_for(
                    client.get_workflow_handle(f"belllabs-run/{source_run}").result(), 240
                )
                replayed = await _replay(
                    client,
                    [
                        f"belllabs-run/{derived_run}",
                        f"family/{derived_run}/1",
                        f"family/{source_run}/1",
                    ],
                )
        evidence = await _stagegraph_evidence(
            stack, source_run, derived_run, snapshot, fork, source_draft_manifest, tmp_path
        )
    evidence["replayed_events"] = replayed
    print("RRM-006 EVIDENCE stagegraph:", json.dumps(evidence, sort_keys=True))


async def _await_wait(stack: ForkStack, run_id: str) -> None:
    """The derived run reaches its declared wait after `draft` (reused) is decided."""

    async with asyncio.timeout(120):
        while True:
            head = await stack.repository.get_family_head(
                SCOPE, run_id, "stagegraph", StageGraphDecisionMutation
            )
            if head is not None and head.decision_kind == "result_decided":
                return
            await asyncio.sleep(0.25)


async def _stagegraph_evidence(
    stack: ForkStack,
    source_run: str,
    derived_run: str,
    snapshot: dict[str, Any],
    fork: dict[str, Any],
    source_draft_manifest: bytes,
    root: Path,
) -> dict[str, Any]:
    source = await stack.run_control.get_run(SCOPE, source_run)
    derived = await stack.run_control.get_run(SCOPE, derived_run)
    assert (source.phase, source.terminal_outcome) == (RunPhase.TERMINAL, RunOutcome.COMPLETED)
    assert (derived.phase, derived.terminal_outcome) == (RunPhase.TERMINAL, RunOutcome.COMPLETED)
    source_outputs = sorted(item.output_ref for item in source.accepted_output_evidence)
    derived_outputs = sorted(item.output_ref for item in derived.accepted_output_evidence)
    by_stage = {
        run: {ref.split(":")[2]: ref for ref in outputs}
        for run, outputs in ((source_run, source_outputs), (derived_run, derived_outputs))
    }
    # The reused draft keeps its immutable artifact; the patched review is a new artifact.
    assert by_stage[derived_run]["draft"] == by_stage[source_run]["draft"]
    assert by_stage[derived_run]["review"] != by_stage[source_run]["review"]

    source_calls = _model_calls(stack, source_run)
    derived_calls = _model_calls(stack, derived_run)
    assert sorted(source_calls.values()) == [2, 2]
    assert len(derived_calls) == 1 and list(derived_calls.values()) == [2]
    assert ":stage:review:" in next(iter(derived_calls))

    reuse = {item["decision"]: item for item in fork["reuse_decisions"]}
    assert set(reuse) == {"reuse"}
    reused_key = reuse["reuse"]["derived_unit_key"]
    async with stack.pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT u.unit_key, u.belllabs_run_id, r.result_manifest_ref,
                   r.checkpoint_transition_id
            FROM belllabs_control.runtime_unit_result_observations r
            JOIN belllabs_control.runtime_units u USING (request_scope, unit_key)
            WHERE u.belllabs_run_id = ANY($1::text[])
            """,
            [source_run, derived_run],
        )
    manifests = {row["unit_key"]: row for row in rows}
    assert len(manifests) == 4
    reused_row = manifests[reused_key]
    assert reused_row["checkpoint_transition_id"] is None
    reused_manifest = json.loads(
        (root / "results")
        .joinpath(reused_row["result_manifest_ref"].removeprefix("file-artifacts://"))
        .read_bytes()
    )
    candidate = snapshot["reuse_candidates"][0]
    assert (
        reused_manifest["reused_result"]["source_result_manifest_ref"]
        == (candidate["result_manifest_ref"])
    )
    # Every derived result manifest is distinct from every source manifest.
    source_refs = {
        row["result_manifest_ref"] for row in rows if row["belllabs_run_id"] == source_run
    }
    derived_refs = {
        row["result_manifest_ref"] for row in rows if row["belllabs_run_id"] == derived_run
    }
    assert not source_refs & derived_refs
    # The source's own result artifact bytes are unchanged.
    assert (root / "results").joinpath(
        candidate["result_manifest_ref"].removeprefix("file-artifacts://")
    ).read_bytes() == source_draft_manifest
    receipt = fork["receipt"]
    return {
        "source_run": source_run,
        "derived_run": derived_run,
        "snapshot": {
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot_digest": snapshot["snapshot_digest"],
            "boundary_kind": snapshot["boundary_kind"],
            "boundary_ref": snapshot["boundary_ref"],
            "projection_version": snapshot["projection_version"],
            "run_phase": snapshot["run_phase"],
            "family_version": snapshot["family_position"]["family_version"],
            "accepted_stage_ids": snapshot["family_position"]["accepted_stage_ids"],
            "reuse_candidates": [item["unit_key"] for item in snapshot["reuse_candidates"]],
            "budget_reservations": snapshot["budget_frontier"]["reservation_ids"],
            "effects_settled": all(item["settled"] for item in snapshot["effect_frontier"]),
            "async_children": snapshot["async_children"],
            "rejections_before_safe": snapshot["_rejections_before"],
        },
        "patch": {
            "patch_digest": receipt["patch_digest"],
            "changes": [stage_objective_path("review")],
            "invalidation_frontier": ["review"],
        },
        "reuse": [
            {
                "decision": item["decision"],
                "reason": item["reason"],
                "source_unit_key": item["source_unit_key"],
                "derived_unit_key": item["derived_unit_key"],
            }
            for item in fork["reuse_decisions"]
        ],
        "lineage": {
            "lineage_digest": receipt["lineage"]["lineage_digest"],
            "derived_execution_epoch": receipt["lineage"]["derived_execution_epoch"],
            "admission_ref": receipt["admission_ref"],
            "reused_unit_keys": receipt["lineage"]["reused_unit_keys"],
            "seed_checkpoint": receipt["lineage"]["seed_checkpoint"],
        },
        "model_calls": {"source": sorted(source_calls.values()), "derived": derived_calls},
        "outputs": {"source": source_outputs, "derived": derived_outputs},
        "terminal": [source.terminal_outcome, derived.terminal_outcome],
        "reused_manifest_names_source": reused_manifest["reused_result"]["source_unit_key"],
    }


# --- GoalDirected --------------------------------------------------------------------------


def _goal_blueprint() -> GoalDirectedBlueprint:
    return GoalDirectedBlueprint.model_validate(
        {
            **GENERIC_GOAL_DIRECTED.model_dump(mode="python"),
            "max_iterations": 1,
            "authority_ceiling": {
                **GENERIC_GOAL_DIRECTED.authority_ceiling.model_dump(mode="python"),
                "budgets": {
                    "dimensions": {"goal.iterations": 1, "tokens.total": 10, "model.turns": 4}
                },
            },
            "iteration_reservation": {"goal.iterations": 1, "tokens.total": 10, "model.turns": 4},
        }
    )


def _goal_revision(run_id: str, objective: str) -> GoalRevision:
    envelope = sha256_digest({"rrm006": "goal-envelope"})
    values = {
        "schema_version": "belllabs.goal-revision.v1",
        "revision_id": sha256_digest({"run_id": run_id, "objective": objective}),
        "revision": 1,
        "parent_revision_id": None,
        "envelope_digest": envelope,
        "objective": objective,
        "tactical_changes": (),
        "evidence_refs": ("input:rrm006-technical",),
        "unmet_obligations": (OBLIGATION,),
        "proposer": "application:qualification",
        "deciding_authority": "authority:qualification",
        "applicability": "remaining_run",
        "tactics": (),
        "subgoals": (),
        "coverage_emphasis": (),
    }
    return GoalRevision(canonical_digest=sha256_digest(values), **values)  # type: ignore[arg-type]


def _goal_input(run_id: str, objective: str, version: int) -> GoalDirectedRunInput:
    blueprint = _goal_blueprint()
    revision = _goal_revision(run_id, objective)
    return GoalDirectedRunInput(
        run_id=run_id,
        request_scope=SCOPE,
        effective_configuration_digest=DIGEST,
        blueprint_digest=sha256_digest(blueprint),
        blueprint=blueprint.model_dump(mode="json"),
        envelope_digest=revision.envelope_digest,
        initial_revision=revision,
        initial_run_version=version,
        task_timeout_seconds=120,
        required_obligation_refs=(OBLIGATION,),
        required_output_contract_refs=("fixture-output",),
        semantic_input_binding_ref=f"semantic-input:rrm006:{run_id}",
    )


class ExactBindingVerifier:
    async def verify(self, configuration_digest: str, blueprint_digest: str) -> None:
        if configuration_digest != DIGEST or blueprint_digest != sha256_digest(_goal_blueprint()):
            raise ValueError("technical lifecycle binding drifted")


def _goal_templates(stack: ForkStack) -> Templates:
    values: dict[str, OperationExecutionRequest] = {}
    for role in ("executor", "verifier"):
        base = operation_request(prompt=f"RRM-006 technical GoalDirected {role}.")
        # RRM-016: the compiled workspace slot, bound by each role under its own root, so
        # the real run-control authority admits the operation.
        workspace = governed_workspace(base.workspace)
        values[role] = OperationExecutionRequest.model_validate(
            {
                **base.model_dump(mode="python"),
                "execution_runtime": "deep_agent",
                "native_placement": None,
                "deep_agent_binding": DeepAgentExecutionBinding.create(
                    **{
                        **stack.binding.model_dump(mode="python", exclude={"binding_digest"}),
                        "workspace": workspace,
                    }
                ),
                "workspace": workspace,
                "output_schema": StructuredOutputBinding(
                    schema_id=f"goal-{role}-output",
                    revision=1,
                    schema_digest=sha256_digest(f"goal-{role}-output-schema"),
                ),
            }
        )
    return Templates(values)


@pytest.mark.asyncio
async def test_goal_directed_fork_starts_fresh_with_the_patched_goal(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,
    tmp_path: Path,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    await _reset(test_application_postgres_dsn)
    families = FamilyAdmissionRegistry()
    configure_goal_directed_family_admissions(families)
    bounded = {
        "tokens.total": 100,
        "model.turns": 20,
        "goal.iterations": 4,
        "operation.attempts": 8,
        "concurrency.slots": 1,
    }
    async with (
        open_fork_stack(
            test_application_postgres_dsn,
            tmp_path,
            mongo_uri=test_mongodb_uri,
            mongo_database=mongo_database,
            families=families,
            policies=ForkPatchPolicyRegistry(),
            required_obligations=frozenset({OBLIGATION}),
            # RRM-016: GoalDirected operations are journaled and settled in run control.
            journaled=True,
        ) as stack,
        Facade(stack) as facade,
    ):
        documents = Documents()
        goal_activities = GoalDirectedActivities(
            operations=GoalDirectedOperationPreparationService(
                templates=_goal_templates(stack),
                operation_bindings=stack.bindings,
                run_control=stack.run_control,
                documents=documents,
                actor=ActorContext(
                    actor_id="rrm006-goal-worker",
                    permissions=frozenset(
                        {"workflow_run.goal_directed", "workflow_run.reserve_budget"}
                    ),
                    authority_refs=frozenset({"authority:rrm006-goal-worker"}),
                ),
            ),
            results=GoalDirectedOperationResultService(
                documents, RunControlGoalOperationSettlements(stack.run_control)
            ),
            lifecycle=RunControlLifecycleGateway(
                stack.run_control, ExactBindingVerifier(), orchestration_lifecycle_actor()
            ),
        )
        source_run = await facade.admit(_bounded_request("rrm006-goal-source", bounded))
        env = await _start_local()
        async with env:
            client = env.client
            submitter = TemporalWorkflowSubmitter.for_production(
                client,
                stagegraph_task_queue=STAGE_QUEUE,
                goal_directed_task_queue=GOAL_QUEUE,
                search_attribute_policy="required",
            )
            activities = OperationExecutionActivities(
                stack.service, worker_identity=f"rrm006-worker:{os.getpid()}"
            )
            async with (
                Worker(
                    client,
                    task_queue=GOAL_QUEUE,
                    workflows=[BellLabsRunWorkflow, GoalDirectedWorkflow, OperationWorkflow],
                    workflow_runner=coordinator_workflow_runner(),
                    activities=[
                        goal_activities.execute_iteration,
                        goal_activities.verify_iteration,
                        goal_activities.prepare_handoff,
                        goal_activities.apply_lifecycle_command,
                    ],
                ),
                Worker(
                    client, task_queue=stack.binding.task_queue, activities=[activities.execute]
                ),
            ):
                await submitter.submit(
                    _goal_input(source_run, GOAL_OBJECTIVE, 1),
                    workflow_id="ignored",
                    blueprint_family=BlueprintFamily.GOAL_DIRECTED,
                )
                await asyncio.wait_for(
                    client.get_workflow_handle(f"belllabs-run/{source_run}").result(), 240
                )
                snapshot = await facade.snapshot(source_run, until_safe=False)
                assert snapshot["boundary_kind"] == "goal_verifier_settled"
                assert snapshot["family_position"]["head_operation_role"] == "verifier"
                source_before = await _source_digest(stack.pool, source_run)
                fork = await facade.fork(
                    source_run,
                    snapshot,
                    changes=[{"path": "goal.objective", "value": PATCHED_GOAL_OBJECTIVE}],
                    invalidation_frontier=["*"],
                )
                receipt = fork["receipt"]
                derived_run = receipt["target_run_id"]
                assert await _source_digest(stack.pool, source_run) == source_before
                request = await stack.forks.receipts.get_request(SCOPE, receipt["request_id"])
                assert request is not None
                patched = request.patch.change("goal.objective")
                assert patched is not None
                derived = await stack.run_control.get_run(SCOPE, derived_run)
                await submitter.submit(
                    _goal_input(derived_run, str(patched.value), derived.version),
                    workflow_id="ignored",
                    blueprint_family=BlueprintFamily.GOAL_DIRECTED,
                    parent_run_id=source_run,
                )
                await _visible(client, f"BellLabsParentRunId = '{source_run}'", 1)
                await asyncio.wait_for(
                    client.get_workflow_handle(f"belllabs-run/{derived_run}").result(), 240
                )
                replayed = await _replay(
                    client,
                    [
                        f"belllabs-run/{source_run}",
                        f"family/{source_run}/1",
                        f"belllabs-run/{derived_run}",
                        f"family/{derived_run}/1",
                    ],
                )
        source = await stack.run_control.get_run(SCOPE, source_run)
        derived = await stack.run_control.get_run(SCOPE, derived_run)
        assert (source.phase, source.terminal_outcome) == (RunPhase.TERMINAL, RunOutcome.COMPLETED)
        assert (derived.phase, derived.terminal_outcome) == (
            RunPhase.TERMINAL,
            RunOutcome.COMPLETED,
        )
        source_outputs = sorted(item.output_ref for item in source.accepted_output_evidence)
        derived_outputs = sorted(item.output_ref for item in derived.accepted_output_evidence)
        assert source_outputs and derived_outputs
        assert not set(source_outputs) & set(derived_outputs)
        assert sorted(_model_calls(stack, source_run).values()) == [2, 2]
        assert sorted(_model_calls(stack, derived_run).values()) == [2, 2]
        assert {item["decision"] for item in fork["reuse_decisions"]} <= {
            "excluded",
            "not_reusable",
            "invalidated",
        }
        # RRM-016: the source's GoalDirected units now carry accepted run-control
        # settlements, so none is excluded as `not_accepted`; they are still never reused.
        assert not any(
            item["reason"] == "not_accepted" for item in snapshot["excluded_units"]
        ), snapshot["excluded_units"]
        assert receipt["lineage"]["reused_unit_keys"] == []
        assert len(snapshot["reuse_candidates"]) == 2
        assert {(item["decision"], item["reason"]) for item in fork["reuse_decisions"]} == {
            ("not_reusable", "goal_revision_identity_is_run_bound")
        }
        accepted = {
            item.settlement_id for item in source.accepted_operation_settlement_evidence
        }
        assert len(accepted) == 2
        evidence = {
            "source_run": source_run,
            "derived_run": derived_run,
            "snapshot": {
                "snapshot_id": snapshot["snapshot_id"],
                "snapshot_digest": snapshot["snapshot_digest"],
                "boundary_kind": snapshot["boundary_kind"],
                "run_phase": snapshot["run_phase"],
                "projection_version": snapshot["projection_version"],
                "goal_iteration": snapshot["family_position"]["goal_iteration"],
                "head_operation_role": snapshot["family_position"]["head_operation_role"],
                "reuse_candidates": len(snapshot["reuse_candidates"]),
                "excluded_units": [
                    [item["unit"]["location"]["operation_role"], item["reason"]]
                    for item in snapshot["excluded_units"]
                ],
            },
            "patch": {
                "patch_digest": receipt["patch_digest"],
                "changes": ["goal.objective"],
                "invalidation_frontier": ["*"],
            },
            "reuse": [[item["decision"], item["reason"]] for item in fork["reuse_decisions"]],
            "lineage": {
                "lineage_digest": receipt["lineage"]["lineage_digest"],
                "derived_execution_epoch": receipt["lineage"]["derived_execution_epoch"],
                "reused_unit_keys": receipt["lineage"]["reused_unit_keys"],
            },
            "outputs": {"source": source_outputs, "derived": derived_outputs},
            "replayed_events": replayed,
        }
    print("RRM-006 EVIDENCE goal_directed:", json.dumps(evidence, sort_keys=True))
