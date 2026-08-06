from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.application.graph_runtime_dispatch import (
    ExactRuntimeSelector,
    GraphRuntimeDispatchService,
)
from app.application.operation_execution import bind_operation_execution_request
from app.application.runtime_execution_bindings import (
    InMemoryRuntimeCoordinationRepository,
)
from app.application.runtime_interventions import (
    RuntimeInterventionAuthorization,
    RuntimeInterventionService,
)
from app.application.runtime_neutral_operations import RuntimeNeutralOperationDispatcher
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.graph_runtime.contracts import (
    ActorRef,
    CancelRunIntervention,
    Correlation,
    GraphExecutionReceipt,
    GraphExecutionSubmission,
    RuntimeExecutionStatus,
)
from app.domain.graph_runtime.definitions import (
    ContentAddressedRef,
    GraphAssemblySpec,
    RunPlan,
    RuntimeDefinitionKind,
)
from app.domain.graph_runtime.identities import ExecutionEpochKey
from app.domain.operation_execution.contracts import (
    MaterializedWorkspace,
    RuntimeInvocation,
    RuntimeResult,
)
from tests.test_operation_execution import operation_request

NOW = datetime(2026, 8, 5, 20, 0, tzinfo=UTC)
DIGEST = "sha256:" + "a" * 64


def runtime_ref(
    kind: RuntimeDefinitionKind,
    name: str,
    digest: str = DIGEST,
) -> ContentAddressedRef:
    return ContentAddressedRef(
        kind=kind,
        logical_id=name,
        schema_version="1",
        digest=digest,
    )


def implementation_ref() -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=DefinitionKind.WORKFLOW_IMPLEMENTATION,
        logical_id="workflow.stagegraph.langgraph",
        revision=1,
        digest=DIGEST,
    )


def run_plan() -> RunPlan:
    graph = GraphAssemblySpec(
        graph_assembly_ref=runtime_ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "assembly"),
        state_schema_digest=DIGEST,
        reducer_registry_digest=DIGEST,
        operation_registry_digest=DIGEST,
        stage_implementations=(),
        compatibility_manifest_digest=DIGEST,
    )
    values = {
        "plan_id": "plan-1",
        "effective_run_configuration_digest": DIGEST,
        "semantic_binding_ref": "semantic-binding-1",
        "workflow_implementation_ref": implementation_ref(),
        "graph_assembly": graph,
        "harness_ref": runtime_ref(RuntimeDefinitionKind.AGENT_HARNESS, "harness"),
        "delegation_policy_ref": runtime_ref(
            RuntimeDefinitionKind.DELEGATION_POLICY, "delegation"
        ),
        "context_assembly_ref": runtime_ref(
            RuntimeDefinitionKind.CONTEXT_ASSEMBLY, "context"
        ),
        "execution_environment_ref": runtime_ref(
            RuntimeDefinitionKind.EXECUTION_ENVIRONMENT, "environment"
        ),
        "capability_manifest_ref": runtime_ref(
            RuntimeDefinitionKind.CAPABILITY_MANIFEST, "capabilities"
        ),
        "evaluation_profile_ref": runtime_ref(
            RuntimeDefinitionKind.EVALUATION_PROFILE, "evaluation"
        ),
        "alias_evidence_digest": sha256_digest([]),
    }
    return RunPlan.create(**values)


def submission(plan: RunPlan) -> GraphExecutionSubmission:
    epoch = ExecutionEpochKey(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        execution_epoch=1,
    )
    values = {
        "submission_id": "submission-1",
        "idempotency_key": "submission-1",
        "epoch": epoch,
        "expected_belllabs_version": 2,
        "run_plan_ref": runtime_ref(
            RuntimeDefinitionKind.RUN_PLAN,
            "run-plan",
            plan.plan_digest,
        ),
        "run_plan_digest": plan.plan_digest,
        "graph_assembly_digest": plan.graph_assembly.graph_assembly_ref.digest,
        "state_schema_digest": plan.graph_assembly.state_schema_digest,
        "input_manifest_ref": "input-manifest-1",
        "actor": ActorRef(
            actor_id="runtime-dispatch",
            actor_type="service",
            authority_ref="authority:runtime-dispatch@1",
        ),
        "correlation": Correlation(correlation_id="correlation-1"),
        "submitted_at": NOW,
    }
    request_digest = sha256_digest(values)
    return GraphExecutionSubmission(**values, request_digest=request_digest)


def cancel_intervention(epoch: ExecutionEpochKey) -> CancelRunIntervention:
    values = {
        "kind": "cancel_run",
        "command_id": "cancel-command-1",
        "idempotency_key": "cancel-idempotency-1",
        "epoch": epoch,
        "expected_belllabs_version": 2,
        "expected_checkpoint": None,
        "actor": ActorRef(
            actor_id="operator-1",
            actor_type="operator",
            authority_ref="authority:operator@1",
        ),
        "reason": "operator cancellation",
        "correlation": Correlation(correlation_id="cancel-correlation"),
        "requested_at": NOW,
        "cancellation_mode": "graceful",
    }
    return CancelRunIntervention(
        **values,
        request_digest=sha256_digest(values),
    )


class DeterministicLegacyClient:
    runtime_provider = "legacy_temporal"

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls = 0

    def deployment_for(self, _submission):  # type: ignore[no-untyped-def]
        return None

    def thread_for(self, _submission):  # type: ignore[no-untyped-def]
        return None

    async def submit(self, request, binding):  # type: ignore[no-untyped-def]
        self.calls += 1
        if self.fail:
            raise TimeoutError("acceptance is ambiguous")
        return GraphExecutionReceipt(
            submission_id=request.submission_id,
            request_digest=request.request_digest,
            epoch=request.epoch,
            status="accepted",
            binding_id=binding.binding_id,
            accepted_at=NOW,
        )

    async def reconcile_submission(self, request, binding):  # type: ignore[no-untyped-def]
        return GraphExecutionReceipt(
            submission_id=request.submission_id,
            request_digest=request.request_digest,
            epoch=request.epoch,
            status="accepted",
            binding_id=binding.binding_id,
            accepted_at=NOW,
        )


@pytest.mark.asyncio
async def test_dispatch_freezes_binding_before_effect_and_replay_does_not_resubmit() -> None:
    plan = run_plan()
    request = submission(plan)
    repository = InMemoryRuntimeCoordinationRepository()
    client = DeterministicLegacyClient()
    selector = ExactRuntimeSelector(
        {
            (
                implementation_ref().logical_id,
                implementation_ref().revision,
                implementation_ref().digest,
            ): client
        }
    )
    service = GraphRuntimeDispatchService(repository=repository, selector=selector)

    first = await service.submit(request, plan)
    second = await service.submit(request, plan)

    assert first.binding_id == second.binding_id
    assert second.status == "existing"
    assert client.calls == 1
    binding = await repository.get_binding(request.epoch)
    assert binding is not None
    assert binding.status == RuntimeExecutionStatus.ACCEPTED
    projection = await repository.projection(request.epoch)
    assert projection is not None
    assert projection.attempts[-1].disposition.value == "accepted"


@pytest.mark.asyncio
async def test_ambiguous_launch_enters_reconciliation_and_is_not_blindly_retried() -> None:
    plan = run_plan()
    request = submission(plan)
    repository = InMemoryRuntimeCoordinationRepository()
    client = DeterministicLegacyClient(fail=True)
    service = GraphRuntimeDispatchService(
        repository=repository,
        selector=ExactRuntimeSelector(
            {
                (
                    implementation_ref().logical_id,
                    implementation_ref().revision,
                    implementation_ref().digest,
                ): client
            }
        ),
    )

    with pytest.raises(TimeoutError, match="ambiguous"):
        await service.submit(request, plan)
    replay = await service.submit(request, plan)

    assert replay.status == "reconciliation_required"
    assert client.calls == 1
    client.fail = False
    reconciled = await service.reconcile(request, plan)
    assert reconciled is not None
    assert reconciled.status == "accepted"
    binding = await repository.get_binding(request.epoch)
    assert binding is not None
    assert binding.status == RuntimeExecutionStatus.ACCEPTED
    projection = await repository.projection(request.epoch)
    assert projection is not None
    assert [attempt.disposition.value for attempt in projection.attempts] == [
        "ambiguous",
        "accepted",
    ]


@pytest.mark.asyncio
async def test_same_frozen_operation_executes_through_legacy_and_graph_stub() -> None:
    request = operation_request()
    binding = bind_operation_execution_request(request)
    invocation = RuntimeInvocation(
        binding=binding,
        prompt_segments=request.prompt_segments,
        workspace=MaterializedWorkspace(
            workspace_id=request.workspace.workspace_id,
            provider="fixture",
            runtime_digest=request.workspace.runtime_digest,
            image_digest=request.workspace.image_digest,
            mount_manifest_digest=DIGEST,
        ),
    )

    class NoOpAdapter:
        def __init__(self) -> None:
            self.binding_digests: list[str] = []

        async def execute(self, runtime_invocation, _secrets):  # type: ignore[no-untyped-def]
            self.binding_digests.append(sha256_digest(runtime_invocation.binding))
            return RuntimeResult(
                output_text="runtime-neutral-ok",
                output_refs=("artifact:runtime-neutral",),
            )

    legacy = NoOpAdapter()
    graph = NoOpAdapter()
    dispatcher = RuntimeNeutralOperationDispatcher(
        {
            "legacy_temporal": legacy,
            "langgraph_agent_server": graph,
        }
    )

    legacy_result = await dispatcher.execute("legacy_temporal", invocation, {})
    graph_result = await dispatcher.execute("langgraph_agent_server", invocation, {})

    assert legacy_result == graph_result
    assert legacy.binding_digests == graph.binding_digests == [sha256_digest(binding)]


@pytest.mark.asyncio
async def test_concurrent_identical_submission_dispatches_provider_once() -> None:
    plan = run_plan()
    request = submission(plan)
    repository = InMemoryRuntimeCoordinationRepository()

    class SlowClient(DeterministicLegacyClient):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def submit(self, request, binding):  # type: ignore[no-untyped-def]
            self.started.set()
            await self.release.wait()
            return await super().submit(request, binding)

    client = SlowClient()
    service = GraphRuntimeDispatchService(
        repository=repository,
        selector=ExactRuntimeSelector(
            {
                (
                    implementation_ref().logical_id,
                    implementation_ref().revision,
                    implementation_ref().digest,
                ): client
            }
        ),
    )
    first_task = asyncio.create_task(service.submit(request, plan))
    await client.started.wait()
    second = await service.submit(request, plan)
    client.release.set()
    first = await first_task

    assert first.binding_id == second.binding_id
    assert client.calls == 1


@pytest.mark.asyncio
async def test_intervention_is_reserved_before_ambiguous_provider_effect() -> None:
    plan = run_plan()
    request = submission(plan)
    repository = InMemoryRuntimeCoordinationRepository()
    dispatch_client = DeterministicLegacyClient()
    dispatch = GraphRuntimeDispatchService(
        repository=repository,
        selector=ExactRuntimeSelector(
            {
                (
                    implementation_ref().logical_id,
                    implementation_ref().revision,
                    implementation_ref().digest,
                ): dispatch_client
            }
        ),
    )
    await dispatch.submit(request, plan)

    class Authority:
        async def current_version(self, _scope, _run):  # type: ignore[no-untyped-def]
            return 2

        async def current_checkpoint(
            self, _scope, _run, _epoch
        ):  # type: ignore[no-untyped-def]
            return None

        async def authorize_privileged_repair(
            self, _intervention
        ):  # type: ignore[no-untyped-def]
            return None

        async def authorize_intervention(
            self, intervention
        ):  # type: ignore[no-untyped-def]
            return RuntimeInterventionAuthorization(
                command_id=intervention.command_id,
                request_scope=intervention.epoch.request_scope,
                actor_id=intervention.actor.actor_id,
                approved=True,
            )

    class AmbiguousClient:
        def __init__(self) -> None:
            self.calls = 0

        async def apply(self, _intervention, *, binding_id):  # type: ignore[no-untyped-def]
            self.calls += 1
            raise TimeoutError(f"ambiguous provider effect for {binding_id}")

    client = AmbiguousClient()
    service = RuntimeInterventionService(
        bindings=repository,
        commands=repository,
        authority=Authority(),
        client=client,
    )
    intervention = cancel_intervention(request.epoch)
    with pytest.raises(TimeoutError, match="ambiguous"):
        await service.apply(intervention)
    replay = await service.apply(intervention)

    assert replay.status == "reconciliation_required"
    assert replay.reason_code == "provider_application_pending"
    assert client.calls == 1
