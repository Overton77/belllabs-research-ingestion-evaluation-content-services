from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.runtime.graph_runtime_dispatch import ExactRuntimeSelector, GraphRuntimeDispatchService
from app.application.runtime.runtime_execution_bindings import InMemoryRuntimeCoordinationRepository
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.graph_runtime.contracts import (
    ActorRef,
    CancelRunIntervention,
    Correlation,
    GraphExecutionSubmission,
    RuntimeExecutionBinding,
    RuntimeExecutionStatus,
)
from app.domain.graph_runtime.definitions import (
    ContentAddressedRef,
    GraphAssemblySpecV2,
    RunPlanV3,
    RuntimeDefinitionKind,
)
from app.domain.graph_runtime.identities import DeploymentIdentity, ExecutionEpochKey
from app.integrations.langgraph_agent_server import (
    AgentServerRuntimeConfig,
    LangGraphAgentServerClient,
    LangGraphAgentServerInterventionClient,
    ResolvedAgentServerAction,
)

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def deployment(revision: str = "revision-n") -> DeploymentIdentity:
    return DeploymentIdentity(
        assistant_id="assistant-n",
        deployment_id="deployment-n",
        deployment_revision=revision,
        deployment_endpoint_id="endpoint-n",
    )


def submission(
    *,
    route: DeploymentIdentity | None = None,
    plan_digest: str = DIGEST,
) -> GraphExecutionSubmission:
    values = {
        "submission_id": "submission-1",
        "idempotency_key": "submission-1",
        "epoch": ExecutionEpochKey(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            execution_epoch=1,
        ),
        "expected_belllabs_version": 3,
        "run_plan_ref": ContentAddressedRef(
            kind=RuntimeDefinitionKind.RUN_PLAN,
            logical_id="plan-1",
            schema_version="belllabs.run-plan.v3",
            digest=plan_digest,
        ),
        "run_plan_digest": plan_digest,
        "graph_assembly_digest": DIGEST,
        "target_deployment": route or deployment(),
        "target_graph_id": "stagegraph",
        "state_schema_digest": DIGEST,
        "input_manifest_ref": "input:manifest:1",
        "actor": ActorRef(
            actor_id="dispatcher",
            actor_type="service",
            authority_ref="authority:dispatcher@1",
        ),
        "correlation": Correlation(correlation_id="correlation-1"),
        "submitted_at": NOW,
    }
    return GraphExecutionSubmission(
        **values,
        request_digest=sha256_digest(values),
    )


class FakeThreads:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def create(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        return {"thread_id": kwargs["thread_id"]}


class FakeRuns:
    def __init__(self) -> None:
        self.created: list[dict[str, object]] = []
        self.persisted: list[dict[str, object]] = []
        self.cancelled: list[tuple[str, str]] = []

    async def create(self, thread_id, assistant_id, **kwargs):  # type: ignore[no-untyped-def]
        run = {
            "run_id": "provider-run-1",
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "metadata": kwargs["metadata"],
            "created_at": NOW.isoformat(),
            "status": "running",
        }
        self.created.append({"thread_id": thread_id, "assistant_id": assistant_id, **kwargs})
        self.persisted.append(run)
        return run

    async def list(self, _thread_id, **_kwargs):  # type: ignore[no-untyped-def]
        return list(self.persisted)

    async def cancel(self, thread_id, run_id, **_kwargs):  # type: ignore[no-untyped-def]
        self.cancelled.append((thread_id, run_id))


class FakeSDK:
    def __init__(self) -> None:
        self.threads = FakeThreads()
        self.runs = FakeRuns()


class InputResolver:
    async def resolve(self, request, binding):  # type: ignore[no-untyped-def]
        return {
            "request_scope": request.epoch.request_scope,
            "belllabs_run_id": request.epoch.belllabs_run_id,
            "execution_epoch": request.epoch.execution_epoch,
            "runtime_binding_ref": binding.binding_id,
        }


def client(fake: FakeSDK) -> LangGraphAgentServerClient:
    return LangGraphAgentServerClient(
        client=fake,
        config=AgentServerRuntimeConfig(
            deployment=deployment(),
            graph_id="stagegraph",
        ),
        input_resolver=InputResolver(),
    )


def implementation_ref() -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=DefinitionKind.WORKFLOW_IMPLEMENTATION,
        logical_id="workflow.stagegraph.langgraph",
        revision=1,
        digest=DIGEST,
    )


def run_plan_v3() -> RunPlanV3:
    return RunPlanV3.create(
        plan_id="plan-1",
        effective_run_configuration_digest=DIGEST,
        semantic_binding_ref="semantic-binding-1",
        workflow_implementation_ref=implementation_ref(),
        graph_assembly=GraphAssemblySpecV2(
            graph_assembly_ref=ContentAddressedRef(
                kind=RuntimeDefinitionKind.GRAPH_ASSEMBLY,
                logical_id="assembly",
                schema_version="belllabs.graph-assembly-spec.v2",
                digest=DIGEST,
            ),
            state_schema_digest=DIGEST,
            reducer_registry_digest=DIGEST,
            operation_registry_digest=DIGEST,
            stage_requirements=(),
            stage_execution_bindings=(),
            compatibility_manifest_digest=DIGEST,
        ),
        alias_evidence_digest=DIGEST,
    )


def binding(
    request: GraphExecutionSubmission,
    runtime_client: LangGraphAgentServerClient,
) -> RuntimeExecutionBinding:
    return RuntimeExecutionBinding(
        binding_id="binding-1",
        epoch=request.epoch,
        submission_id=request.submission_id,
        submission_idempotency_key=request.idempotency_key,
        submission_digest=request.request_digest,
        run_plan_digest=request.run_plan_digest,
        graph_assembly_digest=request.graph_assembly_digest,
        state_schema_digest=request.state_schema_digest,
        runtime_provider="langgraph_agent_server",
        deployment=runtime_client.deployment_for(request),
        agent_thread=runtime_client.thread_for(request),
        graph_id=request.target_graph_id,
        status=RuntimeExecutionStatus.SUBMITTING,
        created_at=NOW,
        updated_at=NOW,
    )


@pytest.mark.asyncio
async def test_client_creates_exact_thread_and_run_with_stable_submission_metadata() -> None:
    fake = FakeSDK()
    runtime_client = client(fake)
    request = submission()
    frozen_binding = binding(request, runtime_client)

    receipt = await runtime_client.submit(request, frozen_binding)

    assert receipt.agent_thread == frozen_binding.agent_thread
    assert receipt.agent_run is not None
    assert receipt.agent_run.deployment_endpoint_id == "endpoint-n"
    assert fake.threads.calls[0]["graph_id"] == "stagegraph"
    assert fake.runs.created[0]["assistant_id"] == "assistant-n"
    assert fake.runs.created[0]["multitask_strategy"] == "reject"
    assert fake.runs.created[0]["durability"] == "sync"
    metadata = fake.runs.created[0]["metadata"]
    assert metadata["belllabs_submission_digest"] == request.request_digest  # type: ignore[index]
    assert metadata["belllabs_binding_id"] == frozen_binding.binding_id  # type: ignore[index]


@pytest.mark.asyncio
async def test_reconciliation_queries_metadata_and_never_creates_a_second_run() -> None:
    fake = FakeSDK()
    runtime_client = client(fake)
    request = submission()
    frozen_binding = binding(request, runtime_client)
    await runtime_client.submit(request, frozen_binding)

    receipt = await runtime_client.reconcile_submission(request, frozen_binding)

    assert receipt is not None
    assert receipt.agent_run is not None
    assert receipt.agent_run.agent_server_run_id == "provider-run-1"
    assert len(fake.runs.created) == 1


def test_wrong_revision_route_is_rejected_before_any_sdk_call() -> None:
    fake = FakeSDK()
    runtime_client = client(fake)
    request = submission(route=deployment("revision-n-plus-1"))

    with pytest.raises(ValueError, match="exact Agent Server deployment"):
        runtime_client.thread_for(request)

    assert fake.threads.calls == []
    assert fake.runs.created == []


class Bindings:
    def __init__(self, value: RuntimeExecutionBinding) -> None:
        self.value = value

    async def get_binding(self, _epoch):  # type: ignore[no-untyped-def]
        return self.value


class CancelResolver:
    async def resolve(self, _intervention, _binding):  # type: ignore[no-untyped-def]
        return ResolvedAgentServerAction(kind="cancel")


def cancel_intervention(epoch: ExecutionEpochKey) -> CancelRunIntervention:
    values = {
        "kind": "cancel_run",
        "command_id": "cancel-1",
        "idempotency_key": "cancel-1",
        "epoch": epoch,
        "expected_belllabs_version": 4,
        "expected_checkpoint": None,
        "actor": ActorRef(
            actor_id="operator-1",
            actor_type="operator",
            authority_ref="authority:operator@1",
        ),
        "reason": "accepted cancellation",
        "correlation": Correlation(correlation_id="correlation-cancel"),
        "requested_at": NOW,
        "cancellation_mode": "graceful",
    }
    return CancelRunIntervention(**values, request_digest=sha256_digest(values))


@pytest.mark.asyncio
async def test_intervention_client_cancels_only_active_runs_on_exact_bound_thread() -> None:
    fake = FakeSDK()
    runtime_client = client(fake)
    request = submission()
    frozen_binding = binding(request, runtime_client)
    await runtime_client.submit(request, frozen_binding)
    intervention_client = LangGraphAgentServerInterventionClient(
        client=fake,
        config=AgentServerRuntimeConfig(
            deployment=deployment(),
            graph_id="stagegraph",
        ),
        bindings=Bindings(frozen_binding),
        resolver=CancelResolver(),
    )

    receipt = await intervention_client.apply(
        cancel_intervention(request.epoch),
        binding_id=frozen_binding.binding_id,
    )

    assert receipt.reason_code == "cancellation_dispatched"
    assert fake.runs.cancelled == [
        (frozen_binding.agent_thread.agent_server_thread_id, "provider-run-1")  # type: ignore[union-attr]
    ]


def test_enqueue_action_requires_explicit_authored_authorization() -> None:
    with pytest.raises(ValueError, match="enqueue requires"):
        ResolvedAgentServerAction(
            kind="input",
            input={"input_manifest_ref": "input:1"},
            multitask_strategy="enqueue",
        )


@pytest.mark.asyncio
async def test_production_dispatch_requires_v3_and_persists_exact_route_before_run() -> None:
    fake = FakeSDK()
    runtime_client = client(fake)
    plan = run_plan_v3()
    request = submission(plan_digest=plan.plan_digest)
    repository = InMemoryRuntimeCoordinationRepository()
    service = GraphRuntimeDispatchService(
        repository=repository,
        selector=ExactRuntimeSelector(
            {
                (
                    implementation_ref().logical_id,
                    implementation_ref().revision,
                    implementation_ref().digest,
                ): runtime_client
            }
        ),
    )

    first = await service.submit(request, plan)
    replay = await service.submit(request, plan)
    persisted = await repository.get_binding(request.epoch)

    assert first.binding_id == replay.binding_id
    assert replay.status == "existing"
    assert persisted is not None
    assert persisted.deployment == deployment()
    assert persisted.graph_id == "stagegraph"
    assert persisted.agent_thread == runtime_client.thread_for(request)
    assert len(fake.runs.created) == 1
