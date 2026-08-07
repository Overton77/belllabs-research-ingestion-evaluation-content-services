from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.runtime_bootstrap import (
    AuthoritativeRuntimeProjection,
    BootstrapRequest,
    CheckpointRuntimeProjection,
    RuntimeBootstrapReconciler,
)
from app.domain.graph_runtime.contracts import RuntimeExecutionBinding
from app.domain.graph_runtime.identities import (
    AgentThreadKey,
    DeploymentIdentity,
    ExecutionEpochKey,
)

DIGEST = "sha256:" + "a" * 64
OTHER_DIGEST = "sha256:" + "b" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def binding() -> RuntimeExecutionBinding:
    epoch = ExecutionEpochKey(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        execution_epoch=1,
    )
    deployment = DeploymentIdentity(
        assistant_id="assistant-n",
        deployment_id="deployment-n",
        deployment_revision="revision-n",
        deployment_endpoint_id="endpoint-n",
    )
    return RuntimeExecutionBinding(
        binding_id="binding-1",
        epoch=epoch,
        submission_id="submission-1",
        submission_idempotency_key="submission-1",
        submission_digest=DIGEST,
        run_plan_digest=DIGEST,
        graph_assembly_digest=DIGEST,
        state_schema_digest=DIGEST,
        runtime_provider="langgraph_agent_server",
        deployment=deployment,
        agent_thread=AgentThreadKey(
            **epoch.model_dump(),
            agent_server_thread_id="thread-1",
            relationship="parent",
        ),
        graph_id="stagegraph",
        version=3,
        created_at=NOW,
        updated_at=NOW,
    )


def authority_projection() -> AuthoritativeRuntimeProjection:
    return AuthoritativeRuntimeProjection(
        binding=binding(),
        lifecycle_version=7,
        lifecycle_projection_ref="run-projection:7",
        lifecycle_projection_digest=DIGEST,
        budget_projection_ref="budget-projection:7",
        decision_projection_ref="decision-projection:7",
    )


class Authority:
    async def load(self, _epoch):  # type: ignore[no-untyped-def]
        return authority_projection()


def request(
    checkpoint: CheckpointRuntimeProjection | None,
    *,
    graph_digest: str = DIGEST,
) -> BootstrapRequest:
    current = binding()
    return BootstrapRequest(
        epoch=current.epoch,
        runtime_binding_ref=current.binding_id,
        run_plan_digest=DIGEST,
        graph_assembly_digest=graph_digest,
        state_schema_digest=DIGEST,
        checkpoint_projection=checkpoint,
    )


def checkpoint(
    *,
    binding_version: int = 3,
    lifecycle_version: int = 7,
    endpoint: str = "endpoint-n",
) -> CheckpointRuntimeProjection:
    return CheckpointRuntimeProjection(
        binding_id="binding-1",
        binding_version=binding_version,
        lifecycle_version=lifecycle_version,
        lifecycle_projection_digest=DIGEST,
        run_plan_digest=DIGEST,
        graph_assembly_digest=DIGEST,
        state_schema_digest=DIGEST,
        deployment_endpoint_id=endpoint,
        deployment_revision="revision-n",
        graph_id="stagegraph",
    )


@pytest.mark.asyncio
async def test_bootstrap_allows_only_matching_authoritative_projection() -> None:
    decision = await RuntimeBootstrapReconciler(Authority()).reconcile(request(checkpoint()))

    assert decision.action == "ready"
    assert decision.lifecycle_version == 7
    assert decision.reason_code == "authoritative_projection_matches"


@pytest.mark.asyncio
async def test_bootstrap_rebuilds_safe_stale_projection_from_authority() -> None:
    decision = await RuntimeBootstrapReconciler(Authority()).reconcile(
        request(checkpoint(binding_version=2, lifecycle_version=6))
    )

    assert decision.action == "rebuild_projection"
    assert decision.binding_version == 3
    assert decision.lifecycle_version == 7


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runtime_request", "reason"),
    [
        (request(checkpoint(endpoint="endpoint-n-plus-1")), "checkpoint_route_incompatible"),
        (request(checkpoint(binding_version=4)), "checkpoint_binding_version_ahead"),
        (request(checkpoint(lifecycle_version=8)), "checkpoint_lifecycle_version_ahead"),
        (request(checkpoint(), graph_digest=OTHER_DIGEST), "frozen_runtime_digest_mismatch"),
    ],
)
async def test_bootstrap_fails_closed_on_incompatible_or_ahead_state(
    runtime_request: BootstrapRequest,
    reason: str,
) -> None:
    decision = await RuntimeBootstrapReconciler(Authority()).reconcile(runtime_request)

    assert decision.action == "interrupt_for_reconciliation"
    assert decision.reason_code == reason
