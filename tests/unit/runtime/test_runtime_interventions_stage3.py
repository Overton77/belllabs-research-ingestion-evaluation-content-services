from __future__ import annotations

from datetime import UTC, datetime

import pytest
from langgraph.types import Overwrite

from app.application.runtime.runtime_interventions import (
    ExactRuntimeInterventionRouter,
    PrivilegedRepairAuthorization,
    RuntimeInterventionAuthorization,
    RuntimeInterventionService,
)
from app.application.runtime.runtime_repairs import (
    PrivilegedRepairObservation,
    PrivilegedRuntimeRepairService,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.contracts import (
    ActorRef,
    CancelRunIntervention,
    Correlation,
    InterventionReceipt,
    PrivilegedOperatorReconcileIntervention,
    RuntimeExecutionBinding,
)
from app.domain.graph_runtime.identities import (
    AgentThreadKey,
    DeploymentIdentity,
    ExecutionEpochKey,
    LangGraphCheckpointKey,
)

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def binding() -> RuntimeExecutionBinding:
    epoch = ExecutionEpochKey(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        execution_epoch=1,
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
        deployment=DeploymentIdentity(
            assistant_id="assistant-n",
            deployment_id="deployment-n",
            deployment_revision="revision-n",
            deployment_endpoint_id="endpoint-n",
        ),
        agent_thread=AgentThreadKey(
            **epoch.model_dump(),
            agent_server_thread_id="thread-1",
            relationship="parent",
        ),
        graph_id="stagegraph",
        created_at=NOW,
        updated_at=NOW,
    )


def cancel() -> CancelRunIntervention:
    current = binding()
    values = {
        "kind": "cancel_run",
        "command_id": "cancel-1",
        "idempotency_key": "cancel-1",
        "epoch": current.epoch,
        "expected_belllabs_version": 3,
        "expected_checkpoint": None,
        "actor": ActorRef(
            actor_id="operator-1",
            actor_type="operator",
            authority_ref="authority:operator@1",
        ),
        "reason": "accepted cancellation",
        "correlation": Correlation(correlation_id="correlation-1"),
        "requested_at": NOW,
        "cancellation_mode": "graceful",
    }
    return CancelRunIntervention(**values, request_digest=sha256_digest(values))


class Bindings:
    def __init__(self, value: RuntimeExecutionBinding) -> None:
        self.value = value

    async def get_binding(self, _epoch):  # type: ignore[no-untyped-def]
        return self.value


class Client:
    def __init__(self) -> None:
        self.calls = 0

    async def apply(self, intervention, *, binding_id):  # type: ignore[no-untyped-def]
        self.calls += 1
        return InterventionReceipt(
            command_id=intervention.command_id,
            status="accepted",
            binding_id=binding_id,
            resulting_belllabs_version=4,
            reason_code="cancellation_dispatched",
            recorded_at=NOW,
        )

    async def reconcile(self, intervention, *, binding_id):  # type: ignore[no-untyped-def]
        return await self.apply(intervention, binding_id=binding_id)


@pytest.mark.asyncio
async def test_intervention_routes_to_exact_persisted_route() -> None:
    runtime_binding = binding()
    exact_client = Client()
    router = ExactRuntimeInterventionRouter(
        bindings=Bindings(runtime_binding),
        clients={
            ("endpoint-n", "revision-n", "assistant-n", "stagegraph"): exact_client,
        },
    )

    receipt = await router.apply(cancel(), binding_id=runtime_binding.binding_id)

    assert receipt.status == "accepted"
    assert exact_client.calls == 1


@pytest.mark.asyncio
async def test_intervention_never_falls_forward_to_an_unbound_revision() -> None:
    runtime_binding = binding()
    wrong_client = Client()
    router = ExactRuntimeInterventionRouter(
        bindings=Bindings(runtime_binding),
        clients={
            ("endpoint-n1", "revision-n1", "assistant-n1", "stagegraph"): wrong_client,
        },
    )

    with pytest.raises(LookupError, match="exact persisted route"):
        await router.apply(cancel(), binding_id=runtime_binding.binding_id)

    assert wrong_client.calls == 0


class Commands:
    def __init__(self) -> None:
        self.value = None

    async def get_intervention(self, _scope, _command_id):  # type: ignore[no-untyped-def]
        return self.value

    async def reserve(self, intervention, *, binding_id):  # type: ignore[no-untyped-def]
        self.value = (
            intervention,
            InterventionReceipt(
                command_id=intervention.command_id,
                status="reconciliation_required",
                binding_id=binding_id,
                resulting_belllabs_version=intervention.expected_belllabs_version,
                reason_code="provider_application_pending",
                recorded_at=intervention.requested_at,
            ),
        )
        return True

    async def record(self, intervention, receipt):  # type: ignore[no-untyped-def]
        self.value = (intervention, receipt)
        return receipt


class Authority:
    async def current_version(self, _scope, _run_id):  # type: ignore[no-untyped-def]
        return 3

    async def current_checkpoint(self, _scope, _run_id, _epoch):  # type: ignore[no-untyped-def]
        return None

    async def authorize_intervention(self, intervention):  # type: ignore[no-untyped-def]
        return RuntimeInterventionAuthorization(
            command_id=intervention.command_id,
            request_scope=intervention.epoch.request_scope,
            actor_id=intervention.actor.actor_id,
            approved=True,
        )

    async def authorize_privileged_repair(self, intervention):  # type: ignore[no-untyped-def]
        return PrivilegedRepairAuthorization(
            command_id=intervention.command_id,
            repair_authorization_ref=intervention.repair_authorization_ref,
            operator_actor_id=intervention.actor.actor_id,
            approved=True,
        )


class AmbiguousClient(Client):
    def __init__(self) -> None:
        super().__init__()
        self.reconcile_calls = 0

    async def apply(self, intervention, *, binding_id):  # type: ignore[no-untyped-def]
        del intervention, binding_id
        self.calls += 1
        raise TimeoutError("provider result ambiguous")

    async def reconcile(self, intervention, *, binding_id):  # type: ignore[no-untyped-def]
        self.reconcile_calls += 1
        return InterventionReceipt(
            command_id=intervention.command_id,
            status="existing",
            binding_id=binding_id,
            resulting_belllabs_version=intervention.expected_belllabs_version,
            reason_code="cancellation_observed",
            recorded_at=NOW,
        )


@pytest.mark.asyncio
async def test_reserved_intervention_reauthorizes_and_reconciles_after_timeout() -> None:
    commands = Commands()
    client = AmbiguousClient()
    service = RuntimeInterventionService(
        bindings=Bindings(binding()),
        commands=commands,
        client=client,
        authority=Authority(),
    )

    with pytest.raises(TimeoutError, match="ambiguous"):
        await service.apply(cancel())
    receipt = await service.apply(cancel())

    assert receipt.status == "existing"
    assert client.calls == 1
    assert client.reconcile_calls == 1


@pytest.mark.asyncio
async def test_uncomposed_intervention_authority_denies_before_provider() -> None:
    client = Client()
    service = RuntimeInterventionService(
        bindings=Bindings(binding()),
        commands=Commands(),
        client=client,
    )

    with pytest.raises(PermissionError, match="authority is not configured"):
        await service.apply(cancel())

    assert client.calls == 0


def repair() -> PrivilegedOperatorReconcileIntervention:
    current = binding()
    checkpoint = LangGraphCheckpointKey(
        deployment_endpoint_id="endpoint-n",
        agent_server_thread_id="thread-1",
        langgraph_checkpoint_id="checkpoint-1",
    )
    values = {
        "kind": "privileged_operator_reconcile",
        "command_id": "repair-1",
        "idempotency_key": "repair-1",
        "epoch": current.epoch,
        "expected_belllabs_version": 3,
        "expected_checkpoint": checkpoint,
        "actor": ActorRef(
            actor_id="operator-1",
            actor_type="operator",
            authority_ref="authority:operator@1",
        ),
        "reason": "authorized compact repair",
        "correlation": Correlation(correlation_id="correlation-repair-1"),
        "requested_at": NOW,
        "repair_authorization_ref": "approval:repair-1",
        "reconciliation_action": "admit_observed_checkpoint",
        "evidence_refs": ("evidence:repair-1",),
    }
    return PrivilegedOperatorReconcileIntervention(
        **values,
        request_digest=sha256_digest(values),
    )


class RepairAuthority(Authority):
    async def current_checkpoint(self, _scope, _run_id, _epoch):  # type: ignore[no-untyped-def]
        return repair().expected_checkpoint


class RepairDeniedAuthority(RepairAuthority):
    async def authorize_privileged_repair(self, intervention):  # type: ignore[no-untyped-def]
        return PrivilegedRepairAuthorization(
            command_id=intervention.command_id,
            repair_authorization_ref=intervention.repair_authorization_ref,
            operator_actor_id=intervention.actor.actor_id,
            approved=False,
        )


class RepairClient:
    def __init__(self) -> None:
        self.overwrite: Overwrite | None = None

    async def apply_overwrite(self, _intervention, _binding, overwrite):  # type: ignore[no-untyped-def]
        self.overwrite = overwrite
        return PrivilegedRepairObservation(
            before_digest=DIGEST,
            after_digest="sha256:" + "b" * 64,
        )

    async def reconcile_overwrite(self, _intervention, _binding):  # type: ignore[no-untyped-def]
        return None


class RepairAudit:
    def __init__(self) -> None:
        self.records = []

    async def record_repair_audit(self, record):  # type: ignore[no-untyped-def]
        self.records.append(record)
        return record


@pytest.mark.asyncio
async def test_privileged_repair_requires_authority_then_overwrites_and_audits() -> None:
    repair_client = RepairClient()
    audit = RepairAudit()
    provider = Client()
    service = RuntimeInterventionService(
        bindings=Bindings(binding()),
        commands=Commands(),
        client=provider,
        authority=RepairAuthority(),
        repair_service=PrivilegedRuntimeRepairService(
            client=repair_client,
            audit=audit,
        ),
    )

    receipt = await service.apply(repair())

    assert receipt.reason_code == "privileged_repair_applied_and_audited"
    assert isinstance(repair_client.overwrite, Overwrite)
    assert repair_client.overwrite.value["command_id"] == "repair-1"
    assert audit.records[0].expected_checkpoint_id == "checkpoint-1"
    assert provider.calls == 0


@pytest.mark.asyncio
async def test_privileged_repair_denial_happens_before_overwrite_or_audit() -> None:
    repair_client = RepairClient()
    audit = RepairAudit()
    service = RuntimeInterventionService(
        bindings=Bindings(binding()),
        commands=Commands(),
        client=Client(),
        authority=RepairDeniedAuthority(),
        repair_service=PrivilegedRuntimeRepairService(
            client=repair_client,
            audit=audit,
        ),
    )

    with pytest.raises(PermissionError, match="matching explicit approval"):
        await service.apply(repair())

    assert repair_client.overwrite is None
    assert audit.records == []
