from __future__ import annotations

import asyncio
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from app.application.control_plane import ControlPlaneService
from app.application.run_control import RunControlService
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, SecretRef
from app.domain.operation_execution.contracts import (
    ArtifactPromotionRequest,
    MaterializedWorkspace,
    OperationExecutionBinding,
    OperationExecutionRequest,
    OperationExecutionResult,
    OperationSettlement,
    PromotedArtifact,
    RuntimeInvocation,
    RuntimeResult,
    RuntimeUsage,
    SnapshotCloneRequest,
    SnapshotCloneResult,
)
from app.domain.run_control.contracts import (
    ActorContext,
    CommandStatus,
    LifecycleCommand,
    RecordUsageAction,
    RunPhase,
)
from app.domain.run_control.errors import IdempotencyConflict


class OperationExecutionInProgress(RuntimeError):
    """A durable claim exists and requires retry or explicit reconciliation."""


class OperationBudgetViolation(ValueError):
    """Runtime usage is outside the operation's immutable budget binding."""


class OperationBudgetReconciliationInProgress(RuntimeError):
    """Concurrent run mutations prevented reconciliation; a retry is safe."""


class OperationAuthorityPort(Protocol):
    async def verify(self, request: OperationExecutionRequest) -> None: ...


class RunControlOperationAuthority:
    """Verifies F1/F2 authority without allowing execution records to mutate it."""

    def __init__(self, run_control: RunControlService, control_plane: ControlPlaneService) -> None:
        self._run_control = run_control
        self._control_plane = control_plane

    async def verify(self, request: OperationExecutionRequest) -> None:
        run = await self._run_control.get_run(request.request_scope, request.identity.run_id)
        if run.version != request.run_control_revision:
            raise ValueError("operation is not bound to the accepted Run Control revision")
        if run.phase != RunPhase.ACTIVE:
            raise ValueError("semantic operations require an active Workflow Run")
        if run.effective_configuration_digest != request.effective_configuration_digest:
            raise ValueError("operation configuration does not match the admitted run")
        configuration = await self._control_plane.retrieve_for_admission(
            request.effective_configuration_digest
        )
        if not (
            request.capability_grant.capabilities <= configuration.effective_authority.capabilities
        ):
            raise ValueError("operation capabilities exceed effective run authority")
        workspace_ref = next(
            (
                ref
                for ref in configuration.source_refs
                if ref.kind == DefinitionKind.WORKSPACE_TEMPLATE
            ),
            None,
        )
        if workspace_ref != request.workspace.template_ref:
            raise ValueError("operation workspace does not match the frozen template")
        configured_slots = {
            (slot.name, slot.path, slot.access)
            for slot in configuration.workflow_workspace_contract.slots
        }
        bound_slots = {
            (slot.slot_name, slot.logical_path, slot.access)
            for slot in request.workspace.slot_bindings
        }
        if configured_slots != bound_slots:
            raise ValueError("operation workspace slots do not exactly match the compiled contract")
        contract_digest = sha256_digest(
            configuration.workflow_workspace_contract.model_dump(mode="json")
        )
        if request.workspace.workflow_contract_digest != contract_digest:
            raise ValueError("operation workspace contract digest is not authoritative")
        allowed_write_paths = {
            path for _name, path, access in configured_slots if access == "exclusive_write"
        }
        if set(request.workspace.exclusive_write_paths) != allowed_write_paths:
            raise ValueError("operation writable paths do not exactly match its workspace contract")
        budget = await self._run_control.get_budget(request.request_scope, request.identity.run_id)
        reservation = budget.reservations.get(request.budget_reservation_id)
        if reservation is None:
            raise ValueError("operation budget reservation is not authoritative")
        if not request.budget_limits.keys() <= reservation.keys():
            raise ValueError("operation budget contains dimensions outside the reservation")
        if any(
            request.budget_limits.get(dimension, 0) > amount
            for dimension, amount in reservation.items()
        ):
            raise ValueError("operation budget limits exceed the authoritative reservation")


class RunControlOperationBudgetAuthority:
    """Idempotently reconciles operation usage through the F2 PostgreSQL authority."""

    def __init__(
        self,
        run_control: RunControlService,
        *,
        actor: ActorContext,
        idempotency_issuer: str = "operation-runtime",
    ) -> None:
        self._run_control = run_control
        self._actor = actor
        self._idempotency_issuer = idempotency_issuer

    async def reconcile(
        self,
        *,
        binding: OperationExecutionBinding,
        settlement_id: str,
        usage: RuntimeUsage,
        budget_violation: bool = False,
    ) -> None:
        if not budget_violation:
            _validate_bound_usage(binding, usage)
        release_amounts = {
            dimension: limit
            - min(
                limit,
                usage.amounts.get(dimension, 0) + usage.pending_external_amounts.get(dimension, 0),
            )
            for dimension, limit in binding.budget_limits.items()
            if limit
            > usage.amounts.get(dimension, 0) + usage.pending_external_amounts.get(dimension, 0)
        }
        for _attempt in range(5):
            budget = await self._run_control.get_budget(binding.request_scope, binding.run_id)
            if settlement_id in budget.usage_ids:
                return
            run = await self._run_control.get_run(binding.request_scope, binding.run_id)
            result = await self._run_control.execute(
                LifecycleCommand(
                    command_id=f"operation-budget:{settlement_id}:v{run.version}",
                    idempotency_issuer=self._idempotency_issuer,
                    request_scope=binding.request_scope,
                    run_id=binding.run_id,
                    expected_run_version=run.version,
                    actor=self._actor,
                    action=RecordUsageAction(
                        usage_id=settlement_id,
                        actual_amounts=usage.amounts,
                        reservation_id=binding.budget_reservation_id,
                        release_amounts=release_amounts,
                        pending_external_amounts=usage.pending_external_amounts,
                    ),
                    reason="Reconcile immutable operation settlement usage",
                    evidence_refs=(binding.binding_id,),
                    occurred_at=datetime.now(UTC),
                    correlation_id=f"operation:{binding.semantic_attempt_key}",
                    causation_id=binding.binding_id,
                )
            )
            if result.status == CommandStatus.ACCEPTED:
                return
            if result.status == CommandStatus.STALE:
                continue
            if result.reason_code == "usage_exists":
                current_budget = await self._run_control.get_budget(
                    binding.request_scope, binding.run_id
                )
                if settlement_id in current_budget.usage_ids:
                    return
            raise ValueError(f"operation budget settlement was not accepted: {result.reason_code}")
        raise OperationBudgetReconciliationInProgress(
            "operation budget settlement remained stale after retries"
        )


class OperationBindingRepository(Protocol):
    async def get_binding(self, semantic_attempt_key: str) -> OperationExecutionBinding | None: ...

    async def get_binding_by_id(self, binding_id: str) -> OperationExecutionBinding | None: ...

    async def create_binding(
        self, binding: OperationExecutionBinding
    ) -> OperationExecutionBinding: ...

    async def get_settlement(self, binding_id: str) -> OperationSettlement | None: ...

    async def claim_execution(self, binding: OperationExecutionBinding) -> bool: ...

    async def settle(self, settlement: OperationSettlement) -> OperationSettlement: ...


class RuntimePort(Protocol):
    async def execute(
        self,
        invocation: RuntimeInvocation,
        resolved_secrets: Mapping[str, str],
    ) -> RuntimeResult: ...


class SandboxPort(Protocol):
    async def materialize(self, binding: OperationExecutionBinding) -> MaterializedWorkspace: ...


class CapabilityAssetPort(Protocol):
    async def verify(self, binding: OperationExecutionBinding) -> None: ...


class MCPRuntimePort(Protocol):
    """Verifies exact server revisions, schemas, filters, and approval policy."""

    async def verify_servers(self, binding: OperationExecutionBinding) -> None: ...


class SecretResolutionPort(Protocol):
    async def resolve(self, refs: tuple[SecretRef, ...]) -> Mapping[str, str]: ...


class OperationEventPort(Protocol):
    async def publish(
        self, *, event_key: str, binding_id: str, payload: dict[str, object]
    ) -> None: ...


class OperationBudgetPort(Protocol):
    async def reconcile(
        self,
        *,
        binding: OperationExecutionBinding,
        settlement_id: str,
        usage: RuntimeUsage,
        budget_violation: bool = False,
    ) -> None: ...


class ArtifactPromotionPort(Protocol):
    async def promote(self, request: ArtifactPromotionRequest) -> PromotedArtifact: ...


class SnapshotPort(Protocol):
    async def clone_restore(self, request: SnapshotCloneRequest) -> SnapshotCloneResult: ...


class OperationExecutionService:
    """Binds immutable intent before invoking any semantic provider side effect."""

    def __init__(
        self,
        *,
        authority: OperationAuthorityPort,
        bindings: OperationBindingRepository,
        runtime: RuntimePort,
        sandbox: SandboxPort,
        assets: CapabilityAssetPort,
        mcp: MCPRuntimePort,
        secrets: SecretResolutionPort,
        events: OperationEventPort,
        budget: OperationBudgetPort,
    ) -> None:
        self._authority = authority
        self._bindings = bindings
        self._runtime = runtime
        self._sandbox = sandbox
        self._assets = assets
        self._mcp = mcp
        self._secrets = secrets
        self._events = events
        self._budget = budget

    async def execute(self, request: OperationExecutionRequest) -> OperationExecutionResult:
        fingerprint = sha256_digest(request.model_dump(mode="json", exclude={"requested_at"}))
        prior = await self._bindings.get_binding(request.identity.semantic_key)
        if prior is not None:
            if prior.request_fingerprint != fingerprint:
                raise IdempotencyConflict(
                    "semantic operation attempt was reused with conflicting execution intent"
                )
            settlement = await self._bindings.get_settlement(prior.binding_id)
            if settlement is not None:
                await self._complete_post_effects(prior, settlement)
                return _public_result(prior, settlement)
            binding = prior
        else:
            await self._authority.verify(request)
            binding = _binding_for(request, fingerprint)
            binding = await self._bindings.create_binding(binding)

        await self._authority.verify(request)
        claimed = await self._bindings.claim_execution(binding)
        if not claimed:
            settlement = await self._bindings.get_settlement(binding.binding_id)
            if settlement is not None:
                await self._complete_post_effects(binding, settlement)
                return _public_result(binding, settlement)
            raise OperationExecutionInProgress(
                "a prior worker owns the durable side-effect claim; retry until its "
                "settlement is visible or explicitly reconcile the claim"
            )
        runtime_invoked = False
        observed_usage = RuntimeUsage()
        try:
            self._validate_policy_support(request)
            await self._assets.verify(binding)
            await self._mcp.verify_servers(binding)
            workspace = await self._sandbox.materialize(binding)
            resolved_secrets = await self._secrets.resolve(binding.secret_refs)
            invocation = RuntimeInvocation(
                binding=binding,
                prompt_segments=request.prompt_segments,
                workspace=workspace,
                resolved_secret_names=tuple(
                    sorted(f"{ref.provider}:{ref.key}" for ref in binding.secret_refs)
                ),
            )
            runtime_invoked = True
            runtime_result = await self._runtime.execute(invocation, resolved_secrets)
            observed_usage = runtime_result.usage
            _validate_bound_usage(binding, runtime_result.usage)
            settlement = OperationSettlement(
                settlement_id=_stable_id("operation-settlement", binding.binding_id),
                binding_id=binding.binding_id,
                status="completed",
                output_text=runtime_result.output_text,
                structured_output=runtime_result.structured_output,
                output_refs=runtime_result.output_refs,
                usage=runtime_result.usage,
                provider_run_id=runtime_result.provider_run_id,
                event_payloads=runtime_result.event_payloads,
                settled_at=datetime.now(UTC),
            )
        except Exception as error:
            settlement = OperationSettlement(
                settlement_id=_stable_id("operation-settlement", binding.binding_id),
                binding_id=binding.binding_id,
                status="failed",
                usage=observed_usage,
                failure_code=(
                    "budget_exceeded"
                    if isinstance(error, OperationBudgetViolation)
                    else "runtime_failed"
                    if runtime_invoked
                    else "preparation_failed"
                ),
                # Provider and secret exception text is deliberately not persisted.
                failure_message=f"{type(error).__name__} at governed operation boundary",
                settled_at=datetime.now(UTC),
            )

        settlement = await self._bindings.settle(settlement)
        await self._complete_post_effects(binding, settlement)
        return _public_result(binding, settlement)

    async def _complete_post_effects(
        self,
        binding: OperationExecutionBinding,
        settlement: OperationSettlement,
    ) -> None:
        for index, payload in enumerate(settlement.event_payloads):
            await self._events.publish(
                event_key=f"{binding.side_effect_key}:event:{index}",
                binding_id=binding.binding_id,
                payload=payload,
            )
        await self._budget.reconcile(
            binding=binding,
            settlement_id=settlement.settlement_id,
            usage=settlement.usage,
            budget_violation=settlement.failure_code == "budget_exceeded",
        )

    @staticmethod
    def _validate_policy_support(request: OperationExecutionRequest) -> None:
        for unsupported in request.unsupported_policies:
            if unsupported.required:
                raise ValueError(f"required runtime policy is unsupported: {unsupported.policy}")
            if unsupported.authored_degradation is None:
                raise ValueError(
                    f"unsupported runtime policy lacks authored degradation: {unsupported.policy}"
                )


class InMemoryOperationBindingRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._bindings: dict[str, OperationExecutionBinding] = {}
        self._settlements: dict[str, OperationSettlement] = {}
        self._claims: dict[str, str] = {}

    async def get_binding(self, semantic_attempt_key: str) -> OperationExecutionBinding | None:
        return deepcopy(self._bindings.get(semantic_attempt_key))

    async def get_binding_by_id(self, binding_id: str) -> OperationExecutionBinding | None:
        binding = next(
            (item for item in self._bindings.values() if item.binding_id == binding_id),
            None,
        )
        return deepcopy(binding)

    async def create_binding(self, binding: OperationExecutionBinding) -> OperationExecutionBinding:
        async with self._lock:
            prior = self._bindings.get(binding.semantic_attempt_key)
            if prior is not None:
                if prior.request_fingerprint != binding.request_fingerprint:
                    raise IdempotencyConflict(
                        "semantic operation binding has a conflicting fingerprint"
                    )
                return deepcopy(prior)
            self._bindings[binding.semantic_attempt_key] = deepcopy(binding)
            return deepcopy(binding)

    async def get_settlement(self, binding_id: str) -> OperationSettlement | None:
        return deepcopy(self._settlements.get(binding_id))

    async def claim_execution(self, binding: OperationExecutionBinding) -> bool:
        async with self._lock:
            prior = self._claims.get(binding.side_effect_key)
            if prior is not None:
                if prior != binding.binding_id:
                    raise IdempotencyConflict(
                        "operation side-effect key belongs to another binding"
                    )
                return False
            self._claims[binding.side_effect_key] = binding.binding_id
            return True

    async def settle(self, settlement: OperationSettlement) -> OperationSettlement:
        async with self._lock:
            prior = self._settlements.get(settlement.binding_id)
            if prior is not None:
                if prior != settlement:
                    # Settlement identity is stable. Provider retries must return the same
                    # observable result and usage for the semantic side-effect key.
                    comparable_prior = prior.model_copy(
                        update={"settled_at": settlement.settled_at}
                    )
                    if comparable_prior != settlement:
                        raise IdempotencyConflict(
                            "operation settlement conflicts with its prior result"
                        )
                return deepcopy(prior)
            self._settlements[settlement.binding_id] = deepcopy(settlement)
            return deepcopy(settlement)


def _binding_for(request: OperationExecutionRequest, fingerprint: str) -> OperationExecutionBinding:
    binding_id = _stable_id("operation-binding", request.identity.semantic_key)
    return OperationExecutionBinding(
        binding_id=binding_id,
        semantic_attempt_key=request.identity.semantic_key,
        request_fingerprint=fingerprint,
        request_scope=request.request_scope,
        run_id=request.identity.run_id,
        operation_id=request.identity.operation_id,
        operation_attempt=request.identity.operation_attempt,
        prior_binding_id=request.prior_binding_id,
        effective_configuration_digest=request.effective_configuration_digest,
        run_control_revision=request.run_control_revision,
        operation_contract_ref=request.operation_contract_ref,
        prompt_sources=tuple(
            (
                segment.source_ref,
                segment.source_revision,
                segment.trust_class,
                segment.rendered_digest,
            )
            for segment in request.prompt_segments
        ),
        model_policy=request.model_policy,
        tools=request.tools,
        mcp_servers=request.mcp_servers,
        skills=request.skills,
        plugins=request.plugins,
        agent_profile_ref=request.agent_profile_ref,
        capability_grant=request.capability_grant,
        workspace=request.workspace,
        secret_refs=request.secret_refs,
        budget_reservation_id=request.budget_reservation_id,
        budget_limits=request.budget_limits,
        tracing_policy_ref=request.tracing_policy_ref,
        sensitive_data_policy_ref=request.sensitive_data_policy_ref,
        snapshot_policy_ref=request.snapshot_policy_ref,
        applied_degradations=tuple(
            policy.authored_degradation
            for policy in request.unsupported_policies
            if not policy.required and policy.authored_degradation is not None
        ),
        side_effect_key=request.idempotency_key,
        bound_at=request.requested_at,
    )


def _public_result(
    binding: OperationExecutionBinding, settlement: OperationSettlement
) -> OperationExecutionResult:
    return OperationExecutionResult(
        binding_id=binding.binding_id,
        semantic_attempt_key=binding.semantic_attempt_key,
        status=settlement.status,
        output_text=settlement.output_text,
        structured_output=settlement.structured_output,
        output_refs=settlement.output_refs,
        usage=settlement.usage,
        failure_code=settlement.failure_code,
        failure_message=settlement.failure_message,
    )


def _validate_bound_usage(binding: OperationExecutionBinding, usage: RuntimeUsage) -> None:
    usage_dimensions = usage.amounts.keys() | usage.pending_external_amounts.keys()
    unbound_dimensions = usage_dimensions - binding.budget_limits.keys()
    if unbound_dimensions:
        raise OperationBudgetViolation(
            "runtime reported unbound budget dimensions: " + ", ".join(sorted(unbound_dimensions))
        )
    exceeded = {
        dimension
        for dimension in usage_dimensions
        if (
            usage.amounts.get(dimension, 0) + usage.pending_external_amounts.get(dimension, 0)
            > binding.budget_limits[dimension]
        )
    }
    if exceeded:
        raise OperationBudgetViolation(
            "runtime usage exceeds immutable operation budget: " + ", ".join(sorted(exceeded))
        )


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))
