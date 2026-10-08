from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from contextvars import ContextVar
from copy import deepcopy
from datetime import UTC, datetime
from typing import Literal, Protocol

from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.execution.harness.deep_agents_harness import DeepAgentsHarness
from mission_control.application.execution.harness.inject import (
    InjectionParked,
    InterruptAndInjectService,
    TurnRecord,
)
from mission_control.application.execution.harness.protocol import OperationLane
from mission_control.application.execution.harness.registry import LaneRegistry
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageService,
    UnitAttempt,
    transition_id_for,
)
from mission_control.application.execution.operations.operation_progress import (
    cancel_requested,
    report_phase,
)
from mission_control.application.execution.service import RunControlService
from mission_control.application.execution.stop_fence import StopFenceRepository
from mission_control.domain.authoring.canonical import contract_fingerprint, sha256_digest
from mission_control.domain.authoring.contracts import DefinitionKind, SecretRef
from mission_control.domain.authoring.identity import stable_id
from mission_control.domain.context.render import is_context_input_slot
from mission_control.domain.execution.checkpoint_lineage import (
    CheckpointCapture,
    CheckpointInvocationPlan,
    CheckpointLineageConflict,
    CheckpointLineageError,
    CheckpointLineageInDoubt,
    InDoubtReason,
    OperationActivityAttempt,
    StaleClaimFence,
    UnitReconciliationIncident,
    UnitResultObservation,
)
from mission_control.domain.execution.contracts import (
    ArtifactPromotionRequest,
    AsyncChildCancellationRecord,
    MaterializedWorkspace,
    OperationExecutionBinding,
    OperationExecutionRequest,
    OperationExecutionResult,
    OperationSettlement,
    PromotedArtifact,
    PromptTrustClass,
    ReusedResultRef,
    RuntimeInvocation,
    RuntimeResult,
    RuntimeUsage,
    SnapshotCloneRequest,
    SnapshotCloneResult,
)
from mission_control.domain.execution.errors import (
    RuntimeInvocationFailure,
    UnsupportedRuntimePolicy,
)
from mission_control.domain.execution.journal import OperationClaimResult, OperationEffectClaim
from mission_control.domain.graph_runtime.identities import QualifiedCheckpointKey
from mission_control.domain.policies.contracts import (
    ActorContext,
    CommandStatus,
    LifecycleCommand,
    RecordUsageAction,
    RunPhase,
    RunProjection,
    UnitReconciliationDecision,
)
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.policies.forks import ForkRejected
from mission_control.domain.programs.runtime_units import (
    goal_unit_operation_id,
    goal_unit_workspace_root,
)

_LOGGER = logging.getLogger(__name__)

# The lease deadline of the attempt holding the claim lease, so that a cancellation raised
# by the deadline is told apart from a Temporal cancel delivered through the heartbeat.
_LEASE_DEADLINE: ContextVar[asyncio.Timeout | None] = ContextVar(
    "belllabs_operation_lease_deadline", default=None
)


def _uncancel_current_task() -> None:
    """Clear the swallowed cancellation so the settlement's own awaits are not cancelled."""

    task = asyncio.current_task()
    if task is not None:
        task.uncancel()


def _lease_deadline_expired() -> bool:
    deadline = _LEASE_DEADLINE.get()
    return deadline is not None and deadline.expired()


class OperationExecutionInProgress(RuntimeError):
    """A durable claim exists and requires retry or explicit reconciliation."""


class OperationLeaseExpired(OperationExecutionInProgress):
    """The holder stopped at its lease deadline; a retry classifies and continues the unit."""


class ForkMaterializationPending(OperationExecutionInProgress):
    """RRM-006: the derived run started before its fork's materialization committed.

    Transient: nothing settles, the lease is released and a retry resolves the reuse
    decision once the materialization is visible.
    """

    code = "fork_not_materialized"


class OperationBudgetViolation(ValueError):
    """Runtime usage is outside the operation's immutable budget binding."""


class OperationBudgetReconciliationInProgress(RuntimeError):
    """Concurrent run mutations prevented reconciliation; a retry is safe."""


class OperationAuthorityPort(Protocol):
    async def verify(self, request: OperationExecutionRequest) -> None:
        """Admit the first binding of a semantic operation attempt."""
        ...

    async def verify_continuation(
        self, request: OperationExecutionRequest, binding: OperationExecutionBinding
    ) -> None:
        """Admit a retry, takeover or recovery of an already bound attempt."""
        ...

    async def verify_cancellation(
        self, request: OperationExecutionRequest, binding: OperationExecutionBinding
    ) -> None:
        """RRM-008: admit the cancellation saga's settlement of a bound attempt.

        The accepted cancel command is the authority; the run is `cancelling` (or still
        running when the family's cancel reached the unit first) and never terminal.
        """
        ...


class AsyncChildCancellationPort(Protocol):
    """REQ-CP-EXEC-008 step 4: cancel every active async child of a unit per its link
    policy and record the provider's acknowledgement or its absence."""

    async def cancel_children(
        self,
        binding: OperationExecutionBinding,
        *,
        reason: str,
        requested_at: datetime,
    ) -> tuple[AsyncChildCancellationRecord, ...]: ...


class CancellableRuntimePort(Protocol):
    """A runtime that can report the latest durable checkpoint of a unit being cancelled."""

    async def observe_latest(
        self,
        invocation: RuntimeInvocation,
        resolved_secrets: Mapping[str, str],
    ) -> RuntimeResult: ...


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
        await self._verify_bound_authority(request, run)

    async def verify_continuation(
        self, request: OperationExecutionRequest, binding: OperationExecutionBinding
    ) -> None:
        """Admit a technical retry, claim takeover or recovery of the same bound attempt.

        REQ-CP-EXEC-005 (AMD-RRM-001): Activity attempts, worker restarts and claim takeovers
        are not disruptive; they continue the same semantic attempt, whose own claim has
        already advanced the run version past the bound revision. So the exact-revision check
        of the first binding does not apply. What must still hold: the binding is the one
        being continued, the run never went backwards, the configuration and reservation
        are the bound ones, and the run is not terminal, paused or cancelling (cancellation
        reconciliation is REQ-CP-EXEC-008's saga). A run that is `waiting` is admitted:
        while a unit is `in_doubt` the run keeps its phase, and other waits do not supersede
        a claimed unit (REQ-CP-RUN-007, REQ-CP-EXEC-014).
        """

        run = await self._run_control.get_run(request.request_scope, request.identity.run_id)
        if (
            binding.run_id != request.identity.run_id
            or binding.request_scope != request.request_scope
            or binding.run_control_revision != request.run_control_revision
        ):
            raise ValueError("continuation does not target the bound operation attempt")
        if run.version < binding.run_control_revision:
            raise ValueError("run authority is older than the operation binding")
        if run.phase not in {RunPhase.ACTIVE, RunPhase.WAITING}:
            raise ValueError(
                f"a bound operation cannot continue in a {run.phase.value} Workflow Run"
            )
        await self._verify_bound_authority(request, run)

    async def verify_cancellation(
        self, request: OperationExecutionRequest, binding: OperationExecutionBinding
    ) -> None:
        """RRM-008 (REQ-CP-EXEC-008): the accepted cancel is the authority for settling the
        bound attempt `cancelled`. The intent is journaled first: the run is `cancelling`
        (the accepted `cancel` command moved it there before any delivery), never pending,
        active or terminal. A Temporal cancellation without that journal entry is not a
        cancel of the unit (review F1). The binding stays the authentic one
        (configuration, prompts, workspace and reservation are the bound ones)."""

        run = await self._run_control.get_run(request.request_scope, request.identity.run_id)
        if (
            binding.run_id != request.identity.run_id
            or binding.request_scope != request.request_scope
            or binding.run_control_revision != request.run_control_revision
        ):
            raise ValueError("cancellation does not target the bound operation attempt")
        if run.version < binding.run_control_revision:
            raise ValueError("run authority is older than the operation binding")
        if run.phase != RunPhase.CANCELLING:
            raise ValueError(
                f"run control journaled no cancel for a {run.phase.value} Workflow Run"
            )
        await self._verify_bound_authority(request, run)

    async def _verify_bound_authority(
        self, request: OperationExecutionRequest, run: RunProjection
    ) -> None:
        if run.effective_configuration_digest != request.effective_configuration_digest:
            raise ValueError("operation configuration does not match the admitted run")
        configuration = await self._control_plane.retrieve_for_admission(
            request.effective_configuration_digest
        )
        if not (
            request.capability_grant.capabilities <= configuration.effective_authority.capabilities
        ):
            raise ValueError("operation capabilities exceed effective run authority")
        authoritative_prompts = {
            f"prompt:{ref.logical_id}@{ref.revision}"
            for ref in configuration.source_refs
            if ref.kind == DefinitionKind.PROMPT
        }
        privileged_prompt_sources = {
            segment.source_ref
            for segment in request.prompt_segments
            if segment.trust_class
            in {
                PromptTrustClass.SYSTEM_AUTHORITY,
                PromptTrustClass.AUTHORED_INSTRUCTION,
            }
        }
        if not privileged_prompt_sources <= authoritative_prompts:
            raise ValueError(
                "privileged prompt segments are not exact accepted configuration sources"
            )
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
        _verify_family_unit(request, run)
        # RRM-016: a GoalDirected unit binds the exact compiled slots under its role-scoped
        # root, which is recomputed here from the digest-bound unit identity (REQ-CP-DA-013;
        # executor and verifier writable paths stay disjoint, REQ-BP-GD-004). The unit itself
        # was cross-checked against the run's family and the operation above.
        slot_root = goal_unit_workspace_root(request.runtime_unit) or ""
        configured_slots = {
            (slot.name, f"{slot_root}{slot.path}", slot.access)
            for slot in configuration.workflow_workspace_contract.slots
        }
        # FT-B2 (ADR-0027): read-only, digest-bound Context Packet inputs join the compiled
        # slots; they cannot write and cannot overlap a compiled path (WorkspaceContract).
        bound_slots = {
            (slot.slot_name, slot.logical_path, slot.access)
            for slot in request.workspace.slot_bindings
            if not is_context_input_slot(slot)
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


def _verify_family_unit(request: OperationExecutionRequest, run: RunProjection) -> None:
    """RRM-016 review fix 1: a GoalDirected unit is admitted only on a GoalDirected run and
    only for the operation its location names, so the workspace root derived from it cannot
    be borrowed by another family's or another iteration's operation. Conversely (re-check
    a), a GoalDirected run admits only GoalDirected units: an operation without one would
    bind unrebased slots, so executor and verifier would no longer be disjoint."""

    unit = request.runtime_unit
    target = run.execution_target
    goal_directed_run = target is not None and target.family == "GoalDirected"
    if unit is None or unit.family != "goal_directed":
        if goal_directed_run:
            raise ValueError("a GoalDirected Workflow Run admits only GoalDirected runtime units")
        return
    if not goal_directed_run:
        raise ValueError("a GoalDirected runtime unit requires a GoalDirected Workflow Run")
    if (
        unit.belllabs_run_id != request.identity.run_id
        or goal_unit_operation_id(unit) != request.identity.operation_id
    ):
        raise ValueError("GoalDirected runtime unit location does not match its operation")


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
    async def get_binding(
        self,
        semantic_attempt_key: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None: ...

    async def get_binding_by_id(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None: ...

    async def create_binding(
        self,
        binding: OperationExecutionBinding,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding: ...

    async def get_settlement(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationSettlement | None: ...

    async def claim_execution(self, binding: OperationExecutionBinding) -> bool: ...

    async def settle(
        self,
        settlement: OperationSettlement,
        *,
        request_scope: str,
    ) -> OperationSettlement: ...


ResultManifestObserver = Callable[[str, str, int], Awaitable[None]]
"""Called with the staged result manifest ref, digest and size before authority settlement."""


class OperationExecutionJournalPort(Protocol):
    async def acquire(
        self,
        binding: OperationExecutionBinding,
        *,
        claimed_by: str,
        at_current_version: bool = False,
    ) -> OperationClaimResult: ...

    async def get_settlement(
        self,
        binding: OperationExecutionBinding,
    ) -> OperationSettlement | None: ...

    async def settle(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim,
        settlement: OperationSettlement,
        *,
        started_at: datetime,
        technical_attempt: int = 1,
        before_authority: ResultManifestObserver | None = None,
    ) -> OperationSettlement: ...

    # --- RRM-004 recovery seams (REQ-CP-DA-018, REQ-CP-RUN-007, `reconcile_unit`) ---

    async def load_result_manifest(
        self,
        binding: OperationExecutionBinding,
        *,
        manifest_ref: str,
        manifest_digest: str,
        manifest_size_bytes: int,
    ) -> OperationSettlement: ...

    async def record_in_doubt(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim,
        incident: UnitReconciliationIncident,
    ) -> None: ...

    async def get_unit_reconciliation(
        self,
        binding: OperationExecutionBinding,
        *,
        unit_key: str,
        execution_generation: int,
        incident_id: str,
    ) -> UnitReconciliationDecision | None: ...

    async def unsettled_effect_ids(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim,
    ) -> tuple[str, ...]: ...

    async def record_reconciliation_applied(
        self,
        binding: OperationExecutionBinding,
        decision: UnitReconciliationDecision,
    ) -> None: ...


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


class ReusedSourceResult(Protocol):
    """An immutable source result a fork-derived unit settles by reference (EXEC-012)."""

    @property
    def ref(self) -> ReusedResultRef: ...

    @property
    def source_settlement(self) -> OperationSettlement: ...


class ForkReusePort(Protocol):
    """RRM-006: the reuse decision recorded for a fork-derived unit, if any.

    Returns `None` for an ordinary unit or one the fork decided to re-execute; raises (fails
    closed) for an unmaterialized fork (transient, retried) or an incompatible restore or
    missing fork lineage (terminal, settled `failed`).
    """

    async def reused_result(
        self, binding: OperationExecutionBinding
    ) -> ReusedSourceResult | None: ...


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
        journal: OperationExecutionJournalPort | None = None,
        journal_claimed_by: str = "operation-runtime",
        lineage: CheckpointLineageService | None = None,
        fork_reuse: ForkReusePort | None = None,
        children: AsyncChildCancellationPort | None = None,
        lanes: LaneRegistry | None = None,
        stop_fences: StopFenceRepository | None = None,
        mailbox: MailboxDeliveryService | None = None,
        injections: InterruptAndInjectService | None = None,
    ) -> None:
        self._authority = authority
        self._bindings = bindings
        self._runtime = runtime
        # FT-G1: dispatch by lane. Without an explicit registry the given runtime is the
        # `deep_agents` lane, exactly as before.
        self._lanes = lanes if lanes is not None else LaneRegistry([DeepAgentsHarness(runtime)])
        # FT-F3: the immediate cancel's Delivery Report milestones (no-op without a fence).
        self._stop_fences = stop_fences
        # FT-F1: the lane boundary consumes delivered mailbox entries when the turn that
        # carries them starts, and completes their Commands when it settles.
        self._mailbox = mailbox
        # FT-F2: interrupt_and_inject by the lane's semantics while a lineage-qualified turn
        # runs (cancel_and_replace with uncertain-effect settlement, or cooperative_inject).
        self._injections = injections
        self._sandbox = sandbox
        self._assets = assets
        self._mcp = mcp
        self._secrets = secrets
        self._events = events
        self._budget = budget
        self._journal = journal
        self._journal_claimed_by = journal_claimed_by
        self._lineage = lineage
        self._fork_reuse = fork_reuse
        self._children = children

    async def execute(
        self,
        request: OperationExecutionRequest,
        attempt: OperationActivityAttempt | None = None,
    ) -> OperationExecutionResult:
        """Execute one bound unit; `attempt` is the observed Temporal Activity delivery."""

        if self._lineage is not None and attempt is None:
            raise ValueError("lineage-qualified execution requires the Activity attempt (EXEC-014)")
        lane = self._lanes.lane_for(request)
        if lane.requires_checkpoint_lineage(request) and self._lineage is None:
            raise ValueError(
                "Deep Agent execution requires checkpoint lineage composition (REQ-CP-DA-016)"
            )
        fingerprint = contract_fingerprint(request, exclude={"requested_at"})
        prior = await self._bindings.get_binding(
            request.identity.semantic_key,
            request_scope=request.request_scope,
        )
        if prior is not None:
            if prior.request_fingerprint != fingerprint:
                raise IdempotencyConflict(
                    "semantic operation attempt was reused with conflicting execution intent"
                )
            # `settled`: an authoritative settlement returns unchanged, with no provider work.
            settlement = await self._get_settlement(prior)
            if settlement is not None:
                await self._complete_post_effects(prior, settlement)
                return _public_result(prior, settlement)
            binding = prior
            # A retry, takeover or recovery continues the bound attempt (REQ-CP-EXEC-005).
            await self._authority.verify_continuation(request, binding)
        else:
            await self._authority.verify(request)
            binding = _binding_for(request, fingerprint)
            binding = await self._bindings.create_binding(
                binding,
                request_scope=request.request_scope,
            )
            await self._authority.verify(request)
        claim_result = (
            await self._journal.acquire(
                binding,
                claimed_by=self._journal_claimed_by,
            )
            if self._journal is not None
            else None
        )
        claim = claim_result.claim if claim_result is not None else None
        if (
            self._lineage is not None
            and attempt is not None
            and (claim_result is None or claim is not None)
        ):
            return await self._execute_unit(request, binding, claim, attempt, self._lineage)
        claimed = (
            claim_result.status == "acquired"
            if claim_result is not None
            else await self._bindings.claim_execution(binding)
        )
        if not claimed:
            settlement = await self._get_settlement(binding)
            if settlement is not None:
                await self._complete_post_effects(binding, settlement)
                return _public_result(binding, settlement)
            raise OperationExecutionInProgress(
                "a prior worker owns the durable side-effect claim; retry until its "
                "settlement is visible or explicitly reconcile the claim"
            )
        return await self._dispatch_and_settle(request, binding, claim, attempt=attempt)

    def _lane(self, binding: OperationExecutionBinding) -> OperationLane:
        return self._lanes.lane_for(binding)

    async def _execute_unit(
        self,
        request: OperationExecutionRequest,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        attempt: OperationActivityAttempt,
        lineage: CheckpointLineageService,
    ) -> OperationExecutionResult:
        """REQ-CP-EXEC-014 + REQ-CP-DA-018: hold the claim lease, classify, then act once.

        The journal claim records that the unit's consequential effect is claimed. Whether
        this Activity attempt may act on it is decided by the unit generation's claim lease:
        the attempt is observed, then holds or takes over the lease (advancing the fence) or
        stands down while a live holder works. Holding the lease, it classifies the unit
        from durable facts before any provider work.
        """

        try:
            admitted = await lineage.admit_attempt(binding, attempt)
        except StaleClaimFence:
            # REQ-CP-EXEC-005: an accepted generation boundary fences this generation; it
            # reports the boundary instead of failing as a lineage conflict.
            unit, generation = lineage.unit_generation(binding)
            incident = await lineage.repository.get_incident(
                binding.request_scope, unit.unit_key, generation
            )
            if incident is None or incident.decision != "start_new_generation":
                raise
            return _unsettled_result(
                binding,
                failure_code="generation_superseded",
                message="an accepted generation boundary superseded this generation",
                incident_id=incident.incident_id,
            )
        return await self._hold_lease(
            binding,
            admitted,
            lineage,
            lambda: self._recover_or_dispatch(request, binding, claim, admitted, lineage),
        )

    async def cancel(
        self,
        request: OperationExecutionRequest,
        attempt: OperationActivityAttempt,
    ) -> OperationExecutionResult:
        """RRM-008 (REQ-CP-EXEC-008 steps 3-6): settle a unit that is being cancelled.

        Cognition is never dispatched or resumed here. The attempt holds the claim lease
        (standing down behind a live holder, which settles the unit `cancelled` itself when
        the cancel reached it through its heartbeat), classifies the unit from durable
        facts and acts once: a settled unit returns unchanged; a fenced result settles as
        recorded; an `in_doubt` unit stays with its incident unless an accepted
        `reconcile_unit` decision resolves it; otherwise the unit settles `cancelled` with
        its latest durable checkpoint as the result checkpoint (its partial evidence), after
        every active async child was cancelled under its link policy.
        """

        if self._lineage is None:
            raise ValueError("cancellation requires checkpoint lineage composition (EXEC-008)")
        lineage = self._lineage
        report_phase("cancelling")
        fingerprint = contract_fingerprint(request, exclude={"requested_at"})
        prior = await self._bindings.get_binding(
            request.identity.semantic_key,
            request_scope=request.request_scope,
        )
        if prior is not None:
            if prior.request_fingerprint != fingerprint:
                raise IdempotencyConflict(
                    "semantic operation attempt was reused with conflicting execution intent"
                )
            settlement = await self._get_settlement(prior)
            if settlement is not None:
                await self._complete_post_effects(prior, settlement)
                return _public_result(prior, settlement)
            binding = prior
        else:
            # Never bound (cancelled before dispatch): the binding exists so that the unit
            # has exactly one settlement, which releases its reservation.
            binding = await self._bindings.create_binding(
                _binding_for(request, fingerprint),
                request_scope=request.request_scope,
            )
        await self._authority.verify_cancellation(request, binding)
        claim: OperationEffectClaim | None = None
        if self._journal is not None:
            # The claim is made at the run's current version: an accepted cancel moved the
            # version past the bound revision, which must not strand the unit (RRM-016 risk).
            claim_result = await self._journal.acquire(
                binding, claimed_by=self._journal_claimed_by, at_current_version=True
            )
            claim = claim_result.claim
            if claim is None:
                settlement = await self._get_settlement(binding)
                if settlement is not None:
                    await self._complete_post_effects(binding, settlement)
                    return _public_result(binding, settlement)
                raise OperationExecutionInProgress(
                    f"cancellation claim was not acquired ({claim_result.reason}); retry"
                )
        try:
            admitted = await lineage.admit_attempt(binding, attempt)
        except StaleClaimFence:
            unit, generation = lineage.unit_generation(binding)
            incident = await lineage.repository.get_incident(
                binding.request_scope, unit.unit_key, generation
            )
            if incident is None or incident.decision != "start_new_generation":
                raise
            return await self._settle_superseded(binding, claim, attempt, incident.incident_id)
        return await self._hold_lease(
            binding,
            admitted,
            lineage,
            lambda: self._cancel_or_settle(request, binding, claim, admitted, lineage),
        )

    async def _hold_lease(
        self,
        binding: OperationExecutionBinding,
        admitted: UnitAttempt,
        lineage: CheckpointLineageService,
        body: Callable[[], Awaitable[OperationExecutionResult]],
    ) -> OperationExecutionResult:
        if not admitted.admission.lease_granted:
            settlement = await self._get_settlement(binding)
            if settlement is not None:
                await self._complete_post_effects(binding, settlement)
                return _public_result(binding, settlement)
            raise OperationExecutionInProgress(
                "a live attempt holds the unit's claim lease; retry until its settlement is "
                "visible or the lease expires (REQ-CP-EXEC-014)"
            )
        budget = admitted.work_budget(lineage.now())
        if budget <= 0:
            with suppress(Exception):
                await lineage.release(admitted)
            raise OperationLeaseExpired(
                "the attempt's claim lease is already within its safety margin"
            )
        try:
            # REQ-CP-EXEC-014: a live holder never outlives its lease. It stops (cancelling
            # in-flight cognition) before a later attempt may take the lease over, so a
            # superseded holder cannot keep calling the model or tools.
            async with asyncio.timeout(budget) as deadline:
                token = _LEASE_DEADLINE.set(deadline)
                try:
                    result = await body()
                finally:
                    _LEASE_DEADLINE.reset(token)
        except TimeoutError as error:
            with suppress(Exception):
                await lineage.release(admitted)
            if not deadline.expired():
                raise
            raise OperationLeaseExpired(
                "the attempt reached its claim lease deadline and stopped; a later attempt "
                "classifies and continues the unit (REQ-CP-EXEC-014)"
            ) from error
        except (Exception, asyncio.CancelledError):
            # The holder knows it is stopping: release the lease so the next attempt takes it
            # over (advancing the fence) instead of waiting for it to expire. A lost worker
            # releases nothing; its lease simply expires with the attempt's deadline.
            with suppress(Exception):
                await lineage.release(admitted)
            raise
        with suppress(Exception):
            await lineage.release(admitted)
        return result

    async def _recover_or_dispatch(
        self,
        request: OperationExecutionRequest,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        admitted: UnitAttempt,
        lineage: CheckpointLineageService,
    ) -> OperationExecutionResult:
        admission = admitted.admission
        # `settled` (a concurrent holder may have settled before this lease was granted).
        settlement = await self._get_settlement(binding)
        if settlement is not None:
            await self._complete_post_effects(binding, settlement)
            return _public_result(binding, settlement)
        # `observed_unsettled`: the fenced result manifest settles without provider work.
        if admission.existing_result is not None:
            return await self._settle_recorded(binding, claim, admitted, admission.existing_result)
        if admission.existing_transition is not None:
            return await self._in_doubt(
                binding, claim, admitted, reason="unrecoverable_result_manifest"
            )
        accepted_leaf: QualifiedCheckpointKey | None = None
        reconciled = False
        if admission.incident is not None:
            decision = await self._reconciliation(binding, admitted, admission.incident)
            if decision is None:
                return await self._in_doubt(
                    binding, claim, admitted, reason=admission.incident.reason
                )
            await lineage.repository.apply_reconciliation(binding.request_scope, decision)
            if self._journal is not None:
                # RRM-007: the operation boundary applied the accepted decision.
                await self._journal.record_reconciliation_applied(binding, decision)
            if decision.decision == "abandon_unit":
                return await self._settle(
                    binding,
                    claim,
                    _failed_settlement(
                        binding,
                        failure_code="in_doubt_abandoned",
                        message="in_doubt unit abandoned by operator reconciliation",
                    ),
                    started_at=datetime.now(UTC),
                    attempt=admitted.attempt,
                    admitted=admitted,
                )
            if decision.decision == "start_new_generation":
                # REQ-CP-EXEC-005: this generation is fenced and can never settle the unit.
                return _unsettled_result(
                    binding,
                    failure_code="generation_superseded",
                    message="an accepted generation boundary superseded this generation",
                    incident_id=admission.incident.incident_id,
                )
            accepted_leaf = decision.accepted_checkpoint
            reconciled = True
        if self._fork_reuse is not None and not reconciled:
            # REQ-CP-EXEC-012: a fork-derived unit inside the reuse frontier settles by the
            # immutable source result; cognition is never re-run and nothing is copied.
            try:
                reused = await self._fork_reuse.reused_result(binding)
            except ForkRejected as error:
                if error.code == "fork_not_materialized":
                    # Transient: the derived root started before the fork's materialization
                    # committed. Nothing settles; the lease is released and a retry
                    # resolves the recorded reuse decision.
                    raise ForkMaterializationPending(error.message) from error
                # Terminal (`incompatible_restore`, `fork_lineage_missing`): fail closed and
                # terminate cleanly. The unit settles `failed` with the typed reason (claim,
                # reservation and lease released); cognition never runs, nothing is reused.
                return await self._settle(
                    binding,
                    claim,
                    _failed_settlement(binding, failure_code=error.code, message=error.message),
                    started_at=datetime.now(UTC),
                    attempt=admitted.attempt,
                    admitted=admitted,
                )
            if reused is not None:
                return await self._settle_reused(binding, claim, admitted, reused)
        if admitted.deep_binding is None and admission.prior_dispatch and not reconciled:
            # A native effect dispatched by an earlier holder is ambiguous: it is reconciled
            # through its claim, never repeated speculatively (REQ-CP-RUN-007).
            return await self._in_doubt(binding, claim, admitted, reason="ambiguous_native_effect")
        return await self._dispatch_and_settle(
            request,
            binding,
            claim,
            attempt=admitted.attempt,
            admitted=admitted,
            plan=lineage.plan(admitted, accepted_leaf=accepted_leaf),
        )

    async def _cancel_or_settle(
        self,
        request: OperationExecutionRequest,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        admitted: UnitAttempt,
        lineage: CheckpointLineageService,
    ) -> OperationExecutionResult:
        """Classify a unit being cancelled from durable facts and act once (EXEC-008)."""

        admission = admitted.admission
        settlement = await self._get_settlement(binding)
        if settlement is not None:
            await self._complete_post_effects(binding, settlement)
            return _public_result(binding, settlement)
        if admission.existing_result is not None:
            return await self._settle_recorded(binding, claim, admitted, admission.existing_result)
        if admission.existing_transition is not None:
            return await self._in_doubt(
                binding, claim, admitted, reason="unrecoverable_result_manifest"
            )
        accepted_leaf: QualifiedCheckpointKey | None = None
        if admission.incident is not None:
            # Step 5: an ambiguous effect is reconciled by the operator, never speculatively.
            decision = await self._reconciliation(binding, admitted, admission.incident)
            if decision is None:
                return await self._in_doubt(
                    binding, claim, admitted, reason=admission.incident.reason
                )
            await lineage.repository.apply_reconciliation(binding.request_scope, decision)
            if self._journal is not None:
                await self._journal.record_reconciliation_applied(binding, decision)
            if decision.decision == "abandon_unit":
                return await self._settle_cancelled(
                    binding, claim, admitted, None, None, failure_code="in_doubt_abandoned"
                )
            if decision.decision == "start_new_generation":
                return await self._settle_superseded(
                    binding, claim, admitted.attempt, admission.incident.incident_id
                )
            accepted_leaf = decision.accepted_checkpoint
        if admitted.deep_binding is None:
            if admission.prior_dispatch:
                return await self._in_doubt(
                    binding, claim, admitted, reason="ambiguous_native_effect"
                )
            return await self._settle_cancelled(binding, claim, admitted, None, None)
        plan = lineage.plan(admitted, accepted_leaf=accepted_leaf)
        invocation = await self._invocation(request, binding, plan)
        resolved_secrets = await self._secrets.resolve(binding.secret_refs)
        try:
            latest = await self._observe_latest(invocation, resolved_secrets)
        except CheckpointLineageInDoubt as error:
            return await self._in_doubt(
                binding, claim, admitted, reason=error.reason, candidates=error.candidates
            )
        return await self._settle_cancelled(binding, claim, admitted, plan, latest)

    async def _observe_latest(
        self, invocation: RuntimeInvocation, resolved_secrets: Mapping[str, str]
    ) -> RuntimeResult:
        observe = getattr(self._lanes.lane_for(invocation.binding), "observe_latest", None)
        if observe is None:
            raise CheckpointLineageInDoubt(
                "the runtime cannot report the latest durable checkpoint of a cancelled unit",
                reason="unclassifiable",
            )
        result: RuntimeResult = await observe(invocation, resolved_secrets)
        return result

    async def _invocation(
        self,
        request: OperationExecutionRequest,
        binding: OperationExecutionBinding,
        plan: CheckpointInvocationPlan | None,
    ) -> RuntimeInvocation:
        workspace = await self._sandbox.materialize(binding)
        return RuntimeInvocation(
            binding=binding,
            prompt_segments=request.prompt_segments,
            workspace=workspace,
            resolved_secret_names=tuple(
                sorted(f"{ref.provider}:{ref.key}" for ref in binding.secret_refs)
            ),
            checkpoint_plan=plan,
        )

    async def _settle_cancelled(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        admitted: UnitAttempt,
        plan: CheckpointInvocationPlan | None,
        latest: RuntimeResult | None,
        *,
        failure_code: str = "cancelled",
    ) -> OperationExecutionResult:
        """Steps 4-5: cancel the active async children, then settle `cancelled` once.

        The settlement records the usage the unit's completed calls incurred (a call in
        flight is unobservable and recorded by nothing; a child's unattributed usage stays
        pending on the child's own effect), releases the rest of the reservation, settles the
        unit's effect claim `cancelled`, and names the latest durable checkpoint as the
        result checkpoint so the namespace head advances over the partial lineage.
        """

        report_phase("settling")
        started_at = datetime.now(UTC)
        # FT-F3: the lane stopped cognition for this unit (the provider acknowledged).
        await self._fence_milestone(binding, "provider_acknowledged")
        children = await self._cancel_children(binding, started_at)
        if self._journal is not None and claim is not None:
            # Step 5 (REQ-CP-RUN-007 narrowed): a consequential effect the unit claimed and
            # could not settle is ambiguous; the unit parks `in_doubt` for the operator. The
            # children just reached are not ambiguous: their outcome is recorded on their
            # own effect, whose pending usage blocks the run's terminal settlement instead.
            handled = {item.effect_id for item in children}
            unsettled = tuple(
                effect_id
                for effect_id in await self._journal.unsettled_effect_ids(binding, claim)
                if effect_id not in handled
            )
            if unsettled:
                return await self._in_doubt(
                    binding,
                    claim,
                    admitted,
                    reason="unsettled_effect_claims",
                    unsettled_effect_ids=unsettled,
                )
        capture = latest.checkpoint if latest is not None else None
        settlement = OperationSettlement(
            settlement_id=operation_settlement_id(binding.binding_id),
            binding_id=binding.binding_id,
            status="cancelled",
            output_text=latest.output_text if latest is not None else "",
            usage=latest.usage if latest is not None else RuntimeUsage(),
            provider_run_id=latest.provider_run_id if latest is not None else None,
            event_payloads=(
                ({"cancelled_async_children": [item.model_dump(mode="json") for item in children]},)
                if children
                else ()
            ),
            failure_code=failure_code,
            failure_message="operation cancelled by the governed cancellation saga",
            settled_at=datetime.now(UTC),
            checkpoint_transition_id=(
                transition_id_for(plan) if plan is not None and capture is not None else None
            ),
            result_checkpoint=capture.result_key if capture is not None else None,
        )
        result = await self._settle(
            binding,
            claim,
            settlement,
            started_at=started_at,
            attempt=admitted.attempt,
            admitted=admitted,
            plan=plan if capture is not None else None,
            capture=capture,
        )
        await self._fence_milestone(binding, "settled")
        return result

    async def _fence_milestone(
        self,
        binding: OperationExecutionBinding,
        milestone: Literal["provider_acknowledged", "settled"],
    ) -> None:
        if self._stop_fences is None:
            return
        try:
            await self._stop_fences.record_milestone(
                binding.request_scope,
                binding.run_id,
                None,
                milestone,
                unit_key=binding.binding_id,
            )
        except Exception:
            # Report evidence only: the settlement above is the authority.
            _LOGGER.warning("stop fence milestone %s was not recorded", milestone, exc_info=True)

    async def _settle_superseded(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        attempt: OperationActivityAttempt,
        incident_id: str,
    ) -> OperationExecutionResult:
        """RRM-008 re-review: settle a generation superseded before the cancel (EXEC-005/008).

        An accepted `start_new_generation` fenced this generation before the run was
        cancelled, and a cancelling run admits no new generation (the reducer rejects it and
        cancellation re-executes nothing), so no later generation will ever settle the unit.
        Its claim, reservation and effect settle `cancelled` here, exactly once, with the
        `generation_superseded` code. The fenced generation writes nothing to its lineage (no
        result observation, no transition) and no cognition is observed or resumed; as with
        `abandon_unit`, no usage is attributed to the in_doubt generation. Active async
        children are cancelled first. An unsettled consequential effect is never settled
        over: the unit then stays unsettled for the operator.
        """

        report_phase("settling")
        started_at = datetime.now(UTC)
        children = await self._cancel_children(binding, started_at)
        if self._journal is not None and claim is not None:
            handled = {item.effect_id for item in children}
            unsettled = tuple(
                effect_id
                for effect_id in await self._journal.unsettled_effect_ids(binding, claim)
                if effect_id not in handled
            )
            if unsettled:
                return _unsettled_result(
                    binding,
                    failure_code="generation_superseded",
                    message=(
                        "the superseded generation holds unsettled effect claims; it awaits "
                        "operator reconciliation"
                    ),
                    incident_id=incident_id,
                )
        settlement = OperationSettlement(
            settlement_id=operation_settlement_id(binding.binding_id),
            binding_id=binding.binding_id,
            status="cancelled",
            event_payloads=(
                ({"cancelled_async_children": [item.model_dump(mode="json") for item in children]},)
                if children
                else ()
            ),
            failure_code="generation_superseded",
            failure_message="superseded generation settled by the governed cancellation saga",
            settled_at=datetime.now(UTC),
        )
        return await self._settle(
            binding,
            claim,
            settlement,
            started_at=started_at,
            attempt=attempt,
            admitted=None,
        )

    async def _cancel_journaled(
        self, request: OperationExecutionRequest, binding: OperationExecutionBinding
    ) -> bool:
        """Whether run control journaled the cancel this attempt is being asked to apply."""

        try:
            await self._authority.verify_cancellation(request, binding)
        except ValueError:
            return False
        return True

    async def _cancel_children(
        self, binding: OperationExecutionBinding, requested_at: datetime
    ) -> tuple[AsyncChildCancellationRecord, ...]:
        if self._children is None:
            return ()
        return await self._children.cancel_children(
            binding,
            reason="parent operation cancelled by the governed cancellation saga",
            requested_at=requested_at,
        )

    async def _dispatch_and_settle(
        self,
        request: OperationExecutionRequest,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        *,
        attempt: OperationActivityAttempt | None,
        admitted: UnitAttempt | None = None,
        plan: CheckpointInvocationPlan | None = None,
    ) -> OperationExecutionResult:
        runtime_invoked = False
        started_at = datetime.now(UTC)
        observed_usage = RuntimeUsage()
        capture: CheckpointCapture | None = None
        turns = TurnRecord()
        report_phase("dispatching")
        try:
            self._validate_policy_support(request)
            await self._assets.verify(binding)
            await self._mcp.verify_servers(binding)
            resolved_secrets = await self._secrets.resolve(binding.secret_refs)
            invocation = await self._invocation(request, binding, plan)
            await self._mailbox_turn_started(request, binding)
            runtime_invoked = True
            report_phase("cognition")
            try:
                runtime_result = await self._execute_turns(
                    request, binding, claim, admitted, invocation, resolved_secrets, turns
                )
            except InjectionParked as parked:
                # FT-F2: the interrupted turn's uncertain effects did not settle; no
                # replacement runs and the unit parks with a typed incident.
                assert admitted is not None
                return await self._in_doubt(
                    binding,
                    claim,
                    admitted,
                    reason="unsettled_effect_claims",
                    unsettled_effect_ids=parked.pending_effect_ids,
                )
            except asyncio.CancelledError:
                # REQ-CP-EXEC-008 step 3: a requested Temporal cancel reached this Activity
                # through its heartbeat and interrupted the in-flight step. The holder
                # records the latest durable checkpoint and settles `cancelled` itself;
                # nothing resumes. Every other cancellation of the task is not a cancel of
                # the unit (REQ-CP-EXEC-011): the lease deadline (REQ-CP-EXEC-014), a worker
                # shutdown, a heartbeat or start-to-close timeout, a pause or a reset. The
                # holder then stands down and the next attempt classifies and recovers. The
                # journal is the authority: run control must hold the accepted cancel.
                if (
                    admitted is None
                    or plan is None
                    or _lease_deadline_expired()
                    or not cancel_requested()
                    or not await self._cancel_journaled(request, binding)
                ):
                    raise
                _uncancel_current_task()
                try:
                    latest = await self._observe_latest(invocation, resolved_secrets)
                except CheckpointLineageInDoubt as error:
                    return await self._in_doubt(
                        binding, claim, admitted, reason=error.reason, candidates=error.candidates
                    )
                return await self._settle_cancelled(binding, claim, admitted, plan, latest)
            observed_usage = runtime_result.usage
            capture = runtime_result.checkpoint
            if plan is not None and capture is None:
                raise CheckpointLineageInDoubt(
                    "a lineage-qualified invocation returned no captured result checkpoint",
                    reason="unclassifiable",
                )
            _validate_bound_usage(binding, runtime_result.usage)
            settlement = OperationSettlement(
                settlement_id=operation_settlement_id(binding.binding_id),
                binding_id=binding.binding_id,
                status="completed",
                output_text=runtime_result.output_text,
                structured_output=runtime_result.structured_output,
                output_refs=runtime_result.output_refs,
                usage=runtime_result.usage,
                provider_run_id=runtime_result.provider_run_id,
                event_payloads=runtime_result.event_payloads,
                settled_at=datetime.now(UTC),
                checkpoint_transition_id=(transition_id_for(plan) if plan is not None else None),
                result_checkpoint=capture.result_key if capture is not None else None,
            )
        except CheckpointLineageInDoubt as error:
            # REQ-CP-DA-018: ambiguity is never settled as an ordinary failure, and never
            # re-executed: it becomes a typed incident awaiting `reconcile_unit`.
            if admitted is None:
                raise
            return await self._in_doubt(
                binding, claim, admitted, reason=error.reason, candidates=error.candidates
            )
        except CheckpointLineageError:
            raise
        except Exception as error:
            if admitted is not None and not isinstance(error, OperationBudgetViolation):
                ambiguity = await self._post_failure_ambiguity(
                    binding, claim, admitted, error, runtime_invoked=runtime_invoked
                )
                if ambiguity is not None:
                    reason, unsettled = ambiguity
                    return await self._in_doubt(
                        binding,
                        claim,
                        admitted,
                        reason=reason,
                        candidates=_failure_candidates(error),
                        unsettled_effect_ids=unsettled,
                    )
            # RRM-008 (RRM-004 review finding 4): a failed unit with a unique stamped lineage
            # records its latest durable checkpoint as the result checkpoint, so a shared
            # session namespace advances over the partial lineage instead of wedging the
            # next unit as `foreign_descendant`.
            if capture is None and isinstance(error, RuntimeInvocationFailure):
                latest_capture = error.latest_capture
                if isinstance(latest_capture, CheckpointCapture):
                    capture = latest_capture
            settlement = OperationSettlement(
                settlement_id=operation_settlement_id(binding.binding_id),
                binding_id=binding.binding_id,
                status="failed",
                usage=observed_usage,
                failure_code=(
                    "budget_exceeded"
                    if isinstance(error, OperationBudgetViolation)
                    else "unsupported_runtime_policy"
                    if isinstance(error, UnsupportedRuntimePolicy)
                    else "runtime_failed"
                    if runtime_invoked
                    else "preparation_failed"
                ),
                # Provider and secret exception text is deliberately not persisted.
                failure_message=f"{_error_type(error)} at governed operation boundary",
                settled_at=datetime.now(UTC),
                checkpoint_transition_id=(
                    transition_id_for(plan) if plan is not None and capture is not None else None
                ),
                result_checkpoint=capture.result_key if capture is not None else None,
            )
        report_phase("settling")
        settled = await self._settle(
            binding,
            claim,
            settlement,
            started_at=started_at,
            attempt=attempt,
            admitted=admitted,
            plan=plan if capture is not None else None,
            capture=capture,
        )
        await self._mailbox_turn_settled(
            request,
            binding,
            settlement,
            runtime_invoked=runtime_invoked,
            injected=tuple(turns.delivery_keys),
        )
        return settled

    async def _execute_turns(
        self,
        request: OperationExecutionRequest,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        admitted: UnitAttempt | None,
        invocation: RuntimeInvocation,
        resolved_secrets: Mapping[str, str],
        turns: TurnRecord,
    ) -> RuntimeResult:
        """The attempt's turn; with injections composed on a lineage-qualified unit, an
        admitted `interrupt_and_inject` interrupts and replaces it (FT-F2)."""

        lane = self._lane(binding)
        if (
            self._injections is None
            or admitted is None
            or request.runtime_unit is None
            or self._mailbox is None
        ):
            return await lane.execute(invocation, resolved_secrets)

        async def unsettled() -> tuple[str, ...]:
            if self._journal is None or claim is None:
                return ()
            return await self._journal.unsettled_effect_ids(binding, claim)

        return await self._injections.run_turn(
            request=request,
            lane=lane,
            invocation=invocation,
            secrets=resolved_secrets,
            unsettled=unsettled,
            record=turns,
        )

    async def _mailbox_turn_started(
        self, request: OperationExecutionRequest, binding: OperationExecutionBinding
    ) -> None:
        """FT-F1: the turn carrying delivered mailbox entries starts; they are consumed once
        (a retried attempt of the same bound operation finds them already consumed)."""

        if self._mailbox is None:
            return
        deep = request.deep_agent_binding
        await self._mailbox.turn_started(
            request.request_scope,
            request.identity.run_id,
            delivery_key=request.idempotency_key,
            lane_profile=self._lane(binding).describe().lane_profile,
            session_ref=(
                deep.cognitive_session_namespace if deep is not None else request.session_id
            ),
        )

    async def _mailbox_turn_settled(
        self,
        request: OperationExecutionRequest,
        binding: OperationExecutionBinding,
        settlement: OperationSettlement,
        *,
        runtime_invoked: bool,
        injected: tuple[str, ...] = (),
    ) -> None:
        if self._mailbox is None:
            return
        try:
            for key in injected:
                # FT-F2: the replacement turn settled; the injected Commands complete.
                await self._mailbox.turn_settled(
                    request.request_scope,
                    request.identity.run_id,
                    delivery_key=key,
                    lane_profile=self._lane(binding).describe().lane_profile,
                    succeeded=settlement.status == "completed",
                    turn_ref=settlement.provider_run_id,
                )
            if not runtime_invoked:
                # SPEC-06: a turn that failed before it started returns its entries.
                await self._mailbox.turn_not_started(
                    request.request_scope,
                    request.identity.run_id,
                    delivery_key=request.idempotency_key,
                )
                return
            await self._mailbox.turn_settled(
                request.request_scope,
                request.identity.run_id,
                delivery_key=request.idempotency_key,
                lane_profile=self._lane(binding).describe().lane_profile,
                succeeded=settlement.status == "completed",
                turn_ref=settlement.provider_run_id,
            )
        except Exception:
            # Receipts are evidence of an already settled unit; the settlement stands and the
            # terminal receipts close the Commands if this record is never written.
            _LOGGER.exception(
                "mailbox settlement receipts were not recorded",
                extra={"run_id": request.identity.run_id, "key": request.idempotency_key},
            )

    async def _post_failure_ambiguity(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        admitted: UnitAttempt,
        error: Exception,
        *,
        runtime_invoked: bool,
    ) -> tuple[InDoubtReason, tuple[str, ...]] | None:
        """REQ-CP-RUN-007 (narrowed): `failed` only without a terminal result and with every
        effect claim of the unit settled; otherwise the disposition is `in_doubt`."""

        terminal_absent = isinstance(error, RuntimeInvocationFailure) and (
            not error.terminal_result_observed
        )
        if isinstance(error, RuntimeInvocationFailure) and error.terminal_result_observed:
            return "terminal_result_after_failure", ()  # candidates travel on the error
        if (
            admitted.deep_binding is not None
            and admitted.admission.prior_dispatch
            and not terminal_absent
        ):
            # An earlier holder dispatched this unit generation and this attempt could not
            # classify the checkpointer: a terminal result is not provably absent.
            return "unclassifiable", ()
        if not runtime_invoked or self._journal is None or claim is None:
            return None
        unsettled = await self._journal.unsettled_effect_ids(binding, claim)
        if unsettled:
            return "unsettled_effect_claims", unsettled
        return None

    async def _settle(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        settlement: OperationSettlement,
        *,
        started_at: datetime,
        attempt: OperationActivityAttempt | None,
        admitted: UnitAttempt | None,
        plan: CheckpointInvocationPlan | None = None,
        capture: CheckpointCapture | None = None,
    ) -> OperationExecutionResult:
        before_authority: ResultManifestObserver | None = None
        if admitted is not None and self._lineage is not None:
            lineage = self._lineage
            unit_attempt = admitted
            settled = settlement

            async def record_result(
                manifest_ref: str, manifest_digest: str, manifest_size_bytes: int
            ) -> None:
                # REQ-CP-EXEC-014: the result manifest is fixed by the current fence holder
                # (with its checkpoint transition, by CAS on the namespace head) before any
                # authority settlement. A superseded holder's write is rejected here.
                await lineage.record_result(
                    unit_attempt,
                    binding_id=binding.binding_id,
                    settlement_id=settled.settlement_id,
                    status=settled.status,
                    result_manifest_ref=manifest_ref,
                    result_manifest_digest=manifest_digest,
                    result_manifest_size_bytes=manifest_size_bytes,
                    plan=plan,
                    capture=capture,
                )

            before_authority = record_result
        technical_attempt = attempt.attempt if attempt is not None else 1
        if self._journal is not None and claim is not None:
            settlement = await self._journal.settle(
                binding,
                claim,
                settlement,
                started_at=started_at,
                technical_attempt=technical_attempt,
                before_authority=before_authority,
            )
        else:
            if before_authority is not None:
                manifest = settlement_result_manifest(settlement)
                await before_authority(
                    f"operation-settlement:{settlement.settlement_id}",
                    f"sha256:{hashlib.sha256(manifest).hexdigest()}",
                    len(manifest),
                )
            settlement = await self._bindings.settle(
                settlement,
                request_scope=binding.request_scope,
            )
        await self._complete_post_effects(binding, settlement)
        report_phase("settled")
        return _public_result(binding, settlement)

    async def _settle_recorded(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        admitted: UnitAttempt,
        recorded: UnitResultObservation,
    ) -> OperationExecutionResult:
        """`observed_unsettled`: settle exactly the recorded manifest; no provider work."""

        if self._journal is None or claim is None:
            return await self._in_doubt(
                binding, claim, admitted, reason="unrecoverable_result_manifest"
            )
        settlement = await self._journal.load_result_manifest(
            binding,
            manifest_ref=recorded.result_manifest_ref,
            manifest_digest=recorded.result_manifest_digest,
            manifest_size_bytes=recorded.result_manifest_size_bytes,
        )
        if (
            settlement.settlement_id != recorded.settlement_id
            or settlement.status != recorded.status
            or settlement.checkpoint_transition_id != recorded.checkpoint_transition_id
        ):
            return await self._in_doubt(
                binding, claim, admitted, reason="unrecoverable_result_manifest"
            )

        async def verify_recorded(
            manifest_ref: str, manifest_digest: str, manifest_size_bytes: int
        ) -> None:
            if (manifest_ref, manifest_digest, manifest_size_bytes) != (
                recorded.result_manifest_ref,
                recorded.result_manifest_digest,
                recorded.result_manifest_size_bytes,
            ):
                raise CheckpointLineageConflict(
                    "the recovered settlement manifest differs from the recorded result"
                )

        settled = await self._journal.settle(
            binding,
            claim,
            settlement,
            started_at=min(datetime.now(UTC), settlement.settled_at),
            technical_attempt=admitted.attempt.attempt,
            before_authority=verify_recorded,
        )
        await self._complete_post_effects(binding, settled)
        return _public_result(binding, settled)

    async def _settle_reused(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        admitted: UnitAttempt,
        reused: ReusedSourceResult,
    ) -> OperationExecutionResult:
        source = reused.source_settlement
        settled_at = datetime.now(UTC)
        settlement = OperationSettlement(
            settlement_id=stable_id("operation-settlement", binding.binding_id),
            binding_id=binding.binding_id,
            status="completed",
            output_text=source.output_text,
            structured_output=source.structured_output,
            output_refs=source.output_refs,
            settled_at=settled_at,
            reused_result=reused.ref,
        )
        return await self._settle(
            binding,
            claim,
            settlement,
            started_at=settled_at,
            attempt=admitted.attempt,
            admitted=admitted,
        )

    async def _reconciliation(
        self,
        binding: OperationExecutionBinding,
        admitted: UnitAttempt,
        incident: UnitReconciliationIncident,
    ) -> UnitReconciliationDecision | None:
        """The accepted operator decision for an in_doubt unit generation, if any."""

        if self._journal is not None:
            decision = await self._journal.get_unit_reconciliation(
                binding,
                unit_key=admitted.unit.unit_key,
                execution_generation=admitted.execution_generation,
                incident_id=incident.incident_id,
            )
            if decision is not None:
                return decision
        if incident.status == "resolved":
            assert incident.decision is not None and incident.decision_id is not None
            return UnitReconciliationDecision(
                decision_id=incident.decision_id,
                unit_key=incident.unit_key,
                execution_generation=incident.execution_generation,
                incident_id=incident.incident_id,
                decision=incident.decision,
                accepted_checkpoint=incident.accepted_checkpoint,
                actor_id="recorded-incident",
                decided_at=incident.recorded_at,
            )
        return None

    async def _in_doubt(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim | None,
        admitted: UnitAttempt,
        *,
        reason: InDoubtReason,
        candidates: tuple[QualifiedCheckpointKey, ...] = (),
        unsettled_effect_ids: tuple[str, ...] = (),
    ) -> OperationExecutionResult:
        """REQ-CP-DA-018 / REQ-CP-RUN-007: a typed incident; no invocation; park the unit."""

        assert self._lineage is not None
        incident = await self._lineage.open_incident(
            admitted,
            binding=binding,
            reason=reason,
            candidates=candidates,
            unsettled_effect_ids=unsettled_effect_ids,
        )
        if self._journal is not None and claim is not None:
            await self._journal.record_in_doubt(binding, claim, incident)
        return _unsettled_result(
            binding,
            failure_code=incident.reason,
            message="runtime unit is in_doubt and awaits operator reconcile_unit",
            incident_id=incident.incident_id,
        )

    async def _complete_post_effects(
        self,
        binding: OperationExecutionBinding,
        settlement: OperationSettlement,
    ) -> None:
        if self._journal is not None:
            return
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

    async def _get_settlement(
        self,
        binding: OperationExecutionBinding,
    ) -> OperationSettlement | None:
        if self._journal is not None:
            return await self._journal.get_settlement(binding)
        return await self._bindings.get_settlement(
            binding.binding_id,
            request_scope=binding.request_scope,
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

    async def get_binding(
        self,
        semantic_attempt_key: str,
        *,
        request_scope: str | None = None,
    ) -> OperationExecutionBinding | None:
        binding = self._bindings.get(semantic_attempt_key)
        if (
            binding is not None
            and request_scope is not None
            and binding.request_scope != request_scope
        ):
            return None
        return deepcopy(binding)

    async def get_binding_by_id(
        self,
        binding_id: str,
        *,
        request_scope: str | None = None,
    ) -> OperationExecutionBinding | None:
        binding = next(
            (item for item in self._bindings.values() if item.binding_id == binding_id),
            None,
        )
        if (
            binding is not None
            and request_scope is not None
            and binding.request_scope != request_scope
        ):
            return None
        return deepcopy(binding)

    async def create_binding(
        self,
        binding: OperationExecutionBinding,
        *,
        request_scope: str | None = None,
    ) -> OperationExecutionBinding:
        if request_scope is not None and binding.request_scope != request_scope:
            raise ValueError("operation binding write cannot cross request scope")
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

    async def get_settlement(
        self,
        binding_id: str,
        *,
        request_scope: str | None = None,
    ) -> OperationSettlement | None:
        if request_scope is not None:
            binding = next(
                (item for item in self._bindings.values() if item.binding_id == binding_id),
                None,
            )
            if binding is None or binding.request_scope != request_scope:
                return None
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

    async def settle(
        self,
        settlement: OperationSettlement,
        *,
        request_scope: str | None = None,
    ) -> OperationSettlement:
        if request_scope is not None:
            binding = next(
                (
                    item
                    for item in self._bindings.values()
                    if item.binding_id == settlement.binding_id
                ),
                None,
            )
            if binding is None or binding.request_scope != request_scope:
                raise ValueError("operation settlement cannot cross request scope")
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


def bind_operation_execution_request(
    request: OperationExecutionRequest,
) -> OperationExecutionBinding:
    """Create the immutable OEB document without invoking its provider side effect."""

    fingerprint = contract_fingerprint(request, exclude={"requested_at"})
    return _binding_for(request, fingerprint)


def _binding_for(request: OperationExecutionRequest, fingerprint: str) -> OperationExecutionBinding:
    binding_id = stable_id(
        "operation-binding",
        f"{request.request_scope}:{request.identity.semantic_key}",
    )
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
        output_schema=request.output_schema,
        guardrails=request.guardrails,
        delegations=request.delegations,
        delegation_ceiling=request.delegation_ceiling,
        session_id=request.session_id,
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
        execution_runtime=request.execution_runtime,
        native_placement=request.native_placement,
        deep_agent_binding=request.deep_agent_binding,
        lane_profile=request.lane_profile,
        cursor_binding=request.cursor_binding,
        side_effect_key=request.idempotency_key,
        bound_at=request.requested_at,
        runtime_unit=request.runtime_unit,
    )


def settlement_result_manifest(settlement: OperationSettlement) -> bytes:
    """Canonical immutable result manifest bytes; transcripts and payloads stay out."""

    excluded = {"output_text", "structured_output", "event_payloads"}
    if settlement.reused_result is None:
        # RRM-006: the field is additive; manifests without a reused result stay byte-identical.
        excluded.add("reused_result")
    return json.dumps(
        settlement.model_dump(mode="json", exclude=excluded),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


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
        unit_key=binding.runtime_unit.unit_key if binding.runtime_unit is not None else None,
        checkpoint_transition_id=settlement.checkpoint_transition_id,
        result_checkpoint=settlement.result_checkpoint,
    )


def _failed_settlement(
    binding: OperationExecutionBinding, *, failure_code: str, message: str
) -> OperationSettlement:
    return OperationSettlement(
        settlement_id=operation_settlement_id(binding.binding_id),
        binding_id=binding.binding_id,
        status="failed",
        failure_code=failure_code,
        failure_message=message,
        settled_at=datetime.now(UTC),
    )


def _unsettled_result(
    binding: OperationExecutionBinding,
    *,
    failure_code: str,
    message: str,
    incident_id: str,
) -> OperationExecutionResult:
    """An `in_doubt` disposition: no settlement exists; the claim stays unsettled."""

    return OperationExecutionResult(
        binding_id=binding.binding_id,
        semantic_attempt_key=binding.semantic_attempt_key,
        status="in_doubt",
        failure_code=failure_code,
        failure_message=message,
        unit_key=binding.runtime_unit.unit_key if binding.runtime_unit is not None else None,
        reconciliation_incident_id=incident_id,
    )


def _failure_candidates(error: BaseException) -> tuple[QualifiedCheckpointKey, ...]:
    if not isinstance(error, RuntimeInvocationFailure):
        return ()
    return tuple(item for item in error.candidates if isinstance(item, QualifiedCheckpointKey))


def _error_type(error: BaseException) -> str:
    if isinstance(error, RuntimeInvocationFailure):
        return error.error_type
    return type(error).__name__


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


def operation_settlement_id(binding_id: str) -> str:
    """The one settlement identity of a bound operation attempt (any status).

    Run control records the operation's usage and settlement evidence under this identity,
    so a family that consumes the settlement (RRM-016) can find exactly that record.
    """

    return stable_id("operation-settlement", binding_id)
