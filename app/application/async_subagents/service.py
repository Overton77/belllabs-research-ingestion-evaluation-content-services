"""Async subagent lifecycle under BellLabs authority (CON-CP-ASYNC-SUBAGENT-V1, AMD-RRM-001).

The service owns the order REQ-CP-DA-008 prescribes: Mongo detail, PostgreSQL authority and
the parent's run-control effect claim exist before any provider submission; submission is
fenced per child so at most one submitter runs at a time and it looks a run up by the
BellLabs spawn key before creating one; ambiguity becomes `in_doubt` with a governed incident
and is left only by observation or by the typed decisions `adopt_provider_run` and
`orphan_child`. Provider state is observation; every accepted decision is BellLabs state.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, ConfigDict, Field

from app.domain.operation_execution.async_subagent_reconciliation import (
    ASYNC_CHILD_RECONCILE_PERMISSION,
    CANCELLED_RUN_DISPOSITIONS,
    TERMINAL_PROVIDER_RUN_STATUSES,
    AsyncProviderRunObservation,
    AsyncProviderRunRecord,
    AsyncServedGraphIdentity,
    AsyncSpawnKeyObservation,
    AsyncSubagentIncident,
    AsyncSubagentReconciliationDecision,
    aggregate_child_usage,
    async_subagent_incident_id,
    classify_spawn_key_observation,
    lifecycle_for_provider_status,
)
from app.domain.operation_execution.contracts import (
    ACTIVE_ASYNC_SUBAGENT_LIFECYCLES,
    AsyncChildCancellationRecord,
    AsyncProviderCheckpointKey,
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    AsyncSubagentExecution,
    AsyncSubagentInDoubtReason,
    AsyncSubagentLifecycle,
    AsyncSubagentMessage,
    AsyncSubagentResultManifest,
    AsyncSubagentUsage,
    OperationExecutionBinding,
    ParentAsyncSubagentLink,
)
from app.domain.run_control.contracts import ActorContext

DEFAULT_SUBMISSION_LEASE = timedelta(seconds=120)
# The fenced work (identity check, submission, first observation) is bounded below the lease
# so a holder never outlives it, whatever the SDK's own timeouts (RRM-013 review N4).
SUBMISSION_LEASE_MARGIN_SECONDS = 10.0


class AsyncSubagentError(RuntimeError):
    pass


class AsyncSubagentSubmissionInProgress(AsyncSubagentError):
    """Another live submitter holds the child's submission fence; retry later."""


class AsyncServedGraphMismatch(AsyncSubagentError):
    """REQ-CP-DA-019: the served graph identity differs from the frozen contract."""


class AsyncSubagentDecisionRejected(AsyncSubagentError):
    """An operator decision lacks authority or lost the exclusive claim on the incident."""


class AsyncProviderAmbiguity(AsyncSubagentError):
    """The provider shows an ambiguous binding for the spawn key (REQ-CP-DA-008)."""

    def __init__(
        self,
        message: str,
        *,
        reason: AsyncSubagentInDoubtReason,
        observation: AsyncSpawnKeyObservation,
    ) -> None:
        super().__init__(message)
        self.reason: AsyncSubagentInDoubtReason = reason
        self.observation = observation


class AsyncSubagentSpawnRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1)
    parent_run_id: str = Field(min_length=1)
    parent_operation_id: str = Field(min_length=1)
    parent_binding_id: str = Field(min_length=1)
    execution_generation: int = Field(ge=1)
    contract: AsyncSubagentContract
    dependency_class: AsyncSubagentDependencyClass
    objective_ref: str = Field(min_length=1)
    objective: str = Field(min_length=1, max_length=100_000)
    context_slice_ref: str = Field(min_length=1)
    reservation_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    requested_at: datetime
    # The parent operation's budget reservation the child's reservation is carved from.
    parent_reservation_id: str | None = None


class ProviderAsyncObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["pending", "running", "waiting", "success", "error", "cancelled", "orphaned"]
    thread_id: str
    run_id: str
    output_ref: str | None = None
    output_text: str | None = None
    evidence_refs: tuple[str, ...] = ()
    # REQ-CP-DA-011: typed provider usage and qualified checkpoint, present on success.
    usage: AsyncSubagentUsage | None = None
    checkpoint: AsyncProviderCheckpointKey | None = None
    effect_refs: tuple[str, ...] = ()
    cancellation_receipt: Literal["provider_acknowledged", "ambiguous"] | None = None
    observed_at: datetime


class AsyncSubagentDetailRepository(Protocol):
    async def create_before_submit(
        self,
        request_scope: str,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        link: ParentAsyncSubagentLink,
    ) -> AsyncSubagentExecution: ...

    async def get_execution(
        self, request_scope: str, child_execution_id: str
    ) -> AsyncSubagentExecution: ...
    async def get_contract(self, request_scope: str, contract_id: str) -> AsyncSubagentContract: ...
    async def get_link(
        self, request_scope: str, child_execution_id: str
    ) -> ParentAsyncSubagentLink: ...
    async def save_execution(
        self, request_scope: str, execution: AsyncSubagentExecution
    ) -> None: ...
    async def save_link(self, request_scope: str, link: ParentAsyncSubagentLink) -> None: ...


class AsyncSubagentAuthorityPort(Protocol):
    """The child's PostgreSQL authority: reservation, fence, facts, decisions, provider runs."""

    async def reserve_and_admit(
        self, request: AsyncSubagentSpawnRequest, child_execution_id: str, link_id: str
    ) -> None: ...

    async def acquire_submission_fence(
        self,
        request_scope: str,
        child_execution_id: str,
        *,
        holder: str,
        lease_expires_at: datetime,
        now: datetime,
    ) -> int | None:
        """Grant the next fence to `holder`, or None while another live lease stands."""
        ...

    async def release_submission_fence(
        self, request_scope: str, child_execution_id: str, fence: int
    ) -> None: ...

    async def record_execution_state(
        self, request_scope: str, execution: AsyncSubagentExecution
    ) -> None:
        """Mirror lifecycle, provider binding and incident onto the authority row."""
        ...

    async def record_provider_run(
        self, request_scope: str, record: AsyncProviderRunRecord
    ) -> None: ...

    async def cancelled_provider_run_ids(
        self, request_scope: str, child_execution_id: str
    ) -> frozenset[str]:
        """Runs BellLabs cancelled as duplicates or orphans; never candidates again."""
        ...

    async def list_provider_runs(
        self, request_scope: str, child_execution_id: str
    ) -> tuple[AsyncProviderRunRecord, ...]: ...

    async def open_incident(self, incident: AsyncSubagentIncident) -> AsyncSubagentIncident: ...
    async def get_incident(
        self, request_scope: str, child_execution_id: str
    ) -> AsyncSubagentIncident | None: ...
    async def resolve_incident(self, incident: AsyncSubagentIncident) -> None: ...

    async def claim_reconciliation_decision(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: AsyncSubagentReconciliationDecision,
        *,
        decision_id: str,
        adopted_run_id: str | None,
        reason: str,
    ) -> bool:
        """Record the child's single decision command; False when another decision holds it."""
        ...

    async def record_fact(
        self, request_scope: str, child_execution_id: str, fact_kind: str, fact_ref: str
    ) -> None: ...

    async def append_message(self, request_scope: str, message: AsyncSubagentMessage) -> None: ...
    async def request_cancellation(
        self, request_scope: str, child_execution_id: str, reason: str
    ) -> None: ...
    async def list_child_ids(
        self, request_scope: str, parent_binding_id: str
    ) -> tuple[str, ...]:
        """RRM-008: every child the parent operation binding spawned, in admission order."""
        ...
    async def decide_result(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: Literal["admit", "conditionally_admit", "reject", "defer"],
        manifest_digest: str,
    ) -> None: ...

    async def settle(
        self, request_scope: str, child_execution_id: str, settlement_ref: str
    ) -> None: ...


class AsyncSubagentParentEffectsPort(Protocol):
    """The parent run's run-control authority: reservation, effect claim, usage settlement.

    REQ-CP-RUN-009 (AMD-RRM-001): the child's reservation is carved from the parent run's budget,
    the child is an effect claim with `operation_ref = parent binding` (so the parent's narrowed
    post-dispatch rule sees an unsettled child), and its usage settles exactly once.
    """

    async def reserve_and_claim(
        self, request: AsyncSubagentSpawnRequest, child_execution_id: str
    ) -> None: ...

    async def observe(
        self,
        request_scope: str,
        parent_run_id: str,
        child_execution_id: str,
        *,
        disposition: Literal["pending", "ambiguous", "succeeded", "failed", "cancelled"],
        observation_id: str,
        provider_effect_ref: str | None,
        evidence_refs: tuple[str, ...],
        observed_at: datetime,
    ) -> None: ...

    async def settle_usage(
        self,
        request_scope: str,
        parent_run_id: str,
        child_execution_id: str,
        *,
        reservation_id: str,
        budget_limits: dict[str, int],
        attributed_amounts: dict[str, int],
        pending_amounts: dict[str, int],
        outcome: Literal["succeeded", "failed", "cancelled"],
        observation_id: str,
        settlement_ref: str,
        settlement_revision: int,
        settled_at: datetime,
    ) -> Literal["settled", "pending_usage"]: ...


class AsyncSubagentProviderPort(Protocol):
    async def verify_served_graph(
        self, contract: AsyncSubagentContract
    ) -> AsyncServedGraphIdentity: ...
    async def start(
        self,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        objective: str,
    ) -> ProviderAsyncObservation: ...
    async def observe_spawn_key(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution
    ) -> AsyncSpawnKeyObservation: ...
    async def check(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution
    ) -> ProviderAsyncObservation: ...
    async def update(
        self,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        message: AsyncSubagentMessage,
    ) -> ProviderAsyncObservation: ...
    async def cancel(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution
    ) -> ProviderAsyncObservation: ...
    async def cancel_run(
        self,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        run_id: str,
    ) -> AsyncProviderRunRecord: ...
    async def list(
        self, executions: tuple[tuple[AsyncSubagentContract, AsyncSubagentExecution], ...]
    ) -> tuple[ProviderAsyncObservation, ...]: ...


class AsyncSubagentService:
    """Coordinates subordinate detail, PostgreSQL authority, parent effects and the provider."""

    def __init__(
        self,
        details: AsyncSubagentDetailRepository,
        authority: AsyncSubagentAuthorityPort,
        provider: AsyncSubagentProviderPort,
        *,
        parent_effects: AsyncSubagentParentEffectsPort | None = None,
        allow_new_spawns: bool = False,
        submitter_identity: str = "belllabs-async-submitter",
        submission_lease: timedelta = DEFAULT_SUBMISSION_LEASE,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._details = details
        self._authority = authority
        self._provider = provider
        self._parent_effects = parent_effects
        self._allow_new_spawns = allow_new_spawns
        self._submitter_identity = submitter_identity
        self._submission_lease = submission_lease
        self._now = now or (lambda: datetime.now(UTC))

    # ------------------------------------------------------------------ spawn and submit

    async def spawn(self, request: AsyncSubagentSpawnRequest) -> AsyncSubagentExecution:
        if request.dependency_class not in request.contract.dependency_classes:
            raise AsyncSubagentError("dependency class exceeds the immutable contract ceiling")
        child_id = str(
            uuid5(
                NAMESPACE_URL, f"async-child:{request.parent_binding_id}:{request.idempotency_key}"
            )
        )
        link_id = str(uuid5(NAMESPACE_URL, f"async-link:{child_id}"))
        execution = AsyncSubagentExecution(
            child_execution_id=child_id,
            contract_id=request.contract.contract_id,
            contract_digest=request.contract.contract_digest,
            parent_run_id=request.parent_run_id,
            parent_operation_id=request.parent_operation_id,
            parent_binding_id=request.parent_binding_id,
            execution_generation=request.execution_generation,
            objective_ref=request.objective_ref,
            context_slice_ref=request.context_slice_ref,
            reservation_id=request.reservation_id,
            lifecycle=AsyncSubagentLifecycle.PROPOSED,
            created_at=request.requested_at,
            updated_at=request.requested_at,
        )
        link = ParentAsyncSubagentLink(
            link_id=link_id,
            child_execution_id=child_id,
            parent_run_id=request.parent_run_id,
            parent_operation_id=request.parent_operation_id,
            dependency_class=request.dependency_class,
            timeout_at=request.requested_at + timedelta(seconds=request.contract.timeout_seconds),
            cancellation_propagation=request.contract.cancellation_propagation,
            late_result_policy=request.contract.late_result_policy,
            fallback_policy=request.contract.fallback_policy,
            result_admission_policy_ref=request.contract.result_admission_policy_ref,
            created_at=request.requested_at,
            updated_at=request.requested_at,
        )
        if not self._allow_new_spawns:
            # The gate forbids new children; a child that already exists (a retry after a crash)
            # still resumes through the fenced path, so reconciliation never depends on the gate.
            try:
                prior = await self._details.get_execution(request.request_scope, child_id)
            except AsyncSubagentError:
                raise AsyncSubagentError(
                    "new async subagent spawning is feature-gated; reconciliation remains available"
                ) from None
        else:
            prior = await self._details.create_before_submit(
                request.request_scope, request.contract, execution, link
            )
        if prior.lifecycle == AsyncSubagentLifecycle.PROPOSED:
            # REQ-CP-DA-008: reservation, link, PostgreSQL authority and the parent's effect
            # claim all exist before any provider submission.
            await self._authority.reserve_and_admit(request, child_id, link_id)
            if self._parent_effects is not None:
                await self._parent_effects.reserve_and_claim(request, child_id)
            admitted = prior.model_copy(
                update={
                    "lifecycle": AsyncSubagentLifecycle.ADMITTED,
                    "updated_at": request.requested_at,
                }
            )
            await self._details.save_execution(request.request_scope, admitted)
            await self._authority.record_execution_state(request.request_scope, admitted)
        elif prior.lifecycle == AsyncSubagentLifecycle.ADMITTED:
            # RRM-001 section 7 #10: an admitted child left unsubmitted (a crash before
            # submission) is resumed by the same fenced submission path, never returned idle.
            admitted = prior
        else:
            # Submitted, terminal or in_doubt: never a second spawn (REQ-CP-DA-008).
            return prior
        return await self._submit_fenced(
            request.request_scope, request.contract, admitted, request.objective
        )

    async def _submit_fenced(
        self,
        request_scope: str,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        objective: str,
    ) -> AsyncSubagentExecution:
        now = self._now()
        fence = await self._authority.acquire_submission_fence(
            request_scope,
            execution.child_execution_id,
            holder=self._submitter_identity,
            lease_expires_at=now + self._submission_lease,
            now=now,
        )
        if fence is None:
            raise AsyncSubagentSubmissionInProgress(
                "another live submitter holds the child's submission fence; retry after its "
                "lease expires or its observation is visible (REQ-CP-DA-008)"
            )
        work_budget = max(
            1.0, self._submission_lease.total_seconds() - SUBMISSION_LEASE_MARGIN_SECONDS
        )
        try:
            try:
                # The holder's provider work never outlives its lease (REQ-CP-DA-008): the
                # identity check, the submission and its first observation share one budget
                # below the lease, whatever the SDK's own timeouts.
                async with asyncio.timeout(work_budget):
                    # REQ-CP-DA-019: the served identity is verified before the first
                    # submission. A mismatch fails closed with no provider work.
                    served = await self._provider.verify_served_graph(contract)
                    if not served.matches(contract):
                        raise AsyncServedGraphMismatch(
                            "served graph identity differs from the frozen async subagent contract"
                        )
                    observation = await self._provider.start(contract, execution, objective)
            except AsyncProviderAmbiguity as ambiguity:
                return await self._enter_in_doubt(
                    request_scope,
                    contract,
                    execution,
                    reason=ambiguity.reason,
                    observation=ambiguity.observation,
                )
            except AsyncServedGraphMismatch:
                raise
            except Exception as submit_error:
                # The submission's result is unknown (an error or the lease budget ran out):
                # classify from the provider's durable state instead of guessing. Nothing
                # here makes the child `orphaned`.
                return await self._classify_unknown_submission(
                    request_scope, contract, execution, submit_error
                )
            return await self._apply_observation(request_scope, execution, observation)
        finally:
            await self._authority.release_submission_fence(
                request_scope, execution.child_execution_id, fence
            )

    async def _classify_unknown_submission(
        self,
        request_scope: str,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        submit_error: Exception,
    ) -> AsyncSubagentExecution:
        try:
            spawn_key = await self._provider.observe_spawn_key(contract, execution)
        except Exception:
            return await self._enter_in_doubt(
                request_scope,
                contract,
                execution,
                reason="submission_unobservable",
                observation=AsyncSpawnKeyObservation(
                    thread_id=execution.child_execution_id,
                    observed_at=self._now(),
                ),
            )
        classified = classify_spawn_key_observation(
            contract,
            spawn_key,
            resolved_run_ids=await self._resolved_runs(request_scope, execution),
        )
        if classified.outcome == "no_provider_run":
            # The submission provably did not happen: the child stays admitted and the caller's
            # retry resumes the fenced path (RRM-001 section 7 #10).
            raise submit_error
        if classified.outcome == "bound":
            assert classified.run is not None
            return await self._bind_observed_run(request_scope, execution, classified.run)
        assert classified.reason is not None
        return await self._enter_in_doubt(
            request_scope, contract, execution, reason=classified.reason, observation=spawn_key
        )

    # ------------------------------------------------------------------ in_doubt

    async def _enter_in_doubt(
        self,
        request_scope: str,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        *,
        reason: AsyncSubagentInDoubtReason,
        observation: AsyncSpawnKeyObservation,
    ) -> AsyncSubagentExecution:
        del contract
        now = observation.observed_at
        prior = await self._authority.get_incident(request_scope, execution.child_execution_id)
        if prior is None:
            revision = 1
        elif prior.status == "resolved":
            revision = prior.revision + 1
        else:
            revision = prior.revision
        incident = AsyncSubagentIncident(
            incident_id=async_subagent_incident_id(
                request_scope, execution.child_execution_id, revision
            ),
            request_scope=request_scope,
            parent_run_id=execution.parent_run_id,
            parent_binding_id=execution.parent_binding_id,
            child_execution_id=execution.child_execution_id,
            revision=revision,
            reason=reason,
            provider_thread_id=observation.thread_id,
            candidate_run_ids=tuple(run.run_id for run in observation.runs),
            recorded_at=now,
        )
        incident = await self._authority.open_incident(incident)
        updated = execution.model_copy(
            update={
                "lifecycle": AsyncSubagentLifecycle.IN_DOUBT,
                "in_doubt_reason": reason,
                "incident_id": incident.incident_id,
                "updated_at": now,
            }
        )
        await self._details.save_execution(request_scope, updated)
        await self._authority.record_execution_state(request_scope, updated)
        await self._authority.record_fact(
            request_scope, execution.child_execution_id, "lifecycle", f"in_doubt:{reason}"
        )
        if self._parent_effects is not None:
            # REQ-CP-RUN-007: the parent's effect claim for this child becomes ambiguous and
            # stays unsettled, so the parent cannot settle `failed` over it.
            await self._parent_effects.observe(
                request_scope,
                execution.parent_run_id,
                execution.child_execution_id,
                disposition="ambiguous",
                observation_id=f"async-in-doubt:{incident.incident_id}",
                provider_effect_ref=None,
                evidence_refs=(incident.incident_id,),
                observed_at=now,
            )
        return updated

    async def reconcile_in_doubt(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: AsyncSubagentReconciliationDecision,
        *,
        actor: ActorContext,
        decision_id: str,
        run_id: str | None = None,
        reason: str,
        decided_at: datetime,
    ) -> AsyncSubagentExecution:
        """REQ-CP-DA-008 operator decisions: adopt one provider run, or orphan the child.

        The decision requires the privileged permission `workflow_run.reconcile_async_child`
        and is exclusive: the child's single decision command is claimed in authority before
        any provider run is cancelled, so of two concurrent decisions exactly one acts. The
        served identity is verified before a run is adopted; `orphan_child` cancels every run
        and needs no identity check, so it remains the exit for a child that is in doubt for
        `graph_identity_mismatch` (re-review G2). Every other provider run carrying the spawn
        key is cancelled; a run whose terminal status is not observed stays `cancel_ambiguous`
        and a candidate until it is. Usage of cancelled runs is pending.
        """

        if ASYNC_CHILD_RECONCILE_PERMISSION not in actor.permissions:
            raise AsyncSubagentDecisionRejected(
                f"reconciling an in_doubt async child requires {ASYNC_CHILD_RECONCILE_PERMISSION}"
            )
        execution = await self._details.get_execution(request_scope, child_execution_id)
        incident = await self._authority.get_incident(request_scope, child_execution_id)
        if incident is not None and incident.status == "resolved":
            # An exact resend of the accepted decision is idempotent; any other decision for
            # a resolved incident is refused.
            if incident.decision_id == decision_id:
                return execution
            raise AsyncSubagentDecisionRejected(
                "the incident is already resolved by another decision"
            )
        if execution.lifecycle != AsyncSubagentLifecycle.IN_DOUBT:
            raise AsyncSubagentError("only an in_doubt child accepts a reconciliation decision")
        if incident is None or incident.incident_id != execution.incident_id:
            raise AsyncSubagentError("the child's in_doubt incident is not recorded")
        if (decision == "adopt_provider_run") != (run_id is not None):
            raise AsyncSubagentError("adopt_provider_run names exactly one provider run")
        contract = await self._details.get_contract(request_scope, execution.contract_id)
        link = await self._details.get_link(request_scope, child_execution_id)
        if decision == "adopt_provider_run":
            # REQ-CP-DA-019: the deployment's served identity is verified before a run is
            # adopted; a mismatch leaves the child in doubt with `orphan_child` as its exit.
            served = await self._provider.verify_served_graph(contract)
            if not served.matches(contract):
                raise AsyncServedGraphMismatch(
                    "served graph identity differs from the frozen async subagent contract"
                )
        spawn_key = await self._provider.observe_spawn_key(contract, execution)
        observed_ids = {run.run_id for run in spawn_key.runs}
        if run_id is not None and run_id not in observed_ids:
            raise AsyncSubagentError("the named run does not carry this child's spawn key")
        claimed = await self._authority.claim_reconciliation_decision(
            request_scope,
            child_execution_id,
            decision,
            decision_id=decision_id,
            adopted_run_id=run_id,
            reason=reason,
        )
        if not claimed:
            raise AsyncSubagentDecisionRejected(
                "another decision already holds this child's incident"
            )
        recorded = {
            record.provider_run_id: record
            for record in await self._authority.list_provider_runs(
                request_scope, child_execution_id
            )
        }
        cancelled_kind = (
            "duplicate_cancelled" if decision == "adopt_provider_run" else "orphaned_cancelled"
        )
        for run in spawn_key.runs:
            if run.run_id == run_id:
                continue
            prior = recorded.get(run.run_id)
            if prior is not None and prior.disposition in CANCELLED_RUN_DISPOSITIONS:
                continue
            record = await self._provider.cancel_run(contract, execution, run.run_id)
            if record.disposition != "cancel_ambiguous":
                record = record.model_copy(update={"disposition": cancelled_kind})
            await self._authority.record_provider_run(request_scope, record)
        resolved = incident.model_copy(
            update={
                "status": "resolved",
                "resolution": decision,
                "decision_id": decision_id,
                "adopted_run_id": run_id,
            }
        )
        await self._authority.resolve_incident(resolved)
        await self._details.save_link(
            request_scope,
            link.model_copy(
                update={
                    "reconciliation_decision": decision,
                    "adopted_provider_run_id": run_id,
                    "updated_at": decided_at,
                }
            ),
        )
        if decision == "adopt_provider_run":
            adopted = next(run for run in spawn_key.runs if run.run_id == run_id)
            return await self._bind_observed_run(request_scope, execution, adopted)
        orphaned = execution.model_copy(
            update={
                "lifecycle": AsyncSubagentLifecycle.ORPHANED,
                "in_doubt_reason": None,
                "incident_id": None,
                "updated_at": decided_at,
            }
        )
        await self._details.save_execution(request_scope, orphaned)
        await self._authority.record_execution_state(request_scope, orphaned)
        await self._authority.record_fact(
            request_scope, child_execution_id, "lifecycle", "orphaned:orphan_child"
        )
        if self._parent_effects is not None:
            await self._parent_effects.observe(
                request_scope,
                execution.parent_run_id,
                child_execution_id,
                disposition="cancelled",
                observation_id=f"async-orphaned:{decision_id}",
                provider_effect_ref=None,
                evidence_refs=(incident.incident_id,),
                observed_at=decided_at,
            )
        return orphaned

    async def _bind_observed_run(
        self,
        request_scope: str,
        execution: AsyncSubagentExecution,
        run: AsyncProviderRunObservation,
    ) -> AsyncSubagentExecution:
        """Bind the unique verified provider run, then observe it through the stock check.

        The incident (if any) is resolved by observation and the run is recorded before the
        completed-run branch observes the result (RRM-013 review N3).
        """

        run_id = run.run_id
        status = run.status
        now = self._now()
        bound = execution.model_copy(
            update={
                "lifecycle": lifecycle_for_provider_status(status),
                "provider_thread_id": execution.child_execution_id,
                "provider_run_id": run_id,
                "in_doubt_reason": None,
                "incident_id": None,
                "updated_at": now,
            }
        )
        incident = await self._authority.get_incident(request_scope, execution.child_execution_id)
        if (
            execution.lifecycle == AsyncSubagentLifecycle.IN_DOUBT
            and incident is not None
            and incident.status != "resolved"
        ):
            await self._authority.resolve_incident(
                incident.model_copy(update={"status": "resolved", "resolution": "observation"})
            )
        await self._authority.record_provider_run(
            request_scope,
            AsyncProviderRunRecord(
                child_execution_id=execution.child_execution_id,
                provider_thread_id=execution.child_execution_id,
                provider_run_id=run_id,
                disposition="bound",
                provider_status=status,
                usage=run.usage
                or AsyncSubagentUsage(provider_run_id=run_id, attribution="pending"),
                observed_at=now,
            ),
        )
        if bound.lifecycle == AsyncSubagentLifecycle.COMPLETED:
            # A completed run needs its typed result: observe it through the provider check
            # so the manifest carries the qualified checkpoint and attributed usage.
            running = bound.model_copy(update={"lifecycle": AsyncSubagentLifecycle.RUNNING})
            await self._details.save_execution(request_scope, running)
            await self._authority.record_execution_state(request_scope, running)
            contract = await self._details.get_contract(request_scope, execution.contract_id)
            observation = await self._provider.check(contract, running)
            return await self._apply_observation(request_scope, running, observation)
        await self._details.save_execution(request_scope, bound)
        await self._authority.record_execution_state(request_scope, bound)
        await self._authority.record_fact(
            request_scope, execution.child_execution_id, "lifecycle", bound.lifecycle.value
        )
        if self._parent_effects is not None:
            await self._parent_effects.observe(
                request_scope,
                execution.parent_run_id,
                execution.child_execution_id,
                disposition="pending",
                observation_id=f"async-bound:{run_id}",
                provider_effect_ref=run_id,
                evidence_refs=(),
                observed_at=now,
            )
        return bound

    async def execution(
        self, request_scope: str, child_execution_id: str
    ) -> AsyncSubagentExecution:
        return await self._details.get_execution(request_scope, child_execution_id)

    async def link(self, request_scope: str, child_execution_id: str) -> ParentAsyncSubagentLink:
        return await self._details.get_link(request_scope, child_execution_id)

    # ------------------------------------------------------------------ observation

    async def reconcile(
        self, request_scope: str, child_execution_id: str
    ) -> AsyncSubagentExecution:
        """Observe the provider's durable state and apply it; never spawn.

        An admitted child without a binding and an `in_doubt` child are classified by their
        spawn key (REQ-CP-DA-008): exactly one verified run binds, none orphans an in_doubt
        child, anything else stays or becomes in_doubt. A bound child is checked through the
        stock middleware tool.
        """

        execution = await self._details.get_execution(request_scope, child_execution_id)
        contract = await self._details.get_contract(request_scope, execution.contract_id)
        if execution.lifecycle not in ACTIVE_ASYNC_SUBAGENT_LIFECYCLES:
            return execution
        # Every reconnect re-verifies the spawn key: exactly one run (REQ-CP-DA-008).
        spawn_key = await self._provider.observe_spawn_key(contract, execution)
        await self._settle_ambiguous_cancellations(request_scope, child_execution_id, spawn_key)
        classified = classify_spawn_key_observation(
            contract,
            spawn_key,
            resolved_run_ids=await self._resolved_runs(request_scope, execution),
        )
        if classified.outcome == "no_provider_run":
            if execution.lifecycle == AsyncSubagentLifecycle.ADMITTED:
                return execution
            if execution.lifecycle == AsyncSubagentLifecycle.IN_DOUBT:
                return await self._orphan_by_observation(request_scope, execution, spawn_key)
            return await self._enter_in_doubt(
                request_scope,
                contract,
                execution,
                reason="provider_binding_lost",
                observation=spawn_key,
            )
        if classified.outcome == "in_doubt":
            assert classified.reason is not None
            if (
                execution.lifecycle == AsyncSubagentLifecycle.IN_DOUBT
                and execution.in_doubt_reason == classified.reason
            ):
                return execution
            return await self._enter_in_doubt(
                request_scope,
                contract,
                execution,
                reason=classified.reason,
                observation=spawn_key,
            )
        assert classified.run is not None
        if execution.provider_run_id != classified.run.run_id:
            return await self._bind_observed_run(request_scope, execution, classified.run)
        try:
            observation = await self._provider.check(contract, execution)
        except AsyncProviderAmbiguity as ambiguity:
            return await self._enter_in_doubt(
                request_scope,
                contract,
                execution,
                reason=ambiguity.reason,
                observation=ambiguity.observation,
            )
        return await self._apply_observation(request_scope, execution, observation)

    async def _settle_ambiguous_cancellations(
        self,
        request_scope: str,
        child_execution_id: str,
        spawn_key: AsyncSpawnKeyObservation,
    ) -> None:
        """A `cancel_ambiguous` run whose terminal status is now observed is cancelled for good."""

        records = await self._authority.list_provider_runs(request_scope, child_execution_id)
        ambiguous = {
            record.provider_run_id: record
            for record in records
            if record.disposition == "cancel_ambiguous"
        }
        if not ambiguous:
            return
        link = await self._details.get_link(request_scope, child_execution_id)
        kind = (
            "orphaned_cancelled"
            if link.reconciliation_decision == "orphan_child"
            else "duplicate_cancelled"
        )
        for run in spawn_key.runs:
            record = ambiguous.get(run.run_id)
            if record is not None and run.status in TERMINAL_PROVIDER_RUN_STATUSES:
                await self._authority.record_provider_run(
                    request_scope,
                    record.model_copy(
                        update={
                            "disposition": kind,
                            "provider_status": run.status,
                            "usage": AsyncSubagentUsage(
                                provider_run_id=run.run_id, attribution="pending"
                            ),
                            "observed_at": spawn_key.observed_at,
                        }
                    ),
                )

    async def _resolved_runs(
        self, request_scope: str, execution: AsyncSubagentExecution
    ) -> frozenset[str]:
        return await self._authority.cancelled_provider_run_ids(
            request_scope, execution.child_execution_id
        )

    async def _orphan_by_observation(
        self,
        request_scope: str,
        execution: AsyncSubagentExecution,
        spawn_key: AsyncSpawnKeyObservation,
    ) -> AsyncSubagentExecution:
        incident = await self._authority.get_incident(request_scope, execution.child_execution_id)
        if incident is not None and incident.status != "resolved":
            await self._authority.resolve_incident(
                incident.model_copy(update={"status": "resolved", "resolution": "observation"})
            )
        orphaned = execution.model_copy(
            update={
                "lifecycle": AsyncSubagentLifecycle.ORPHANED,
                "in_doubt_reason": None,
                "incident_id": None,
                "updated_at": spawn_key.observed_at,
            }
        )
        await self._details.save_execution(request_scope, orphaned)
        await self._authority.record_execution_state(request_scope, orphaned)
        await self._authority.record_fact(
            request_scope, execution.child_execution_id, "lifecycle", "orphaned:no_provider_run"
        )
        if self._parent_effects is not None:
            await self._parent_effects.observe(
                request_scope,
                execution.parent_run_id,
                execution.child_execution_id,
                disposition="cancelled",
                observation_id=f"async-orphaned:observation:{execution.child_execution_id}",
                provider_effect_ref=None,
                evidence_refs=(),
                observed_at=spawn_key.observed_at,
            )
        return orphaned

    async def send_message(
        self,
        request_scope: str,
        child_execution_id: str,
        *,
        payload_ref: str,
        correlation_id: str,
        created_at: datetime,
        context_authority: Literal[
            "instruction", "admitted_context", "untrusted_observation"
        ] = "instruction",
    ) -> AsyncSubagentMessage:
        execution = await self._details.get_execution(request_scope, child_execution_id)
        contract = await self._details.get_contract(request_scope, execution.contract_id)
        link = await self._details.get_link(request_scope, child_execution_id)
        sequence = 1 + max(
            (item.target_sequence for item in link.messages if item.direction == "parent_to_child"),
            default=0,
        )
        message = AsyncSubagentMessage(
            message_id=str(
                uuid5(NAMESPACE_URL, f"async-message:{child_execution_id}:parent:{sequence}")
            ),
            child_execution_id=child_execution_id,
            direction="parent_to_child",
            target_sequence=sequence,
            correlation_id=correlation_id,
            payload_ref=payload_ref,
            context_authority=context_authority,
            created_at=created_at,
        )
        await self._authority.append_message(request_scope, message)
        link = link.model_copy(
            update={"messages": (*link.messages, message), "updated_at": created_at}
        )
        await self._details.save_link(request_scope, link)
        observation = await self._provider.update(contract, execution, message)
        applied = message.model_copy(update={"receipt": "provider_applied"})
        link = link.model_copy(
            update={
                "messages": (*link.messages[:-1], applied),
                "updated_at": observation.observed_at,
            }
        )
        await self._details.save_link(request_scope, link)
        await self._apply_observation(request_scope, execution, observation)
        return applied

    async def receive_child_message(
        self,
        request_scope: str,
        child_execution_id: str,
        *,
        payload_ref: str,
        correlation_id: str,
        created_at: datetime,
    ) -> AsyncSubagentMessage:
        """Durably accept an observed child message without treating thread content as delivery."""

        link = await self._details.get_link(request_scope, child_execution_id)
        sequence = 1 + max(
            (item.target_sequence for item in link.messages if item.direction == "child_to_parent"),
            default=0,
        )
        message = AsyncSubagentMessage(
            message_id=str(
                uuid5(NAMESPACE_URL, f"async-message:{child_execution_id}:child:{sequence}")
            ),
            child_execution_id=child_execution_id,
            direction="child_to_parent",
            target_sequence=sequence,
            correlation_id=correlation_id,
            payload_ref=payload_ref,
            context_authority="untrusted_observation",
            receipt="checkpoint_committed",
            created_at=created_at,
        )
        await self._authority.append_message(request_scope, message)
        await self._details.save_link(
            request_scope,
            link.model_copy(
                update={"messages": (*link.messages, message), "updated_at": created_at}
            ),
        )
        return message

    async def cancel(
        self, request_scope: str, child_execution_id: str, reason: str, requested_at: datetime
    ) -> AsyncSubagentExecution:
        """Journal the parent's cancel intent, reach the provider run, record its acknowledgement.

        REQ-CP-DA-011 / REQ-CP-EXEC-008 (AMD-RRM-001): the provider's acknowledgement, or the
        ambiguity of its outcome, is recorded on the link; it is never assumed.
        """

        execution = await self._details.get_execution(request_scope, child_execution_id)
        contract = await self._details.get_contract(request_scope, execution.contract_id)
        link = await self._details.get_link(request_scope, child_execution_id)
        await self._authority.request_cancellation(request_scope, child_execution_id, reason)
        link = link.model_copy(
            update={
                "cancellation_requested": True,
                "cancellation_reason": reason,
                "updated_at": requested_at,
            }
        )
        await self._details.save_link(request_scope, link)
        if execution.provider_run_id is None:
            # Nothing was submitted (admitted or in_doubt without a binding): the provider has
            # no run to cancel, which is recorded as an acknowledged cancellation of nothing.
            await self._details.save_link(
                request_scope,
                link.model_copy(update={"cancellation_receipt": "provider_acknowledged"}),
            )
            return await self._apply_observation(
                request_scope,
                execution,
                ProviderAsyncObservation(
                    status="cancelled",
                    thread_id=execution.provider_thread_id or execution.child_execution_id,
                    run_id=execution.provider_run_id or "none",
                    cancellation_receipt="provider_acknowledged",
                    observed_at=requested_at,
                ),
                bind_provider=execution.provider_run_id is not None,
            )
        try:
            observation = await self._provider.cancel(contract, execution)
        except Exception:
            await self._details.save_link(
                request_scope, link.model_copy(update={"cancellation_receipt": "ambiguous"})
            )
            await self._authority.record_fact(
                request_scope, child_execution_id, "cancellation", "ambiguous"
            )
            raise
        await self._details.save_link(
            request_scope,
            link.model_copy(
                update={"cancellation_receipt": observation.cancellation_receipt or "ambiguous"}
            ),
        )
        await self._authority.record_fact(
            request_scope,
            child_execution_id,
            "cancellation",
            observation.cancellation_receipt or "ambiguous",
        )
        return await self._apply_observation(request_scope, execution, observation)

    async def cancel_children(
        self,
        binding: OperationExecutionBinding,
        *,
        reason: str,
        requested_at: datetime,
    ) -> tuple[AsyncChildCancellationRecord, ...]:
        """RRM-008 (REQ-CP-EXEC-008 step 4): cancel every active child of a parent unit.

        Each active child is cancelled through `cancel` (journaled intent, provider request,
        acknowledgement or ambiguity recorded). A child that reached a terminal lifecycle is
        then settled against the parent budget: a blocking child without a result decision is
        rejected first (its late result can never mutate the cancelled parent). Usage the
        provider could not attribute stays pending on the child's effect, which keeps the
        parent run from terminalizing until a privileged `reconcile_usage` settles it
        (REQ-CP-RUN-009). Nothing is re-spawned and no outcome is assumed.
        """

        records: list[AsyncChildCancellationRecord] = []
        for child_id in await self._authority.list_child_ids(
            binding.request_scope, binding.binding_id
        ):
            execution = await self._details.get_execution(binding.request_scope, child_id)
            receipt: Literal["provider_acknowledged", "ambiguous", "not_requested"] = (
                "not_requested"
            )
            if execution.lifecycle in ACTIVE_ASYNC_SUBAGENT_LIFECYCLES:
                try:
                    execution = await self.cancel(
                        binding.request_scope, child_id, reason, requested_at
                    )
                except Exception:  # noqa: BLE001 - the ambiguity is recorded, never assumed
                    execution = await self._details.get_execution(binding.request_scope, child_id)
                link = await self._details.get_link(binding.request_scope, child_id)
                receipt = link.cancellation_receipt or "ambiguous"
            link = await self._details.get_link(binding.request_scope, child_id)
            disposition: Literal["settled", "pending_usage", "unsettled"] = "unsettled"
            if execution.lifecycle in {
                AsyncSubagentLifecycle.COMPLETED,
                AsyncSubagentLifecycle.FAILED,
                AsyncSubagentLifecycle.CANCELLED,
                AsyncSubagentLifecycle.ORPHANED,
            }:
                if link.result_decision is None and link.dependency_class in {
                    AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
                    AsyncSubagentDependencyClass.DEGRADABLE_BLOCKING,
                }:
                    link = await self.decide_result(
                        binding.request_scope,
                        child_id,
                        "reject",
                        parent_open=True,
                        current_generation=execution.execution_generation,
                        decided_at=requested_at,
                    )
                settled = await self.settle(
                    binding.request_scope,
                    child_id,
                    f"settlement:{child_id}:cancelled-parent",
                    requested_at,
                )
                disposition = "settled" if settled.settled else "pending_usage"
                link = settled
            records.append(
                AsyncChildCancellationRecord(
                    child_execution_id=child_id,
                    effect_id=f"async-child-effect:{child_id}",
                    lifecycle=execution.lifecycle,
                    cancellation_receipt=receipt,
                    usage_disposition=disposition,
                    result_decision=link.result_decision,
                )
            )
        return tuple(records)

    async def decide_result(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: Literal["admit", "conditionally_admit", "reject", "defer"],
        *,
        parent_open: bool,
        current_generation: int,
        decided_at: datetime,
    ) -> ParentAsyncSubagentLink:
        execution = await self._details.get_execution(request_scope, child_execution_id)
        link = await self._details.get_link(request_scope, child_execution_id)
        manifest = execution.result_manifest
        if manifest is None:
            # A child that ended without a result (failed, cancelled, orphaned) can only be
            # rejected or deferred; admission always needs the typed manifest (REQ-CP-DA-011).
            if decision in {"admit", "conditionally_admit"} or execution.lifecycle not in {
                AsyncSubagentLifecycle.FAILED,
                AsyncSubagentLifecycle.CANCELLED,
                AsyncSubagentLifecycle.ORPHANED,
            }:
                raise AsyncSubagentError("result admission requires a typed result manifest")
            decision_ref = f"no-result:{execution.lifecycle.value}"
        else:
            decision_ref = manifest.manifest_digest
        late = not parent_open or execution.execution_generation != current_generation
        if late and decision in {"admit", "conditionally_admit"}:
            await self._authority.record_fact(
                request_scope, child_execution_id, "result", f"late_rejected:{decision_ref}"
            )
            raise AsyncSubagentError("late or superseded child result cannot mutate the parent")
        await self._authority.decide_result(
            request_scope, child_execution_id, decision, decision_ref
        )
        updated = link.model_copy(
            update={
                "result_decision": decision,
                "admitted_manifest_digest": decision_ref
                if decision in {"admit", "conditionally_admit"}
                else None,
                "updated_at": decided_at,
            }
        )
        await self._details.save_link(request_scope, updated)
        return updated

    async def settle(
        self, request_scope: str, child_execution_id: str, settlement_ref: str, settled_at: datetime
    ) -> ParentAsyncSubagentLink:
        """Settle the child against the parent run's budget ledger exactly once (REQ-CP-RUN-009).

        Usage is aggregated over every provider run the child had. A run whose usage the
        provider did not attribute keeps the parent's effect unsettled (`pending_usage`) until
        `reconcile_usage` supplies the amounts; the link is `settled` only on an actual
        settlement. Each attempt is a numbered settlement revision.
        """

        link = await self._details.get_link(request_scope, child_execution_id)
        execution = await self._details.get_execution(request_scope, child_execution_id)
        if link.result_decision is None and link.dependency_class in {
            AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
            AsyncSubagentDependencyClass.DEGRADABLE_BLOCKING,
        }:
            raise AsyncSubagentError("blocking child cannot settle before a result decision")
        if execution.lifecycle not in {
            AsyncSubagentLifecycle.COMPLETED,
            AsyncSubagentLifecycle.FAILED,
            AsyncSubagentLifecycle.CANCELLED,
            AsyncSubagentLifecycle.ORPHANED,
        }:
            raise AsyncSubagentError("only a terminal child settles against the parent budget")
        if link.settled:
            return link
        await self._authority.settle(request_scope, child_execution_id, settlement_ref)
        contract = await self._details.get_contract(request_scope, execution.contract_id)
        manifest = execution.result_manifest
        records = await self._authority.list_provider_runs(request_scope, child_execution_id)
        attributed, pending = aggregate_child_usage(
            records,
            contract.budget_limits,
            manifest_usage=manifest.usage if manifest is not None else None,
        )
        revision = link.settlement_revision + 1
        disposition: Literal["settled", "pending_usage"] = "settled"
        if self._parent_effects is not None:
            outcome: Literal["succeeded", "failed", "cancelled"] = (
                "succeeded"
                if execution.lifecycle == AsyncSubagentLifecycle.COMPLETED
                else "failed"
                if execution.lifecycle == AsyncSubagentLifecycle.FAILED
                else "cancelled"
            )
            disposition = await self._parent_effects.settle_usage(
                request_scope,
                execution.parent_run_id,
                child_execution_id,
                reservation_id=execution.reservation_id,
                budget_limits=dict(contract.budget_limits),
                attributed_amounts=attributed,
                pending_amounts=pending,
                outcome=outcome,
                observation_id=f"async-terminal:{child_execution_id}",
                settlement_ref=settlement_ref,
                settlement_revision=revision,
                settled_at=settled_at,
            )
        await self._authority.record_fact(
            request_scope,
            child_execution_id,
            "settlement",
            f"{disposition}:{settlement_ref}:revision:{revision}",
        )
        updated = link.model_copy(
            update={
                "settled": disposition == "settled",
                "settlement_revision": revision,
                "usage_disposition": disposition,
                "updated_at": settled_at,
            }
        )
        await self._details.save_link(request_scope, updated)
        return updated

    async def reconcile_usage(
        self,
        request_scope: str,
        child_execution_id: str,
        *,
        actor: ActorContext,
        run_usage: Mapping[str, AsyncSubagentUsage],
        settlement_ref: str,
        reconciled_at: datetime,
    ) -> ParentAsyncSubagentLink:
        """Supply provider-attributed usage for runs whose usage was pending, then settle.

        REQ-CP-RUN-009: pending usage is reconciled through the parent operation's authority
        (a later settlement revision that settles the outstanding usage), never dropped. The
        caller supplies attributed usage per provider run (for example from the provider's
        thread state once a cancelled run has a terminal checkpoint). Asserting attributed
        usage settles the parent's effect, so it requires the same privilege as the other
        reconciliation decisions, `workflow_run.reconcile_async_child` (re-review G3).
        """

        if ASYNC_CHILD_RECONCILE_PERMISSION not in actor.permissions:
            raise AsyncSubagentDecisionRejected(
                f"reconciling an async child's usage requires {ASYNC_CHILD_RECONCILE_PERMISSION}"
            )
        records = {
            record.provider_run_id: record
            for record in await self._authority.list_provider_runs(
                request_scope, child_execution_id
            )
        }
        for run_id, usage in run_usage.items():
            record = records.get(run_id)
            if record is None:
                raise AsyncSubagentError(f"provider run {run_id} is not recorded for the child")
            if usage.provider_run_id != run_id:
                raise AsyncSubagentError("usage names a different provider run")
            await self._authority.record_provider_run(
                request_scope,
                record.model_copy(update={"usage": usage, "observed_at": reconciled_at}),
            )
        return await self.settle(request_scope, child_execution_id, settlement_ref, reconciled_at)

    @staticmethod
    def parent_dependency(link: ParentAsyncSubagentLink) -> Literal["wait", "proceed", "degrade"]:
        if link.dependency_class == AsyncSubagentDependencyClass.REQUIRED_BLOCKING:
            return "proceed" if link.result_decision in {"admit", "conditionally_admit"} else "wait"
        if link.dependency_class == AsyncSubagentDependencyClass.DEGRADABLE_BLOCKING:
            if link.result_decision in {"admit", "conditionally_admit"}:
                return "proceed"
            return "degrade" if link.result_decision == "reject" else "wait"
        return "proceed"

    async def _apply_observation(
        self,
        request_scope: str,
        execution: AsyncSubagentExecution,
        observation: ProviderAsyncObservation,
        *,
        bind_provider: bool = True,
    ) -> AsyncSubagentExecution:
        statuses = {
            "pending": AsyncSubagentLifecycle.SUBMITTED,
            "running": AsyncSubagentLifecycle.RUNNING,
            "waiting": AsyncSubagentLifecycle.WAITING,
            "success": AsyncSubagentLifecycle.COMPLETED,
            "error": AsyncSubagentLifecycle.FAILED,
            "cancelled": AsyncSubagentLifecycle.CANCELLED,
            "orphaned": AsyncSubagentLifecycle.ORPHANED,
        }
        manifest = execution.result_manifest
        if observation.status == "success":
            if (
                not observation.output_ref
                or observation.usage is None
                or observation.checkpoint is None
            ):
                raise AsyncSubagentError("provider success lacks canonical result references")
            manifest = AsyncSubagentResultManifest.create(
                manifest_id=str(
                    uuid5(
                        NAMESPACE_URL,
                        f"async-result:{execution.child_execution_id}:{execution.execution_generation}",
                    )
                ),
                child_execution_id=execution.child_execution_id,
                execution_generation=execution.execution_generation,
                output_refs=(observation.output_ref,),
                evidence_refs=observation.evidence_refs,
                usage_ref=observation.usage.usage_ref,
                checkpoint_ref=observation.checkpoint.ref,
                provider_checkpoint=observation.checkpoint,
                usage=observation.usage,
                effect_refs=observation.effect_refs,
                completed_at=observation.observed_at,
            )
        lifecycle = statuses[observation.status]
        updated = execution.model_copy(
            update={
                "lifecycle": lifecycle,
                "provider_thread_id": (
                    observation.thread_id if bind_provider else execution.provider_thread_id
                ),
                "provider_run_id": (
                    observation.run_id if bind_provider else execution.provider_run_id
                ),
                "result_manifest": (
                    manifest
                    if lifecycle == AsyncSubagentLifecycle.COMPLETED
                    else execution.result_manifest
                ),
                "result_output_text": (
                    observation.output_text
                    if lifecycle == AsyncSubagentLifecycle.COMPLETED
                    else execution.result_output_text
                ),
                "in_doubt_reason": None,
                "incident_id": None,
                "updated_at": observation.observed_at,
            }
        )
        await self._details.save_execution(request_scope, updated)
        await self._authority.record_execution_state(request_scope, updated)
        if bind_provider and (
            execution.provider_run_id != observation.run_id or observation.usage is not None
        ):
            await self._authority.record_provider_run(
                request_scope,
                AsyncProviderRunRecord(
                    child_execution_id=execution.child_execution_id,
                    provider_thread_id=observation.thread_id,
                    provider_run_id=observation.run_id,
                    disposition="bound",
                    provider_status=observation.status,
                    usage=observation.usage
                    or AsyncSubagentUsage(
                        provider_run_id=observation.run_id, attribution="pending"
                    ),
                    observed_at=observation.observed_at,
                ),
            )
        fact_ref = (
            manifest.manifest_digest
            if manifest is not None and lifecycle == AsyncSubagentLifecycle.COMPLETED
            else observation.status
        )
        await self._authority.record_fact(
            request_scope,
            execution.child_execution_id,
            "result" if lifecycle == AsyncSubagentLifecycle.COMPLETED else "lifecycle",
            fact_ref,
        )
        if self._parent_effects is not None:
            disposition: Literal["pending", "ambiguous", "succeeded", "failed", "cancelled"] = (
                "succeeded"
                if lifecycle == AsyncSubagentLifecycle.COMPLETED
                else "failed"
                if lifecycle == AsyncSubagentLifecycle.FAILED
                else "cancelled"
                if lifecycle in {AsyncSubagentLifecycle.CANCELLED, AsyncSubagentLifecycle.ORPHANED}
                else "pending"
            )
            await self._parent_effects.observe(
                request_scope,
                execution.parent_run_id,
                execution.child_execution_id,
                disposition=disposition,
                observation_id=(
                    f"async-terminal:{execution.child_execution_id}"
                    if disposition in {"succeeded", "failed", "cancelled"}
                    else f"async-observed:{observation.run_id}:{observation.status}"
                ),
                provider_effect_ref=observation.run_id if bind_provider else None,
                evidence_refs=(fact_ref,),
                observed_at=observation.observed_at,
            )
        return updated


class InMemoryAsyncSubagentDetailRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.contracts: dict[tuple[str, str], AsyncSubagentContract] = {}
        self.executions: dict[tuple[str, str], AsyncSubagentExecution] = {}
        self.links: dict[tuple[str, str], ParentAsyncSubagentLink] = {}

    async def create_before_submit(
        self,
        request_scope: str,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        link: ParentAsyncSubagentLink,
    ) -> AsyncSubagentExecution:
        key = (request_scope, execution.child_execution_id)
        async with self._lock:
            prior = self.executions.get(key)
            if prior is not None:
                if prior.contract_digest != execution.contract_digest:
                    raise AsyncSubagentError("async child identity conflicts with prior contract")
                return deepcopy(prior)
            self.contracts[(request_scope, contract.contract_id)] = deepcopy(contract)
            self.executions[key] = deepcopy(execution)
            self.links[key] = deepcopy(link)
            return deepcopy(execution)

    async def get_execution(
        self, request_scope: str, child_execution_id: str
    ) -> AsyncSubagentExecution:
        try:
            return deepcopy(self.executions[(request_scope, child_execution_id)])
        except KeyError as error:
            raise AsyncSubagentError("async child execution not found") from error

    async def get_contract(self, request_scope: str, contract_id: str) -> AsyncSubagentContract:
        try:
            return deepcopy(self.contracts[(request_scope, contract_id)])
        except KeyError as error:
            raise AsyncSubagentError("async subagent contract not found") from error

    async def get_link(
        self, request_scope: str, child_execution_id: str
    ) -> ParentAsyncSubagentLink:
        try:
            return deepcopy(self.links[(request_scope, child_execution_id)])
        except KeyError as error:
            raise AsyncSubagentError("async child link not found") from error

    async def save_execution(self, request_scope: str, execution: AsyncSubagentExecution) -> None:
        self.executions[(request_scope, execution.child_execution_id)] = deepcopy(execution)

    async def save_link(self, request_scope: str, link: ParentAsyncSubagentLink) -> None:
        self.links[(request_scope, link.child_execution_id)] = deepcopy(link)


class InMemoryAsyncSubagentAuthority:
    """Test authority mirroring the dedicated PostgreSQL command/fact ledger and fence."""

    def __init__(self) -> None:
        self.reservations: dict[tuple[str, str], dict[str, object]] = {}
        self.facts: list[tuple[str, str, str, str]] = []
        self.messages: list[tuple[str, AsyncSubagentMessage]] = []
        self.cancellations: dict[tuple[str, str], str] = {}
        self.decisions: dict[tuple[str, str], tuple[str, str]] = {}
        self.settlements: dict[tuple[str, str], str] = {}
        self.fences: dict[tuple[str, str], dict[str, object]] = {}
        self.states: dict[tuple[str, str], AsyncSubagentExecution] = {}
        self.provider_runs: list[tuple[str, AsyncProviderRunRecord]] = []
        self.incidents: dict[tuple[str, str], AsyncSubagentIncident] = {}
        self.reconciliation_decisions: dict[tuple[str, str], tuple[str, str, str | None]] = {}

    async def reserve_and_admit(
        self, request: AsyncSubagentSpawnRequest, child_execution_id: str, link_id: str
    ) -> None:
        self.reservations.setdefault(
            (request.request_scope, child_execution_id),
            {
                "reservation_id": request.reservation_id,
                "link_id": link_id,
                "dependency_class": request.dependency_class.value,
            },
        )

    async def acquire_submission_fence(
        self,
        request_scope: str,
        child_execution_id: str,
        *,
        holder: str,
        lease_expires_at: datetime,
        now: datetime,
    ) -> int | None:
        key = (request_scope, child_execution_id)
        current = self.fences.get(key)
        if current is not None and current.get("holder") is not None:
            expires = current.get("lease_expires_at")
            if isinstance(expires, datetime) and expires > now:
                return None
        fence = int(str(current["fence"])) + 1 if current is not None else 1
        self.fences[key] = {
            "fence": fence,
            "holder": holder,
            "lease_expires_at": lease_expires_at,
        }
        return fence

    async def release_submission_fence(
        self, request_scope: str, child_execution_id: str, fence: int
    ) -> None:
        current = self.fences.get((request_scope, child_execution_id))
        if current is not None and current["fence"] == fence:
            current["holder"] = None
            current["lease_expires_at"] = None

    async def record_execution_state(
        self, request_scope: str, execution: AsyncSubagentExecution
    ) -> None:
        self.states[(request_scope, execution.child_execution_id)] = deepcopy(execution)

    async def record_provider_run(self, request_scope: str, record: AsyncProviderRunRecord) -> None:
        for index, (scope, item) in enumerate(self.provider_runs):
            if (
                scope == request_scope
                and item.child_execution_id == record.child_execution_id
                and item.provider_run_id == record.provider_run_id
            ):
                # A bound record may be superseded by a cancellation disposition, never the
                # reverse (mirrors the PostgreSQL upsert rule).
                if item.disposition == "bound" or record.disposition != "bound":
                    self.provider_runs[index] = (request_scope, deepcopy(record))
                return
        self.provider_runs.append((request_scope, deepcopy(record)))

    async def cancelled_provider_run_ids(
        self, request_scope: str, child_execution_id: str
    ) -> frozenset[str]:
        return frozenset(
            record.provider_run_id
            for scope, record in self.provider_runs
            if scope == request_scope
            and record.child_execution_id == child_execution_id
            and record.disposition in CANCELLED_RUN_DISPOSITIONS
        )

    async def open_incident(self, incident: AsyncSubagentIncident) -> AsyncSubagentIncident:
        key = (incident.request_scope, incident.child_execution_id)
        prior = self.incidents.get(key)
        if prior is not None and prior.revision >= incident.revision:
            return deepcopy(prior)
        self.incidents[key] = deepcopy(incident)
        return deepcopy(incident)

    async def get_incident(
        self, request_scope: str, child_execution_id: str
    ) -> AsyncSubagentIncident | None:
        incident = self.incidents.get((request_scope, child_execution_id))
        return deepcopy(incident) if incident is not None else None

    async def resolve_incident(self, incident: AsyncSubagentIncident) -> None:
        key = (incident.request_scope, incident.child_execution_id)
        prior = self.incidents.get(key)
        if prior is None or prior.revision != incident.revision:
            raise AsyncSubagentError("no open incident matches the resolution")
        if prior.status == "resolved" and prior.decision_id != incident.decision_id:
            raise AsyncSubagentError("the incident is already resolved by another decision")
        self.incidents[key] = deepcopy(incident)

    async def claim_reconciliation_decision(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: AsyncSubagentReconciliationDecision,
        *,
        decision_id: str,
        adopted_run_id: str | None,
        reason: str,
    ) -> bool:
        del reason
        key = (request_scope, child_execution_id)
        prior = self.reconciliation_decisions.get(key)
        if prior is not None:
            return prior[0] == decision_id
        self.reconciliation_decisions[key] = (decision_id, decision, adopted_run_id)
        return True

    async def list_provider_runs(
        self, request_scope: str, child_execution_id: str
    ) -> tuple[AsyncProviderRunRecord, ...]:
        return tuple(
            deepcopy(record)
            for scope, record in self.provider_runs
            if scope == request_scope and record.child_execution_id == child_execution_id
        )

    async def record_fact(
        self, request_scope: str, child_execution_id: str, fact_kind: str, fact_ref: str
    ) -> None:
        item = (request_scope, child_execution_id, fact_kind, fact_ref)
        if item not in self.facts:
            self.facts.append(item)

    async def append_message(self, request_scope: str, message: AsyncSubagentMessage) -> None:
        expected = 1 + max(
            (
                item.target_sequence
                for scope, item in self.messages
                if scope == request_scope
                and item.child_execution_id == message.child_execution_id
                and item.direction == message.direction
            ),
            default=0,
        )
        if message.target_sequence != expected:
            raise AsyncSubagentError("message target sequence is not monotonic")
        self.messages.append((request_scope, deepcopy(message)))

    async def request_cancellation(
        self, request_scope: str, child_execution_id: str, reason: str
    ) -> None:
        self.cancellations.setdefault((request_scope, child_execution_id), reason)

    async def list_child_ids(
        self, request_scope: str, parent_binding_id: str
    ) -> tuple[str, ...]:
        return tuple(
            child_id
            for (scope, child_id), execution in self.states.items()
            if scope == request_scope and execution.parent_binding_id == parent_binding_id
        )

    async def decide_result(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: Literal["admit", "conditionally_admit", "reject", "defer"],
        manifest_digest: str,
    ) -> None:
        key = (request_scope, child_execution_id)
        prior = self.decisions.get(key)
        if prior is not None and prior != (decision, manifest_digest):
            raise AsyncSubagentError("result has a conflicting authority decision")
        self.decisions[key] = (decision, manifest_digest)

    async def settle(
        self, request_scope: str, child_execution_id: str, settlement_ref: str
    ) -> None:
        self.settlements.setdefault((request_scope, child_execution_id), settlement_ref)
