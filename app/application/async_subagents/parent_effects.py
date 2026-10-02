"""The parent run's run-control authority for its async children (REQ-CP-RUN-009, RUN-007).

Every child is, in the parent run's ledger:

- a budget reservation carved from the run's budget (`ReserveBudgetAction`, sponsored by the
  parent operation's reservation);
- a consequential effect claim with `operation_ref = parent binding_id`
  (`effect_kind = async_subagent.child`), so the parent's narrowed post-dispatch rule
  (`unsettled_effect_ids`, RRM-004) sees an unsettled or ambiguous child and settles
  `in_doubt` instead of `failed`;
- a registered async child, so the run cannot terminalize before a result decision.

Usage settles exactly once: attributed amounts are consumed, unused reservation is released,
and amounts the provider could not attribute stay `pending_external` and leave the effect
unsettled until reconciled (never dropped). Commands are idempotent by identity, so a crashed
and retried spawn replays them.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Literal

from app.application.async_subagents.service import AsyncSubagentError, AsyncSubagentSpawnRequest
from app.application.run_control.service import RunControlService
from app.domain.control_plane.canonical import sha256_digest
from app.domain.run_control.contracts import (
    ActorContext,
    ApplyAuthorityBatchAction,
    AsyncChildDependencyClass,
    ClaimEffectAction,
    CommandResult,
    CommandStatus,
    EffectDisposition,
    EffectSettlementOutcome,
    LifecycleAction,
    LifecycleCommand,
    ObserveEffectAction,
    RecordAsyncChildFactAction,
    RecordUsageAction,
    RegisterAsyncChildAction,
    ReserveBudgetAction,
    SettleEffectAction,
    SettlePendingUsageAction,
)

ASYNC_CHILD_EFFECT_KIND = "async_subagent.child"
ISSUER = "async-subagent-service"


def async_child_effect_id(child_execution_id: str) -> str:
    return f"async-child-effect:{child_execution_id}"


def async_child_usage_id(child_execution_id: str) -> str:
    return f"async-child-usage:{child_execution_id}"


class RunControlAsyncChildEffects:
    """`AsyncSubagentParentEffectsPort` over the parent run's `RunControlService`."""

    def __init__(
        self,
        run_control: RunControlService,
        *,
        actor: ActorContext,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._run_control = run_control
        self._actor = actor
        self._now = now

    async def reserve_and_claim(
        self, request: AsyncSubagentSpawnRequest, child_execution_id: str
    ) -> None:
        correlation = f"async-child:{child_execution_id}"
        await self._execute(
            request.request_scope,
            request.parent_run_id,
            command_id=f"async-child-reserve:{child_execution_id}",
            action=ReserveBudgetAction(
                reservation_id=request.reservation_id,
                amounts=dict(request.contract.budget_limits),
                parent_reservation_id=request.parent_reservation_id,
            ),
            reason="Carve the async child's reservation from the parent run's budget",
            evidence_refs=(request.parent_binding_id,),
            occurred_at=request.requested_at,
            correlation_id=correlation,
            causation_id=request.parent_binding_id,
            tolerated={"reservation_exists"},
        )
        await self._execute(
            request.request_scope,
            request.parent_run_id,
            command_id=f"async-child-claim:{child_execution_id}",
            action=ClaimEffectAction(
                effect_id=async_child_effect_id(child_execution_id),
                effect_kind=ASYNC_CHILD_EFFECT_KIND,
                operation_ref=request.parent_binding_id,
                provider_idempotency_key=f"async-spawn:{child_execution_id}",
                reservation_id=request.reservation_id,
                claim_payload_digest=sha256_digest(
                    {
                        "child_execution_id": child_execution_id,
                        "contract_digest": request.contract.contract_digest,
                        "objective_ref": request.objective_ref,
                    }
                ),
            ),
            reason="Claim the async child as a consequential effect of the parent operation",
            evidence_refs=(request.parent_binding_id,),
            occurred_at=request.requested_at,
            correlation_id=correlation,
            causation_id=request.parent_binding_id,
            tolerated={"effect_claim_exists"},
        )
        await self._execute(
            request.request_scope,
            request.parent_run_id,
            command_id=f"async-child-register:{child_execution_id}",
            action=RegisterAsyncChildAction(
                child_execution_id=child_execution_id,
                parent_operation_ref=request.parent_binding_id,
                dependency_class=AsyncChildDependencyClass(request.dependency_class.value),
                reservation_id=request.reservation_id,
            ),
            reason="Register the async child under the parent run's authority",
            evidence_refs=(request.parent_binding_id,),
            occurred_at=request.requested_at,
            correlation_id=correlation,
            causation_id=request.parent_binding_id,
            tolerated={"async_child_exists"},
        )

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
    ) -> None:
        await self._execute(
            request_scope,
            parent_run_id,
            command_id=f"async-child-observe:{observation_id}",
            action=ObserveEffectAction(
                effect_id=async_child_effect_id(child_execution_id),
                observation_id=observation_id,
                disposition=EffectDisposition(disposition),
                provider_effect_ref=provider_effect_ref,
                evidence_refs=evidence_refs,
            ),
            reason=f"Observe the async child's provider state as {disposition}",
            evidence_refs=evidence_refs,
            occurred_at=observed_at,
            correlation_id=f"async-child:{child_execution_id}",
            causation_id=async_child_effect_id(child_execution_id),
            tolerated={"effect_observation_exists", "effect_already_settled"},
        )
        await self._execute(
            request_scope,
            parent_run_id,
            command_id=f"async-child-fact:{observation_id}",
            action=RecordAsyncChildFactAction(
                fact_id=f"async-child-fact:{observation_id}",
                child_execution_id=child_execution_id,
                fact_kind="lifecycle",
                lifecycle_status=disposition,
                evidence_refs=evidence_refs,
            ),
            reason=f"Record the async child lifecycle fact {disposition}",
            evidence_refs=evidence_refs,
            occurred_at=observed_at,
            correlation_id=f"async-child:{child_execution_id}",
            causation_id=async_child_effect_id(child_execution_id),
            tolerated={"async_child_fact_exists"},
        )

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
    ) -> Literal["settled", "pending_usage"]:
        """Settle the child's usage against the parent run; revision n is one command.

        First revision: record the usage (attributed consumed, unused reservation released,
        pending amounts `pending_external`) and settle the effect only when nothing is
        pending. A later revision with the pending amounts now known settles the outstanding
        usage exactly against its source pending amounts (`SettlePendingUsageAction`) and then
        the effect. Every command id carries the revision, so a revision never replays as
        another revision's no-op (RRM-013 review N1).
        """

        # Only the child's declared budget dimensions settle against the parent run; the
        # manifest and provider-run records keep every reported amount.
        attributed_amounts = {
            dimension: amount
            for dimension, amount in attributed_amounts.items()
            if dimension in budget_limits
        }
        pending_amounts = {
            dimension: amount
            for dimension, amount in pending_amounts.items()
            if dimension in budget_limits and amount > 0
        }
        effects = await self._run_control.get_effects(request_scope, parent_run_id)
        claim = effects.claims.get(async_child_effect_id(child_execution_id))
        if claim is None:
            raise AsyncSubagentError("the async child has no effect claim in the parent run")
        if claim.settlement is not None:
            return "settled"
        # The settlement binds the claim's latest observation of this outcome (a cancelled
        # child's terminal observation may be an orphan or cancel observation, not the generic
        # terminal id the caller names).
        observation_id = next(
            (
                item.observation_id
                for item in reversed(claim.observations)
                if item.disposition.value == outcome
            ),
            observation_id,
        )
        budget = await self._run_control.get_budget(request_scope, parent_run_id)
        usage_id = async_child_usage_id(child_execution_id)
        existing = budget.usage_records.get(usage_id)
        command_id = f"async-child-settle:{child_execution_id}:revision:{settlement_revision}"
        correlation = f"async-child:{child_execution_id}"
        if existing is None:
            release = {
                dimension: limit
                - min(
                    limit,
                    attributed_amounts.get(dimension, 0) + pending_amounts.get(dimension, 0),
                )
                for dimension, limit in budget_limits.items()
                if limit > attributed_amounts.get(dimension, 0) + pending_amounts.get(dimension, 0)
            }
            usage = RecordUsageAction(
                usage_id=usage_id,
                authority_ref=claim.operation_ref,
                actual_amounts=dict(attributed_amounts),
                reservation_id=reservation_id,
                release_amounts=release,
                pending_external_amounts=dict(pending_amounts),
            )
            pending = bool(pending_amounts)
            actions: tuple[LifecycleAction, ...] = (
                (usage,)
                if pending
                else (
                    usage,
                    SettleEffectAction(
                        effect_id=claim.effect_id,
                        settlement_id=f"async-child-settlement:{child_execution_id}",
                        observation_id=observation_id,
                        outcome=EffectSettlementOutcome(outcome),
                        usage_settlement_ref=usage_id,
                        evidence_refs=(settlement_ref,),
                    ),
                )
            )
            await self._execute(
                request_scope,
                parent_run_id,
                command_id=command_id,
                action=ApplyAuthorityBatchAction(actions=actions),
                reason=(
                    "Record the async child's pending usage before later reconciliation"
                    if pending
                    else "Settle the async child's usage and effect against the parent budget"
                ),
                evidence_refs=(settlement_ref,),
                occurred_at=settled_at,
                correlation_id=correlation,
                causation_id=claim.effect_id,
                tolerated={"usage_exists", "effect_already_settled"},
            )
            return "pending_usage" if pending else "settled"
        if usage_id not in budget.outstanding_usage_ids:
            return "settled"
        if pending_amounts:
            # The pending part is still unknown: nothing to settle yet.
            return "pending_usage"
        source_pending = {
            dimension: amount
            for dimension, amount in existing.pending_external_amounts.items()
            if amount > 0
        }
        settled_now: dict[str, int] = {}
        released_now: dict[str, int] = {}
        for dimension, pending_amount in source_pending.items():
            newly_attributed = max(
                0, attributed_amounts.get(dimension, 0) - existing.actual_amounts.get(dimension, 0)
            )
            consumed = min(newly_attributed, pending_amount)
            if consumed:
                settled_now[dimension] = consumed
            if pending_amount - consumed:
                released_now[dimension] = pending_amount - consumed
        settlement_id = f"{usage_id}:settlement:{settlement_revision}"
        await self._execute(
            request_scope,
            parent_run_id,
            command_id=command_id,
            action=ApplyAuthorityBatchAction(
                actions=(
                    SettlePendingUsageAction(
                        settlement_id=settlement_id,
                        usage_id=usage_id,
                        actual_amounts=settled_now,
                        pending_release_amounts=released_now,
                    ),
                    SettleEffectAction(
                        effect_id=claim.effect_id,
                        settlement_id=f"async-child-settlement:{child_execution_id}",
                        observation_id=observation_id,
                        outcome=EffectSettlementOutcome(outcome),
                        usage_settlement_ref=settlement_id,
                        evidence_refs=(settlement_ref,),
                    ),
                )
            ),
            reason="Reconcile the async child's pending usage and settle its effect",
            evidence_refs=(settlement_ref,),
            occurred_at=settled_at,
            correlation_id=correlation,
            causation_id=claim.effect_id,
            tolerated={"settlement_exists", "effect_already_settled"},
        )
        return "settled"

    async def _execute(
        self,
        request_scope: str,
        run_id: str,
        *,
        command_id: str,
        action: LifecycleAction,
        reason: str,
        evidence_refs: tuple[str, ...],
        occurred_at: datetime,
        correlation_id: str,
        causation_id: str,
        tolerated: set[str],
    ) -> CommandResult:
        """Execute once by command identity; a replay returns the stored result."""

        for _attempt in range(8):
            prior = await self._run_control.get_command_result(
                request_scope, run_id, ISSUER, command_id
            )
            if prior is not None:
                if prior.status != CommandStatus.ACCEPTED and prior.reason_code not in tolerated:
                    raise AsyncSubagentError(
                        f"parent run control rejected {command_id}: {prior.reason_code}"
                    )
                return prior
            run = await self._run_control.get_run(request_scope, run_id)
            result = await self._run_control.execute(
                LifecycleCommand(
                    command_id=command_id,
                    idempotency_issuer=ISSUER,
                    request_scope=request_scope,
                    run_id=run_id,
                    expected_run_version=run.version,
                    actor=self._actor,
                    action=action,
                    reason=reason,
                    evidence_refs=evidence_refs,
                    occurred_at=occurred_at,
                    correlation_id=correlation_id,
                    causation_id=causation_id,
                )
            )
            if result.status == CommandStatus.ACCEPTED or result.reason_code in tolerated:
                return result
            if result.status == CommandStatus.STALE:
                continue
            raise AsyncSubagentError(
                f"parent run control rejected {command_id}: {result.reason_code}"
            )
        raise AsyncSubagentError(f"parent run control stayed stale for {command_id}")
