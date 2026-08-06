from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from typing import Protocol

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.journal import (
    EffectClaimStatus,
    OperationClaimResult,
    OperationEffectClaim,
    OperationJournalSettlement,
    OperationTechnicalAttempt,
)
from app.domain.run_control.contracts import (
    BudgetLedgerEntry,
    BudgetState,
    CommandResult,
    DomainEventEnvelope,
    LifecycleTransitionRecord,
    RunProjection,
)
from app.domain.run_control.errors import IdempotencyConflict, RunVersionConflict


@dataclass(frozen=True)
class OperationJournalMutation:
    """One operation boundary committed on one acquired connection and transaction."""

    request_scope: str
    belllabs_run_id: str
    expected_run_version: int
    claim: OperationEffectClaim
    attempt: OperationTechnicalAttempt | None = None
    settlement: OperationJournalSettlement | None = None
    command_result: CommandResult | None = None
    resulting_run: RunProjection | None = None
    resulting_budget: BudgetState | None = None
    transition: LifecycleTransitionRecord | None = None
    ledger_entries: tuple[BudgetLedgerEntry, ...] = ()
    outbox_events: tuple[DomainEventEnvelope, ...] = ()

    @property
    def mutation_id(self) -> str:
        if self.transition is not None:
            return f"run-transition:{self.transition.transition_id}"
        if self.settlement is not None:
            return f"settlement:{self.settlement.settlement_id}"
        if self.attempt is not None:
            return f"attempt:{self.attempt.operation_attempt_id}"
        return f"claim:{self.claim.effect_claim_id}"

    @property
    def mutation_digest(self) -> str:
        return sha256_digest(
            {
                "request_scope": self.request_scope,
                "belllabs_run_id": self.belllabs_run_id,
                "expected_run_version": self.expected_run_version,
                "claim": self.claim,
                "attempt": self.attempt,
                "settlement": self.settlement,
                "command_result": self.command_result,
                "resulting_run": self.resulting_run,
                "resulting_budget": self.resulting_budget,
                "transition": self.transition,
                "ledger_entries": self.ledger_entries,
                "outbox_events": self.outbox_events,
            }
        )

    def validate(self) -> None:
        if (
            self.claim.request_scope != self.request_scope
            or self.claim.belllabs_run_id != self.belllabs_run_id
        ):
            raise ValueError("operation journal mutation crosses run or request scope")
        if self.attempt is not None and (
            self.attempt.request_scope != self.request_scope
            or self.attempt.effect_claim_id != self.claim.effect_claim_id
        ):
            raise ValueError("technical attempt does not belong to the effect claim")
        if self.settlement is not None and (
            self.settlement.request_scope != self.request_scope
            or self.settlement.effect_claim_id != self.claim.effect_claim_id
        ):
            raise ValueError("settlement does not belong to the effect claim")
        coordinated = (
            self.resulting_run,
            self.resulting_budget,
            self.transition,
            self.command_result,
        )
        if any(item is not None for item in coordinated) and not all(
            item is not None for item in coordinated
        ):
            raise ValueError(
                "command result, run, budget, and lifecycle writes form one atomic unit"
            )
        if self.resulting_run is not None:
            if (
                self.resulting_run.request_scope != self.request_scope
                or self.resulting_run.run_id != self.belllabs_run_id
                or self.resulting_run.version != self.expected_run_version + 1
            ):
                raise ValueError("resulting run does not advance the claimed run by one version")
        if self.resulting_budget is not None:
            if self.resulting_budget.run_id != self.belllabs_run_id:
                raise ValueError("resulting budget belongs to another run")
        if self.transition is not None and (
            self.transition.run_id != self.belllabs_run_id
            or self.transition.prior_version != self.expected_run_version
            or self.transition.resulting_version != self.expected_run_version + 1
            or self.transition.causation_id != self.claim.effect_claim_id
        ):
            raise ValueError(
                "lifecycle transition does not match the claimed operation boundary"
            )
        if self.command_result is not None and (
            self.command_result.run_id != self.belllabs_run_id
            or self.command_result.resulting_run_version != self.expected_run_version + 1
        ):
            raise ValueError("command result does not match the claimed operation boundary")
        if any(
            event.aggregate_id != self.belllabs_run_id
            or event.causation_id != self.claim.effect_claim_id
            for event in self.outbox_events
        ):
            raise ValueError("operation outbox events must be caused by the effect claim")
        ledger_id = (
            self.settlement.settlement_id
            if self.settlement is not None
            else f"operation:{self.claim.effect_claim_id}"
        )
        if any(
            entry.run_id != self.belllabs_run_id or entry.idempotency_id != ledger_id
            for entry in self.ledger_entries
        ):
            raise ValueError("operation ledger entries must be caused by the effect claim")


class AtomicOperationJournalRepository(Protocol):
    async def commit(
        self,
        mutation: OperationJournalMutation,
    ) -> OperationClaimResult: ...

    async def get_claim(
        self,
        request_scope: str,
        effect_claim_id: str,
    ) -> OperationEffectClaim | None: ...

    async def get_settlement(
        self,
        request_scope: str,
        effect_claim_id: str,
    ) -> OperationJournalSettlement | None: ...


class OperationJournalService:
    def __init__(self, repository: AtomicOperationJournalRepository) -> None:
        self._repository = repository

    async def commit(self, mutation: OperationJournalMutation) -> OperationClaimResult:
        mutation.validate()
        if mutation.claim.claim_mode == "shadow":
            return OperationClaimResult(
                status="shadow_denied",
                reason="shadow execution cannot acquire a consequential effect claim",
            )
        return await self._repository.commit(mutation)

    async def get_settlement(
        self,
        request_scope: str,
        effect_claim_id: str,
    ) -> OperationJournalSettlement | None:
        return await self._repository.get_settlement(request_scope, effect_claim_id)


class InMemoryAtomicOperationJournalRepository:
    """Atomic behavioral adapter used for crash and idempotency tests."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._claims: dict[str, OperationEffectClaim] = {}
        self._claim_keys: dict[tuple[str, str, str], str] = {}
        self._attempts: dict[str, list[OperationTechnicalAttempt]] = {}
        self._settlements: dict[str, OperationJournalSettlement] = {}
        self._run_versions: dict[tuple[str, str], int] = {}
        self._budgets: dict[str, BudgetState] = {}
        self._transitions: dict[str, list[LifecycleTransitionRecord]] = {}
        self._command_results: dict[tuple[str, str, str], CommandResult] = {}
        self._ledger: dict[str, list[BudgetLedgerEntry]] = {}
        self._outbox: dict[str, DomainEventEnvelope] = {}
        self._mutations: dict[tuple[str, str], str] = {}

    def seed_run(self, run: RunProjection, budget: BudgetState) -> None:
        self._run_versions[(run.request_scope, run.run_id)] = run.version
        self._budgets[run.run_id] = deepcopy(budget)

    async def commit(
        self,
        mutation: OperationJournalMutation,
    ) -> OperationClaimResult:
        mutation.validate()
        key = (
            mutation.request_scope,
            mutation.claim.operation_contract_digest,
            mutation.claim.idempotency_key,
        )
        async with self._lock:
            mutation_key = (mutation.request_scope, mutation.mutation_id)
            prior_mutation_digest = self._mutations.get(mutation_key)
            if prior_mutation_digest is not None:
                if prior_mutation_digest != mutation.mutation_digest:
                    raise IdempotencyConflict(
                        "operation journal mutation identity has conflicting intent"
                    )
                prior_claim = self._claims.get(mutation.claim.effect_claim_id)
                return OperationClaimResult(
                    status="existing",
                    claim=deepcopy(prior_claim),
                    reason="same operation journal mutation already committed",
                )
            prior_id = self._claim_keys.get(key)
            if prior_id is not None:
                prior = self._claims[prior_id]
                if not _same_claim_intent(prior, mutation.claim):
                    raise IdempotencyConflict(
                        "effect claim key was reused with a conflicting request "
                        "or immutable identity"
                    )
                if prior.effect_claim_id != mutation.claim.effect_claim_id:
                    if _has_claim_children(mutation):
                        raise IdempotencyConflict(
                            "claim replay regenerated identity while carrying child mutations"
                        )
                    return OperationClaimResult(
                        status="existing",
                        claim=deepcopy(prior),
                        reason="same claim key and request digest already exists",
                    )
                if prior.status in {
                    EffectClaimStatus.SETTLED,
                    EffectClaimStatus.CANCELLED,
                }:
                    raise IdempotencyConflict(
                        "terminal operation claim cannot accept another mutation"
                    )
                claims = deepcopy(self._claims)
                attempts = deepcopy(self._attempts)
                settlements = deepcopy(self._settlements)
                run_versions = deepcopy(self._run_versions)
                budgets = deepcopy(self._budgets)
                transitions = deepcopy(self._transitions)
                command_results = deepcopy(self._command_results)
                ledger = deepcopy(self._ledger)
                outbox = deepcopy(self._outbox)
                try:
                    self._commit_children(mutation, replay=True)
                    self._mutations[mutation_key] = mutation.mutation_digest
                except Exception:
                    self._claims = claims
                    self._attempts = attempts
                    self._settlements = settlements
                    self._run_versions = run_versions
                    self._budgets = budgets
                    self._transitions = transitions
                    self._command_results = command_results
                    self._ledger = ledger
                    self._outbox = outbox
                    raise
                return OperationClaimResult(
                    status="existing",
                    claim=deepcopy(self._claims[prior_id]),
                    reason="same claim key and digest already exists",
                )

            run_key = (mutation.request_scope, mutation.belllabs_run_id)
            current_version = self._run_versions.get(run_key, mutation.expected_run_version)
            if current_version != mutation.expected_run_version:
                raise RunVersionConflict(
                    f"expected version {mutation.expected_run_version}, current version "
                    f"is {current_version}"
                )

            claims = deepcopy(self._claims)
            claim_keys = deepcopy(self._claim_keys)
            attempts = deepcopy(self._attempts)
            settlements = deepcopy(self._settlements)
            run_versions = deepcopy(self._run_versions)
            budgets = deepcopy(self._budgets)
            transitions = deepcopy(self._transitions)
            command_results = deepcopy(self._command_results)
            ledger = deepcopy(self._ledger)
            outbox = deepcopy(self._outbox)
            mutations = deepcopy(self._mutations)
            try:
                self._claims[mutation.claim.effect_claim_id] = deepcopy(mutation.claim)
                self._claim_keys[key] = mutation.claim.effect_claim_id
                self._commit_children(mutation, replay=False)
                self._mutations[mutation_key] = mutation.mutation_digest
            except Exception:
                self._claims = claims
                self._claim_keys = claim_keys
                self._attempts = attempts
                self._settlements = settlements
                self._run_versions = run_versions
                self._budgets = budgets
                self._transitions = transitions
                self._command_results = command_results
                self._ledger = ledger
                self._outbox = outbox
                self._mutations = mutations
                raise
            return OperationClaimResult(
                status="acquired",
                claim=deepcopy(mutation.claim),
                reason="consequential effect claim acquired",
            )

    def _commit_children(
        self,
        mutation: OperationJournalMutation,
        *,
        replay: bool,
    ) -> None:
        claim_id = mutation.claim.effect_claim_id
        if mutation.attempt is not None:
            attempts = self._attempts.setdefault(claim_id, [])
            prior = next(
                (
                    item
                    for item in attempts
                    if item.technical_attempt == mutation.attempt.technical_attempt
                ),
                None,
            )
            if prior is None:
                attempts.append(deepcopy(mutation.attempt))
            elif prior != mutation.attempt:
                raise IdempotencyConflict("technical attempt replay conflicts")
        if mutation.settlement is not None:
            prior_settlement = self._settlements.get(claim_id)
            if prior_settlement is None:
                self._settlements[claim_id] = deepcopy(mutation.settlement)
            elif (
                prior_settlement.settlement_digest
                != mutation.settlement.settlement_digest
            ):
                raise IdempotencyConflict("operation settlement replay conflicts")
            claim_status = (
                EffectClaimStatus.RECONCILIATION_REQUIRED
                if mutation.settlement.status == "reconciliation_required"
                else EffectClaimStatus.CANCELLED
                if mutation.settlement.status == "cancelled"
                else EffectClaimStatus.SETTLED
            )
            self._claims[claim_id] = self._claims[claim_id].model_copy(
                update={
                    "status": claim_status,
                    "heartbeat_at": mutation.settlement.settled_at,
                }
            )
        if mutation.resulting_run is not None:
            assert mutation.resulting_budget is not None
            assert mutation.transition is not None
            run_key = (mutation.request_scope, mutation.belllabs_run_id)
            resulting_version = mutation.resulting_run.version
            current = self._run_versions.get(run_key, mutation.expected_run_version)
            if replay and current == resulting_version:
                return
            if current != mutation.expected_run_version:
                raise RunVersionConflict("operation journal run version is stale")
            self._run_versions[run_key] = resulting_version
            self._budgets[mutation.belllabs_run_id] = deepcopy(
                mutation.resulting_budget
            )
            self._transitions.setdefault(mutation.belllabs_run_id, []).append(
                deepcopy(mutation.transition)
            )
            assert mutation.command_result is not None
            command_key = (
                mutation.command_result.run_id,
                mutation.command_result.idempotency_issuer,
                mutation.command_result.command_id,
            )
            prior_result = self._command_results.get(command_key)
            if prior_result is not None and prior_result != mutation.command_result:
                raise IdempotencyConflict("operation lifecycle command result collision")
            self._command_results[command_key] = deepcopy(mutation.command_result)
            self._ledger.setdefault(mutation.belllabs_run_id, []).extend(
                deepcopy(mutation.ledger_entries)
            )
            for event in mutation.outbox_events:
                prior_event = self._outbox.get(event.event_id)
                if prior_event is not None and prior_event != event:
                    raise IdempotencyConflict("operation outbox event collision")
                self._outbox[event.event_id] = deepcopy(event)

    async def get_claim(
        self,
        request_scope: str,
        effect_claim_id: str,
    ) -> OperationEffectClaim | None:
        claim = self._claims.get(effect_claim_id)
        if claim is None or claim.request_scope != request_scope:
            return None
        return deepcopy(claim)

    async def get_settlement(
        self,
        request_scope: str,
        effect_claim_id: str,
    ) -> OperationJournalSettlement | None:
        settlement = self._settlements.get(effect_claim_id)
        if settlement is None or settlement.request_scope != request_scope:
            return None
        return deepcopy(settlement)


def _has_claim_children(mutation: OperationJournalMutation) -> bool:
    return any(
        value is not None
        for value in (
            mutation.attempt,
            mutation.settlement,
            mutation.resulting_run,
            mutation.resulting_budget,
            mutation.transition,
            mutation.command_result,
        )
    ) or bool(mutation.ledger_entries or mutation.outbox_events)


def _same_claim_intent(
    prior: OperationEffectClaim,
    candidate: OperationEffectClaim,
) -> bool:
    fields = (
        "request_scope",
        "belllabs_run_id",
        "operation_contract_digest",
        "idempotency_key",
        "request_digest",
        "semantic_binding_id",
        "semantic_binding_digest",
        "semantic_attempt_key",
        "claim_mode",
    )
    return all(getattr(prior, field) == getattr(candidate, field) for field in fields)
