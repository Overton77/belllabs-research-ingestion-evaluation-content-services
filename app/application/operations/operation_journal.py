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
    ApplyAuthorityBatchAction,
    BudgetLedgerEntry,
    BudgetState,
    ClaimEffectAction,
    CommandResult,
    CommandStatus,
    DomainEventEnvelope,
    EffectSettlementOutcome,
    LifecycleCommand,
    LifecycleTransitionRecord,
    RecordOperationSettlementEvidenceAction,
    RecordUsageAction,
    RunProjection,
    SettleEffectAction,
    SettlePendingUsageAction,
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
    prior_settlement: OperationJournalSettlement | None = None
    authority_command: LifecycleCommand | None = None
    authority_result: CommandResult | None = None
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
            return (
                f"settlement:{self.settlement.settlement_id}:"
                f"revision:{self.settlement.settlement_revision}"
            )
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
                "prior_settlement": self.prior_settlement,
                "authority_command": self.authority_command,
                "authority_result": self.authority_result,
                "command_result": self.command_result,
                "resulting_run": self.resulting_run,
                "resulting_budget": self.resulting_budget,
                "transition": self.transition,
                "ledger_entries": self.ledger_entries,
                "outbox_events": self.outbox_events,
            }
        )

    def validate(self) -> None:
        for subject, value in (
            ("claim", self.claim),
            ("attempt", self.attempt),
            ("settlement", self.settlement),
            ("prior settlement", self.prior_settlement),
            ("authority command", self.authority_command),
            ("authority result", self.authority_result),
        ):
            if value is None:
                continue
            try:
                validated = type(value).model_validate(
                    value.model_dump(mode="python", warnings=False)
                )
            except (TypeError, ValueError) as error:
                raise ValueError(f"{subject} failed strict revalidation") from error
            if type(validated) is not type(value) or validated != value:
                raise ValueError(f"{subject} failed exact reconstruction")
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
            or self.settlement.digest_version != "complete-v2"
        ):
            raise ValueError(
                "new settlement must use complete-v2 digest and match the effect claim"
            )
        if self.prior_settlement is not None and (
            self.settlement is None
            or self.prior_settlement.request_scope != self.request_scope
            or self.prior_settlement.effect_claim_id != self.claim.effect_claim_id
            or self.prior_settlement.settlement_id != self.settlement.settlement_id
            or self.prior_settlement.settlement_revision + 1
            != self.settlement.settlement_revision
            or self.prior_settlement.status != "reconciliation_required"
        ):
            raise ValueError("prior operation settlement proof is invalid")
        if (
            self.settlement is not None
            and self.settlement.settlement_revision > 1
            and self.prior_settlement is None
        ):
            raise ValueError("revised settlement requires its exact prior revision")
        if self.settlement is not None:
            authority = self.authority_result or self.command_result
            expected_authority_version = (
                self.expected_run_version
                if self.authority_result is not None
                else self.expected_run_version + 1
            )
            if (
                authority is None
                or authority.status != CommandStatus.ACCEPTED
                or authority.run_id != self.belllabs_run_id
                or authority.resulting_run_version != expected_authority_version
                or (
                    self.authority_result is not None
                    and (
                        authority.idempotency_issuer != "operation-journal"
                        or authority.command_id
                        != (
                            f"operation-authority-settlement:{self.settlement.settlement_id}:"
                            f"revision:{self.settlement.settlement_revision}"
                        )
                    )
                )
            ):
                raise ValueError(
                    "journal settlement must bind its exact accepted authority result"
                )
            if self.authority_result is not None:
                command = self.authority_command
                if command is None:
                    raise ValueError(
                        "journal-only settlement requires its exact lifecycle command"
                    )
                fingerprint = sha256_digest(
                    command.model_dump(mode="json", exclude={"occurred_at"})
                )
                if (
                    command.request_scope != self.request_scope
                    or command.run_id != self.belllabs_run_id
                    or command.command_id != authority.command_id
                    or command.idempotency_issuer != authority.idempotency_issuer
                    or command.expected_run_version + 1
                    != authority.resulting_run_version
                    or fingerprint != authority.command_fingerprint
                ):
                    raise ValueError(
                        "journal authority command and accepted result do not match"
                    )
                self._validate_authority_action(command)
        elif self.authority_result is not None or self.authority_command is not None:
            command = self.authority_command
            authority = self.authority_result
            if command is None or authority is None:
                raise ValueError("claim authority requires exact command and result")
            fingerprint = sha256_digest(
                command.model_dump(mode="json", exclude={"occurred_at"})
            )
            if (
                authority.status != CommandStatus.ACCEPTED
                or command.request_scope != self.request_scope
                or command.run_id != self.belllabs_run_id
                or command.command_id != authority.command_id
                or command.idempotency_issuer != authority.idempotency_issuer
                or command.expected_run_version + 1
                != authority.resulting_run_version
                or fingerprint != authority.command_fingerprint
                or not isinstance(command.action, ClaimEffectAction)
                or command.action.effect_id != self.claim.effect_claim_id
                or command.action.operation_ref != self.claim.semantic_binding_id
                or command.action.provider_idempotency_key
                != self.claim.idempotency_key
                or command.action.claim_payload_digest
                != sha256_digest(self.claim.model_dump(mode="json"))
            ):
                raise ValueError("claim authority command or payload proof is unrelated")
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
            raise ValueError("lifecycle transition does not match the claimed operation boundary")
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

    def _validate_authority_action(self, command: LifecycleCommand) -> None:
        assert self.settlement is not None
        action = command.action
        manifest_ref = self.settlement.result_manifest_ref
        if manifest_ref is None or manifest_ref not in command.evidence_refs:
            raise ValueError("journal authority command omits the exact result manifest")
        if self.settlement.status == "reconciliation_required":
            if not isinstance(action, ApplyAuthorityBatchAction):
                raise ValueError("pending journal authority requires an authority batch")
            pending_usage_actions = [
                item for item in action.actions if isinstance(item, RecordUsageAction)
            ]
            evidence_actions = [
                item
                for item in action.actions
                if isinstance(item, RecordOperationSettlementEvidenceAction)
            ]
            if (
                len(action.actions) != 2
                or len(pending_usage_actions) != 1
                or len(evidence_actions) != 1
            ):
                raise ValueError("pending journal authority action is incomplete")
            pending_usage = pending_usage_actions[0]
            evidence = evidence_actions[0].evidence
            if (
                pending_usage.usage_id != self.settlement.settlement_id
                or pending_usage.authority_ref != self.claim.semantic_binding_id
                or pending_usage.actual_amounts != self.settlement.usage
                or pending_usage.release_amounts != self.settlement.released_usage
                or pending_usage.pending_external_amounts
                != self.settlement.pending_external_usage
                or not self.settlement.pending_external_usage
                or evidence.settlement_id != self.settlement.settlement_id
                or evidence.settlement_payload_digest
                != self.settlement.settlement_digest
                or evidence.accepted_by_authority_ref
                != self.claim.semantic_binding_id
            ):
                raise ValueError("pending journal authority action is unrelated")
            return
        if not isinstance(action, ApplyAuthorityBatchAction):
            raise ValueError("terminal journal settlement requires an authority batch")
        effect_actions = [
            item for item in action.actions if isinstance(item, SettleEffectAction)
        ]
        usage_actions = [
            item
            for item in action.actions
            if isinstance(item, RecordUsageAction | SettlePendingUsageAction)
        ]
        evidence_actions = [
            item
            for item in action.actions
            if isinstance(item, RecordOperationSettlementEvidenceAction)
        ]
        if (
            len(action.actions) != 3
            or len(effect_actions) != 1
            or len(usage_actions) != 1
            or len(evidence_actions) != 1
        ):
            raise ValueError(
                "journal authority batch must bind usage, effect, and settlement evidence"
            )
        effect = effect_actions[0]
        usage = usage_actions[0]
        evidence = evidence_actions[0].evidence
        usage_ref = (
            usage.usage_id
            if isinstance(usage, RecordUsageAction)
            else usage.settlement_id
        )
        usage_authority = (
            usage.authority_ref
            if isinstance(usage, RecordUsageAction)
            else self.claim.semantic_binding_id
        )
        expected_outcome = EffectSettlementOutcome(
            "succeeded"
            if self.settlement.status == "completed"
            else "cancelled"
            if self.settlement.status == "cancelled"
            else "failed"
        )
        usage_matches = False
        if isinstance(usage, RecordUsageAction):
            usage_matches = (
                self.prior_settlement is None
                and usage.actual_amounts == self.settlement.usage
                and usage.release_amounts == self.settlement.released_usage
                and usage.pending_external_amounts
                == self.settlement.pending_external_usage
                and not self.settlement.pending_external_usage
            )
        elif self.prior_settlement is not None:
            prior = self.prior_settlement
            usage_matches = (
                usage.usage_id == prior.settlement_id
                and usage.settlement_id
                == f"pending:{self.settlement.settlement_id}"
                and not self.settlement.pending_external_usage
                and {
                    dimension: prior.usage.get(dimension, 0)
                    + usage.actual_amounts.get(dimension, 0)
                    for dimension in prior.usage.keys()
                    | usage.actual_amounts.keys()
                }
                == self.settlement.usage
                and {
                    dimension: prior.released_usage.get(dimension, 0)
                    + usage.pending_release_amounts.get(dimension, 0)
                    for dimension in prior.released_usage.keys()
                    | usage.pending_release_amounts.keys()
                }
                == self.settlement.released_usage
                and {
                    dimension: usage.actual_amounts.get(dimension, 0)
                    + usage.pending_release_amounts.get(dimension, 0)
                    for dimension in usage.actual_amounts.keys()
                    | usage.pending_release_amounts.keys()
                }
                == prior.pending_external_usage
            )
        if (
            effect.effect_id != self.claim.effect_claim_id
            or effect.settlement_id != self.settlement.settlement_id
            or effect.usage_settlement_ref != usage_ref
            or effect.outcome != expected_outcome
            or effect.evidence_refs != (manifest_ref,)
            or usage_authority != self.claim.semantic_binding_id
            or not usage_matches
            or evidence.settlement_id != self.settlement.settlement_id
            or evidence.settlement_payload_digest
            != self.settlement.settlement_digest
            or evidence.accepted_by_authority_ref
            != self.claim.semantic_binding_id
        ):
            raise ValueError("journal authority batch effect or usage binding is unrelated")


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
        self._authority_proofs: dict[
            tuple[str, str, str],
            tuple[LifecycleCommand, CommandResult, DomainEventEnvelope],
        ] = {}

    def seed_run(self, run: RunProjection, budget: BudgetState) -> None:
        self._run_versions[(run.request_scope, run.run_id)] = run.version
        self._budgets[run.run_id] = deepcopy(budget)

    def seed_authority_proof(
        self,
        command: LifecycleCommand,
        result: CommandResult,
        event: DomainEventEnvelope,
    ) -> None:
        key = (command.run_id, command.idempotency_issuer, command.command_id)
        self._authority_proofs[key] = (
            deepcopy(command),
            deepcopy(result),
            deepcopy(event),
        )

    async def commit(
        self,
        mutation: OperationJournalMutation,
    ) -> OperationClaimResult:
        mutation.validate()
        if _is_journal_only_authority_mutation(mutation):
            assert mutation.authority_command is not None
            assert mutation.authority_result is not None
            proof_key = (
                mutation.authority_command.run_id,
                mutation.authority_command.idempotency_issuer,
                mutation.authority_command.command_id,
            )
            proof = self._authority_proofs.get(proof_key)
            expected_event_type = (
                "workflow_run.apply_authority_batch"
                if isinstance(
                    mutation.authority_command.action,
                    ApplyAuthorityBatchAction,
                )
                else "workflow_run.record_usage"
                if isinstance(mutation.authority_command.action, RecordUsageAction)
                else "workflow_run.claim_effect"
            )
            if (
                proof is None
                or proof[0] != mutation.authority_command
                or proof[1] != mutation.authority_result
                or proof[2].aggregate_version
                != mutation.authority_result.resulting_run_version
                or proof[2].event_type != expected_event_type
                or proof[2].payload.get("command_id")
                != mutation.authority_command.command_id
                or (
                    isinstance(
                        mutation.authority_command.action,
                        ApplyAuthorityBatchAction,
                    )
                    and proof[2].payload.get("authority_batch_digest")
                    != sha256_digest(mutation.authority_command.action)
                )
            ):
                raise IdempotencyConflict(
                    "journal settlement authority proof is missing or unrelated"
                )
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
            journal_only_settlement = _is_journal_only_authority_mutation(mutation)
            if current_version != mutation.expected_run_version and not (
                journal_only_settlement
                and current_version >= mutation.expected_run_version
            ):
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
                if mutation.settlement.settlement_revision != 1:
                    raise IdempotencyConflict(
                        "initial operation settlement revision must be 1"
                    )
                self._settlements[claim_id] = deepcopy(mutation.settlement)
            elif (
                prior_settlement.settlement_revision
                == mutation.settlement.settlement_revision
            ):
                if prior_settlement.settlement_digest != mutation.settlement.settlement_digest:
                    raise IdempotencyConflict("operation settlement replay conflicts")
            elif (
                prior_settlement.status == "reconciliation_required"
                and mutation.settlement.settlement_revision
                == prior_settlement.settlement_revision + 1
                and mutation.prior_settlement == prior_settlement
            ):
                self._settlements[claim_id] = deepcopy(mutation.settlement)
            else:
                raise IdempotencyConflict("operation settlement revision conflicts")
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
            self._budgets[mutation.belllabs_run_id] = deepcopy(mutation.resulting_budget)
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
            mutation.authority_command,
            mutation.authority_result,
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


def _is_journal_only_authority_mutation(
    mutation: OperationJournalMutation,
) -> bool:
    return (
        mutation.authority_command is not None
        and mutation.authority_result is not None
        and mutation.resulting_run is None
        and mutation.resulting_budget is None
        and mutation.transition is None
        and mutation.command_result is None
        and not mutation.ledger_entries
        and not mutation.outbox_events
    )
