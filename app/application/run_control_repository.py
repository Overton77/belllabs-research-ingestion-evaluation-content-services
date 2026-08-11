from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import Awaitable, Callable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, TypeVar

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.journal import OperationJournalSettlement
from app.domain.run_control.budget import roll_up_child_budget
from app.domain.run_control.contracts import (
    AdmissionDecision,
    BudgetLedgerEntry,
    BudgetState,
    CommandResult,
    CommandStatus,
    ConsumerApplyResult,
    ConsumerApplyStatus,
    ConsumerCursor,
    DomainEventEnvelope,
    EffectLedgerEntry,
    EffectLedgerState,
    LifecycleTransitionRecord,
    OutboxCursor,
    OutboxRecord,
    RunProjection,
    UsageRecord,
)
from app.domain.run_control.errors import (
    IdempotencyConflict,
    RunControlNotFound,
    RunVersionConflict,
)
from app.domain.run_control.family_admission import (
    AtomicFamilyMutation,
    AuthorityStateConflict,
    FamilyAdmissionReceipt,
    FamilyVersionConflict,
)

M = TypeVar("M", bound=AtomicFamilyMutation)
FailureHook = Callable[[str], Awaitable[None] | None]


def authority_state_digest(state: BudgetState | EffectLedgerState) -> str:
    """Canonical CAS digest that preserves set/map semantics before JSON encoding."""

    return sha256_digest(state.model_dump(mode="python"))


def upgrade_legacy_operation_pending_usage(
    budget: BudgetState,
    effects: EffectLedgerState,
    settlement: OperationJournalSettlement,
) -> BudgetState:
    """Reconstruct one unambiguous legacy pending usage from journal/effect authority."""

    if (
        settlement.status != "reconciliation_required"
        or not settlement.pending_external_usage
        or settlement.effect_claim_id not in effects.claims
        or settlement.settlement_id in budget.usage_ids
        or budget.usage_records
        or budget.outstanding_usage_ids
    ):
        raise ValueError("legacy pending usage evidence is absent or ambiguous")
    claim = effects.claims[settlement.effect_claim_id]
    if claim.run_id != budget.run_id or claim.settlement is not None:
        raise ValueError("legacy effect claim does not match the pending budget")
    pending = {
        dimension: amount
        for dimension, amount in budget.pending_settlement.items()
        if amount > 0
    }
    if pending != settlement.pending_external_usage:
        raise ValueError("legacy pending usage totals do not match journal evidence")
    usage = UsageRecord(
        usage_id=settlement.settlement_id,
        reservation_id=claim.reservation_id,
        authority_ref=claim.operation_ref,
        actual_amounts=settlement.usage,
        release_amounts=settlement.released_usage,
        pending_external_amounts=settlement.pending_external_usage,
    )
    return budget.model_copy(
        update={
            "usage_ids": budget.usage_ids | {usage.usage_id},
            "usage_records": {usage.usage_id: usage},
            "outstanding_usage_ids": frozenset({usage.usage_id}),
        }
    )


@dataclass(frozen=True)
class AdmissionMutation:
    decision: AdmissionDecision
    projection: RunProjection | None = None
    budget: BudgetState | None = None
    effects: EffectLedgerState | None = None
    transition: LifecycleTransitionRecord | None = None
    ledger_entries: tuple[BudgetLedgerEntry, ...] = ()
    effect_entries: tuple[EffectLedgerEntry, ...] = ()
    events: tuple[DomainEventEnvelope, ...] = ()


@dataclass(frozen=True)
class CommandMutation:
    result: CommandResult
    request_scope: str
    expected_version: int
    expected_budget_digest: str | None = None
    expected_effects_digest: str | None = None
    projection: RunProjection | None = None
    budget: BudgetState | None = None
    effects: EffectLedgerState | None = None
    transition: LifecycleTransitionRecord | None = None
    ledger_entries: tuple[BudgetLedgerEntry, ...] = ()
    effect_entries: tuple[EffectLedgerEntry, ...] = ()
    events: tuple[DomainEventEnvelope, ...] = ()


@dataclass(frozen=True)
class FamilyAdmissionCommit:
    command: CommandMutation
    family_mutation: AtomicFamilyMutation
    family_mutation_fingerprint: str
    receipt: FamilyAdmissionReceipt

    def __post_init__(self) -> None:
        command = self.command
        mutation = self.family_mutation
        result = command.result
        receipt = self.receipt
        digests = (
            command.expected_budget_digest,
            command.expected_effects_digest,
            self.family_mutation_fingerprint,
        )
        if any(
            item is None or re.fullmatch(r"sha256:[0-9a-f]{64}", item) is None
            for item in digests
        ):
            raise ValueError("family admission commit contains an invalid authority digest")
        expected_family_fingerprint = sha256_digest(
            mutation.model_dump(mode="json", exclude={"decided_at"})
        )
        if self.family_mutation_fingerprint != expected_family_fingerprint:
            raise ValueError("family mutation fingerprint does not match mutation content")
        if (
            command.request_scope != mutation.request_scope
            or result.run_id != mutation.run_id
            or receipt.command_result != result
            or receipt.family_mutation_fingerprint != self.family_mutation_fingerprint
        ):
            raise ValueError("family admission command, mutation, and receipt identities differ")
        accepted = result.status == CommandStatus.ACCEPTED
        if accepted != (command.projection is not None):
            raise ValueError("only accepted family commands may carry a run-control mutation")
        family_receipt = receipt.family_receipt
        if accepted:
            projection = command.projection
            transition = command.transition
            if (
                projection is None
                or command.budget is None
                or command.effects is None
                or transition is None
                or not command.events
                or projection.run_id != mutation.run_id
                or projection.request_scope != mutation.request_scope
                or projection.version != command.expected_version + 1
                or result.resulting_run_version != projection.version
                or command.budget.run_id != mutation.run_id
                or command.effects.run_id != mutation.run_id
                or transition.command_id != result.command_id
                or transition.run_id != mutation.run_id
                or transition.prior_version != command.expected_version
                or transition.resulting_projection != projection
            ):
                raise ValueError("accepted family command mutation is internally inconsistent")
            if family_receipt is None or (
                family_receipt.family_kind != mutation.family_kind
                or family_receipt.mutation_kind != mutation.mutation_kind
                or family_receipt.mutation_id != mutation.mutation_id
                or family_receipt.mutation_fingerprint
                != self.family_mutation_fingerprint
                or family_receipt.family_version
                != mutation.expected_family_version + 1
                or family_receipt.exact_operation_request_ref
                != mutation.exact_operation_request_ref
            ):
                raise ValueError("accepted family receipt does not match its mutation")
            final_event = command.events[-1]
            expected_reference_digest = sha256_digest(
                mutation.exact_operation_request_ref
            )
            if (
                final_event.event_type != "workflow_run.family_admission_committed"
                or final_event.aggregate_id != mutation.run_id
                or final_event.aggregate_version != projection.version
                or not final_event.is_version_final
                or final_event.payload.get("family_kind") != mutation.family_kind
                or final_event.payload.get("mutation_kind") != mutation.mutation_kind
                or final_event.payload.get("mutation_id") != mutation.mutation_id
                or final_event.payload.get("mutation_fingerprint")
                != self.family_mutation_fingerprint
                or final_event.payload.get("family_version")
                != family_receipt.family_version
                or final_event.payload.get("operation_request_ref_digest")
                != expected_reference_digest
            ):
                raise ValueError("family admission final outbox event is inconsistent")
        elif any(
            item is not None
            for item in (
                command.projection,
                command.budget,
                command.effects,
                command.transition,
            )
        ) or command.events:
            raise ValueError("non-accepted family command cannot carry authoritative mutations")


class RunControlRepository(Protocol):
    async def get_admission_decision(
        self, request_scope: str, idempotency_issuer: str, request_id: str
    ) -> AdmissionDecision | None: ...

    async def commit_admission(self, mutation: AdmissionMutation) -> AdmissionDecision: ...

    async def get_command_result(
        self,
        request_scope: str,
        run_id: str,
        idempotency_issuer: str,
        command_id: str,
    ) -> CommandResult | None: ...

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection: ...

    async def get_budget(self, request_scope: str, run_id: str) -> BudgetState: ...

    async def get_effects(self, request_scope: str, run_id: str) -> EffectLedgerState: ...

    async def commit_command(self, mutation: CommandMutation) -> CommandResult: ...

    async def get_family_admission_receipt(
        self,
        request_scope: str,
        run_id: str,
        idempotency_issuer: str,
        command_id: str,
    ) -> FamilyAdmissionReceipt | None: ...

    async def get_family_head(
        self,
        request_scope: str,
        run_id: str,
        family_kind: str,
        mutation_type: type[M],
    ) -> M | None: ...

    async def commit_family_admission(
        self, commit: FamilyAdmissionCommit
    ) -> FamilyAdmissionReceipt: ...

    async def list_transitions(
        self, request_scope: str, run_id: str
    ) -> tuple[LifecycleTransitionRecord, ...]: ...

    async def list_budget_ledger(
        self, request_scope: str, run_id: str
    ) -> tuple[BudgetLedgerEntry, ...]: ...

    async def list_effect_ledger(
        self, request_scope: str, run_id: str
    ) -> tuple[EffectLedgerEntry, ...]: ...

    async def list_outbox(
        self,
        request_scope: str,
        *,
        after: OutboxCursor | None = None,
        limit: int = 100,
    ) -> tuple[OutboxRecord, ...]: ...

    async def mark_outbox_delivered(
        self, request_scope: str, event_id: str, delivered_at: datetime
    ) -> None: ...

    async def apply_consumer_event(
        self, request_scope: str, consumer_id: str, envelope: DomainEventEnvelope
    ) -> ConsumerApplyResult: ...


class InMemoryRunControlRepository:
    """Behavioral test adapter with the same atomic boundaries as PostgreSQL."""

    def __init__(self, *, before_commit: FailureHook | None = None) -> None:
        self._lock = asyncio.Lock()
        self._before_commit = before_commit
        self._admissions: dict[tuple[str, str, str], AdmissionDecision] = {}
        self._commands: dict[tuple[str, str, str], CommandResult] = {}
        self._runs: dict[str, RunProjection] = {}
        self._budgets: dict[str, BudgetState] = {}
        self._effects: dict[str, EffectLedgerState] = {}
        self._transitions: dict[str, list[LifecycleTransitionRecord]] = {}
        self._ledger: dict[str, list[BudgetLedgerEntry]] = {}
        self._effect_ledger: dict[str, list[EffectLedgerEntry]] = {}
        self._outbox: dict[str, OutboxRecord] = {}
        self._next_outbox_position = 1
        self._cursors: dict[tuple[str, str], ConsumerCursor] = {}
        self._family_heads: dict[tuple[str, str, str], AtomicFamilyMutation] = {}
        self._family_journal: dict[tuple[str, str, str, str], tuple[str, AtomicFamilyMutation]] = {}
        self._family_results: dict[
            tuple[str, str, str, str], FamilyAdmissionReceipt
        ] = {}

    async def get_admission_decision(
        self, request_scope: str, idempotency_issuer: str, request_id: str
    ) -> AdmissionDecision | None:
        decision = self._admissions.get((request_scope, idempotency_issuer, request_id))
        return deepcopy(decision)

    async def commit_admission(self, mutation: AdmissionMutation) -> AdmissionDecision:
        key = (
            mutation.decision.request_scope,
            mutation.decision.idempotency_issuer,
            mutation.decision.request_id,
        )
        async with self._lock:
            prior = self._admissions.get(key)
            if prior is not None:
                if prior.request_fingerprint != mutation.decision.request_fingerprint:
                    raise IdempotencyConflict(
                        "run request identity was reused with a conflicting payload"
                    )
                return deepcopy(prior)
            if mutation.projection is not None:
                self._validate_accepted_admission(mutation)
                if mutation.projection.run_id in self._runs:
                    raise IdempotencyConflict("workflow run identity already exists")
            if mutation.projection is not None:
                assert mutation.budget is not None
                assert mutation.effects is not None
                assert mutation.transition is not None
                run_id = mutation.projection.run_id
                self._apply_parent_rollup(
                    None,
                    mutation.budget,
                    idempotency_id=f"admission:{mutation.decision.request_id}",
                    occurred_at=mutation.decision.recorded_at,
                )
                self._runs[run_id] = deepcopy(mutation.projection)
                self._budgets[run_id] = deepcopy(mutation.budget)
                self._effects[run_id] = deepcopy(mutation.effects)
                self._transitions[run_id] = [deepcopy(mutation.transition)]
                self._ledger[run_id] = list(deepcopy(mutation.ledger_entries))
                self._effect_ledger[run_id] = list(deepcopy(mutation.effect_entries))
                self._insert_events(mutation.events)
            self._admissions[key] = deepcopy(mutation.decision)
            return deepcopy(mutation.decision)

    async def get_command_result(
        self,
        request_scope: str,
        run_id: str,
        idempotency_issuer: str,
        command_id: str,
    ) -> CommandResult | None:
        self._require_scope(request_scope, run_id)
        if (request_scope, run_id, idempotency_issuer, command_id) in self._family_results:
            raise IdempotencyConflict(
                "combined family command identity cannot be replayed as a plain command"
            )
        return deepcopy(self._commands.get((run_id, idempotency_issuer, command_id)))

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection:
        self._require_scope(request_scope, run_id)
        try:
            return deepcopy(self._runs[run_id])
        except KeyError as exc:
            raise RunControlNotFound(f"workflow run not found: {run_id}") from exc

    async def get_budget(self, request_scope: str, run_id: str) -> BudgetState:
        self._require_scope(request_scope, run_id)
        try:
            return deepcopy(self._budgets[run_id])
        except KeyError as exc:
            raise RunControlNotFound(f"budget account not found for run: {run_id}") from exc

    async def get_effects(self, request_scope: str, run_id: str) -> EffectLedgerState:
        self._require_scope(request_scope, run_id)
        try:
            return deepcopy(self._effects[run_id])
        except KeyError as exc:
            raise RunControlNotFound(f"effect ledger not found for run: {run_id}") from exc

    async def commit_command(self, mutation: CommandMutation) -> CommandResult:
        key = (
            mutation.request_scope,
            mutation.result.run_id,
            mutation.result.idempotency_issuer,
            mutation.result.command_id,
        )
        async with self._lock:
            if key in self._family_results:
                raise IdempotencyConflict(
                    "combined family command identity cannot be replayed as a plain command"
                )
            return self._commit_command_unlocked(mutation)

    def _commit_command_unlocked(self, mutation: CommandMutation) -> CommandResult:
        key = (
            mutation.result.run_id,
            mutation.result.idempotency_issuer,
            mutation.result.command_id,
        )
        prior = self._commands.get(key)
        if prior is not None:
            if prior.command_fingerprint != mutation.result.command_fingerprint:
                raise IdempotencyConflict(
                    "lifecycle command identity was reused with a conflicting payload"
                )
            return deepcopy(prior)
        current = self._runs.get(mutation.result.run_id)
        if current is None or current.request_scope != mutation.request_scope:
            raise RunControlNotFound(f"workflow run not found: {mutation.result.run_id}")
        current_budget_digest = authority_state_digest(
            self._budgets[mutation.result.run_id]
        )
        current_effects_digest = authority_state_digest(
            self._effects[mutation.result.run_id]
        )
        if (
            mutation.expected_budget_digest != current_budget_digest
            or mutation.expected_effects_digest != current_effects_digest
        ):
            raise AuthorityStateConflict(
                "budget or effect authority changed while the command was being decided"
            )
        if mutation.projection is None:
            raced = current.version != mutation.expected_version
            result = mutation.result.model_copy(
                update={
                    "status": (
                        CommandStatus.STALE
                        if raced and mutation.result.status == CommandStatus.REJECTED
                        else mutation.result.status
                    ),
                    "resulting_run_version": current.version,
                    "phase": current.phase,
                    "terminal_outcome": current.terminal_outcome,
                    "reason_code": (
                        "stale_run_version"
                        if raced and mutation.result.status == CommandStatus.REJECTED
                        else mutation.result.reason_code
                    ),
                    "reason": (
                        "run advanced while the rejected command was being decided"
                        if raced and mutation.result.status == CommandStatus.REJECTED
                        else mutation.result.reason
                    ),
                }
            )
            self._commands[key] = deepcopy(result)
            return deepcopy(result)
        if current.version != mutation.expected_version:
            raise RunVersionConflict(
                f"expected version {mutation.expected_version}, "
                f"current version is {current.version}"
            )
        self._validate_accepted_command(mutation)
        assert mutation.budget is not None
        assert mutation.effects is not None
        assert mutation.transition is not None
        self._apply_parent_rollup(
            self._budgets[mutation.result.run_id],
            mutation.budget,
            idempotency_id=f"command:{mutation.result.command_id}",
            occurred_at=mutation.result.recorded_at,
        )
        self._runs[mutation.result.run_id] = deepcopy(mutation.projection)
        self._budgets[mutation.result.run_id] = deepcopy(mutation.budget)
        self._effects[mutation.result.run_id] = deepcopy(mutation.effects)
        self._transitions[mutation.result.run_id].append(deepcopy(mutation.transition))
        self._ledger[mutation.result.run_id].extend(deepcopy(mutation.ledger_entries))
        self._effect_ledger[mutation.result.run_id].extend(deepcopy(mutation.effect_entries))
        self._insert_events(mutation.events)
        self._commands[key] = deepcopy(mutation.result)
        return deepcopy(mutation.result)

    async def get_family_admission_receipt(
        self,
        request_scope: str,
        run_id: str,
        idempotency_issuer: str,
        command_id: str,
    ) -> FamilyAdmissionReceipt | None:
        self._require_scope(request_scope, run_id)
        return deepcopy(
            self._family_results.get(
                (request_scope, run_id, idempotency_issuer, command_id)
            )
        )

    async def get_family_head(
        self,
        request_scope: str,
        run_id: str,
        family_kind: str,
        mutation_type: type[M],
    ) -> M | None:
        self._require_scope(request_scope, run_id)
        mutation = self._family_heads.get((request_scope, run_id, family_kind))
        return mutation_type.model_validate(mutation.model_dump()) if mutation is not None else None

    async def commit_family_admission(
        self, commit: FamilyAdmissionCommit
    ) -> FamilyAdmissionReceipt:
        commit.__post_init__()
        mutation = commit.family_mutation
        result = commit.command.result
        result_key = (
            mutation.request_scope,
            mutation.run_id,
            result.idempotency_issuer,
            result.command_id,
        )
        command_key = (mutation.run_id, result.idempotency_issuer, result.command_id)
        journal_key = (
            mutation.request_scope,
            mutation.run_id,
            mutation.family_kind,
            mutation.mutation_id,
        )
        head_key = (mutation.request_scope, mutation.run_id, mutation.family_kind)
        async with self._lock:
            prior_receipt = self._family_results.get(result_key)
            if prior_receipt is not None:
                if (
                    prior_receipt.command_result.command_fingerprint
                    != result.command_fingerprint
                    or prior_receipt.family_mutation_fingerprint
                    != commit.family_mutation_fingerprint
                ):
                    raise IdempotencyConflict(
                        "family admission identity was reused with conflicting content"
                    )
                return deepcopy(prior_receipt)
            if command_key in self._commands:
                raise IdempotencyConflict(
                    "plain lifecycle command identity cannot acquire a family mutation"
                )
            prior_mutation = self._family_journal.get(journal_key)
            if prior_mutation is not None:
                if prior_mutation[0] != commit.family_mutation_fingerprint:
                    raise IdempotencyConflict(
                        "family mutation identity was reused with conflicting content"
                    )
                raise IdempotencyConflict(
                    "family mutation identity was reused by another command"
                )
            current = self._runs.get(mutation.run_id)
            if current is None or current.request_scope != mutation.request_scope:
                raise RunControlNotFound(f"workflow run not found: {mutation.run_id}")
            accepted = commit.receipt.family_receipt is not None
            if accepted:
                current_head = self._family_heads.get(head_key)
                current_family_version = (
                    current_head.expected_family_version + 1 if current_head is not None else 0
                )
                if mutation.expected_family_version != current_family_version:
                    raise FamilyVersionConflict(
                        f"expected family version {mutation.expected_family_version}, "
                        f"current version is {current_family_version}"
                    )
            state_fields = (
                "_admissions",
                "_commands",
                "_runs",
                "_budgets",
                "_effects",
                "_transitions",
                "_ledger",
                "_effect_ledger",
                "_outbox",
                "_next_outbox_position",
                "_cursors",
                "_family_heads",
                "_family_journal",
                "_family_results",
            )
            working = object.__new__(type(self))
            working.__dict__ = {
                name: deepcopy(getattr(self, name)) for name in state_fields
            }
            committed_result = working._commit_command_unlocked(commit.command)
            if committed_result != commit.receipt.command_result:
                raise RunVersionConflict("combined command result changed while committing")
            await self._inject("family_admission.after_run_control")
            if accepted:
                working._family_journal[journal_key] = (
                    commit.family_mutation_fingerprint,
                    deepcopy(mutation),
                )
                working._family_heads[head_key] = deepcopy(mutation)
            working._family_results[result_key] = deepcopy(commit.receipt)
            await self._inject("family_admission.after_family")
            for name in state_fields:
                setattr(self, name, getattr(working, name))
            return deepcopy(commit.receipt)

    async def list_transitions(
        self, request_scope: str, run_id: str
    ) -> tuple[LifecycleTransitionRecord, ...]:
        self._require_scope(request_scope, run_id)
        return tuple(deepcopy(self._transitions[run_id]))

    async def list_budget_ledger(
        self, request_scope: str, run_id: str
    ) -> tuple[BudgetLedgerEntry, ...]:
        self._require_scope(request_scope, run_id)
        return tuple(deepcopy(self._ledger[run_id]))

    async def list_effect_ledger(
        self, request_scope: str, run_id: str
    ) -> tuple[EffectLedgerEntry, ...]:
        self._require_scope(request_scope, run_id)
        return tuple(deepcopy(self._effect_ledger[run_id]))

    async def list_outbox(
        self,
        request_scope: str,
        *,
        after: OutboxCursor | None = None,
        limit: int = 100,
    ) -> tuple[OutboxRecord, ...]:
        records = sorted(
            (
                item
                for item in self._outbox.values()
                if item.delivered_at is None
                and self._runs[item.envelope.aggregate_id].request_scope == request_scope
            ),
            key=lambda item: (item.cursor.position,),
        )
        if after is not None:
            records = [item for item in records if item.cursor.position > after.position]
        return tuple(deepcopy(records[:limit]))

    async def mark_outbox_delivered(
        self, request_scope: str, event_id: str, delivered_at: datetime
    ) -> None:
        async with self._lock:
            try:
                current = self._outbox[event_id]
            except KeyError as exc:
                raise RunControlNotFound(f"outbox event not found: {event_id}") from exc
            self._require_scope(request_scope, current.envelope.aggregate_id)
            self._outbox[event_id] = current.model_copy(
                update={
                    "delivery_attempts": current.delivery_attempts + 1,
                    "delivered_at": delivered_at,
                }
            )

    async def apply_consumer_event(
        self, request_scope: str, consumer_id: str, envelope: DomainEventEnvelope
    ) -> ConsumerApplyResult:
        authoritative = self._outbox.get(envelope.event_id)
        if authoritative is None or authoritative.envelope != envelope:
            raise RunControlNotFound(f"authoritative outbox event not found: {envelope.event_id}")
        self._require_scope(request_scope, envelope.aggregate_id)
        key = (consumer_id, envelope.aggregate_id)
        async with self._lock:
            cursor = self._cursors.get(
                key,
                ConsumerCursor(
                    consumer_id=consumer_id,
                    aggregate_id=envelope.aggregate_id,
                    last_aggregate_version=0,
                ),
            )
            same_version_next_sequence = (
                envelope.aggregate_version == cursor.last_aggregate_version
                and not cursor.last_version_final
                and envelope.sequence == cursor.last_sequence + 1
            )
            next_version_first_sequence = (
                envelope.aggregate_version == cursor.last_aggregate_version + 1
                and (cursor.last_aggregate_version == 0 or cursor.last_version_final)
                and envelope.sequence == 1
            )
            expected = (
                cursor.last_aggregate_version
                if cursor.last_sequence and same_version_next_sequence
                else cursor.last_aggregate_version + 1
            )
            already_applied = envelope.aggregate_version < cursor.last_aggregate_version or (
                envelope.aggregate_version == cursor.last_aggregate_version
                and envelope.sequence <= cursor.last_sequence
            )
            if already_applied:
                status = ConsumerApplyStatus.DUPLICATE
                next_cursor = cursor
            elif not (same_version_next_sequence or next_version_first_sequence):
                status = ConsumerApplyStatus.GAP
                next_cursor = cursor
            else:
                status = ConsumerApplyStatus.APPLIED
                next_cursor = cursor.model_copy(
                    update={
                        "last_aggregate_version": envelope.aggregate_version,
                        "last_sequence": envelope.sequence,
                        "last_version_final": envelope.is_version_final,
                    }
                )
                self._cursors[key] = next_cursor
            return ConsumerApplyResult(
                status=status,
                cursor=deepcopy(next_cursor),
                expected_version=expected,
                observed_version=envelope.aggregate_version,
            )

    def _require_scope(self, request_scope: str, run_id: str) -> None:
        projection = self._runs.get(run_id)
        if projection is None or projection.request_scope != request_scope:
            raise RunControlNotFound(f"workflow run not found: {run_id}")

    def _apply_parent_rollup(
        self,
        prior_child: BudgetState | None,
        child: BudgetState,
        *,
        idempotency_id: str,
        occurred_at: datetime,
    ) -> None:
        if child.parent_account_id is None:
            return
        parent_run_id = next(
            (
                run_id
                for run_id, budget in self._budgets.items()
                if budget.account_id == child.parent_account_id
            ),
            None,
        )
        if parent_run_id is None:
            raise RunControlNotFound(f"parent budget account not found: {child.parent_account_id}")
        parent = self._budgets[parent_run_id]
        updated, entries = roll_up_child_budget(
            parent,
            prior_child,
            child,
            idempotency_id=idempotency_id,
            occurred_at=occurred_at,
        )
        self._apply_parent_rollup(
            parent,
            updated,
            idempotency_id=idempotency_id,
            occurred_at=occurred_at,
        )
        self._budgets[parent_run_id] = updated
        self._ledger[parent_run_id].extend(entries)

    def _insert_events(self, events: Sequence[DomainEventEnvelope]) -> None:
        for event in events:
            prior = self._outbox.get(event.event_id)
            if prior is not None and prior.envelope != event:
                raise IdempotencyConflict(f"outbox event collision: {event.event_id}")
            self._outbox[event.event_id] = OutboxRecord(
                envelope=deepcopy(event),
                cursor=OutboxCursor(
                    position=self._next_outbox_position,
                    recorded_at=event.recorded_at,
                    aggregate_id=event.aggregate_id,
                    aggregate_version=event.aggregate_version,
                    sequence=event.sequence,
                ),
            )
            self._next_outbox_position += 1

    async def _inject(self, boundary: str) -> None:
        if self._before_commit is None:
            return
        result = self._before_commit(boundary)
        if inspect.isawaitable(result):
            await result

    @staticmethod
    def _validate_accepted_admission(mutation: AdmissionMutation) -> None:
        if (
            mutation.budget is None
            or mutation.effects is None
            or mutation.transition is None
            or not mutation.events
        ):
            raise ValueError("accepted admission must include all transactional effects")

    @staticmethod
    def _validate_accepted_command(mutation: CommandMutation) -> None:
        if (
            mutation.budget is None
            or mutation.effects is None
            or mutation.transition is None
            or not mutation.events
        ):
            raise ValueError("accepted command must include all transactional effects")
