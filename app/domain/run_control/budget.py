from __future__ import annotations

from datetime import datetime
from uuid import NAMESPACE_URL, uuid5

from app.domain.run_control.contracts import (
    BudgetLedgerEntry,
    BudgetLedgerKind,
    BudgetState,
)
from app.domain.run_control.reducer import ReductionRejected


def roll_up_child_budget(
    parent: BudgetState,
    prior_child: BudgetState | None,
    child: BudgetState,
    *,
    idempotency_id: str,
    occurred_at: datetime,
) -> tuple[BudgetState, tuple[BudgetLedgerEntry, ...]]:
    """Apply one child's authoritative deltas to its locked parent account."""
    if child.parent_account_id != parent.account_id:
        raise ReductionRejected(
            "parent_budget_mismatch", "child does not reference the locked parent account"
        )
    prior_reserved = prior_child.reserved if prior_child else {}
    prior_consumed = prior_child.consumed if prior_child else {}
    prior_pending = prior_child.pending_settlement if prior_child else {}
    dimensions = (
        child.reserved.keys()
        | child.consumed.keys()
        | child.pending_settlement.keys()
        | prior_reserved.keys()
        | prior_consumed.keys()
        | prior_pending.keys()
    )
    reserved = dict(parent.reserved)
    consumed = dict(parent.consumed)
    pending = dict(parent.pending_settlement)
    entries: list[BudgetLedgerEntry] = []

    reserved_delta: dict[str, int] = {}
    released_delta: dict[str, int] = {}
    consumed_delta: dict[str, int] = {}
    pending_delta: dict[str, int] = {}
    settled_pending_delta: dict[str, int] = {}
    for dimension in dimensions:
        reserve_change = child.reserved.get(dimension, 0) - prior_reserved.get(dimension, 0)
        consume_change = child.consumed.get(dimension, 0) - prior_consumed.get(dimension, 0)
        pending_change = child.pending_settlement.get(dimension, 0) - prior_pending.get(
            dimension, 0
        )
        if consume_change < 0:
            raise ReductionRejected("invalid_parent_rollup", "child consumption cannot decrease")
        reserved[dimension] = reserved.get(dimension, 0) + reserve_change
        consumed[dimension] = consumed.get(dimension, 0) + consume_change
        pending[dimension] = pending.get(dimension, 0) + pending_change
        if min(reserved[dimension], consumed[dimension], pending[dimension]) < 0:
            raise ReductionRejected("invalid_parent_rollup", "parent budget rollup became negative")
        if reserve_change > 0:
            reserved_delta[dimension] = reserve_change
        elif reserve_change < 0:
            released_delta[dimension] = -reserve_change
        if consume_change:
            consumed_delta[dimension] = consume_change
        if pending_change > 0:
            pending_delta[dimension] = pending_change
        elif pending_change < 0:
            settled_pending_delta[dimension] = -pending_change

    updated = parent.model_copy(
        update={
            "reserved": reserved,
            "consumed": consumed,
            "pending_settlement": pending,
            "reservations": {
                **parent.reservations,
                f"child:{child.account_id}": dict(child.reserved),
            },
        }
    )
    for limit in updated.limits:
        if limit.hard_cap is None or reserved_delta.get(limit.dimension, 0) <= 0:
            continue
        exposure = (
            updated.reserved.get(limit.dimension, 0)
            + updated.consumed.get(limit.dimension, 0)
            + updated.pending_settlement.get(limit.dimension, 0)
        )
        if exposure > limit.hard_cap:
            raise ReductionRejected(
                "parent_budget_hard_cap_exceeded",
                f"parent hard cap exceeded for {limit.dimension}",
            )

    effects = (
        (BudgetLedgerKind.RESERVATION, reserved_delta),
        (BudgetLedgerKind.RELEASE, released_delta),
        (BudgetLedgerKind.CONSUMPTION, consumed_delta),
        (BudgetLedgerKind.PENDING_SETTLEMENT, pending_delta),
        (BudgetLedgerKind.SETTLEMENT, settled_pending_delta),
    )
    for kind, amounts in effects:
        if amounts:
            entries.append(
                BudgetLedgerEntry(
                    entry_id=_stable_id(
                        "parent-ledger",
                        parent.account_id,
                        kind.value,
                        idempotency_id,
                    ),
                    account_id=parent.account_id,
                    run_id=parent.run_id,
                    kind=kind,
                    idempotency_id=f"child:{child.account_id}:{idempotency_id}",
                    amounts=amounts,
                    occurred_at=occurred_at,
                    parent_account_id=parent.parent_account_id,
                )
            )
    return updated, tuple(entries)


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))
