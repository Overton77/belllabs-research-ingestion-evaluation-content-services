"""REQ-CP-RUN-009 / REQ-CP-RUN-007: an async child in the parent run's run-control ledger.

The child is a reservation carved from the parent run's budget, an effect claim whose
`operation_ref` is the parent binding, and a registered async child. Its usage settles exactly
once; pending usage stays pending and leaves the effect unsettled.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from app.application.async_subagents.parent_effects import (
    RunControlAsyncChildEffects,
    async_child_effect_id,
    async_child_usage_id,
)
from app.application.async_subagents.service import (
    AsyncSubagentService,
    InMemoryAsyncSubagentAuthority,
    InMemoryAsyncSubagentDetailRepository,
)
from app.domain.run_control.contracts import (
    BudgetApplicability,
    BudgetDimensionLimit,
    CommandStatus,
    EffectDisposition,
    ReserveBudgetAction,
    StartAction,
)
from tests.acceptance.control_plane.test_wp_cp_045 import NOW, DeterministicProvider, request
from tests.unit.run_control.test_run_control import actor, command
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service


async def admitted_parent(*, bounded: dict[str, int] | None = None) -> tuple[object, str]:
    """An admitted, started parent run; `bounded` turns the named dimensions into bounded ones."""

    run_control, _ = run_control_service()
    parent = run_request(request_id="rrm-013-parent")
    if bounded:
        envelope = parent.budget_envelope
        dimensions = tuple(
            BudgetDimensionLimit(
                dimension=limit.dimension,
                applicability=BudgetApplicability.BOUNDED,
                hard_cap=bounded[limit.dimension],
            )
            if limit.dimension in bounded
            else limit
            for limit in envelope.dimensions
        )
        parent = parent.model_copy(
            update={"budget_envelope": envelope.model_copy(update={"dimensions": dimensions})}
        )
    decision = await run_control.admit(parent)
    assert decision.run_id is not None
    started = await run_control.execute(command(decision.run_id, 1, "start", StartAction()))
    assert started.status == CommandStatus.ACCEPTED
    reserved = await run_control.execute(
        command(
            decision.run_id,
            started.resulting_run_version,
            "reserve-parent",
            ReserveBudgetAction(
                reservation_id="reservation:parent-operation", amounts={"tokens.total": 20}
            ),
        )
    )
    assert reserved.status == CommandStatus.ACCEPTED
    return run_control, decision.run_id


def spawn(run_id: str, *, limits: dict[str, int] | None = None):  # type: ignore[no-untyped-def]
    base = request()
    contract = base.contract.model_copy(update={"budget_limits": limits or {"tokens.total": 10}})
    contract = type(contract).create(
        **contract.model_dump(mode="python", exclude={"contract_digest"})
    )
    return base.model_copy(
        update={
            "request_scope": "tenant-1",
            "parent_run_id": run_id,
            "contract": contract,
            "parent_reservation_id": "reservation:parent-operation",
        }
    )


@pytest.mark.asyncio
async def test_child_is_claimed_as_a_parent_effect_before_submission_and_usage_settles_once() -> (
    None
):
    run_control, run_id = await admitted_parent()
    effects = RunControlAsyncChildEffects(run_control, actor=actor())  # type: ignore[arg-type]
    events: list[str] = []
    provider = DeterministicProvider(events)
    service = AsyncSubagentService(
        InMemoryAsyncSubagentDetailRepository(),
        InMemoryAsyncSubagentAuthority(),
        provider,
        parent_effects=effects,
        allow_new_spawns=True,
    )
    child = await service.spawn(spawn(run_id))
    ledger = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    claim = ledger.claims[async_child_effect_id(child.child_execution_id)]
    assert claim.operation_ref == "binding-1"
    assert claim.effect_kind == "async_subagent.child"
    assert claim.reservation_id == "reservation-child-1"
    assert claim.disposition == EffectDisposition.PENDING
    budget = await run_control.get_budget("tenant-1", run_id)  # type: ignore[attr-defined]
    assert budget.reservations["reservation-child-1"] == {"tokens.total": 10}
    projection = await run_control.get_run("tenant-1", run_id)  # type: ignore[attr-defined]
    assert [item.child_execution_id for item in projection.async_children] == [
        child.child_execution_id
    ]
    assert events.index("provider.start") > 0

    # A retried spawn replays the same commands without a second reservation or claim.
    await service.spawn(spawn(run_id))
    assert provider.starts == 1
    budget = await run_control.get_budget("tenant-1", run_id)  # type: ignore[attr-defined]
    assert budget.reserved["tokens.total"] == 50

    provider.next_status = "success"
    completed = await service.reconcile("tenant-1", child.child_execution_id)
    assert completed.result_manifest is not None
    await service.decide_result(
        "tenant-1",
        child.child_execution_id,
        "admit",
        parent_open=True,
        current_generation=1,
        decided_at=NOW,
    )
    await service.settle("tenant-1", child.child_execution_id, "settlement:child", NOW)
    await service.settle("tenant-1", child.child_execution_id, "settlement:child", NOW)
    budget = await run_control.get_budget("tenant-1", run_id)  # type: ignore[attr-defined]
    usage = budget.usage_records[async_child_usage_id(child.child_execution_id)]
    assert usage.actual_amounts == {"tokens.total": 7}
    assert usage.release_amounts == {"tokens.total": 3}
    assert budget.consumed["tokens.total"] == 7
    assert "reservation-child-1" not in budget.reservations
    assert budget.reserved["tokens.total"] == 40
    ledger = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    settled = ledger.claims[async_child_effect_id(child.child_execution_id)]
    assert settled.disposition == EffectDisposition.SUCCEEDED
    assert settled.settlement is not None
    assert settled.settlement.usage_settlement_ref == async_child_usage_id(child.child_execution_id)


@pytest.mark.asyncio
async def test_in_doubt_child_leaves_its_parent_effect_ambiguous_and_unsettled() -> None:
    run_control, run_id = await admitted_parent()
    effects = RunControlAsyncChildEffects(run_control, actor=actor())  # type: ignore[arg-type]
    events: list[str] = []
    provider = DeterministicProvider(events)
    service = AsyncSubagentService(
        InMemoryAsyncSubagentDetailRepository(),
        InMemoryAsyncSubagentAuthority(),
        provider,
        parent_effects=effects,
        allow_new_spawns=True,
    )

    async def ambiguous_start(*_args: object) -> object:
        raise ConnectionError("submission result unknown")

    async def unobservable(*_args: object) -> object:
        raise ConnectionError("server unreachable")

    provider.start = ambiguous_start  # type: ignore[method-assign]
    provider.observe_spawn_key = unobservable  # type: ignore[method-assign]
    child = await service.spawn(spawn(run_id))
    assert child.lifecycle.value == "in_doubt"
    ledger = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    claim = ledger.claims[async_child_effect_id(child.child_execution_id)]
    assert claim.disposition == EffectDisposition.AMBIGUOUS
    assert claim.settlement is None
    assert claim.operation_ref == "binding-1"


@pytest.mark.asyncio
async def test_pending_usage_is_recorded_pending_and_blocks_effect_settlement() -> None:
    run_control, run_id = await admitted_parent()
    effects = RunControlAsyncChildEffects(run_control, actor=actor())  # type: ignore[arg-type]
    disposition = None
    child_id = "child-pending"
    await effects.reserve_and_claim(spawn(run_id), child_id)
    await effects.observe(
        "tenant-1",
        run_id,
        child_id,
        disposition="cancelled",
        observation_id=f"async-terminal:{child_id}",
        provider_effect_ref="run-x",
        evidence_refs=(),
        observed_at=NOW,
    )
    disposition = await effects.settle_usage(
        "tenant-1",
        run_id,
        child_id,
        reservation_id="reservation-child-1",
        budget_limits={"tokens.total": 10},
        attributed_amounts={},
        pending_amounts={"tokens.total": 5},
        outcome="cancelled",
        observation_id=f"async-terminal:{child_id}",
        settlement_ref="settlement:pending",
        settlement_revision=1,
        settled_at=NOW + timedelta(seconds=1),
    )
    assert disposition == "pending_usage"
    budget = await run_control.get_budget("tenant-1", run_id)  # type: ignore[attr-defined]
    assert budget.pending_settlement["tokens.total"] == 5
    assert async_child_usage_id(child_id) in budget.outstanding_usage_ids
    ledger = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    assert ledger.claims[async_child_effect_id(child_id)].settlement is None

    # RRM-013 review N1: a later revision with the amounts known settles the outstanding usage
    # exactly against its source pending amounts, then the effect; a replay is a no-op.
    disposition = await effects.settle_usage(
        "tenant-1",
        run_id,
        child_id,
        reservation_id="reservation-child-1",
        budget_limits={"tokens.total": 10},
        attributed_amounts={"tokens.total": 3},
        pending_amounts={},
        outcome="cancelled",
        observation_id=f"async-terminal:{child_id}",
        settlement_ref="settlement:pending",
        settlement_revision=2,
        settled_at=NOW + timedelta(seconds=2),
    )
    assert disposition == "settled"
    budget = await run_control.get_budget("tenant-1", run_id)  # type: ignore[attr-defined]
    assert budget.pending_settlement["tokens.total"] == 0
    assert budget.consumed["tokens.total"] == 3
    assert async_child_usage_id(child_id) not in budget.outstanding_usage_ids
    settlement = budget.usage_settlements[f"{async_child_usage_id(child_id)}:settlement:2"]
    assert settlement.settled_amounts == {"tokens.total": 3}
    assert settlement.released_amounts == {"tokens.total": 2}
    ledger = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    claim = ledger.claims[async_child_effect_id(child_id)]
    assert claim.disposition == EffectDisposition.CANCELLED
    assert claim.settlement is not None
    assert claim.settlement.usage_settlement_ref == f"{async_child_usage_id(child_id)}:settlement:2"
    replay = await effects.settle_usage(
        "tenant-1",
        run_id,
        child_id,
        reservation_id="reservation-child-1",
        budget_limits={"tokens.total": 10},
        attributed_amounts={"tokens.total": 3},
        pending_amounts={},
        outcome="cancelled",
        observation_id=f"async-terminal:{child_id}",
        settlement_ref="settlement:pending",
        settlement_revision=3,
        settled_at=NOW + timedelta(seconds=3),
    )
    assert replay == "settled"


@pytest.mark.asyncio
async def test_reconciled_usage_above_the_pending_ceiling_is_consumed_in_full() -> None:
    """RRM-013 re-review G3: the attributed fact is never capped at the pending ceiling."""

    run_control, run_id = await admitted_parent()
    effects = RunControlAsyncChildEffects(run_control, actor=actor())  # type: ignore[arg-type]
    child_id = "child-overage"
    await effects.reserve_and_claim(spawn(run_id), child_id)
    await effects.observe(
        "tenant-1",
        run_id,
        child_id,
        disposition="cancelled",
        observation_id=f"async-terminal:{child_id}",
        provider_effect_ref="run-x",
        evidence_refs=(),
        observed_at=NOW,
    )
    first = await effects.settle_usage(
        "tenant-1",
        run_id,
        child_id,
        reservation_id="reservation-child-1",
        budget_limits={"tokens.total": 10},
        attributed_amounts={},
        pending_amounts={"tokens.total": 5},
        outcome="cancelled",
        observation_id=f"async-terminal:{child_id}",
        settlement_ref="settlement:overage",
        settlement_revision=1,
        settled_at=NOW + timedelta(seconds=1),
    )
    assert first == "pending_usage"
    second = await effects.settle_usage(
        "tenant-1",
        run_id,
        child_id,
        reservation_id="reservation-child-1",
        budget_limits={"tokens.total": 10},
        attributed_amounts={"tokens.total": 8},
        pending_amounts={},
        outcome="cancelled",
        observation_id=f"async-terminal:{child_id}",
        settlement_ref="settlement:overage",
        settlement_revision=2,
        settled_at=NOW + timedelta(seconds=2),
    )
    assert second == "settled"
    budget = await run_control.get_budget("tenant-1", run_id)  # type: ignore[attr-defined]
    # 8 attributed: 5 reconcile the pending amount exactly, 3 are consumed as overage.
    assert budget.consumed["tokens.total"] == 8
    assert budget.pending_settlement["tokens.total"] == 0
    assert async_child_usage_id(child_id) not in budget.outstanding_usage_ids
    settlement = budget.usage_settlements[f"{async_child_usage_id(child_id)}:settlement:2"]
    assert settlement.settled_amounts == {"tokens.total": 8}
    assert settlement.released_amounts == {}
    assert settlement.source_pending_amounts == {"tokens.total": 5}
    ledger = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    claim = ledger.claims[async_child_effect_id(child_id)]
    assert claim.settlement is not None
    assert claim.settlement.usage_settlement_ref == f"{async_child_usage_id(child_id)}:settlement:2"
