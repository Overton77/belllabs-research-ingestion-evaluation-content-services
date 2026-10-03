"""RRM-021 review fixes: `StageGraphDecisionService.settle_baseline` edge cases.

A run-control double drives the service: the unit under test is its handling of zero-amount
baseline dimensions and of repeated stale run versions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from mission_control.application.programs.service import (
    BASELINE_SETTLEMENT_ATTEMPTS,
    StageGraphDecisionService,
)
from mission_control.domain.policies.contracts import (
    CommandStatus,
    LifecycleCommand,
    RecordUsageAction,
)
from mission_control.domain.programs.contracts import StageGraphBaselineSettlementRequest

SCOPE = "tenant-1"
RUN = "run-rrm-021"


@dataclass
class _Result:
    status: CommandStatus
    resulting_run_version: int
    reason_code: str


@dataclass
class _RunControl:
    """Reserved baseline; the first `stale` executions find the run moved on."""

    baseline: dict[str, int]
    stale: int = 0
    version: int = 1
    commands: list[LifecycleCommand] = field(default_factory=list)

    async def get_budget(self, _scope: str, _run_id: str) -> Any:
        reservations = {"baseline": dict(self.baseline)} if self.baseline else {}
        return type("Budget", (), {"reservations": reservations})()

    async def get_run(self, _scope: str, _run_id: str) -> Any:
        return type("Run", (), {"version": self.version})()

    async def execute(self, command: LifecycleCommand) -> _Result:
        self.commands.append(command)
        if self.stale > 0:
            self.stale -= 1
            self.version += 1  # an outside command moved the run
            return _Result(CommandStatus.STALE, self.version, "stale_run_version")
        assert command.expected_run_version == self.version
        assert isinstance(command.action, RecordUsageAction)
        self.baseline = {}
        self.version += 1
        return _Result(CommandStatus.ACCEPTED, self.version, "accepted")


def _request(baseline: dict[str, int]) -> StageGraphBaselineSettlementRequest:
    return StageGraphBaselineSettlementRequest(
        run_id=RUN,
        request_scope=SCOPE,
        occurred_at=datetime.now(UTC),
        idempotency_issuer="rrm-021",
        correlation_id="rrm-021:unit",
        baseline_reservation=baseline,
    )


def _service(run_control: _RunControl) -> StageGraphDecisionService:
    return StageGraphDecisionService(cast(Any, run_control), cast(Any, None))


@pytest.mark.asyncio
async def test_a_zero_amount_baseline_dimension_does_not_trip_the_mismatch_check() -> None:
    run_control = _RunControl(baseline={"tokens.total": 20, "cost.usd": 0})
    result = await _service(run_control).settle_baseline(
        _request({"tokens.total": 20, "cost.usd": 0})
    )
    assert result.accepted
    action = run_control.commands[0].action
    assert isinstance(action, RecordUsageAction)
    assert action.release_amounts == {"tokens.total": 20}
    assert run_control.baseline == {}


@pytest.mark.asyncio
async def test_a_zero_amount_dimension_on_one_side_only_is_still_the_same_baseline() -> None:
    run_control = _RunControl(baseline={"tokens.total": 20})
    result = await _service(run_control).settle_baseline(
        _request({"tokens.total": 20, "cost.usd": 0})
    )
    assert result.accepted and run_control.baseline == {}


@pytest.mark.asyncio
async def test_a_genuinely_different_baseline_is_still_refused() -> None:
    run_control = _RunControl(baseline={"tokens.total": 20, "cost.usd": 0})
    with pytest.raises(ValueError, match="differs from the admitted"):
        await _service(run_control).settle_baseline(_request({"tokens.total": 21}))
    assert run_control.commands == []


@pytest.mark.asyncio
async def test_repeated_stale_versions_are_retried_under_new_command_identities() -> None:
    run_control = _RunControl(baseline={"tokens.total": 20}, stale=BASELINE_SETTLEMENT_ATTEMPTS - 1)
    result = await _service(run_control).settle_baseline(_request({"tokens.total": 20}))
    assert result.accepted and run_control.baseline == {}
    identities = [command.command_id for command in run_control.commands]
    assert len(identities) == BASELINE_SETTLEMENT_ATTEMPTS
    assert len(set(identities)) == len(identities), "each retry is a new command"


@pytest.mark.asyncio
async def test_a_run_that_keeps_moving_raises_a_retryable_error_then_settles_once() -> None:
    run_control = _RunControl(baseline={"tokens.total": 20}, stale=BASELINE_SETTLEMENT_ATTEMPTS)
    service = _service(run_control)
    with pytest.raises(RuntimeError, match="stale"):
        await service.settle_baseline(_request({"tokens.total": 20}))
    assert run_control.baseline == {"tokens.total": 20}, "nothing released while stale"
    # The Activity retry re-reads the run and releases the baseline exactly once.
    result = await service.settle_baseline(_request({"tokens.total": 20}))
    assert result.accepted and run_control.baseline == {}
    again = await service.settle_baseline(_request({"tokens.total": 20}))
    assert again.accepted and len(run_control.commands) == BASELINE_SETTLEMENT_ATTEMPTS + 1
