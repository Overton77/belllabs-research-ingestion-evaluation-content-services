"""MP-20: a Session Lane settlement charges the run budget with the lane's closing usage and
parses its Completion Candidate from the full final text.

Found by the MP-20 parity work: a manifest Stage Graph stage reserves `operation.attempts`
and `concurrency.slots` (a Goal Loop iteration `goal.iterations`), so a lane settlement that
charged only the unit's bounded dimensions recorded no tokens against the run on any lane;
and the candidate was parsed from the 4,096-character closing excerpt. Here the lane turn
service runs a scripted lane through the governed in-memory boundary with an authority that
names the run's declared dimensions (`RunBudgetDimensionsPort`, as `RunControlOperation
Authority` does in production).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from mission_control.application.execution.harness.lane_turns import (
    FinalTextLane,
    LaneExecutionIdentity,
)
from mission_control.application.execution.operations.operation_execution import (
    OperationBudgetViolation,
    RunBudgetDimensionsPort,
    RunControlOperationBudgetAuthority,
    _validate_bound_usage,
    bind_operation_execution_request,
    lane_usage_amounts,
)
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    OperationExecutionResult,
    RuntimeUsage,
)
from mission_control.domain.execution.lane_turns import (
    MAX_EXCERPT_CHARS,
    ClosingFacts,
    LaneTurnRequest,
)
from mission_control.domain.execution.lanes import LaneFrame, TurnHandle, UsageReport
from mission_control.domain.policies.contracts import (
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    CommandResult,
    CommandStatus,
    LifecycleCommand,
    RunPhase,
)
from tests.fixtures.lane_turns import (
    RecordingSignals,
    ScriptedSessionLane,
    cursor_operation,
    lane_stack,
    scripted_frames,
)

STAGE_RESERVATION = {"operation.attempts": 1, "concurrency.slots": 1}
RUN_DIMENSIONS = frozenset({"tokens.total", "goal.iterations", "operation.attempts"})


def _stage_operation() -> OperationExecutionRequest:
    """A Stage Graph stage unit: its limits are its reservation (no token dimension)."""

    return cursor_operation().model_copy(update={"budget_limits": dict(STAGE_RESERVATION)})


def _turn(operation: OperationExecutionRequest) -> LaneTurnRequest:
    return LaneTurnRequest.model_validate(
        {"operation": operation, "lane_profile": "cursor_local", "generation": 1}
    )


class _DeclaringAuthority:
    """The conformance authority plus the run's declared dimensions."""

    def __init__(self, inner: Any, dimensions: frozenset[str]) -> None:
        self._inner = inner
        self.dimensions = dimensions

    async def verify(self, request: Any) -> None:
        await self._inner.verify(request)

    async def verify_continuation(self, request: Any, binding: Any) -> None:
        await self._inner.verify_continuation(request, binding)

    async def verify_cancellation(self, request: Any, binding: Any) -> None:
        await self._inner.verify_cancellation(request, binding)

    async def charged_budget_dimensions(self, request_scope: str, run_id: str) -> frozenset[str]:
        del request_scope, run_id
        return self.dimensions


@dataclass
class _RecordingBudget:
    settlements: dict[str, RuntimeUsage] = field(default_factory=dict)

    async def reconcile(
        self, *, binding: Any, settlement_id: str, usage: RuntimeUsage, budget_violation: bool
    ) -> None:
        del binding, budget_violation
        self.settlements[settlement_id] = usage


def _stack(
    operation: OperationExecutionRequest,
    lane: ScriptedSessionLane | None = None,
    dimensions: frozenset[str] | None = RUN_DIMENSIONS,
) -> tuple[Any, _RecordingBudget]:
    stack = lane_stack(lane, operation=operation)
    budget = _RecordingBudget()
    stack.boundary._budget = budget
    if dimensions is not None:
        stack.boundary._authority = _DeclaringAuthority(stack.boundary._authority, dimensions)
    return stack, budget


def _signals(stack: Any, operation: OperationExecutionRequest) -> RecordingSignals:
    identity = LaneExecutionIdentity.of(operation, "cursor_local", 1)
    return RecordingSignals(stack.frames, identity.harness_execution_id)


# --- usage --------------------------------------------------------------------------------------


async def test_a_stage_unit_charges_its_settled_tokens_on_the_runs_declared_dimension() -> None:
    operation = _stage_operation()
    stack, budget = _stack(operation)
    assert isinstance(stack.boundary._authority, RunBudgetDimensionsPort)

    result = await stack.service.turn(_turn(operation), _signals(stack, operation))

    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert settled.status == "completed"
    # The lane's settled usage (15 tokens) is charged once on `tokens.total`; the run does
    # not declare `tokens.input`/`tokens.output`, so nothing is invented for them.
    assert settled.usage.amounts == {"tokens.total": 15}
    assert settled.usage.pending_external_amounts == {}
    (charged,) = budget.settlements.values()
    assert charged.amounts == {"tokens.total": 15}


async def test_without_the_dimensions_port_a_stage_unit_charges_only_its_reservation() -> None:
    operation = _stage_operation()
    stack, budget = _stack(operation, dimensions=None)

    result = await stack.service.turn(_turn(operation), _signals(stack, operation))

    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert settled.status == "completed" and settled.usage.amounts == {}
    (charged,) = budget.settlements.values()
    assert charged.amounts == {}


@dataclass
class _UnknownUsageLane(ScriptedSessionLane):
    async def usage(self, request: Any) -> UsageReport:
        self.calls.append("usage")
        return UsageReport(disposition="unknown")

    def closing_facts(self, turn: TurnHandle, frame: LaneFrame) -> ClosingFacts:
        return (
            super()
            .closing_facts(turn, frame)
            .model_copy(
                update={"usage": UsageReport(disposition="unknown"), "cost_disposition": "unknown"}
            )
        )


async def test_unknown_lane_usage_charges_nothing_and_is_never_zero() -> None:
    operation = _stage_operation()
    stack, budget = _stack(operation, _UnknownUsageLane(frames=scripted_frames()))

    result = await stack.service.turn(_turn(operation), _signals(stack, operation))

    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert settled.status == "completed"
    assert "tokens.total" not in settled.usage.amounts
    assert result.closing_facts is not None
    assert result.closing_facts.usage.disposition == "unknown"
    (charged,) = budget.settlements.values()
    assert charged.amounts == {} and charged.pending_external_amounts == {}


def test_lane_usage_amounts_follow_the_disposition_and_the_declared_dimensions() -> None:
    binding = bind_operation_execution_request(_stage_operation())
    estimated = ClosingFacts(
        native_status="finished",
        usage=UsageReport(
            disposition="estimated", input_tokens=7, output_tokens=3, total_tokens=10
        ),
    )
    assert lane_usage_amounts(binding, estimated, frozenset({"tokens.total"})) == {
        "tokens.total": 10
    }
    assert lane_usage_amounts(binding, estimated) == {}
    declared = frozenset({"tokens.input", "tokens.output", "tokens.total"})
    assert lane_usage_amounts(binding, estimated, declared) == {
        "tokens.input": 7,
        "tokens.output": 3,
        "tokens.total": 10,
    }
    unknown = ClosingFacts(native_status="finished")
    assert lane_usage_amounts(binding, unknown, declared) == {}


def test_unreserved_dimensions_are_charged_but_never_pending_or_undeclared() -> None:
    binding = bind_operation_execution_request(_stage_operation())
    unreserved = frozenset({"tokens.total"})
    _validate_bound_usage(
        binding, RuntimeUsage(amounts={"tokens.total": 10**9}), unreserved=unreserved
    )
    with pytest.raises(OperationBudgetViolation, match="unbound"):
        _validate_bound_usage(binding, RuntimeUsage(amounts={"tokens.total": 1}))
    with pytest.raises(OperationBudgetViolation, match="unbound"):
        _validate_bound_usage(
            binding,
            RuntimeUsage(pending_external_amounts={"tokens.total": 1}),
            unreserved=unreserved,
        )
    with pytest.raises(OperationBudgetViolation, match="unbound"):
        _validate_bound_usage(
            binding, RuntimeUsage(amounts={"tool.calls.total": 1}), unreserved=unreserved
        )
    # A bounded dimension keeps its bound.
    with pytest.raises(OperationBudgetViolation, match="exceeds"):
        _validate_bound_usage(
            binding, RuntimeUsage(amounts={"operation.attempts": 2}), unreserved=unreserved
        )


async def test_the_run_control_budget_adapter_records_unreserved_token_usage() -> None:
    binding = bind_operation_execution_request(_stage_operation())
    limits = (
        BudgetDimensionLimit(
            dimension="tokens.total", applicability=BudgetApplicability.BOUNDED, hard_cap=1000
        ),
        BudgetDimensionLimit(
            dimension="operation.attempts", applicability=BudgetApplicability.BOUNDED, hard_cap=5
        ),
    )

    class _RunControl:
        def __init__(self) -> None:
            self.commands: list[LifecycleCommand] = []

        async def get_run(self, _scope: str, _run_id: str) -> SimpleNamespace:
            return SimpleNamespace(version=3)

        async def get_budget(self, _scope: str, _run_id: str) -> SimpleNamespace:
            return SimpleNamespace(limits=limits, usage_ids=frozenset())

        async def execute(self, command: LifecycleCommand) -> CommandResult:
            self.commands.append(command)
            return CommandResult(
                command_id=command.command_id,
                idempotency_issuer=command.idempotency_issuer,
                run_id=command.run_id,
                command_fingerprint="sha256:" + "0" * 64,
                status=CommandStatus.ACCEPTED,
                resulting_run_version=4,
                phase=RunPhase.ACTIVE,
                reason_code="accepted",
                reason="usage recorded",
                recorded_at=command.occurred_at,
            )

    run_control = _RunControl()
    adapter = RunControlOperationBudgetAuthority(
        run_control,  # type: ignore[arg-type]
        actor=ActorContext(
            actor_id="operation-runtime", permissions=frozenset({"workflow_run.report_usage"})
        ),
    )
    await adapter.reconcile(
        binding=binding,
        settlement_id="settlement-1",
        usage=RuntimeUsage(amounts={"tokens.total": 40}),
    )
    (command,) = run_control.commands
    assert command.action.actual_amounts == {"tokens.total": 40}
    # The unit's own reservation is released in full; nothing is drawn from it for tokens.
    assert command.action.release_amounts == STAGE_RESERVATION


# --- the Completion Candidate from the full final text --------------------------------------------


@dataclass
class _FinalTextLane(ScriptedSessionLane):
    """A scripted lane whose terminal body holds a final answer longer than the excerpt."""

    def closing_facts(self, turn: TurnHandle, frame: LaneFrame) -> ClosingFacts:
        del turn
        body = frame.body if isinstance(frame.body, dict) else {}
        return ClosingFacts(
            native_status=body.get("status", "finished"),
            result_excerpt=str(body.get("result", ""))[:MAX_EXCERPT_CHARS],
            usage=UsageReport(disposition="estimated", total_tokens=12),
            cost_disposition="estimated",
        )

    def final_text(self, turn: TurnHandle, frame: LaneFrame) -> str | None:
        del turn
        body = frame.body if isinstance(frame.body, dict) else {}
        result = body.get("result")
        return result if isinstance(result, str) else None


async def test_the_candidate_is_parsed_from_the_lanes_full_final_text() -> None:
    answer = json.dumps({"obligation_refs": ["handed_off"], "note": "x" * (3 * MAX_EXCERPT_CHARS)})
    frames = scripted_frames()
    frames[-1] = ("result", {"status": "finished", "result": answer})
    lane = _FinalTextLane(frames=frames)
    assert isinstance(lane, FinalTextLane)
    operation = cursor_operation()
    stack, _budget = _stack(operation, lane, dimensions=None)

    result = await stack.service.turn(_turn(operation), _signals(stack, operation))

    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert settled.status == "completed"
    assert settled.structured_output is not None
    assert settled.structured_output["obligation_refs"] == ["handed_off"]
    assert result.closing_facts is not None
    assert len(result.closing_facts.result_excerpt) == MAX_EXCERPT_CHARS
