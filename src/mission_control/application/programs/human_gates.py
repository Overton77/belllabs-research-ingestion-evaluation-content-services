"""Human Gate wiring for the program families (MP-10).

- ``stagegraph_human_gates``: the gate specs of a manifest Stage Graph, keyed by the lowered
  stage id (a ``human_gate`` child lowers to the stage of the same key). A remediation route
  is attached only when the caller declares it; the frozen manifest schema has no field for
  one, so a gate lowered from a manifest admits ``approve | deny`` until it does.
- ``goal_human_review``: the review spec of a Goal Loop whose root acceptance (or a
  criterion's) requires ``human``; remediation is the loop's own executor.
- ``GateReservationSettlement``: a gate stage is admitted like any stage (the reducer owns
  admission and its reservation), but no cognition runs; its reservation is released whole
  against zero usage before the result is decided, as the baseline is (RRM-021).
"""

from __future__ import annotations

from collections.abc import Mapping

from mission_control.application.execution.service import RunControlService
from mission_control.application.programs.service import orchestration_lifecycle_actor
from mission_control.domain.authoring.manifest import Behavior
from mission_control.domain.authoring.mission_definition import (
    AcceptanceExpression,
    MissionDefinition,
)
from mission_control.domain.policies.contracts import (
    CommandStatus,
    LifecycleCommand,
    RecordUsageAction,
)
from mission_control.domain.programs.human_gate import (
    GateReservationSettlementRequest,
    GateReservationSettlementResult,
    HumanGateSpec,
    gate_spec_from_manifest,
)

GOAL_REVIEW_GATE_KEY = "goal-review"
SETTLEMENT_ATTEMPTS = 4


def stagegraph_human_gates(
    definition: MissionDefinition,
    *,
    remediation: Mapping[str, tuple[str, int]] | None = None,
) -> tuple[HumanGateSpec, ...]:
    """Specs for every ``human_gate`` stage; ``remediation[gate] = (target stage, rounds)``."""

    root = definition.program
    children = root.nodes if root.behavior is Behavior.STAGE_GRAPH else (root,)
    keys = {child.key for child in children}
    specs = []
    for child in children:
        if child.behavior is not Behavior.HUMAN_GATE:
            continue
        target, rounds = (remediation or {}).get(child.key, (None, 1))
        if target is not None and target not in keys:
            raise ValueError(f"human gate {child.key}: remediation target {target} is not a stage")
        specs.append(
            gate_spec_from_manifest(child, remediation_target=target, max_review_rounds=rounds)
        )
    return tuple(specs)


def _requires_human(expression: AcceptanceExpression | None) -> bool:
    if expression is None:
        return False
    if expression.op == "human":
        return True
    return any(_requires_human(item) for item in expression.operands)


def goal_human_review(
    definition: MissionDefinition,
    *,
    reviewers: tuple[str, ...],
    max_review_rounds: int = 2,
    timeout_seconds: int | None = None,
) -> HumanGateSpec | None:
    """The review gate of a Goal Loop whose acceptance requires an attributable human."""

    root = definition.program
    if root.behavior is not Behavior.GOAL_LOOP:
        return None
    required = _requires_human(root.completion) or any(
        _requires_human(item.acceptance) for item in definition.criteria
    )
    if not required:
        return None
    return HumanGateSpec(
        gate_key=GOAL_REVIEW_GATE_KEY,
        task_kind="REVIEW",
        prompt=f"Review the verified outputs of mission {definition.mission_key}.",
        reviewers=reviewers,
        timeout_seconds=timeout_seconds,
        remediation_target="goal/executor",
        max_review_rounds=max_review_rounds,
    )


class GateReservationSettlement:
    """Release one gate stage's reservation through run control (idempotent)."""

    def __init__(self, run_control: RunControlService) -> None:
        self._run_control = run_control

    async def settle(
        self, request: GateReservationSettlementRequest
    ) -> GateReservationSettlementResult:
        result_version = 0
        reason_code = ""
        accepted = True
        for attempt in range(SETTLEMENT_ATTEMPTS):
            budget = await self._run_control.get_budget(request.request_scope, request.run_id)
            run = await self._run_control.get_run(request.request_scope, request.run_id)
            remaining = {
                dimension: amount
                for dimension, amount in budget.reservations.get(request.reservation_id, {}).items()
                if amount > 0
            }
            if not remaining:
                return GateReservationSettlementResult(
                    accepted=True, resulting_run_version=run.version
                )
            result = await self._run_control.execute(
                LifecycleCommand(
                    command_id=(
                        f"human-gate:{request.reservation_id}:settlement"
                        if attempt == 0
                        else f"human-gate:{request.reservation_id}:settlement:"
                        f"at-version:{run.version}"
                    ),
                    idempotency_issuer=request.idempotency_issuer,
                    request_scope=request.request_scope,
                    run_id=request.run_id,
                    expected_run_version=run.version,
                    actor=orchestration_lifecycle_actor(),
                    action=RecordUsageAction(
                        usage_id=f"human-gate-usage:{request.reservation_id}",
                        actual_amounts={},
                        reservation_id=request.reservation_id,
                        release_amounts=remaining,
                    ),
                    reason="Release a Human Gate stage reservation (no cognition ran)",
                    occurred_at=request.occurred_at,
                    correlation_id=request.correlation_id,
                )
            )
            accepted = result.status == CommandStatus.ACCEPTED
            result_version = result.resulting_run_version
            reason_code = result.reason_code
            if result.status != CommandStatus.STALE:
                break
        else:
            raise RuntimeError("human gate reservation settlement is stale; retrying")
        return GateReservationSettlementResult(
            accepted=accepted, resulting_run_version=result_version, reason_code=reason_code
        )


__all__ = [
    "GOAL_REVIEW_GATE_KEY",
    "GateReservationSettlement",
    "goal_human_review",
    "stagegraph_human_gates",
]
