"""Heartbeat timeouts per operation class (RRM-009 composition of RRM-008, REQ-CP-EXEC-008).

`OperationWorkflowRequest.heartbeat_timeout_seconds` is the heartbeat timeout of the unit's
`operation.execute` and `operation.cancel` Activities. It bounds two things:

* worker-loss detection: a holder that stops heartbeating is detected after it;
* cancel latency: a requested cancel reaches the running Activity on the next heartbeat the
  SDK actually sends. The Activity heartbeats every third of the timeout, but the worker
  throttles sends to at most one per `0.8 * heartbeat_timeout` (capped at 60 s), so a cancel
  lands within about `0.8 * timeout` (plus one delivery round trip) of its delivery.

A deployment chooses the timeout per operation class, so a unit that holds async children
(whose provider runs keep spending until they are cancelled) reaches its cancel sooner than
plain cognition. A worker's graceful shutdown must be shorter than every timeout it serves:
the draining holder then ends (never settling anything: a worker shutdown is not a cancel,
RRM-008 F1) before the server treats it as lost, and Temporal's retry lands on a live worker.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from mission_control.domain.execution.contracts import OperationExecutionRequest
from mission_control.domain.execution.lanes import LaneSegmentBounds

OperationHeartbeatClass = Literal["deep_agent", "deep_agent_async_children", "bound"]
# temporalio throttles heartbeat sends to `heartbeat_timeout * 0.8`, capped by the worker's
# `max_heartbeat_throttle_interval` (default 60 s).
SDK_HEARTBEAT_THROTTLE_FRACTION = 0.8
SDK_MAX_HEARTBEAT_THROTTLE_SECONDS = 60.0
MIN_HEARTBEAT_TIMEOUT_SECONDS = 1
MAX_HEARTBEAT_TIMEOUT_SECONDS = 3_600


def operation_heartbeat_class(operation: OperationExecutionRequest) -> OperationHeartbeatClass:
    """The class of a bound operation: Deep Agent cognition, with or without async children,
    or another bound runtime."""

    binding = operation.deep_agent_binding
    if binding is None:
        return "bound"
    return "deep_agent_async_children" if binding.async_subagents else "deep_agent"


def cancel_latency_bound_seconds(heartbeat_timeout_seconds: int) -> float:
    """Upper bound on the time from an accepted cancel's delivery to the running Activity
    observing it: the SDK sends at most one heartbeat per `0.8 * timeout` (at most 60 s)."""

    return min(
        SDK_HEARTBEAT_THROTTLE_FRACTION * heartbeat_timeout_seconds,
        SDK_MAX_HEARTBEAT_THROTTLE_SECONDS,
    )


@dataclass(frozen=True)
class OperationHeartbeatPolicy:
    """Heartbeat timeouts (seconds) per operation class. The defaults equal the contract's
    default (30 s), so a family composed without a policy behaves exactly as before."""

    deep_agent_seconds: int = 30
    deep_agent_async_children_seconds: int = 30
    bound_seconds: int = 30
    # FT-G2: run Deep Agents units through the `lane.turn` segment loop (Cursor units always
    # are). Off by default: `operation.execute` stays the Deep Agents path until FT-G6.
    deep_agent_segment_loop: bool = False

    def __post_init__(self) -> None:
        for value in self.timeouts().values():
            if not MIN_HEARTBEAT_TIMEOUT_SECONDS <= value <= MAX_HEARTBEAT_TIMEOUT_SECONDS:
                raise ValueError(
                    "operation heartbeat timeouts must be between "
                    f"{MIN_HEARTBEAT_TIMEOUT_SECONDS} and {MAX_HEARTBEAT_TIMEOUT_SECONDS} s"
                )

    def timeouts(self) -> dict[OperationHeartbeatClass, int]:
        return {
            "deep_agent": self.deep_agent_seconds,
            "deep_agent_async_children": self.deep_agent_async_children_seconds,
            "bound": self.bound_seconds,
        }

    def timeout_for(self, operation: OperationExecutionRequest) -> int:
        return self.timeouts()[operation_heartbeat_class(operation)]

    def segments_for(self, operation: OperationExecutionRequest) -> LaneSegmentBounds | None:
        """`OperationWorkflowRequest.segments`: present when the unit runs through the
        `lane.turn` segment loop (SPEC-07 section 4.2)."""

        if operation.execution_runtime == "cursor":
            binding = operation.cursor_binding
            assert binding is not None
            # `wait_then_send` is bounded by the binding's wall clock (SPEC-07 Further Notes).
            return LaneSegmentBounds(
                heartbeat_timeout_s=min(self.timeout_for(operation), 600),
                busy_wait_s=min(binding.budgets.wall_clock_s, 86_400),
            )
        if self.deep_agent_segment_loop and operation.execution_runtime == "deep_agent":
            return LaneSegmentBounds()
        return None

    @property
    def shortest_seconds(self) -> int:
        return min(self.timeouts().values())

    def verify_graceful_shutdown(self, graceful_shutdown_seconds: float) -> None:
        """Refuse a worker drain that would outlive the shortest heartbeat timeout."""

        if graceful_shutdown_seconds < 0:
            raise ValueError("worker graceful shutdown must be non-negative")
        if graceful_shutdown_seconds >= self.shortest_seconds:
            raise ValueError(
                f"worker graceful shutdown ({graceful_shutdown_seconds:g} s) must be shorter "
                f"than the shortest operation heartbeat timeout ({self.shortest_seconds} s)"
            )

    def disclosure(self) -> dict[str, dict[str, float]]:
        return {
            name: {
                "heartbeat_timeout_seconds": value,
                "cancel_latency_bound_seconds": cancel_latency_bound_seconds(value),
            }
            for name, value in self.timeouts().items()
        }


DEFAULT_OPERATION_HEARTBEATS = OperationHeartbeatPolicy()

__all__ = [
    "DEFAULT_OPERATION_HEARTBEATS",
    "SDK_HEARTBEAT_THROTTLE_FRACTION",
    "SDK_MAX_HEARTBEAT_THROTTLE_SECONDS",
    "OperationHeartbeatClass",
    "OperationHeartbeatPolicy",
    "cancel_latency_bound_seconds",
    "operation_heartbeat_class",
]
