"""Composition helpers for the `claude_agent_sdk` lane (MP-07; mirrors `adapters/codex/compose.py`).

`bootstrap/**` and `adapters/temporal/deployment_composition.py` are integrator-owned. This
module keeps the lane's own composition in one place so that wiring is a few lines:

- `broker_permissions` builds the production `PermissionBindingPort` over MP-11's worker
  `ApprovalBroker` and checks that the bounded approval wait stays below the segment budget;
- `compose_claude_local` consults the host gate (`host.HostUnsupported` on Windows) before it
  builds anything, then the harness over the production `SdkClientFactory`, the MP-04
  workspace port, the projection source, MP-05 auth admission, the explicit child
  environment builder, the FT-G3 fences/intents and the permission port
  (`DenyWithoutGateway` - fail closed - when no broker is passed).
"""

from __future__ import annotations

from collections.abc import Mapping

from mission_control.adapters.claude.approvals import BrokerPermissionBinding
from mission_control.adapters.claude.harness import (
    AuthAdmitter,
    ChildEnvironmentBuilder,
    ClaudeAgentSdkHarness,
    ClaudeLaneSettings,
)
from mission_control.adapters.claude.host import HostGate, host_gate
from mission_control.adapters.claude.permissions import DenyWithoutGateway, PermissionBindingPort
from mission_control.adapters.claude.session import ClaudeClientFactory
from mission_control.adapters.claude.transport import SdkClientFactory
from mission_control.adapters.claude.workspace import ClaudeWorkspace
from mission_control.adapters.cursor.projection import (
    DurableInputReader,
    ProjectionSource,
    RowsResolver,
)
from mission_control.application.execution.approvals import ApprovalTimeoutPolicy
from mission_control.application.execution.approvals_broker import ApprovalBroker
from mission_control.application.execution.harness.hook_callbacks import HookIntentLedger
from mission_control.application.execution.operations.lane_outputs import LaneOutputCustody
from mission_control.application.execution.stop_fence import StopFenceRepository
from mission_control.application.frames.kinds import UnknownKindCounter
from mission_control.domain.execution.lanes import LaneSegmentBounds

DEFAULT_SEGMENT_BUDGET_S = float(LaneSegmentBounds().max_duration_s)


def broker_permissions(
    broker: ApprovalBroker,
    *,
    wait_seconds: float,
    reviewers: tuple[str, ...],
    segment_budget_s: float = DEFAULT_SEGMENT_BUDGET_S,
    timeout_seconds: int | None = None,
    on_timeout: ApprovalTimeoutPolicy = "keep_waiting",
) -> BrokerPermissionBinding:
    """The production permission port: bind through the worker's broker (one per worker
    process, `connection_ref` = the MP-06 session owner ref) and wait at most `wait_seconds`,
    which must stay below the segment budget the `lane.turn` activity runs under."""

    return BrokerPermissionBinding(
        broker,
        wait_seconds=wait_seconds,
        reviewers=reviewers,
        timeout_seconds=timeout_seconds,
        on_timeout=on_timeout,
        segment_budget_s=segment_budget_s,
    )


def compose_claude_local(
    *,
    workspaces: ClaudeWorkspace,
    projections: ProjectionSource,
    auth: AuthAdmitter,
    child_environment: ChildEnvironmentBuilder,
    environ: Mapping[str, str],
    rows: RowsResolver | None = None,
    fences: StopFenceRepository | None = None,
    intents: HookIntentLedger | None = None,
    inputs: DurableInputReader | None = None,
    settings: ClaudeLaneSettings | None = None,
    permissions: PermissionBindingPort | None = None,
    clients: ClaudeClientFactory | None = None,
    unknown_kinds: UnknownKindCounter | None = None,
    host: HostGate | None = None,
    outputs: LaneOutputCustody | None = None,
) -> ClaudeAgentSdkHarness:
    """The `claude_agent_sdk` harness; refuses (typed `HostUnsupported`) on a Windows host."""

    (host or host_gate()).require()
    return ClaudeAgentSdkHarness(
        clients=clients or SdkClientFactory(),
        workspaces=workspaces,
        projections=projections,
        auth=auth,
        child_environment=child_environment,
        environ=environ,
        permissions=permissions or DenyWithoutGateway(),
        fences=fences,
        intents=intents,
        inputs=inputs,
        rows=rows,
        settings=settings or ClaudeLaneSettings(),
        unknown_kinds=unknown_kinds,
        outputs=outputs,
    )


__all__ = ["DEFAULT_SEGMENT_BUDGET_S", "broker_permissions", "compose_claude_local"]
