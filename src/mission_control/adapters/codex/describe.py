"""The `codex` lane's own describe matrix (MP-08): what the harness implements, none of it
qualified.

The integrator-owned `application/execution/harness/describe.py::CODEX_DESCRIBE` stays the
registered stub until the integrator applies the delta proposed in the MP-08 handoff; this
module is that proposal in code, and the harness reports it. Every cell is honest about this
pin (`codex-cli 0.162.0`, app-server protocol v2 over stdio, Linux/WSL workers):

- controls: `prepare`, `start`, `send_turn`, `cancel_turn`, `observe`, `usage`,
  `end_session` native; `reattach` and `snapshot` emulated (a relaunch plus `thread/resume`
  and history; a git patch); `pause` emulated (approval requests are held at the tool gate,
  `pause_at_tool_gate`); `fork` unqualified (not built: a conversation fork is not a Git
  branch and no drill backs `thread/fork`).
- delivery: `queue_instruction` and `resume` are `wait_then_send` (a `turn/start` on an
  active thread would steer it, so the lane only sends on an idle thread);
  `interrupt_and_inject` reads the frozen `LANE_COMMAND_SEMANTICS` cell so the describe and
  the workflow's receipts always agree (the harness implements both `cooperative_inject` via
  `turn/steer` and `cancel_and_replace`); `cancel` is `turn_boundary_guaranteed`.
- identity: thread id, turn id, item id; the cursor is `turn_id@connection_epoch:event_seq`.
- features: implemented on fixtures only; `qualified=False` everywhere until the owner-run
  drill (docs/qualification/lanes/codex/README.md); `environment_selection` is unsupported
  on a local workspace lane.
"""

from __future__ import annotations

from typing import Final

from mission_control.adapters.codex.protocol import (
    PINNED_CODEX_CLI_VERSION,
    PINNED_PROTOCOL_VERSION,
    PINNED_SCHEMA_SHA256,
)
from mission_control.application.execution.harness.describe import CODEX_DESCRIBE
from mission_control.domain.execution.lanes import LaneDescribe

TRANSPORT: Final = "app-server jsonrpc v2 over stdio"


def codex_local_describe() -> LaneDescribe:
    """The describe the harness reports: the declared matrix, checked against this pin."""

    versions = CODEX_DESCRIBE.versions
    if (
        versions.get("codex_cli") != PINNED_CODEX_CLI_VERSION
        or versions.get("app_server_protocol") != PINNED_PROTOCOL_VERSION
        or versions.get("app_server_schema_sha256") != PINNED_SCHEMA_SHA256
    ):
        raise RuntimeError("the declared codex describe does not match the pinned app-server")
    return CODEX_DESCRIBE


CODEX_LOCAL_DESCRIBE: Final = codex_local_describe()

__all__ = ["CODEX_LOCAL_DESCRIBE", "TRANSPORT", "codex_local_describe"]
