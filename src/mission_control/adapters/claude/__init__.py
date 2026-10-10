"""The local Claude Agent SDK lane (`claude_agent_sdk`; MP-07 / OVE-70).

`harness.ClaudeAgentSdkHarness` is the Session Lane; `transport.SdkClientFactory` is its
production client factory over the pinned `claude-agent-sdk==0.2.165`; `describe` holds the
declared describe matrix (re-exported); `frames`, `session`, `hooks`, `permissions` and
`workspace` are the mapping, stream ownership, Kernel Hook, approval-binding and state-root
pieces. Imported by name (not re-exported here, to keep this package import light):
`approvals.BrokerPermissionBinding` (MP-11 broker port), `continuation` (MP-12 snapshots,
hydrator and registration), `host` (worker-host gate) and `compose` (composition helpers).
"""

from mission_control.adapters.claude.describe import CLAUDE_AGENT_SDK_LANE_DESCRIBE, PROFILE
from mission_control.adapters.claude.harness import (
    AuthAdmitter,
    ChildEnvironmentBuilder,
    ClaudeAgentSdkHarness,
    ClaudeLaneError,
    ClaudeLaneSettings,
    StaticAuthAdmitter,
)
from mission_control.adapters.claude.permissions import (
    DenyWithoutGateway,
    PermissionBindingPort,
    PermissionOutcome,
    PermissionRequest,
)
from mission_control.adapters.claude.session import ClaudeClient, ClaudeClientFactory, LiveSession
from mission_control.adapters.claude.workspace import AllocatedWorkspace, ClaudeWorkspace

__all__ = [
    "CLAUDE_AGENT_SDK_LANE_DESCRIBE",
    "PROFILE",
    "AllocatedWorkspace",
    "AuthAdmitter",
    "ChildEnvironmentBuilder",
    "ClaudeAgentSdkHarness",
    "ClaudeClient",
    "ClaudeClientFactory",
    "ClaudeLaneError",
    "ClaudeLaneSettings",
    "ClaudeWorkspace",
    "DenyWithoutGateway",
    "LiveSession",
    "PermissionBindingPort",
    "PermissionOutcome",
    "PermissionRequest",
    "StaticAuthAdmitter",
]
