"""The typed subset of the Codex app-server protocol v2 the lane speaks (MP-08).

Every shape below is transcribed from the pinned generated schema
(`tests/fixtures/provider_frames/codex/schema/codex_app_server_protocol.v2.schemas.json`,
`codex-cli 0.162.0`, `PIN.json`); the unit suite checks the method registries against the
committed registry files, so a re-pin that renames a method or a field is a failing test,
never a silent drift. Fields are snake_case with the wire's camelCase as alias
(`populate_by_name`); inbound models tolerate fields this pin does not name
(`extra="allow"`), outbound params are sent exactly as declared (`by_alias`, `exclude_none`).

Schema facts the lane relies on (definition names in the bundle):

- `ThreadStartParams.approvalPolicy: AskForApproval = untrusted | on-request | never |
  {granular}`; `SandboxMode = read-only | workspace-write | danger-full-access`;
  `ApprovalsReviewer = user | auto_review | guardian_subagent`.
- `TurnStartParams.clientUserMessageId: string | null` and
  `UserMessageThreadItem.clientId: string | null`; `TurnStartParams.turnTrigger` says "Ignored
  when this request steers an already-active turn": a `turn/start` on an active thread is a
  steer, so the lane never issues one unless the thread is idle.
- `TurnSteerParams.expectedTurnId` is a "required active turn id precondition. The request
  fails when it does not match the currently active turn".
- `TurnStatus = completed | interrupted | failed | inProgress`; `TurnError.codexErrorInfo:
  CodexErrorInfo` whose string variants include `usageLimitExceeded`, `rateLimitExceeded`,
  `serverOverloaded`, `unauthorized`, `contextWindowExceeded`.
- `ThreadStatus.type = notLoaded | idle | systemError | active` with
  `ThreadActiveFlag = waitingOnApproval | waitingOnUserInput`.
- `ThreadReadParams.includeTurns` includes turns and their items from rollout history;
  `ThreadResumeParams` resumes "by thread_id: load the thread from disk".
- `ThreadCompactStartParams{threadId}`; the deprecated `thread/compacted` notification and the
  `ContextCompactionThreadItem` both report a compaction.
- Server requests (`ServerRequest.json`): `item/commandExecution/requestApproval`
  (`CommandExecutionApprovalDecision = accept | acceptForSession | decline | cancel |
  {acceptWithExecpolicyAmendment} | {applyNetworkPolicyAmendment}`),
  `item/fileChange/requestApproval` (`FileChangeApprovalDecision = accept | acceptForSession |
  decline | cancel`), `item/tool/requestUserInput` (`answers: {question_id: {answers: [..]}}`),
  `mcpServer/elicitation/request` (`McpServerElicitationAction = accept | decline | cancel`),
  `item/permissions/requestApproval` (a response grants `permissions`; refusal is a JSON-RPC
  error), `item/tool/call`, `account/chatgptAuthTokens/refresh`, `attestation/generate` and
  the v1 `applyPatchApproval` / `execCommandApproval`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.execution.usage_admission import LimitKind, ProviderLimitSignal

PINNED_CODEX_CLI_VERSION: Final = "0.162.0"
PINNED_PROTOCOL_VERSION: Final = "v2"
PINNED_SCHEMA_SHA256: Final = "0bf5254bede109d4ae03ce2e81372e4c93a30b359c0749ec7dce7a9382a7f857"
SCHEMA_FIXTURE_DIR: Final = "tests/fixtures/provider_frames/codex/schema"
CLIENT_NAME: Final = "mission-control"

# --- method registries (ClientRequest / ServerRequest / ServerNotification / ClientNotification)

CLIENT_REQUEST_METHODS: Final[frozenset[str]] = frozenset(
    {
        "initialize",
        "thread/start",
        "thread/resume",
        "thread/fork",
        "thread/archive",
        "thread/delete",
        "thread/unsubscribe",
        "thread/name/set",
        "thread/goal/set",
        "thread/goal/get",
        "thread/goal/clear",
        "thread/metadata/update",
        "thread/attachment/add",
        "thread/attachment/list",
        "thread/attachmentOwner/list",
        "thread/attachment/remove",
        "thread/section/move",
        "thread/unarchive",
        "thread/compact/start",
        "thread/shellCommand",
        "thread/approveGuardianDeniedAction",
        "thread/revert",
        "thread/list",
        "threadSection/list",
        "threadSection/create",
        "threadSection/update",
        "threadSection/delete",
        "thread/loaded/list",
        "thread/read",
        "thread/turns/list",
        "thread/items/list",
        "thread/inject_items",
        "skills/list",
        "skills/extraRoots/set",
        "hooks/list",
        "marketplace/add",
        "marketplace/remove",
        "marketplace/upgrade",
        "plugin/list",
        "plugin/installed",
        "plugin/reconcile",
        "plugin/read",
        "plugin/skill/read",
        "plugin/share/save",
        "plugin/share/updateTargets",
        "plugin/share/list",
        "plugin/share/checkout",
        "plugin/share/delete",
        "app/read",
        "app/list",
        "app/installed",
        "fs/readFile",
        "fs/writeFile",
        "fs/createDirectory",
        "fs/getMetadata",
        "fs/readDirectory",
        "fs/remove",
        "fs/copy",
        "fs/watch",
        "fs/unwatch",
        "skills/config/write",
        "plugin/install",
        "plugin/uninstall",
        "turn/start",
        "turn/steer",
        "turn/interrupt",
        "review/start",
        "model/list",
        "account/gatewayOAuth/read",
        "account/gatewayOAuth/login",
        "account/gatewayOAuth/cancel",
        "modelProvider/capabilities/read",
        "experimentalFeature/list",
        "permissionProfile/list",
        "experimentalFeature/enablement/set",
        "mcpServer/oauth/login",
        "config/mcpServer/reload",
        "mcpServerStatus/list",
        "mcpServer/resource/read",
        "mcpServer/tool/call",
        "windowsSandbox/setupStart",
        "windowsSandbox/readiness",
        "account/login/start",
        "account/login/cancel",
        "account/logout",
        "account/rateLimits/read",
        "account/rateLimitResetCredit/consume",
        "account/usage/read",
        "account/workspaceMessages/read",
        "account/sendAddCreditsNudgeEmail",
        "feedback/upload",
        "command/exec",
        "command/exec/write",
        "command/exec/terminate",
        "command/exec/resize",
        "config/read",
        "externalAgentConfig/detect",
        "externalAgentConfig/import",
        "externalAgentConfig/import/recordHistory",
        "externalAgentConfig/import/readHistories",
        "config/value/write",
        "config/batchWrite",
        "configRequirements/read",
        "account/read",
        "fuzzyFileSearch",
    }
)

SERVER_REQUEST_METHODS: Final[frozenset[str]] = frozenset(
    {
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
        "item/tool/requestUserInput",
        "mcpServer/elicitation/request",
        "item/permissions/requestApproval",
        "item/tool/call",
        "account/chatgptAuthTokens/refresh",
        "attestation/generate",
        "applyPatchApproval",
        "execCommandApproval",
    }
)

SERVER_NOTIFICATION_METHODS: Final[frozenset[str]] = frozenset(
    {
        "error",
        "thread/started",
        "thread/status/changed",
        "thread/archived",
        "thread/deleted",
        "thread/unarchived",
        "thread/closed",
        "thread/reverted",
        "skills/changed",
        "thread/name/updated",
        "thread/attachment/updated",
        "thread/goal/updated",
        "thread/prediction/updated",
        "thread/goal/cleared",
        "thread/queue/changed",
        "project/changed",
        "thread/project/updated",
        "thread/environment/connected",
        "thread/environment/disconnected",
        "thread/settings/updated",
        "thread/tokenUsage/updated",
        "turn/started",
        "hook/started",
        "turn/completed",
        "hook/completed",
        "turn/diff/updated",
        "turn/plan/updated",
        "item/started",
        "item/autoApprovalReview/started",
        "item/autoApprovalReview/completed",
        "autoApprovalReview/strictReviewRequired",
        "item/completed",
        "item/agentMessage/delta",
        "item/plan/delta",
        "command/exec/outputDelta",
        "process/outputDelta",
        "process/exited",
        "item/commandExecution/outputDelta",
        "item/commandExecution/terminalInteraction",
        "item/fileChange/outputDelta",
        "item/fileChange/patchUpdated",
        "serverRequest/resolved",
        "item/mcpToolCall/progress",
        "mcpServer/oauthLogin/completed",
        "mcpServer/startupStatus/updated",
        "mcpServer/event/stream/notification",
        "account/updated",
        "account/gatewayOAuth/changed",
        "account/rateLimits/updated",
        "app/list/updated",
        "remoteControl/status/changed",
        "externalAgentConfig/import/progress",
        "externalAgentConfig/import/completed",
        "fs/changed",
        "item/reasoning/summaryTextDelta",
        "item/reasoning/summaryPartAdded",
        "item/reasoning/textDelta",
        "thread/compacted",
        "model/rerouted",
        "model/verification",
        "modelProvider/authRecoveryStarted",
        "modelProvider/authRecoveryCompleted",
        "turn/moderationMetadata",
        "model/safetyBuffering/updated",
        "warning",
        "guardianWarning",
        "deprecationNotice",
        "configWarning",
        "fuzzyFileSearch/sessionUpdated",
        "fuzzyFileSearch/sessionCompleted",
        "thread/realtime/started",
        "thread/realtime/itemAdded",
        "thread/realtime/item/started",
        "thread/realtime/item/transcript/delta",
        "thread/realtime/item/completed",
        "thread/realtime/transcript/delta",
        "thread/realtime/transcript/done",
        "thread/realtime/outputAudio/delta",
        "thread/realtime/sdp",
        "thread/realtime/error",
        "thread/realtime/closed",
        "windows/worldWritableWarning",
        "windowsSandbox/setupCompleted",
        "account/login/completed",
    }
)

CLIENT_NOTIFICATION_METHODS: Final[frozenset[str]] = frozenset({"initialized"})

# The methods the lane issues (a subset of CLIENT_REQUEST_METHODS; checked at import).
M_INITIALIZE: Final = "initialize"
M_INITIALIZED: Final = "initialized"
M_THREAD_START: Final = "thread/start"
M_THREAD_RESUME: Final = "thread/resume"
M_THREAD_READ: Final = "thread/read"
M_THREAD_COMPACT_START: Final = "thread/compact/start"
M_TURN_START: Final = "turn/start"
M_TURN_STEER: Final = "turn/steer"
M_TURN_INTERRUPT: Final = "turn/interrupt"
LANE_REQUEST_METHODS: Final[frozenset[str]] = frozenset(
    {
        M_INITIALIZE,
        M_THREAD_START,
        M_THREAD_RESUME,
        M_THREAD_READ,
        M_THREAD_COMPACT_START,
        M_TURN_START,
        M_TURN_STEER,
        M_TURN_INTERRUPT,
    }
)
if not LANE_REQUEST_METHODS <= CLIENT_REQUEST_METHODS:  # pragma: no cover - import guard
    raise RuntimeError("the lane issues a method the pinned registry does not know")

# Notifications the lane reads for lifecycle (every other one is still a frame).
N_THREAD_STARTED: Final = "thread/started"
N_THREAD_STATUS_CHANGED: Final = "thread/status/changed"
N_TURN_STARTED: Final = "turn/started"
N_TURN_COMPLETED: Final = "turn/completed"
N_ITEM_STARTED: Final = "item/started"
N_ITEM_COMPLETED: Final = "item/completed"
N_USAGE_UPDATED: Final = "thread/tokenUsage/updated"
N_ERROR: Final = "error"
N_SERVER_REQUEST_RESOLVED: Final = "serverRequest/resolved"
N_THREAD_COMPACTED: Final = "thread/compacted"
N_RATE_LIMITS: Final = "account/rateLimits/updated"

# Server requests the lane answers through the approval-correlation port.
R_COMMAND_APPROVAL: Final = "item/commandExecution/requestApproval"
R_FILE_CHANGE_APPROVAL: Final = "item/fileChange/requestApproval"
R_USER_INPUT: Final = "item/tool/requestUserInput"
R_MCP_ELICITATION: Final = "mcpServer/elicitation/request"
R_PERMISSIONS_APPROVAL: Final = "item/permissions/requestApproval"
APPROVAL_REQUEST_METHODS: Final[frozenset[str]] = frozenset(
    {
        R_COMMAND_APPROVAL,
        R_FILE_CHANGE_APPROVAL,
        R_USER_INPUT,
        R_MCP_ELICITATION,
        R_PERMISSIONS_APPROVAL,
    }
)
# Server requests the lane never answers with a credential or an effect of its own.
REFUSED_SERVER_REQUEST_METHODS: Final[frozenset[str]] = SERVER_REQUEST_METHODS - (
    APPROVAL_REQUEST_METHODS
)

TurnStatus = Literal["completed", "interrupted", "failed", "inProgress"]
TERMINAL_TURN_STATUSES: Final[frozenset[str]] = frozenset({"completed", "interrupted", "failed"})
ThreadStatusType = Literal["notLoaded", "idle", "systemError", "active"]
AskForApproval = Literal["untrusted", "on-request", "never"]
SandboxMode = Literal["read-only", "workspace-write", "danger-full-access"]
ApprovalsReviewer = Literal["user", "auto_review", "guardian_subagent"]

# JSON-RPC error codes the lane emits when it refuses a server request (application range).
ERR_REFUSED_BY_MISSION_CONTROL: Final = -32001
ERR_DECLINED: Final = -32002


class _Outbound(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    def params(self) -> dict[str, Any]:
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)


class _Inbound(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True, populate_by_name=True)

    def wire(self) -> dict[str, Any]:
        """The shape as the server sends it (camelCase), for synthesized events."""

        return self.model_dump(mode="json", by_alias=True)


# --- outbound params ---------------------------------------------------------------------------


class ClientInfo(_Outbound):
    name: str
    title: str | None = None
    version: str


class InitializeCapabilities(_Outbound):
    experimental_api: bool = Field(default=False, alias="experimentalApi")


class InitializeParams(_Outbound):
    client_info: ClientInfo = Field(alias="clientInfo")
    capabilities: InitializeCapabilities | None = None


class ThreadStartParams(_Outbound):
    cwd: str | None = None
    model: str | None = None
    approval_policy: AskForApproval | None = Field(default=None, alias="approvalPolicy")
    approvals_reviewer: ApprovalsReviewer | None = Field(default=None, alias="approvalsReviewer")
    sandbox: SandboxMode | None = None
    ephemeral: bool | None = None
    developer_instructions: str | None = Field(default=None, alias="developerInstructions")
    service_name: str | None = Field(default=None, alias="serviceName")


class ThreadResumeParams(_Outbound):
    thread_id: str = Field(alias="threadId")
    cwd: str | None = None
    model: str | None = None
    approval_policy: AskForApproval | None = Field(default=None, alias="approvalPolicy")
    approvals_reviewer: ApprovalsReviewer | None = Field(default=None, alias="approvalsReviewer")
    sandbox: SandboxMode | None = None
    exclude_turns: bool = Field(default=False, alias="excludeTurns")


class ThreadReadParams(_Outbound):
    thread_id: str = Field(alias="threadId")
    include_turns: bool = Field(default=False, alias="includeTurns")


class ThreadCompactStartParams(_Outbound):
    thread_id: str = Field(alias="threadId")


class TextUserInput(_Outbound):
    type: Literal["text"] = "text"
    text: str


class TurnStartParams(_Outbound):
    thread_id: str = Field(alias="threadId")
    input: tuple[TextUserInput, ...]
    client_user_message_id: str | None = Field(default=None, alias="clientUserMessageId")
    effort: str | None = None


class TurnSteerParams(_Outbound):
    thread_id: str = Field(alias="threadId")
    expected_turn_id: str = Field(alias="expectedTurnId")
    input: tuple[TextUserInput, ...]
    client_user_message_id: str | None = Field(default=None, alias="clientUserMessageId")


class TurnInterruptParams(_Outbound):
    thread_id: str = Field(alias="threadId")
    turn_id: str = Field(alias="turnId")


# --- inbound shapes ----------------------------------------------------------------------------


class TurnError(_Inbound):
    message: str = ""
    codex_error_info: Any = Field(default=None, alias="codexErrorInfo")
    additional_details: str | None = Field(default=None, alias="additionalDetails")


class Turn(_Inbound):
    id: str
    status: str = "inProgress"
    items: tuple[dict[str, Any], ...] = ()
    error: TurnError | None = None
    started_at: int | None = Field(default=None, alias="startedAt")
    completed_at: int | None = Field(default=None, alias="completedAt")
    duration_ms: int | None = Field(default=None, alias="durationMs")

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL_TURN_STATUSES


class ThreadStatus(_Inbound):
    type: str = "idle"
    active_flags: tuple[str, ...] = Field(default=(), alias="activeFlags")


class Thread(_Inbound):
    id: str
    status: ThreadStatus = Field(default_factory=ThreadStatus)
    turns: tuple[Turn, ...] = ()
    model: str | None = None
    model_provider: str | None = Field(default=None, alias="modelProvider")
    cwd: str | None = None
    cli_version: str | None = Field(default=None, alias="cliVersion")
    session_id: str | None = Field(default=None, alias="sessionId")
    parent_thread_id: str | None = Field(default=None, alias="parentThreadId")


class ThreadStartResponse(_Inbound):
    thread: Thread
    model: str | None = None
    approval_policy: Any = Field(default=None, alias="approvalPolicy")


class ThreadResumeResponse(_Inbound):
    thread: Thread
    model: str | None = None


class ThreadReadResponse(_Inbound):
    thread: Thread


class TurnStartResponse(_Inbound):
    turn: Turn


class TurnSteerResponse(_Inbound):
    turn_id: str = Field(alias="turnId")


class InitializeResponse(_Inbound):
    user_agent: str = Field(default="", alias="userAgent")
    platform_os: str | None = Field(default=None, alias="platformOs")
    platform_family: str | None = Field(default=None, alias="platformFamily")
    codex_home: str | None = Field(default=None, alias="codexHome")


class TokenUsageBreakdown(_Inbound):
    input_tokens: int = Field(default=0, alias="inputTokens")
    cached_input_tokens: int = Field(default=0, alias="cachedInputTokens")
    output_tokens: int = Field(default=0, alias="outputTokens")
    reasoning_output_tokens: int = Field(default=0, alias="reasoningOutputTokens")
    total_tokens: int = Field(default=0, alias="totalTokens")


class ThreadTokenUsage(_Inbound):
    last: TokenUsageBreakdown = Field(default_factory=TokenUsageBreakdown)
    total: TokenUsageBreakdown = Field(default_factory=TokenUsageBreakdown)
    model_context_window: int | None = Field(default=None, alias="modelContextWindow")


class TokenUsageNotification(_Inbound):
    thread_id: str = Field(alias="threadId")
    turn_id: str = Field(alias="turnId")
    token_usage: ThreadTokenUsage = Field(alias="tokenUsage")


class TurnNotification(_Inbound):
    """`turn/started` and `turn/completed` params."""

    thread_id: str = Field(alias="threadId")
    turn: Turn


class ItemNotification(_Inbound):
    """`item/started` and `item/completed` params."""

    thread_id: str = Field(alias="threadId")
    turn_id: str = Field(alias="turnId")
    item: dict[str, Any]


class ErrorNotification(_Inbound):
    thread_id: str = Field(alias="threadId")
    turn_id: str = Field(alias="turnId")
    error: TurnError
    will_retry: bool = Field(default=False, alias="willRetry")


# --- server request shapes ---------------------------------------------------------------------


class ApprovalRequestParams(_Inbound):
    """The common identity of the approval-class server requests (item, turn, thread)."""

    thread_id: str = Field(alias="threadId")
    turn_id: str | None = Field(default=None, alias="turnId")
    item_id: str | None = Field(default=None, alias="itemId")
    reason: str | None = None


def approval_identity(method: str, params: Mapping[str, Any]) -> ApprovalRequestParams:
    del method  # every approval-class request carries at least `threadId`
    return ApprovalRequestParams.model_validate(dict(params))


CommandExecutionApprovalDecision = Literal["accept", "acceptForSession", "decline", "cancel"]
FileChangeApprovalDecision = Literal["accept", "acceptForSession", "decline", "cancel"]
McpServerElicitationAction = Literal["accept", "decline", "cancel"]


# --- limit refusals ----------------------------------------------------------------------------

# `CodexErrorInfo` string variants that are provider capacity conditions (the pinned enum).
CODEX_ERROR_INFO_LIMIT_KINDS: Final[Mapping[str, LimitKind]] = {
    "usageLimitExceeded": "quota_exhausted",
    "rateLimitExceeded": "rate_limited",
    "serverOverloaded": "overloaded",
    "unauthorized": "auth_failed",
}


def codex_error_info(value: Any) -> str | None:
    """The string variant of a `CodexErrorInfo` (object variants name their kind by key)."""

    if isinstance(value, str) and value:
        return value
    if isinstance(value, Mapping) and len(value) == 1:
        return str(next(iter(value)))
    return None


def limit_signal_from_error_info(
    info: Any, *, source: str, fixture: bool = False
) -> ProviderLimitSignal | None:
    """A `ProviderLimitSignal` when `info` is a capacity-class `CodexErrorInfo`, else None."""

    name = codex_error_info(info)
    kind = CODEX_ERROR_INFO_LIMIT_KINDS.get(name or "")
    if name is None or kind is None:
        return None
    return ProviderLimitSignal(
        lane_profile="codex", kind=kind, source=source, native_code=name[:128], fixture=fixture
    )


def limit_signal_from_rpc_error(
    data: Any, *, source: str, fixture: bool = False
) -> ProviderLimitSignal | None:
    """Classify a JSON-RPC error of a `turn/start` / `thread/start` as a limit refusal.

    Only an explicit `codexErrorInfo` (either spelling) in the error data is classified; the
    message text is never parsed, so an unrelated error is an error, not a wait.
    """

    if isinstance(data, Mapping):
        for key in ("codexErrorInfo", "codex_error_info"):
            if key in data:
                return limit_signal_from_error_info(data[key], source=source, fixture=fixture)
    return None


__all__ = [
    "APPROVAL_REQUEST_METHODS",
    "CLIENT_NAME",
    "CLIENT_NOTIFICATION_METHODS",
    "CLIENT_REQUEST_METHODS",
    "CODEX_ERROR_INFO_LIMIT_KINDS",
    "ERR_DECLINED",
    "ERR_REFUSED_BY_MISSION_CONTROL",
    "LANE_REQUEST_METHODS",
    "M_INITIALIZE",
    "M_INITIALIZED",
    "M_THREAD_COMPACT_START",
    "M_THREAD_READ",
    "M_THREAD_RESUME",
    "M_THREAD_START",
    "M_TURN_INTERRUPT",
    "M_TURN_START",
    "M_TURN_STEER",
    "N_ERROR",
    "N_ITEM_COMPLETED",
    "N_ITEM_STARTED",
    "N_RATE_LIMITS",
    "N_SERVER_REQUEST_RESOLVED",
    "N_THREAD_COMPACTED",
    "N_THREAD_STARTED",
    "N_THREAD_STATUS_CHANGED",
    "N_TURN_COMPLETED",
    "N_TURN_STARTED",
    "N_USAGE_UPDATED",
    "PINNED_CODEX_CLI_VERSION",
    "PINNED_PROTOCOL_VERSION",
    "PINNED_SCHEMA_SHA256",
    "REFUSED_SERVER_REQUEST_METHODS",
    "R_COMMAND_APPROVAL",
    "R_FILE_CHANGE_APPROVAL",
    "R_MCP_ELICITATION",
    "R_PERMISSIONS_APPROVAL",
    "R_USER_INPUT",
    "SCHEMA_FIXTURE_DIR",
    "SERVER_NOTIFICATION_METHODS",
    "SERVER_REQUEST_METHODS",
    "TERMINAL_TURN_STATUSES",
    "ApprovalRequestParams",
    "ClientInfo",
    "ErrorNotification",
    "InitializeCapabilities",
    "InitializeParams",
    "InitializeResponse",
    "ItemNotification",
    "TextUserInput",
    "Thread",
    "ThreadCompactStartParams",
    "ThreadReadParams",
    "ThreadReadResponse",
    "ThreadResumeParams",
    "ThreadResumeResponse",
    "ThreadStartParams",
    "ThreadStartResponse",
    "ThreadStatus",
    "ThreadTokenUsage",
    "TokenUsageBreakdown",
    "TokenUsageNotification",
    "Turn",
    "TurnError",
    "TurnInterruptParams",
    "TurnNotification",
    "TurnStartParams",
    "TurnStartResponse",
    "TurnSteerParams",
    "TurnSteerResponse",
    "approval_identity",
    "codex_error_info",
    "limit_signal_from_error_info",
    "limit_signal_from_rpc_error",
]
