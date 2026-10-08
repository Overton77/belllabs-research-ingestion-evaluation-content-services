"""Lane contracts: `mc.lane_describe.v1`, the AgentHarness request and handle contracts and
`mc.cursor_binding.v1` (SPEC-07 sections 1 to 3, ADR-0018, ADR-0030; FT-G1).

A Harness is the provider-neutral protocol; a Lane is a qualified implementation; a Lane
Profile is one placement of a lane with its own control matrix. Everything here is a strict,
frozen, secret-free value: handles carry native identity, never credentials. The protocol
itself lives in `application/execution/harness/protocol.py`; these contracts are domain values
so that `domain/execution/contracts.py` can pair a `cursor` runtime with its binding.
"""

from __future__ import annotations

from typing import Any, Final, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationInfo, model_validator

from mission_control.domain.authoring.canonical import stable_json_digest

LANE_DESCRIBE_SCHEMA: Final = "mc.lane_describe.v1"
CURSOR_BINDING_SCHEMA: Final = "mc.cursor_binding.v1"
DIGEST_PATTERN: Final = r"^sha256:[0-9a-f]{64}$"

LaneProfileName = Literal["deep_agents", "cursor_local", "cursor_cloud"]
LaneName = Literal["deep_agents", "cursor"]
ExecutionRuntime = Literal["native", "deep_agent", "cursor"]
ControlSupport = Literal["native", "emulated", "unsupported", "unqualified"]
DeliverySemantics = Literal[
    "turn_boundary_guaranteed",
    "cooperative_inject",
    "cancel_and_replace",
    "wait_then_send",
    "pause_at_tool_gate",
    "emulated",
    "unsupported",
]
Placement = Literal["worker_hosted", "cloud"]
UsageDisposition = Literal["settled", "estimated", "unknown"]

LANE_PROFILES: Final[tuple[LaneProfileName, ...]] = ("deep_agents", "cursor_local", "cursor_cloud")
LANE_OF_PROFILE: Final[dict[str, LaneName]] = {
    "deep_agents": "deep_agents",
    "cursor_local": "cursor",
    "cursor_cloud": "cursor",
}
RUNTIME_OF_LANE: Final[dict[str, ExecutionRuntime]] = {
    "deep_agents": "deep_agent",
    "cursor": "cursor",
}
DEFAULT_LANE_PROFILE: Final[LaneProfileName] = "deep_agents"

# The nine AgentHarness operations beside `describe` (SPEC-07 section 1).
HARNESS_OPERATIONS: Final = (
    "prepare",
    "start",
    "reattach",
    "send_turn",
    "cancel_turn",
    "observe",
    "snapshot",
    "usage",
    "end_session",
)
# `controls` of a describe: the operations plus the two run-level controls.
LANE_CONTROLS: Final = (*HARNESS_OPERATIONS, "pause", "fork")
# `delivery_semantics` of a describe: what each command kind actually does on the lane.
DELIVERY_COMMANDS: Final = (
    "queue_instruction",
    "interrupt_and_inject",
    "pause",
    "hard_pause",
    "resume",
    "cancel",
    "fork",
    "request_continuation",
)
# `mc.hook_event` vocabulary (SPEC-01 / SPEC-07 section 5.4).
HOOK_EVENTS: Final = frozenset(
    {
        "session_start",
        "before_prompt",
        "before_tool",
        "after_tool",
        "after_tool_failure",
        "before_shell",
        "after_shell",
        "after_file_edit",
        "before_model",
        "after_model",
        "before_compaction",
        "subagent_start",
        "subagent_stop",
        "stop",
        "session_end",
    }
)


class LaneContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class LaneIdentityMap(LaneContract):
    """How the provider's native identities map onto Mission Control's."""

    session_ref: str = Field(min_length=1)
    turn_ref: str = Field(min_length=1)
    effect_ref: str = Field(min_length=1)
    cursor: str = Field(min_length=1)


class LaneHooks(LaneContract):
    mechanism: str = Field(min_length=1)
    events_supported: tuple[str, ...] = Field(min_length=1)
    fail_closed: bool

    @model_validator(mode="after")
    def known_events(self) -> LaneHooks:
        unknown = set(self.events_supported) - HOOK_EVENTS
        if unknown:
            raise ValueError(f"undeclared hook events: {sorted(unknown)}")
        if len(set(self.events_supported)) != len(self.events_supported):
            raise ValueError("hook events must be unique")
        return self


class LaneSubagents(LaneContract):
    file: str | None = None
    inline: str | None = None
    readonly_supported_inline: bool = False


class LaneUsage(LaneContract):
    tokens: str = Field(min_length=1)
    cost: str = Field(min_length=1)


class LaneDescribe(LaneContract):
    """`mc.lane_describe.v1`: one lane profile's honest control matrix (pure, cheap)."""

    schema_version: Literal["mc.lane_describe.v1"] = LANE_DESCRIBE_SCHEMA
    lane: LaneName
    lane_profile: LaneProfileName
    versions: dict[str, str] = Field(default_factory=dict)
    controls: dict[str, ControlSupport]
    delivery_semantics: dict[str, DeliverySemantics]
    identity: LaneIdentityMap
    hooks: LaneHooks
    instruction_channel: tuple[str, ...] = Field(min_length=1)
    subagents: LaneSubagents
    usage: LaneUsage
    placement: Placement
    qualified: bool = False

    @model_validator(mode="after")
    def complete_matrix(self) -> LaneDescribe:
        if LANE_OF_PROFILE[self.lane_profile] != self.lane:
            raise ValueError(f"lane profile {self.lane_profile} does not belong to {self.lane}")
        if set(self.controls) != set(LANE_CONTROLS):
            raise ValueError(f"controls must name exactly {list(LANE_CONTROLS)}")
        if set(self.delivery_semantics) != set(DELIVERY_COMMANDS):
            raise ValueError(f"delivery_semantics must name exactly {list(DELIVERY_COMMANDS)}")
        if self.qualified and "unqualified" in self.controls.values():
            raise ValueError("a qualified lane cannot report unqualified controls")
        return self

    def control(self, operation: str) -> ControlSupport:
        try:
            return self.controls[operation]
        except KeyError as error:
            raise ValueError(f"undeclared lane control: {operation}") from error

    def implemented(self, operation: str) -> bool:
        return self.control(operation) in {"native", "emulated"}

    @property
    def digest(self) -> str:
        """Digest a Validation Report records as the describe it validated against."""

        return stable_json_digest(self)

    def unqualified(self) -> LaneDescribe:
        """The same profile with every control `unqualified` (a registered stub)."""

        return self.model_copy(
            update={
                "controls": dict.fromkeys(LANE_CONTROLS, "unqualified"),
                "qualified": False,
            }
        )


# --- Segmented lane turns (FT-G2) ----------------------------------------------------------


class LaneSegmentBounds(LaneContract):
    """How long one `lane.turn` segment may observe before the workflow re-schedules it.

    `start_to_close_s` bounds the activity (30 to 60 min per SPEC-07); a segment returns
    `done=False` at `max_duration_s` or `max_frames`, well inside it. `heartbeat_timeout_s`
    detects a lost worker; heartbeats are throttled, so the persisted frames, not the
    heartbeat details, are the resume truth. `status_poll_*` bound the cancel path's
    `lane.status` reconciliation, after which the unit is `in_doubt`; `busy_wait_s` bounds
    `wait_then_send` before the attempt classifies `failed(capacity)`.
    """

    max_duration_s: int = Field(default=1_800, ge=1, le=3_600)
    max_frames: int = Field(default=5_000, ge=1, le=100_000)
    start_to_close_s: int = Field(default=2_700, ge=2, le=3_600)
    heartbeat_timeout_s: int = Field(default=30, ge=1, le=600)
    status_poll_limit: int = Field(default=12, ge=1, le=1_000)
    status_poll_interval_s: int = Field(default=5, ge=1, le=600)
    busy_wait_s: int = Field(default=900, ge=1, le=86_400)
    max_segments: int = Field(default=400, ge=1, le=100_000)

    @model_validator(mode="after")
    def segment_ends_inside_the_activity(self) -> LaneSegmentBounds:
        if self.max_duration_s >= self.start_to_close_s:
            raise ValueError("a segment must end before its activity's start-to-close timeout")
        return self


class LaneResumePoint(LaneContract):
    """Where the segment loop stands, carried across continue-as-new (SPEC-07 section 4.4)."""

    phase: Literal["start", "resume"] = "start"
    cursor: str | None = Field(default=None, min_length=1, max_length=1_024)
    segment_no: int = Field(default=1, ge=1)
    turn_no: int = Field(default=1, ge=1)


# --- AgentHarness request and handle contracts --------------------------------------------------


class HarnessScope(LaneContract):
    installation_id: str = Field(min_length=1)
    application_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    actor_id: str = Field(min_length=1)


class HarnessLease(LaneContract):
    """A fenced lease over the harness execution (workspace, bridge, turn)."""

    lease_id: str = Field(min_length=1)
    fence: int = Field(ge=0)
    expires_at: AwareDatetime


class HarnessRequest(LaneContract):
    """Fields every AgentHarness request carries (SPEC-07 section 1)."""

    scope: HarnessScope
    lane_profile: LaneProfileName
    harness_execution_id: str = Field(min_length=1)
    binding_digest: str = Field(pattern=DIGEST_PATTERN)
    idempotency_key: str = Field(min_length=1, max_length=512)
    generation: int = Field(ge=1)
    lease: HarnessLease | None = None
    deadline: AwareDatetime | None = None


class PrepareRequest(HarnessRequest):
    run_id: str = Field(min_length=1)
    operation_id: str = Field(min_length=1)
    attempt_no: int = Field(ge=1)
    packet_ref: str | None = Field(default=None, min_length=1)


class PreparedSession(LaneContract):
    lane_profile: LaneProfileName
    harness_execution_id: str = Field(min_length=1)
    generation: int = Field(ge=1)
    workspace_ref: str | None = None
    projection_digests: dict[str, str] = Field(default_factory=dict)
    materialization_ref: str | None = None


class StartRequest(HarnessRequest):
    prepared: PreparedSession


class SessionHandle(LaneContract):
    """Native session identity (agent id, thread id); never a credential."""

    lane_profile: LaneProfileName
    harness_execution_id: str = Field(min_length=1)
    generation: int = Field(ge=1)
    native_session_ref: str | None = Field(default=None, min_length=1)
    # FT-G3: secret-free placement facts the lane records with the native identity
    # (`cursor_sdk_version`, `bridge_state_root`, `cloud_branch`, `cloud_agent_url`).
    native_details: dict[str, str] = Field(default_factory=dict)


class ReattachRequest(HarnessRequest):
    native_session_ref: str = Field(min_length=1)
    native_turn_ref: str | None = Field(default=None, min_length=1)


class SendTurnRequest(HarnessRequest):
    session: SessionHandle
    turn_no: int = Field(ge=1)
    instruction_ref: str = Field(min_length=1)
    packet_ref: str | None = Field(default=None, min_length=1)


class TurnHandle(LaneContract):
    session: SessionHandle
    turn_no: int = Field(ge=1)
    native_turn_ref: str | None = Field(default=None, min_length=1)
    status: Literal["accepted", "busy"] = "accepted"


class CancelTurnRequest(HarnessRequest):
    turn: TurnHandle
    reason: str = Field(min_length=1, max_length=512)
    urgency: Literal["normal", "immediate"] = "normal"


class CancelReceipt(LaneContract):
    acknowledged: bool
    already_terminal: bool = False
    native_status: str | None = None


class ObserveRequest(HarnessRequest):
    turn: TurnHandle
    after: str | None = Field(default=None, min_length=1)
    max_frames: int = Field(default=1_000, ge=1, le=100_000)


class LaneFrame(LaneContract):
    """One observed provider event as the harness yields it (maps onto `mc.provider_frame.v1`,
    SPEC-03): identity, resumable cursor, kind and a digest plus bounded excerpt, never a
    full body or a secret."""

    harness_execution_id: str = Field(min_length=1)
    generation: int = Field(ge=1)
    provider_key: str = Field(min_length=1, max_length=512)
    cursor: str = Field(min_length=1, max_length=512)
    kind: str = Field(min_length=1, max_length=64)
    terminal: bool = False
    digest: str = Field(pattern=DIGEST_PATTERN)
    excerpt: str = Field(default="", max_length=4_096)
    # FT-G2: what `lane.turn` hands the frame writer (SPEC-03 `FrameObservation`). The body
    # is the provider payload as JSON values; the writer redacts, digests and excerpts it
    # before anything is stored. Absent on lanes that persist their own frames.
    raw_kind: str | None = Field(default=None, min_length=1, max_length=256)
    body: Any = None
    native_turn_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    tool_call_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    provider_timestamp: AwareDatetime | None = None


class SnapshotRequest(HarnessRequest):
    session: SessionHandle
    reason: str = Field(min_length=1, max_length=512)


class SnapshotManifest(LaneContract):
    lane_profile: LaneProfileName
    harness_execution_id: str = Field(min_length=1)
    generation: int = Field(ge=1)
    kind: Literal["checkpoint", "git_patch", "branch"]
    refs: tuple[str, ...] = Field(min_length=1)
    emulated: bool
    digest: str = Field(pattern=DIGEST_PATTERN)


class UsageRequest(HarnessRequest):
    session: SessionHandle
    turn: TurnHandle | None = None


class UsageReport(LaneContract):
    disposition: UsageDisposition
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    total_tokens: int = Field(default=0, ge=0)
    cost_micros_usd: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def unknown_is_not_zero(self) -> UsageReport:
        if self.disposition == "unknown" and (
            self.input_tokens or self.output_tokens or self.total_tokens
        ):
            raise ValueError("unknown usage carries no token counts")
        return self


class EndSessionRequest(HarnessRequest):
    session: SessionHandle
    reason: str = Field(min_length=1, max_length=512)


class CleanupReceipt(LaneContract):
    released: bool
    artifact_refs: tuple[str, ...] = ()
    # FT-G3: the session's patch artifact (git diff plus untracked files), stored before the
    # workspace lease was released.
    patch_ref: str | None = None


class StatusRequest(HarnessRequest):
    """`lane.status`: reconcile a session (and turn) by native identity (FT-G2)."""

    session: SessionHandle
    turn: TurnHandle | None = None


class ProviderStatus(LaneContract):
    """What the provider says about a session now: run status, terminal or not, idle."""

    status: str = Field(min_length=1, max_length=64)
    terminal: bool
    idle: bool = False
    usage: UsageReport | None = None


# --- mc.cursor_binding.v1 ----------------------------------------------------------------------


class CursorPins(LaneContract):
    cursor_sdk: str = Field(min_length=1)
    bridge: str = Field(min_length=1)
    protocol: str = Field(min_length=1)
    cloud_api: str = Field(min_length=1)


class CursorWorkspace(LaneContract):
    repo_url: str | None = Field(default=None, min_length=1)
    base_ref: str = Field(min_length=1)
    lease_root: str | None = Field(default=None, min_length=1)
    branch_prefix: str = Field(default="mc/", min_length=1)


class CursorProjections(LaneContract):
    """Exact catalog pins of the files the lane places (digests, never bodies)."""

    rules_digest: str = Field(pattern=DIGEST_PATTERN)
    agents_digest: str = Field(pattern=DIGEST_PATTERN)
    skills: tuple[str, ...] = ()
    mcp_servers: tuple[str, ...] = ()
    hooks_digest: str = Field(pattern=DIGEST_PATTERN)


class CursorHookCallback(LaneContract):
    listen: str = Field(pattern=r"^127\.0\.0\.1:[0-9]{1,5}$")
    token_ttl_s: int = Field(ge=30, le=86_400)


class CursorCloudOptions(LaneContract):
    auto_create_pr: bool = False
    env_vars_ref: str | None = Field(default=None, min_length=1)
    metadata: dict[str, str] = Field(default_factory=dict)
    environment: Literal["cloud", "pool", "machine"] = "cloud"
    # FT-G5: create idempotently with a client-supplied `agentId` (`409 agent_id_conflict`
    # means already created). The API refuses `envVars` with `agentId`, so a binding that
    # needs env vars sets this false and creates with an `Idempotency-Key` (dedupe window
    # UNVERIFIED). Omitted from dumps and digests at its default.
    client_agent_id: bool = Field(default=True, exclude_if=lambda value: value is True)

    @model_validator(mode="after")
    def env_vars_need_server_minted_ids(self) -> CursorCloudOptions:
        if self.env_vars_ref is not None and self.client_agent_id:
            raise ValueError(
                "env_vars cannot be combined with a client-supplied agent id; "
                "set client_agent_id false (Idempotency-Key create)"
            )
        return self


class CursorBudgets(LaneContract):
    max_turns: int = Field(ge=1)
    max_segments: int = Field(ge=1)
    wall_clock_s: int = Field(ge=1)


class CursorExecutionBinding(LaneContract):
    """`mc.cursor_binding.v1`: SDK and bridge pins, model, mode, workspace, projections and
    the hook callback of one Cursor execution. Secret values never appear here."""

    schema_version: Literal["mc.cursor_binding.v1"] = CURSOR_BINDING_SCHEMA
    lane_profile: Literal["cursor_local", "cursor_cloud"]
    pins: CursorPins
    model_id: str = Field(min_length=1)
    mode: Literal["agent", "plan"] = "agent"
    workspace: CursorWorkspace
    projections: CursorProjections
    inline_subagents: tuple[str, ...] = ()
    disallowed_tools: tuple[str, ...] = ()
    sandbox_enabled: bool = False
    hook_callback: CursorHookCallback | None = None
    cloud: CursorCloudOptions | None = None
    budgets: CursorBudgets
    # FT-G2: the worker task queue that serves this binding's `lane.*` activities (absent:
    # left out of dumps and digests, as for every binding sealed before it existed).
    task_queue: str | None = Field(
        default=None, min_length=1, max_length=255, exclude_if=lambda value: value is None
    )
    binding_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def profile_shape(self, info: ValidationInfo) -> CursorExecutionBinding:
        if self.lane_profile == "cursor_cloud":
            if self.cloud is None or self.workspace.repo_url is None:
                raise ValueError("cursor_cloud binds a repository and cloud options")
            if self.disallowed_tools:
                raise ValueError("disallowed_tools applies to cursor_local only")
        else:
            if self.cloud is not None:
                raise ValueError("cursor_local carries no cloud options")
            if self.hook_callback is None:
                raise ValueError("cursor_local requires the loopback hook callback")
        sealing = bool(info.context and info.context.get("seal_cursor_binding"))
        if not sealing and self.binding_digest != self.computed_digest():
            raise ValueError("binding_digest does not match the binding content")
        return self

    def computed_digest(self) -> str:
        return stable_json_digest(self, exclude={"binding_digest"})

    @classmethod
    def sealed(cls, **fields: Any) -> CursorExecutionBinding:
        """Build a binding and compute its digest."""

        draft = cls.model_validate(
            {**fields, "binding_digest": "sha256:" + "0" * 64},
            context={"seal_cursor_binding": True},
        )
        return cls.model_validate({**fields, "binding_digest": draft.computed_digest()})


LANE_CONTRACTS: Final[dict[str, type[BaseModel]]] = {
    "lane_describe": LaneDescribe,
    "cursor_binding": CursorExecutionBinding,
    "prepare_request": PrepareRequest,
    "prepared_session": PreparedSession,
    "start_request": StartRequest,
    "session_handle": SessionHandle,
    "reattach_request": ReattachRequest,
    "send_turn_request": SendTurnRequest,
    "turn_handle": TurnHandle,
    "cancel_turn_request": CancelTurnRequest,
    "cancel_receipt": CancelReceipt,
    "observe_request": ObserveRequest,
    "lane_frame": LaneFrame,
    "snapshot_request": SnapshotRequest,
    "snapshot_manifest": SnapshotManifest,
    "usage_request": UsageRequest,
    "usage_report": UsageReport,
    "end_session_request": EndSessionRequest,
    "cleanup_receipt": CleanupReceipt,
    "status_request": StatusRequest,
    "provider_status": ProviderStatus,
}


def lane_contract_schemas() -> dict[str, dict[str, Any]]:
    """JSON Schema export of every lane contract (`mc.lane_describe.v1` and the harness)."""

    return {name: model.model_json_schema() for name, model in LANE_CONTRACTS.items()}
