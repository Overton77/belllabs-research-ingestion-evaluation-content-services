"""The `claude_agent_sdk` Lane Profile: the Python Claude Agent SDK on the worker (MP-07).

One Session Turn of a bound operation (`mc.execution_binding.v2`), driven by `lane.turn`:

- `prepare` leases a workspace through the MP-04 allocator, materializes the Context Packet
  (`.mission/`, `inputs/`, `outputs/`), renders the MP-03 Host Projection for this profile and
  writes it with `materialize_projection` (fail-closed on digest drift, traversal, collision
  and missing launch executables), and creates the state root under the lease;
- `start` admits the auth route (MP-05), builds the explicit child environment through the
  injected `provider_child_environment`, journals the create in the state-root ledger,
  connects one `ClaudeSDKClient` (`session.LiveSession`, the single stream owner) with the
  projected `setting_sources`, inline agents, Kernel Hook callbacks (`hooks.py`), the
  permission binder (`permissions.py`) and the transcript mirror (`workspace.py`), and
  records the `system/init` session id as the native session reference;
- `send_turn` writes one user message stamped with the turn's uuid (the native turn
  reference), `busy` while a turn or its interrupted drain is running, and refuses with
  `ProviderCapacityLimited` while the provider's last capacity signal rejects sends;
- `observe` replays the session log of one turn from the cursor to its terminal `result`
  frame; `closing_facts` reads that frame (status from `subtype` / `is_error` /
  `terminal_reason` / `api_error_status`, never from the result text);
- `cancel_turn` interrupts and drains the interrupted response before anything else is sent;
- `reattach` returns the live session when this process holds it; after process death it
  resumes the session by id into a new connection/generation only when no turn was running
  and the state root holds the history, and raises `NativeTurnLost` otherwise (a resumed
  history never proves the previous tool execution did not happen);
- `end_session` disconnects, captures the workspace for custody and releases the lease only
  after the custody succeeded.

`reconcile_dispatch` (MP-06 `DispatchReconcilingLane`) answers from the state-root ledger:
`acknowledged` -> found, `intended` -> unknown, absent -> `not_received` (the ledger is
written before any native write, so an absent key was never sent).

Native permission requests go through the `PermissionBindingPort` (`approvals.
BrokerPermissionBinding` over MP-11's durable broker in production; `DenyWithoutGateway`, the
fail-closed default, otherwise) with `policy_digest` = the execution's binding digest and the
turn request's generation; after process death `reattach` calls the port's `recover` before
the session is resumed.

MP-12 pieces: `context_occupancy` (`ContextOccupancyLane`) reads the live window through
`ClaudeSDKClient.get_context_usage()` (`totalTokens` against `rawMaxTokens`, the raw model
window); `hydrate_session` / `pending_handover` / `complete_handover` (`SessionHandoverLane`)
let `continuation.ClaudeSessionHydrator` start a *fresh* `ClaudeSDKClient` session (no
`resume`, no `fork_session`: the conversation is not forked; the sealed packet is what
carries over) whose first turn is the continuation turn.

Not implemented (describe cells `unqualified`): `pause` (`PreToolUse` defer) and `fork`
hydration. Qualification stays `False` until the owner-run live drill
(`docs/qualification/lanes/claude_agent_sdk/README.md`).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final, Protocol, cast
from uuid import UUID, uuid5

from claude_agent_sdk.types import AgentDefinition, ClaudeAgentOptions, SettingSource

from mission_control.adapters.claude.describe import CLAUDE_AGENT_SDK_LANE_DESCRIBE, PROFILE
from mission_control.adapters.claude.frames import closing_facts as result_closing_facts
from mission_control.adapters.claude.hooks import KernelHookCallbacks, KernelHookContext
from mission_control.adapters.claude.permissions import (
    DenyWithoutGateway,
    PermissionBinder,
    PermissionBindingPort,
    PermissionScope,
    RecoveringPermissionPort,
)
from mission_control.adapters.claude.session import (
    ClaudeClientFactory,
    LiveSession,
    SessionEnded,
)
from mission_control.adapters.claude.workspace import (
    ClaudeWorkspace,
    DispatchLedger,
    StateRootSessionStore,
    config_dir,
    ensure_state_root,
    state_root,
)
from mission_control.adapters.cursor.projection import (
    DurableInputReader,
    ProjectionSource,
    RowsResolver,
    materialize_packet,
    turn_text,
)
from mission_control.application.agentic_components.materialization import (
    ProjectionMaterialization,
    materialize_projection,
    projection_digest,
    required_executables,
)
from mission_control.application.execution.auth_admission import AuthAdmission
from mission_control.application.execution.harness.controls import SessionHandover
from mission_control.application.execution.harness.dispatch import (
    DispatchLookup,
    DispatchRecord,
)
from mission_control.application.execution.harness.hook_callbacks import HookIntentLedger
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.execution.harness.leases import WorkspaceLease
from mission_control.application.execution.harness.protocol import NativeTurnLost
from mission_control.application.execution.operations.lane_outputs import LaneOutputCustody
from mission_control.application.execution.stop_fence import (
    KernelHookFenceGate,
    StopFenceRepository,
)
from mission_control.application.frames.kinds import UnknownKindCounter
from mission_control.application.workspaces.service import AllocationRequest
from mission_control.application.workspaces.snapshots import CapturedSnapshot
from mission_control.domain.agentic_components.projection import HostProjection
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_digest
from mission_control.domain.context.pressure import ContextOccupancy
from mission_control.domain.execution.bindings import ClaudeSdkOptions, ProviderExecutionBinding
from mission_control.domain.execution.contracts import OperationExecutionRequest
from mission_control.domain.execution.lane_turns import ClosingFacts
from mission_control.domain.execution.lanes import (
    CancelReceipt,
    CancelTurnRequest,
    CleanupReceipt,
    EndSessionRequest,
    HarnessRequest,
    LaneDescribe,
    LaneFrame,
    LaneProfileName,
    ObserveRequest,
    PreparedSession,
    PrepareRequest,
    ProviderStatus,
    ReattachRequest,
    SendTurnRequest,
    SessionHandle,
    SnapshotManifest,
    SnapshotRequest,
    StartRequest,
    StatusRequest,
    TurnHandle,
    UsageReport,
    UsageRequest,
)

_LOGGER = logging.getLogger(__name__)
CAPABILITY_DRIFT: Final = "CAPABILITY_DRIFT"
BINDING_MISMATCH: Final = "BINDING_MISMATCH"
OUTPUTS_DIR: Final = "outputs"
_TURN_NAMESPACE: Final = UUID("8c0e8b7a-5a0c-4f0e-9a1e-2f3b4c5d6e7f")
# Auth routes whose credential travels in the child environment: the CLI needs nothing from
# the worker's own config dir, so its config dir moves under the lease. Login routes keep the
# owner's config dir, where the CLI's credentials live (`session_resume.py` copies them for
# store-backed resumes; this lane never copies a credential).
DEFAULT_RELOCATED_ROUTES: Final = frozenset({"api_key", "oauth_token_env"})
CONTINUATION_TURN_PREFIX: Final = "continuation:"
HANDOVER_PENDING: Final = "HANDOVER_PENDING"


class ClaudeLaneError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


class AuthAdmitter(Protocol):
    """`application/execution/auth_admission.AuthAdmissionService.admit` or a local stand-in."""

    async def admit(
        self,
        lane_profile: LaneProfileName,
        profile_id: str | None,
        *,
        pointer: str = "/environment",
        now: datetime | None = None,
    ) -> AuthAdmission: ...


class StaticAuthAdmitter:
    """A pre-computed admission (local proof and fixtures); production composes the service."""

    def __init__(self, admission: AuthAdmission) -> None:
        self._admission = admission

    async def admit(
        self,
        lane_profile: LaneProfileName,
        profile_id: str | None,
        *,
        pointer: str = "/environment",
        now: datetime | None = None,
    ) -> AuthAdmission:
        del pointer, now
        if self._admission.lane_profile != lane_profile or (
            profile_id is not None and self._admission.profile_id != profile_id
        ):
            raise ClaudeLaneError(
                "AUTH_PROFILE_MISMATCH",
                f"admission {self._admission.profile_id} does not cover {profile_id}",
            )
        return self._admission


class ChildEnvironmentBuilder(Protocol):
    """`bootstrap.provider_auth.provider_child_environment` (injected; adapters never import
    bootstrap): the explicit environment of the admitted provider process."""

    def __call__(
        self,
        environ: Mapping[str, str],
        admission: AuthAdmission,
        *,
        extra: Mapping[str, str] | None = None,
    ) -> dict[str, str]: ...


@dataclass(frozen=True)
class ClaudeLaneSettings:
    mount_root: str = ""
    init_timeout_s: float = 60.0
    drain_timeout_s: float = 20.0
    require_executables: bool = True
    forward_subagent_text: bool = True
    include_hook_events: bool = False
    relocated_config_routes: frozenset[str] = DEFAULT_RELOCATED_ROUTES
    permission_deadline: timedelta | None = None
    default_base_ref: str = "HEAD"
    control_timeout_s: float = 10.0
    """Bound on a control request the lane makes outside a turn (`get_context_usage`)."""


@dataclass
class _Execution:
    operation: OperationExecutionRequest
    binding: ProviderExecutionBinding
    identity: LaneExecutionIdentity
    lease: WorkspaceLease | None = None
    projection: HostProjection | None = None
    materialization: ProjectionMaterialization | None = None
    digests: dict[str, str] = field(default_factory=dict)
    admission: AuthAdmission | None = None
    environment: dict[str, str] = field(default_factory=dict)
    session: LiveSession | None = None
    session_id: str | None = None
    hooks: KernelHookCallbacks | None = None
    permissions: PermissionBinder | None = None
    turn_texts: dict[str, str] = field(default_factory=dict)
    last_turn_ref: str | None = None
    pending: _PendingHandover | None = None
    recovered: tuple[Any, ...] = ()
    prepared: PreparedSession | None = None

    @property
    def root(self) -> Path:
        if self.lease is None:
            raise NativeTurnLost("the session holds no workspace lease")
        return Path(self.lease.path)

    @property
    def state_root(self) -> Path:
        return state_root(self.root)


@dataclass
class _PendingHandover:
    """A hydrated continuation target: connected, not yet adopted (the next send adopts it)."""

    handover: SessionHandover
    session: LiveSession
    hooks: KernelHookCallbacks
    permissions: PermissionBinder


def _utc_now() -> datetime:
    return datetime.now(UTC)


def approval_policy_digest(operation: OperationExecutionRequest) -> str:
    """The execution's binding digest as `harness_execution.intended_binding_digest` records
    it (`lane_turns.execution_start`): the Cursor binding digest for a Cursor runtime, else the
    operation's effective configuration digest (a claude unit carries no Cursor binding).
    MP-11's `PostgresApprovalContextProbe` revalidates approvals against exactly this value."""

    if operation.cursor_binding is not None:
        return operation.cursor_binding.binding_digest
    return operation.effective_configuration_digest


def occupancy_from_context_usage(usage: Mapping[str, Any]) -> ContextOccupancy:
    """`types.ContextUsageResponse`: `totalTokens` currently in the window against
    `rawMaxTokens` ("Raw model context window size"); `maxTokens` is the effective limit
    reduced by the autocompact buffer, so it is not the model window. Anything else is
    `unknown` (a cumulative billed total is never passed as occupancy)."""

    used = usage.get("totalTokens")
    window = usage.get("rawMaxTokens")
    if (
        isinstance(used, int)
        and not isinstance(used, bool)
        and isinstance(window, int)
        and not isinstance(window, bool)
        and used >= 0
        and window >= 1
    ):
        return cast(
            ContextOccupancy,
            ContextOccupancy.measured(used, window, "provider_context_window"),
        )
    return ContextOccupancy.unknown("get_context_usage reported no token count or model window")


def turn_reference(idempotency_key: str) -> str:
    """The uuid stamped on the user message of one send: stable per idempotency key, so a
    reconciled or repeated dispatch names the same native turn."""

    return str(uuid5(_TURN_NAMESPACE, idempotency_key))


def declared_outputs(
    operation: OperationExecutionRequest, *, mount_root: str = ""
) -> tuple[str, ...]:
    prefix = mount_root.rstrip("/")
    declared: list[str] = []
    for path in operation.workspace.exclusive_write_paths:
        logical = path[len(prefix) :] if prefix and path.startswith(prefix + "/") else path
        relative = logical.lstrip("/")
        if relative.startswith(OUTPUTS_DIR + "/"):
            declared.append(relative)
    return tuple(dict.fromkeys(declared))


def missing_outputs(root: Path, declared: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(path for path in declared if not (root / path).is_file())


def _output_files(root: Path) -> list[tuple[str, bytes]]:
    outputs = root / OUTPUTS_DIR
    if not outputs.is_dir() or outputs.is_symlink():
        return []
    return [
        (item.relative_to(root).as_posix(), item.read_bytes())
        for item in sorted(outputs.rglob("*"))
        if item.is_file() and not item.is_symlink()
    ]


def _agent_definitions(raw: object) -> dict[str, AgentDefinition] | None:
    items = raw if isinstance(raw, list | tuple) else ()
    agents: dict[str, AgentDefinition] = {}
    for item in items:
        if not isinstance(item, Mapping) or not item.get("name"):
            continue
        model = item.get("model")
        agents[str(item["name"])] = AgentDefinition(
            description=str(item.get("description") or ""),
            prompt=str(item.get("prompt") or ""),
            model=str(model) if isinstance(model, str) and model else None,
        )
    return agents or None


class ClaudeAgentSdkHarness:
    """`claude_agent_sdk` Session Lane over a `ClaudeClientFactory` (the pinned SDK in
    production, a fixture client in tests)."""

    def __init__(
        self,
        *,
        clients: ClaudeClientFactory,
        workspaces: ClaudeWorkspace,
        projections: ProjectionSource,
        auth: AuthAdmitter,
        child_environment: ChildEnvironmentBuilder,
        environ: Mapping[str, str],
        permissions: PermissionBindingPort | None = None,
        fences: StopFenceRepository | None = None,
        intents: HookIntentLedger | None = None,
        inputs: DurableInputReader | None = None,
        rows: RowsResolver | None = None,
        settings: ClaudeLaneSettings | None = None,
        describe: LaneDescribe = CLAUDE_AGENT_SDK_LANE_DESCRIBE,
        clock: Callable[[], datetime] = _utc_now,
        unknown_kinds: UnknownKindCounter | None = None,
        outputs: LaneOutputCustody | None = None,
    ) -> None:
        if describe.lane_profile != PROFILE:
            raise ValueError("the Claude harness describes the claude_agent_sdk profile")
        self._clients = clients
        # MP-20: declared outputs (`outputs/`) become workspace candidates when composed;
        # without it the lane registers its workspace snapshot only, as before.
        self._outputs = outputs
        self._workspaces = workspaces
        self._projections = projections
        self._auth = auth
        self._child_environment = child_environment
        self._environ = dict(environ)
        self._permissions: PermissionBindingPort = permissions or DenyWithoutGateway()
        self._fence_gate = KernelHookFenceGate(fences) if fences is not None else None
        self._intents = intents
        self._inputs = inputs
        self._rows = rows
        self._settings = settings or ClaudeLaneSettings()
        self._describe = describe
        self._clock = clock
        self._counter = unknown_kinds
        self._executions: dict[str, _Execution] = {}

    def describe(self) -> LaneDescribe:
        return self._describe

    # --- staging ----------------------------------------------------------------------------

    def stage(self, harness_execution_id: str, operation: OperationExecutionRequest) -> None:
        binding = operation.provider_binding
        if binding is None or binding.lane_profile != PROFILE:
            raise ValueError(
                "claude_agent_sdk runs only an operation with a claude_agent_sdk binding"
            )
        current = self._executions.get(harness_execution_id)
        if current is not None:
            if current.operation != operation:
                raise ValueError("harness execution is staged with another operation")
            return
        self._executions[harness_execution_id] = _Execution(
            operation=operation,
            binding=binding,
            identity=LaneExecutionIdentity.of(operation, PROFILE, 1),
        )

    def _execution(self, request: HarnessRequest) -> _Execution:
        execution = self._executions.get(request.harness_execution_id)
        if execution is None:
            raise NativeTurnLost(f"harness execution {request.harness_execution_id} is not staged")
        if execution.identity.attempt_no != request.generation:
            execution.identity = LaneExecutionIdentity.of(
                execution.operation, PROFILE, request.generation
            )
        return execution

    def execution_view(self, harness_execution_id: str) -> _Execution | None:
        """Read-only access for tests and composition (never a credential)."""

        return self._executions.get(harness_execution_id)

    # --- prepare ----------------------------------------------------------------------------

    async def prepare(self, request: PrepareRequest) -> PreparedSession:
        execution = self._execution(request)
        binding = execution.binding
        if binding.binding_digest != request.binding_digest and (
            execution.operation.effective_configuration_digest != request.binding_digest
        ):
            raise ClaudeLaneError(BINDING_MISMATCH, "the request names another binding digest")
        prepared = execution.prepared
        if (
            prepared is not None
            and execution.lease is not None
            and prepared.generation == request.generation
        ):
            # Idempotent per generation (as on Codex): a later turn of the same generation (the
            # MP-12 continuation turn on the activated target) never re-materializes the
            # packet over the continuation packet the hydrator wrote into the lease.
            return prepared
        lease = await self._workspaces.acquire(
            AllocationRequest(
                request_scope=execution.identity.request_scope,
                lane_profile=PROFILE,
                harness_execution_id=UUID(request.harness_execution_id),
                generation=request.generation,
                run_id=request.run_id,
                attempt_no=request.attempt_no,
                policy=binding.workspace_policy,
                repository=binding.repo_url,
                base_ref=binding.repo_ref or self._settings.default_base_ref,
            )
        )
        execution.lease = lease
        root = Path(lease.path)
        packet = await materialize_packet(
            root, execution.operation, self._inputs, mount_root=self._settings.mount_root
        )
        projection = await self._projections.project(
            execution.operation, profile=PROFILE, packet_index=None
        )
        digest = projection_digest(projection)
        if digest != binding.materialization_digest:
            raise ClaudeLaneError(
                CAPABILITY_DRIFT,
                f"projection digest {digest} differs from the binding's "
                f"{binding.materialization_digest}",
            )
        executables: dict[str, tuple[str, ...]] | None = None
        if self._rows is not None:
            executables = required_executables(await self._rows(execution.operation), PROFILE)
        materialization = await asyncio.to_thread(
            materialize_projection,
            projection,
            root,
            expected_digest=binding.materialization_digest,
            executables=executables,
            require_executables=self._settings.require_executables,
        )
        await asyncio.to_thread(ensure_state_root, state_root(root))
        execution.projection = projection
        execution.materialization = materialization
        execution.digests = {
            "projection_digest": digest,
            "packet_digest": sha256_digest(packet),
            "materialization_digest": stable_json_digest(materialization),
        }
        execution.prepared = PreparedSession(
            lane_profile=PROFILE,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            workspace_ref=f"workspace_lease:{lease.lease_id}",
            projection_digests=dict(execution.digests),
            materialization_ref=execution.digests["materialization_digest"],
        )
        return execution.prepared

    # --- start, reattach --------------------------------------------------------------------

    def _child_env(self, execution: _Execution, admission: AuthAdmission) -> dict[str, str]:
        extra: dict[str, str] = {}
        if admission.route in self._settings.relocated_config_routes:
            extra["CLAUDE_CONFIG_DIR"] = str(config_dir(execution.state_root))
        return self._child_environment(self._environ, admission, extra=extra)

    def _cli_path(self, execution: _Execution) -> str | None:
        ref = self._sdk_options(execution).cli_path_ref
        if ref is None:
            return None  # the SDK's bundled Claude Code (SubprocessCLITransport._find_cli)
        kind, _sep, value = ref.partition(":")
        if kind == "path" and value:
            return value
        if kind == "env" and value:
            return execution.environment.get(value) or self._environ.get(value)
        raise ClaudeLaneError("CLI_PATH_REF", f"unsupported cli_path_ref {ref!r} (path:/env:)")

    @staticmethod
    def _sdk_options(execution: _Execution) -> ClaudeSdkOptions:
        opts = execution.binding.provider_options
        if not isinstance(opts, ClaudeSdkOptions):
            raise ClaudeLaneError(BINDING_MISMATCH, "binding carries no claude_agent_sdk options")
        return opts

    def _options(
        self, execution: _Execution, session: LiveSession, *, resume: str | None
    ) -> tuple[ClaudeAgentOptions, KernelHookCallbacks, PermissionBinder]:
        opts = self._sdk_options(execution)
        send_options = execution.projection.send_options if execution.projection else {}
        sources = send_options.get("setting_sources")
        setting_sources = (
            list(sources) if isinstance(sources, list | tuple) else list(opts.setting_sources)
        )
        kernel = KernelHookCallbacks.from_send_options(
            send_options,
            context=KernelHookContext(
                request_scope=execution.identity.request_scope,
                run_id=execution.operation.identity.run_id,
                generation=execution.identity.attempt_no,
                harness_execution_id=execution.identity.harness_execution_id,
                lane_profile=PROFILE,
                disallowed_tools=tuple(opts.disallowed_tools),
            ),
            emitter=session,
            fences=self._fence_gate,
            intents=self._intents,
            clock=self._clock,
        )
        binder = PermissionBinder(
            self._permissions,
            lane_profile=PROFILE,
            harness_execution_id=str(execution.identity.harness_execution_id),
            generation=execution.identity.attempt_no,
            policy_digest=approval_policy_digest(execution.operation),
            emitter=session,
            session_ref=lambda: session.session_id,
            turn_ref=lambda: session.active.turn_ref if session.active is not None else None,
            scope=PermissionScope(
                request_scope=execution.identity.request_scope,
                run_id=execution.identity.run_key,
            ),
            clock=self._clock,
            deadline=self._settings.permission_deadline,
        )
        matchers = kernel.matchers()
        options = ClaudeAgentOptions(
            cwd=str(execution.root),
            model=execution.binding.model.model_id,
            permission_mode=opts.permission_mode,
            allowed_tools=list(opts.allowed_tools),
            disallowed_tools=list(opts.disallowed_tools),
            setting_sources=cast(list[SettingSource], setting_sources),
            max_turns=opts.max_turns,
            cli_path=self._cli_path(execution),
            resume=resume,
            agents=_agent_definitions(send_options.get("agents")),
            hooks=matchers or None,
            can_use_tool=binder.can_use_tool,
            session_store=StateRootSessionStore(execution.state_root),
            session_store_flush="eager",
            forward_subagent_text=self._settings.forward_subagent_text,
            include_hook_events=self._settings.include_hook_events,
        )
        return options, kernel, binder

    async def _open_session(
        self,
        execution: _Execution,
        harness_execution_id: str,
        generation: int,
        *,
        resume: str | None,
    ) -> tuple[LiveSession, KernelHookCallbacks, PermissionBinder]:
        """Connect one new `ClaudeSDKClient` session (fresh, or `resume=<id>`); not installed."""

        if execution.admission is None:
            execution.admission = await self._auth.admit(
                PROFILE, execution.binding.auth.profile, now=self._clock()
            )
            execution.environment = self._child_env(execution, execution.admission)
        session = LiveSession(
            self._clients,
            ClaudeAgentOptions(),
            execution.environment,
            harness_execution_id=harness_execution_id,
            generation=generation,
            lane_profile=PROFILE,
            ledger=DispatchLedger(execution.state_root),
            clock=self._clock,
            init_timeout_s=self._settings.init_timeout_s,
            drain_timeout_s=self._settings.drain_timeout_s,
            counter=self._counter,
        )
        # Options need the session (hooks and permissions emit frames into its log).
        options, kernel, binder = self._options(execution, session, resume=resume)
        session.configure(options)
        await session.start()
        return session, kernel, binder

    async def _connect(
        self, execution: _Execution, request: HarnessRequest, *, resume: str | None
    ) -> LiveSession:
        session, kernel, binder = await self._open_session(
            execution, request.harness_execution_id, request.generation, resume=resume
        )
        execution.session, execution.session_id = session, session.session_id
        execution.hooks, execution.permissions = kernel, binder
        return session

    def _handle(self, request: HarnessRequest, execution: _Execution) -> SessionHandle:
        versions = self._clients.versions
        details = {
            "claude_agent_sdk_version": versions.get("claude_agent_sdk", ""),
            "bridge_state_root": str(execution.state_root) if execution.lease else "",
        }
        if execution.admission is not None:
            details["auth_route"] = execution.admission.route
            details["config_dir_relocated"] = str(
                execution.admission.route in self._settings.relocated_config_routes
            ).lower()
        return SessionHandle(
            lane_profile=PROFILE,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            native_session_ref=execution.session_id,
            native_details={key: value for key, value in details.items() if value},
        )

    async def start(self, request: StartRequest) -> SessionHandle:
        execution = self._execution(request)
        if execution.lease is None:
            raise ValueError("start requires a prepared workspace lease")
        ledger = DispatchLedger(execution.state_root)
        await ledger.intend("create", request.idempotency_key)
        session = await self._connect(execution, request, resume=None)
        assert session.session_id is not None
        await ledger.acknowledge("create", request.idempotency_key, session.session_id)
        return self._handle(request, execution)

    async def _lease(self, execution: _Execution, request: HarnessRequest) -> WorkspaceLease:
        lease = execution.lease
        if lease is None:
            lease = await self._workspaces.find(
                execution.identity.request_scope,
                UUID(request.harness_execution_id),
                request.generation,
            )
        if lease is None or lease.released or not await asyncio.to_thread(Path(lease.path).is_dir):
            raise NativeTurnLost("the workspace lease of this session is gone")
        execution.lease = lease
        return lease

    async def reattach(self, request: ReattachRequest) -> SessionHandle:
        execution = self._execution(request)
        pending = execution.pending
        if (
            pending is not None
            and not pending.session.ended
            and pending.session.session_id == request.native_session_ref
        ):
            # MP-12: the activated continuation target (recorded as the execution's native
            # session) is the hydrated fresh session this process holds: adopt it, never
            # resume it into a second connection (as on Codex).
            await self._adopt(execution, pending.handover.session)
        session = execution.session
        if (
            session is not None
            and not session.ended
            and session.session_id == request.native_session_ref
        ):
            if (
                request.native_turn_ref is not None
                and session.turn(request.native_turn_ref) is None
            ):
                raise NativeTurnLost(
                    f"turn {request.native_turn_ref} is unknown to the live session"
                )
            return self._handle(request, execution)
        # Process death: the agent loop died with the worker. Resume is a new connection.
        lease = await self._lease(execution, request)
        if request.native_turn_ref is not None:
            raise NativeTurnLost(
                "the running turn cannot be observed after process death; resuming the session "
                "is a new connection/generation, not proof of what the turn did"
            )
        store = StateRootSessionStore(state_root(Path(lease.path)))
        if not await asyncio.to_thread(store.transcript_present, request.native_session_ref):
            raise NativeTurnLost(
                f"no history of session {request.native_session_ref} under the leased state root"
            )
        if execution.projection is None:
            execution.projection = await self._projections.project(
                execution.operation, profile=PROFILE, packet_index=None
            )
        if isinstance(self._permissions, RecoveringPermissionPort):
            # MP-11: correlations the dead process held are closed `lost` (never reused)
            # before this process resumes the session and anything is re-dispatched.
            execution.recovered = tuple(
                await self._permissions.recover(
                    execution.identity.request_scope, request.harness_execution_id
                )
            )
        try:
            resumed = await self._connect(execution, request, resume=request.native_session_ref)
        except SessionEnded as error:
            raise NativeTurnLost(f"the session could not be resumed: {error}") from error
        if resumed.session_id != request.native_session_ref:
            _LOGGER.warning(
                "resume of %s reported session %s", request.native_session_ref, resumed.session_id
            )
        return self._handle(request, execution)

    # --- turn -------------------------------------------------------------------------------

    def _live(self, execution: _Execution) -> LiveSession:
        session = execution.session
        if session is None or session.ended:
            raise NativeTurnLost("no live Claude session in this process")
        return session

    def _held(self, execution: _Execution) -> LiveSession:
        """The session this process holds, live or ended: its log stays readable after the
        subprocess died (the synthesized terminal frame is in it)."""

        session = execution.session
        if session is None:
            raise NativeTurnLost("no Claude session in this process")
        return session

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        execution = self._execution(request)
        await self._adopt(execution, request.session)
        session = self._live(execution)
        text = execution.turn_texts.get(request.instruction_ref) or turn_text(execution.operation)
        turn_ref = turn_reference(request.idempotency_key)
        outcome = await session.send(
            text, turn_ref=turn_ref, idempotency_key=request.idempotency_key
        )
        if outcome == "busy":
            return TurnHandle(session=request.session, turn_no=request.turn_no, status="busy")
        execution.last_turn_ref = turn_ref
        return TurnHandle(
            session=request.session, turn_no=request.turn_no, native_turn_ref=turn_ref
        )

    def stage_turn(self, harness_execution_id: str, instruction_ref: str, text: str) -> None:
        execution = self._executions.get(harness_execution_id)
        if execution is None:
            raise NativeTurnLost(f"harness execution {harness_execution_id} is not staged")
        execution.turn_texts[instruction_ref] = text

    async def observe(self, request: ObserveRequest) -> AsyncIterator[LaneFrame]:
        execution = self._execution(request)
        turn_ref = request.turn.native_turn_ref or execution.last_turn_ref
        if turn_ref is None:
            raise NativeTurnLost("no native turn to observe")
        session = self._held(execution)
        after = _ordinal(request.after)
        async for frame in session.observe(turn_ref, after):
            yield frame

    def closing_facts(self, turn: TurnHandle, frame: LaneFrame) -> ClosingFacts:
        body = frame.body if isinstance(frame.body, dict) else {}
        execution = self._executions.get(turn.session.harness_execution_id)
        model = None
        if execution is not None and execution.session is not None:
            model = execution.session.state.last_model
        facts = result_closing_facts(body, model=model)
        if facts.native_status != "finished" or execution is None or execution.lease is None:
            return facts
        declared = declared_outputs(execution.operation, mount_root=self._settings.mount_root)
        missing = missing_outputs(execution.root, declared)
        return facts.model_copy(update={"missing_outputs": missing}) if missing else facts

    def final_text(self, turn: TurnHandle, frame: LaneFrame) -> str | None:
        """`FinalTextLane` (MP-20): the turn's whole final answer, `ResultMessage.result` of
        the terminal frame body (the closing facts keep only a bounded excerpt of it)."""

        del turn
        body = frame.body if isinstance(frame.body, Mapping) else {}
        result = body.get("result")
        return result if isinstance(result, str) else None

    def resume_cursor(self, provider_key: str) -> str | None:
        """Claude provider keys (`claude:<uuid>[:<block>]:<raw_kind>`) carry no ordinal; the
        heartbeat (the session-log ordinal) is the resume hint, and the frame store dedupes
        by provider key."""

        del provider_key
        return None

    async def cancel_turn(self, request: CancelTurnRequest) -> CancelReceipt:
        execution = self._execution(request)
        turn_ref = request.turn.native_turn_ref or execution.last_turn_ref
        session = execution.session
        if turn_ref is None:
            return CancelReceipt(acknowledged=True, already_terminal=True, native_status="idle")
        if session is None:
            return CancelReceipt(acknowledged=False, native_status="unknown")
        try:
            already, drained = await session.interrupt(turn_ref)
        except NativeTurnLost:
            return CancelReceipt(acknowledged=False, native_status="unknown")
        record = session.turn(turn_ref)
        status = record.status if record is not None else "unknown"
        if already:
            return CancelReceipt(acknowledged=True, already_terminal=True, native_status=status)
        return CancelReceipt(
            acknowledged=True,
            already_terminal=False,
            native_status=status if drained else "draining",
        )

    async def status(self, request: StatusRequest) -> ProviderStatus:
        execution = self._execution(request)
        turn_ref = (
            request.turn.native_turn_ref if request.turn else None
        ) or execution.last_turn_ref
        session = execution.session
        if session is None:
            if turn_ref is None:
                return ProviderStatus(status="idle", terminal=False, idle=True)
            raise NativeTurnLost("no live Claude session in this process")
        status, terminal, idle = session.status_of(turn_ref)
        usage = None
        if terminal and session.state.last_result is not None:
            report = result_closing_facts(session.state.last_result).usage
            usage = report if report.disposition != "unknown" else None
        return ProviderStatus(status=status, terminal=terminal, idle=idle, usage=usage)

    async def usage(self, request: UsageRequest) -> UsageReport:
        """Tokens settle per turn from `ResultMessage.usage`; cost is the SDK's own estimate
        and stays `estimated` (never reported as a settled amount)."""

        execution = self._execution(request)
        session = execution.session
        if session is None:
            return UsageReport(disposition="unknown")
        turn_ref = request.turn.native_turn_ref if request.turn is not None else None
        record = session.turn(turn_ref) if turn_ref is not None else None
        body = record.result if record is not None and record.result else session.state.last_result
        if body is None:
            return UsageReport(disposition="unknown")
        return result_closing_facts(body).usage

    # --- MP-12: context occupancy and the continuation handover -----------------------------

    async def context_occupancy(
        self, harness_execution_id: str, session: SessionHandle, turn: TurnHandle | None
    ) -> ContextOccupancy | None:
        """`ContextOccupancyLane`: the live window from `ClaudeSDKClient.get_context_usage()`
        (a control request at the turn boundary, bounded by `control_timeout_s`); `unknown`
        with the reason when the session is gone or the CLI does not answer."""

        del session, turn
        execution = self._executions.get(harness_execution_id)
        live = execution.session if execution is not None else None
        if live is None or live.ended:
            return ContextOccupancy.unknown("no live Claude session in this process")
        try:
            usage = await live.context_usage(timeout_s=self._settings.control_timeout_s)
        except Exception as error:  # the control request failed or timed out
            return ContextOccupancy.unknown(f"get_context_usage failed: {type(error).__name__}")
        return occupancy_from_context_usage(usage)

    def live_execution(self, session_ref: str) -> str | None:
        """The staged execution whose live session is `session_ref` (this process only)."""

        for heid, execution in self._executions.items():
            session = execution.session
            if session is not None and not session.ended and session.session_id == session_ref:
                return heid
        return None

    def lease_path(self, harness_execution_id: str) -> Path:
        execution = self._executions.get(harness_execution_id)
        if execution is None:
            raise NativeTurnLost(f"harness execution {harness_execution_id} is not staged")
        return execution.root

    async def hydrate_session(
        self,
        harness_execution_id: str,
        *,
        transfer_id: str,
        prompt_text: str,
        source_session_ref: str,
    ) -> SessionHandover:
        """Connect a *fresh* `ClaudeSDKClient` session in the same lease (no `resume`, no
        `fork_session`: nothing of the source conversation is claimed to carry over), stage the
        continuation turn's text and offer the handover; the next `lane.turn` sends that turn
        to the target and adopts it. Idempotent per transfer."""

        execution = self._executions.get(harness_execution_id)
        if execution is None:
            raise NativeTurnLost(f"harness execution {harness_execution_id} is not staged")
        pending = execution.pending
        if pending is not None:
            if pending.handover.transfer_id == transfer_id:
                return pending.handover
            raise ClaudeLaneError(
                HANDOVER_PENDING, f"transfer {pending.handover.transfer_id} is not adopted yet"
            )
        source = execution.session
        if source is None or source.ended or source.session_id != source_session_ref:
            raise NativeTurnLost(f"session {source_session_ref} is not live in this process")
        generation = execution.identity.attempt_no
        key = f"{harness_execution_id}:{generation}:continuation:{transfer_id}"
        ledger = DispatchLedger(execution.state_root)
        await ledger.intend("create", key)
        target, kernel, binder = await self._open_session(
            execution, harness_execution_id, generation, resume=None
        )
        assert target.session_id is not None
        await ledger.acknowledge("create", key, target.session_id)
        instruction_ref = f"{CONTINUATION_TURN_PREFIX}{transfer_id}"
        execution.turn_texts[instruction_ref] = prompt_text
        details = {
            "claude_agent_sdk_version": self._clients.versions.get("claude_agent_sdk", ""),
            "bridge_state_root": str(execution.state_root),
        }
        handover = SessionHandover(
            transfer_id=transfer_id,
            session=SessionHandle(
                lane_profile=PROFILE,
                harness_execution_id=harness_execution_id,
                generation=generation,
                native_session_ref=target.session_id,
                native_details={name: value for name, value in details.items() if value},
            ),
            instruction_ref=instruction_ref,
            source_session_ref=source_session_ref,
        )
        execution.pending = _PendingHandover(handover, target, kernel, binder)
        return handover

    async def pending_handover(self, harness_execution_id: str) -> SessionHandover | None:
        execution = self._executions.get(harness_execution_id)
        if execution is None or execution.pending is None:
            return None
        return execution.pending.handover

    async def complete_handover(self, harness_execution_id: str, transfer_id: str) -> None:
        execution = self._executions.get(harness_execution_id)
        if execution is None or execution.pending is None:
            return
        if execution.pending.handover.transfer_id != transfer_id:
            return
        await self._adopt(execution, execution.pending.handover.session)
        execution.pending = None

    async def _adopt(self, execution: _Execution, session: SessionHandle) -> None:
        """The send to (or the reattach of) a hydrated target makes it this execution's
        session and consumes the pending handover; the superseded source connection is closed
        (its finished turn is already settled into frames).

        Consuming the handover here is what keeps the continuation turn single: a handover
        still pending after adoption would be offered again at the target's own terminal
        frame (`LaneTurnService._after_terminal`) and send the continuation turn a second
        time to the target (MP-20 finding; Codex's `_adopt` clears it the same way)."""

        pending = execution.pending
        if pending is None or session.native_session_ref != pending.session.session_id:
            return
        execution.pending = None
        if execution.session is pending.session:
            return
        source = execution.session
        execution.session, execution.session_id = pending.session, pending.session.session_id
        execution.hooks, execution.permissions = pending.hooks, pending.permissions
        execution.last_turn_ref = None
        if source is not None:
            await source.close()

    # --- snapshot, end ----------------------------------------------------------------------

    async def _custody(
        self, execution: _Execution, request: HarnessRequest, reason: str
    ) -> CapturedSnapshot | None:
        if execution.lease is None:
            return None
        return await self._workspaces.capture(
            execution.lease,
            name=f"claude-agent-sdk/{request.harness_execution_id}/{request.generation}/{reason}",
        )

    async def snapshot(self, request: SnapshotRequest) -> SnapshotManifest:
        """Emulated: the leased worktree captured through the MP-04 allocator."""

        execution = self._execution(request)
        if execution.lease is None:
            raise ValueError("no leased workspace to snapshot")
        captured = await self._custody(execution, request, request.reason)
        if captured is None:
            raise ValueError("the workspace port captured nothing")
        refs = tuple(
            ref
            for ref in (
                captured.snapshot_ref,
                captured.snapshot.patch_artifact_ref,
                captured.snapshot.untracked_artifact_ref,
            )
            if ref
        )
        return SnapshotManifest(
            lane_profile=PROFILE,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            kind="git_patch",
            refs=refs,
            emulated=True,
            digest=sha256_digest(list(refs)),
        )

    async def _declared_outputs(self, execution: _Execution) -> list[str]:
        """MP-20: the files under the lease's `outputs/`, registered through the custody port
        (regular files only; a symlink never leaves the lease)."""

        if self._outputs is None:
            return []
        files = await asyncio.to_thread(_output_files, execution.root)
        refs: list[str] = []
        for path, content in files:
            ref = await self._outputs.register(
                execution.operation, path, content, mount_root=self._settings.mount_root
            )
            if ref is not None:
                refs.append(ref)
        return refs

    async def end_session(self, request: EndSessionRequest) -> CleanupReceipt:
        execution = self._execution(request)
        session = execution.session
        if session is not None:
            await session.close()
        if execution.pending is not None:
            await execution.pending.session.close()
            execution.pending = None
        if execution.lease is None:
            try:
                await self._lease(execution, request)
            except NativeTurnLost:
                self._executions.pop(request.harness_execution_id, None)
                return CleanupReceipt(released=False)
        assert execution.lease is not None
        captured = await self._custody(execution, request, f"end_session:{request.reason}")
        patch_ref = captured.snapshot.patch_artifact_ref if captured is not None else None
        refs: list[str] = []
        if captured is not None:
            refs.append(captured.snapshot_ref)
            if captured.snapshot.untracked_artifact_ref:
                refs.append(captured.snapshot.untracked_artifact_ref)
        refs.extend(await self._declared_outputs(execution))
        # The lease is released only after custody succeeded (SPEC-01 step 8).
        await self._workspaces.release(execution.lease, captured)
        self._executions.pop(request.harness_execution_id, None)
        return CleanupReceipt(
            released=True,
            artifact_refs=tuple(dict.fromkeys((*([patch_ref] if patch_ref else []), *refs))),
            patch_ref=patch_ref,
        )

    # --- MP-06 dispatch reconciliation ------------------------------------------------------

    def _execution_of(
        self, record: DispatchRecord, session: SessionHandle | None
    ) -> _Execution | None:
        """The staged execution a journaled dispatch belongs to: the session handle names it;
        a `create` (no session yet) is keyed `<harness_execution_id>:<generation>:...` by
        `LaneTurnService._fields`."""

        if session is not None:
            return self._executions.get(session.harness_execution_id)
        prefix = record.idempotency_key.split(":", 1)[0]
        execution = self._executions.get(prefix)
        if execution is not None and execution.identity.attempt_no == record.expected_generation:
            return execution
        return None

    async def reconcile_dispatch(
        self, record: DispatchRecord, *, session: SessionHandle | None
    ) -> DispatchLookup:
        """Answer from the state-root ledger: `acknowledged` -> found, `intended` -> unknown,
        absent -> `not_received` (nothing is written to the CLI before its ledger entry)."""

        execution = self._execution_of(record, session)
        if execution is None or execution.lease is None:
            return DispatchLookup(outcome="unknown", detail="no leased state root in this process")
        live = execution.session
        if live is not None and record.kind == "send":
            for turn in live.turns.values():
                if turn.idempotency_key == record.idempotency_key and turn.status != "lost":
                    return DispatchLookup(outcome="found", native_ref=turn.turn_ref)
        entry = await DispatchLedger(execution.state_root).lookup(
            record.kind, record.idempotency_key
        )
        if entry is None:
            return DispatchLookup(
                outcome="not_received", detail="the state-root ledger never journaled this key"
            )
        if entry.phase == "acknowledged" and entry.native_ref is not None:
            return DispatchLookup(outcome="found", native_ref=entry.native_ref)
        return DispatchLookup(
            outcome="unknown", detail="journaled intended: the write may have reached the CLI"
        )


def _ordinal(cursor: str | None) -> int | None:
    if cursor is None:
        return None
    try:
        return int(cursor)
    except ValueError:
        return None


__all__ = [
    "BINDING_MISMATCH",
    "CAPABILITY_DRIFT",
    "CONTINUATION_TURN_PREFIX",
    "DEFAULT_RELOCATED_ROUTES",
    "HANDOVER_PENDING",
    "AuthAdmitter",
    "ChildEnvironmentBuilder",
    "ClaudeAgentSdkHarness",
    "ClaudeLaneError",
    "ClaudeLaneSettings",
    "StaticAuthAdmitter",
    "approval_policy_digest",
    "declared_outputs",
    "missing_outputs",
    "occupancy_from_context_usage",
    "turn_reference",
]
