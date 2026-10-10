"""The `cursor_cloud` Lane Profile: Cursor Cloud Agents API v1 (SPEC-07 section 6; FT-G5).

One Session Turn of a bound operation, driven by `lane.turn` (FT-G2):

- `prepare` pushes branch `mc/<run_id>` from the binding's base ref carrying the Host
  Projection (rules, agents, command hooks, MCP; cloud agents read project config from the
  repository) and the Context Packet files, records the branch head and, when a
  `WorkspaceAllocator` is composed, the provider workspace lease (MP-04 `provider_workspace`
  policy: Mission Control holds the fenced record of branch and base commit);
- `start` creates the agent with a client-supplied `agentId` (`409 agent_id_conflict` means it
  already exists: reattach) or, when the binding needs `envVars`, with an `Idempotency-Key`;
  `metadata` that the account cannot use (`403 feature_unavailable`) is recorded, not fatal.
  The agent works on the pre-created branch (`startingRef` plus `workOnCurrentBranch`). The
  create request enqueues the first run, so turn 1 is that run;
- `send_turn` for later turns returns `busy` on `409 agent_busy` (`wait_then_send`,
  ADR-0030: the agent's own active run is a scheduling condition the workflow waits out at
  the boundary, never a send retry); a `429` is `ProviderCapacityLimited` (MP-05/06: the
  dispatch is journaled `declined`, the workflow waits on a Temporal timer, nothing is
  re-sent here);
- `observe` reads the run's SSE stream with `Last-Event-ID`: frames are keyed by the event id,
  the id-less leading `status` is deduplicated by content, heartbeats are not persisted, the
  retention header is recorded. After `410 stream_expired` the lane writes one `stream.expired`
  status frame, remembers the run as expired, and reconciles the terminal state from
  `GET .../runs/{runId}` (a `run.final` frame that names `observation.stream = "expired"`
  separately from the run's own `FINISHED` / `ERROR`); a run still running after expiry is
  polled through the run record at a bounded interval, with a status frame per change;
- `reconcile_dispatch` (MP-06 `DispatchReconcilingLane`): a journaled create is looked up by
  its deterministic client `agentId` (`404` is the provider's authoritative "not received"),
  a journaled send by the agent's `latestRunId` against the runs this lane acknowledged
  (the create enqueues turn 1; a later run that is not one of ours was accepted; the latest
  run being one of ours means the lost send was not); anything the lane cannot anchor stays
  `unknown` and parks `in_doubt`;
- closing facts keep `git.branches[]` only when the agent's `latestRunId` is this run;
- `usage` settles tokens from `/usage?runId=` only when the API reports this run's usage, and
  cost only when a cost reader reports it; otherwise the usage stays `unknown`;
- `end_session` fetches the branch diff from the SCM as the patch, downloads artifacts through
  their presigned URLs and registers them with digests, records the branch head as the
  provider workspace snapshot (`branch:<branch>@<sha>`, the producer ADR-0037 asked for),
  releases the lease after that custody, then archives the agent.

Kernel Hooks cannot call the worker's loopback listener from the cloud VM, so the cloud
projection carries catalog command hooks only; an immediate cancel reaches the agent through
`POST .../cancel`. Qualification stays false until a recorded live drill (FT-G6 / OVE-55).

FT-G6 controls (SPEC-07 section 7, the describe-honesty suite): SSE events are keyed and
cursored per run (`sse:<run>:<id>`, cursor `<run>@<id>`), so a replacement or continuation
run never resumes with another run's `Last-Event-ID` (`400 invalid_last_event_id`) and never
collides with its frames; a later turn's text is staged by its instruction ref
(`cancel_and_replace` replacement, continuation prompt); `snapshot` names the branch's current
head (`branch:<branch>@<sha>`) and a fork's `prepare` publishes the derived run's branch from
it; a continuation commits the checkpoint packet's files to the branch and creates a fresh
agent on it (`CursorCloudSessionHydrator` in `adapters/cursor/controls.py`), whose first run is
the next turn.

A reattach after a lost worker rehydrates what the process no longer holds (MP-09 option
rehydration): the run branch from the remote (never re-published), the Host Projection paths
the patch excludes, the provider workspace lease (re-acquired by the same holder) and the
last acknowledged run as the reconcile anchor. Cloud agent options (`customSubagents`,
model, mode) live on the agent record itself; the per-run request carries only the prompt.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final
from uuid import UUID, uuid5

from mission_control.adapters.cursor.bridge import (
    AgentBusy,
    BridgeError,
    FeatureUnavailable,
    RunNotCancellable,
    RunNotFound,
)
from mission_control.adapters.cursor.cloud_api import (
    AgentIdConflict,
    AgentNotFound,
    CloudAgentsClient,
    CloudCapacityLimited,
    InvalidLastEventId,
    StreamExpired,
    StreamOpened,
)
from mission_control.adapters.cursor.frames import closing_facts as body_closing_facts
from mission_control.adapters.cursor.frames import first
from mission_control.adapters.cursor.local import LaneArtifactSink
from mission_control.adapters.cursor.projection import (
    DurableInputReader,
    LaneProjectionError,
    ProjectionSource,
    packet_files,
    projection_digests,
    safe_relative,
    turn_text,
    verify_projection,
)
from mission_control.adapters.cursor.scm import GitBranchPublisher, PublishedBranch
from mission_control.application.execution.harness.controls import SessionHandover
from mission_control.application.execution.harness.describe import CURSOR_CLOUD_DESCRIBE
from mission_control.application.execution.harness.dispatch import (
    DispatchLookup,
    DispatchRecord,
    ProviderCapacityLimited,
)
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.execution.harness.leases import WorkspaceLease
from mission_control.application.execution.harness.protocol import NativeTurnLost
from mission_control.application.execution.operations.lane_outputs import LaneOutputCustody
from mission_control.application.execution.usage_admission import ProviderLimitSignal
from mission_control.application.frames.kinds import bounded_key, cursor_cloud_final_key
from mission_control.application.workspaces.errors import WorkspaceError, WorkspaceLeaseHeld
from mission_control.application.workspaces.service import AllocationRequest, WorkspaceAllocator
from mission_control.application.workspaces.snapshots import CapturedSnapshot
from mission_control.domain.agentic_components.projection import HostProjection
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.context.render import INPUTS_MANIFEST_PATH, bytes_digest
from mission_control.domain.execution.bindings import WorkspacePolicyPin
from mission_control.domain.execution.contracts import OperationExecutionRequest
from mission_control.domain.execution.lane_turns import ClosingFacts
from mission_control.domain.execution.lanes import (
    CancelReceipt,
    CancelTurnRequest,
    CleanupReceipt,
    CursorExecutionBinding,
    EndSessionRequest,
    HarnessRequest,
    LaneDescribe,
    LaneFrame,
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

PROFILE: Final = "cursor_cloud"
STREAM_START: Final = "^start"
_AGENT_NAMESPACE = UUID("8d6c1b2a-4f3e-4b5d-9a7c-0e1f2d3c4b5a")
_TERMINAL = {"finished", "error", "cancelled", "expired"}
_REF_CHARS = re.compile(r"[^A-Za-z0-9._/-]")
CUSTOM_SUBAGENT_LIMIT: Final = 20
# After `410 stream_expired` a run that is still running is reconciled through the run record
# at this cadence, at most this many times per observe call (the segment bound still applies).
EXPIRED_POLL_INTERVAL_S: Final = 5.0
EXPIRED_POLL_LIMIT: Final = 120
# The branch is per run (`mc/<run>`): one writer at a time across the run's executions.
CURSOR_CLOUD_POLICY: Final = WorkspacePolicyPin(
    mode="provider_workspace",
    reuse="within_run",
    dirty_input="reject",
    cleanup="retain_until_artifacts_registered",
)

CloudEnvResolver = Callable[[str], Awaitable[Mapping[str, str]]]
CloudCostReader = Callable[[str, str], Awaitable[int | None]]


def client_agent_id(harness_execution_id: str, generation: int) -> str:
    """The deterministic `bc-<uuid>` a create sends, so a lost response never duplicates."""

    return f"bc-{uuid5(_AGENT_NAMESPACE, f'{harness_execution_id}:{generation}')}"


def run_branch(binding: CursorExecutionBinding, run_id: str) -> str:
    return binding.workspace.branch_prefix + _REF_CHARS.sub("-", run_id)


def sse_provider_key(event_id: str, run_id: str | None = None) -> str:
    """`sse:<run>:<id>`: SSE ids are per run (`Last-Event-ID` from another run is a `400`)."""

    return bounded_key("sse", run_id, event_id) if run_id else bounded_key("sse", event_id)


def stream_cursor(run_id: str, event_id: str) -> str:
    return f"{run_id}@{event_id}"


def last_event_id(cursor: str | None, run_id: str) -> str | None:
    """The `Last-Event-ID` a cursor names for `run_id` (another run's cursor names none)."""

    if cursor is None or cursor == STREAM_START:
        return None
    run, separator, event_id = cursor.rpartition("@")
    if not separator:
        return cursor
    if run != run_id or not event_id or event_id == STREAM_START:
        return None
    return event_id


BRANCH_SNAPSHOT_PREFIX: Final = "branch:"
CHECKPOINT_INVALID: Final = "CHECKPOINT_INVALID"


def branch_snapshot(branch: str, head: str) -> str:
    return f"{BRANCH_SNAPSHOT_PREFIX}{branch}@{head}"


def parse_branch_snapshot(ref: str) -> tuple[str, str] | None:
    if not ref.startswith(BRANCH_SNAPSHOT_PREFIX):
        return None
    branch, separator, head = ref.removeprefix(BRANCH_SNAPSHOT_PREFIX).rpartition("@")
    return (branch, head) if separator and branch and head else None


def _content_key(kind: str, run_id: str, data: str) -> str:
    return bounded_key("sse-content", kind, run_id, sha256_digest(data))


def _status(value: Any) -> str:
    return str(value or "").lower()


@dataclass(frozen=True)
class DispatchKeyParts:
    """What a Mission Control dispatch key names (`<heid>:<generation>:turn:<n>` for the
    create and the turn sends, `<heid>:<generation>:continuation:<transfer>` for a handover,
    `<heid>:<generation>:replace:<run>` for a replacement)."""

    harness_execution_id: str
    generation: int
    kind: str
    turn_no: int | None


def parse_dispatch_key(key: str) -> DispatchKeyParts | None:
    parts = key.split(":", 3)
    if len(parts) != 4 or not parts[1].isdigit():
        return None
    turn_no = int(parts[3]) if parts[2] == "turn" and parts[3].isdigit() else None
    return DispatchKeyParts(parts[0], int(parts[1]), parts[2], turn_no)


def capacity_signal(error: CloudCapacityLimited, *, now: datetime) -> ProviderLimitSignal:
    """The MP-05 signal of a `429`: a rate limit with the provider-stated reset when
    `Retry-After` was given (the Cloud Agents numeric limit itself is UNVERIFIED)."""

    return ProviderLimitSignal(
        lane_profile=PROFILE,
        kind="rate_limited",
        resets_at=(
            now + timedelta(seconds=error.retry_after_s)
            if error.retry_after_s is not None
            else None
        ),
        source="cursor_cloud.http_429",
        native_code=error.native_code[:128] if error.native_code else None,
    )


@dataclass
class _CloudSession:
    operation: OperationExecutionRequest
    binding: CursorExecutionBinding
    identity: LaneExecutionIdentity
    projection: HostProjection | None = None
    branch: PublishedBranch | None = None
    agent_id: str | None = None
    initial_run_id: str | None = None
    agent_url: str | None = None
    metadata: str = "sent"
    retention_seconds: int | None = None
    # FT-G6: staged texts of later turns, a forked base, a hydrated continuation.
    turn_texts: dict[str, str] | None = None
    forked_from: str | None = None
    handover: SessionHandover | None = None
    first_runs: dict[str, str] | None = None
    # MP-09: the provider workspace lease and its last recorded custody.
    lease: WorkspaceLease | None = None
    custody: CapturedSnapshot | None = None
    # MP-09 reconcile anchors: runs this lane acknowledged (in-process) and the last run the
    # journal acknowledged (handed in by `ReattachRequest.native_turn_ref`), the run each
    # dispatch key produced, and the runs whose stream retention has passed.
    known_runs: set[str] = field(default_factory=set)
    anchor_run: str | None = None
    sent_keys: dict[str, str] = field(default_factory=dict)
    expired_streams: dict[str, str] = field(default_factory=dict)
    rehydrated: tuple[str, ...] = ()


class CursorCloudHarness:
    """`cursor_cloud` Session Lane over `CloudAgentsClient` (an HTTP fake in tests)."""

    def __init__(
        self,
        *,
        client: CloudAgentsClient,
        publisher: GitBranchPublisher,
        projections: ProjectionSource,
        artifacts: LaneArtifactSink,
        inputs: DurableInputReader | None = None,
        env_resolver: CloudEnvResolver | None = None,
        cost_reader: CloudCostReader | None = None,
        archive_on_end: bool = True,
        describe: LaneDescribe = CURSOR_CLOUD_DESCRIBE,
        workspaces: WorkspaceAllocator | None = None,
        clock: Callable[[], datetime] | None = None,
        expired_poll_interval_s: float = EXPIRED_POLL_INTERVAL_S,
        expired_poll_limit: int = EXPIRED_POLL_LIMIT,
        outputs: LaneOutputCustody | None = None,
    ) -> None:
        if describe.lane_profile != PROFILE:
            raise ValueError("the Cursor cloud harness describes the cursor_cloud profile")
        self._client = client
        # MP-20: downloaded `outputs/` artifacts become workspace candidates when composed.
        self._outputs = outputs
        self._publisher = publisher
        self._projections = projections
        self._artifacts = artifacts
        self._inputs = inputs
        self._env_resolver = env_resolver
        self._cost_reader = cost_reader
        self._archive = archive_on_end
        self._describe = describe
        self._workspaces = workspaces
        self._clock = clock or (lambda: datetime.now(UTC))
        self._expired_poll_interval_s = expired_poll_interval_s
        self._expired_poll_limit = expired_poll_limit
        self._sessions: dict[str, _CloudSession] = {}

    def describe(self) -> LaneDescribe:
        return self._describe

    def stage(self, harness_execution_id: str, operation: OperationExecutionRequest) -> None:
        binding = operation.cursor_binding
        if binding is None or binding.lane_profile != PROFILE or binding.cloud is None:
            raise ValueError("cursor_cloud runs only an operation with a cursor_cloud binding")
        current = self._sessions.get(harness_execution_id)
        if current is not None:
            if current.operation != operation:
                raise ValueError("harness execution is staged with another operation")
            return
        self._sessions[harness_execution_id] = _CloudSession(
            operation=operation,
            binding=binding,
            identity=LaneExecutionIdentity.of(operation, PROFILE, 1),
        )

    def _session(self, request: HarnessRequest) -> _CloudSession:
        session = self._sessions.get(request.harness_execution_id)
        if session is None:
            raise NativeTurnLost(f"harness execution {request.harness_execution_id} is not staged")
        if session.identity.attempt_no != request.generation:
            session.identity = LaneExecutionIdentity.of(
                session.operation, PROFILE, request.generation
            )
        return session

    @staticmethod
    def _repository(binding: CursorExecutionBinding) -> str:
        assert binding.workspace.repo_url is not None
        return binding.workspace.repo_url

    # --- prepare --------------------------------------------------------------------------------

    async def prepare(self, request: PrepareRequest) -> PreparedSession:
        session = self._session(request)
        binding = session.binding
        projection = await self._projections.project(
            session.operation, profile=PROFILE, packet_index=None
        )
        digests = projection_digests(projection)
        verify_projection(binding, digests)
        packet = await packet_files(session.operation, self._inputs)
        files = [(item.path, item.content) for item in projection.files] + packet
        base_ref = binding.workspace.base_ref
        forked = _workspace_item(packet)
        if forked is not None:
            # A fork: the derived run's branch starts at the source branch's snapshot head.
            parsed = parse_branch_snapshot(forked)
            if parsed is None:
                raise LaneProjectionError(
                    CHECKPOINT_INVALID, f"{forked} is not a cursor_cloud branch snapshot"
                )
            base_ref = parsed[1]
            session.forked_from = forked
        branch = await self._publisher.publish(
            repository=self._repository(binding),
            base_ref=base_ref,
            branch=run_branch(binding, request.run_id),
            files=files,
        )
        session.projection = projection
        session.branch = branch
        await self._allocate_workspace(
            session, request, base_ref=base_ref, branch=branch, run_id=request.run_id
        )
        return PreparedSession(
            lane_profile=PROFILE,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            workspace_ref=f"branch:{branch.branch}@{branch.head}",
            projection_digests={
                **digests,
                "packet_digest": sha256_digest(
                    sorted((path, bytes_digest(content)) for path, content in packet)
                ),
            },
            materialization_ref=branch.head,
        )

    async def _allocate_workspace(
        self,
        session: _CloudSession,
        request: HarnessRequest,
        *,
        base_ref: str,
        branch: PublishedBranch,
        run_id: str,
        attempt_no: int | None = None,
    ) -> None:
        """Record the provider workspace through the allocator (MP-04 `provider_workspace`):
        the same holder re-acquiring its active lease gets it back unchanged, so a retried
        prepare or a reattach is idempotent; a slot held by another generation fences this
        one out."""

        if self._workspaces is None:
            return
        try:
            session.lease = await self._workspaces.allocate_provider_workspace(
                AllocationRequest(
                    request_scope=session.identity.request_scope,
                    lane_profile=PROFILE,
                    harness_execution_id=UUID(request.harness_execution_id),
                    generation=request.generation,
                    run_id=run_id,
                    attempt_no=attempt_no or session.identity.attempt_no,
                    policy=CURSOR_CLOUD_POLICY,
                    repository=self._repository(session.binding),
                    base_ref=base_ref,
                ),
                branch=branch.branch,
                base_commit=branch.base_commit,
            )
        except WorkspaceLeaseHeld as error:
            raise NativeTurnLost(f"the run branch workspace is held elsewhere: {error}") from error

    # --- start, reattach, send ------------------------------------------------------------------

    def _handle(self, request: HarnessRequest, session: _CloudSession) -> SessionHandle:
        assert session.agent_id is not None
        details = {"cloud_branch": "", "cloud_agent_url": session.agent_url or ""}
        if session.branch is not None:
            details["cloud_branch"] = f"{session.branch.branch}@{session.branch.head}"
        if session.rehydrated:
            details["rehydrated"] = ",".join(session.rehydrated)
        return SessionHandle(
            lane_profile=PROFILE,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            native_session_ref=session.agent_id,
            native_details={key: value for key, value in details.items() if value},
        )

    def _subagents(self, session: _CloudSession) -> list[dict[str, Any]]:
        if session.projection is None:
            return []
        raw = session.projection.send_options.get("agents")
        items = raw if isinstance(raw, list | tuple) else ()
        return [dict(item) for item in items if isinstance(item, dict)][:CUSTOM_SUBAGENT_LIMIT]

    async def start(self, request: StartRequest) -> SessionHandle:
        session = self._session(request)
        binding = session.binding
        cloud = binding.cloud
        assert cloud is not None and session.branch is not None
        body: dict[str, Any] = {
            "prompt": {"text": turn_text(session.operation)},
            "model": {"id": binding.model_id},
            "name": f"mc-{session.identity.run_key}-{session.identity.attempt_no}"[:100],
            "repos": [{"url": self._repository(binding), "startingRef": session.branch.branch}],
            "workOnCurrentBranch": True,
            "autoCreatePR": cloud.auto_create_pr,
            "mode": binding.mode,
        }
        if cloud.environment != "cloud":
            body["env"] = {"type": cloud.environment}
        subagents = self._subagents(session)
        if subagents:
            body["customSubagents"] = subagents
        idempotency_key: str | None = None
        if cloud.client_agent_id:
            body["agentId"] = client_agent_id(request.harness_execution_id, request.generation)
        else:
            idempotency_key = f"{request.harness_execution_id}:{request.generation}:create"
            if cloud.env_vars_ref is not None:
                if self._env_resolver is None:
                    raise LaneProjectionError(
                        "UNSUPPORTED_BEHAVIOR", "env vars are bound but no resolver is composed"
                    )
                body["envVars"] = dict(await self._env_resolver(cloud.env_vars_ref))
        body["metadata"] = {"mc_run_id": session.identity.run_key, **cloud.metadata}
        try:
            created = await self._create(body, idempotency_key, session)
        except AgentIdConflict:
            # Already created by an earlier attempt of this activity: reattach to it.
            agent = await self._client.get_agent(str(body["agentId"]))
            session.agent_id = str(agent["id"])
            session.initial_run_id = agent.get("latestRunId")
            session.agent_url = agent.get("url")
            if session.initial_run_id:
                session.known_runs.add(str(session.initial_run_id))
            return self._handle(request, session)
        except CloudCapacityLimited as error:
            raise ProviderCapacityLimited(capacity_signal(error, now=self._clock())) from error
        agent = created.get("agent") or {}
        run = created.get("run") or {}
        session.agent_id = str(agent["id"])
        session.initial_run_id = str(run["id"]) if run.get("id") else agent.get("latestRunId")
        session.agent_url = agent.get("url")
        if session.initial_run_id:
            session.known_runs.add(str(session.initial_run_id))
        return self._handle(request, session)

    async def _create(
        self, body: dict[str, Any], idempotency_key: str | None, session: _CloudSession
    ) -> dict[str, Any]:
        try:
            return await self._client.create_agent(body, idempotency_key=idempotency_key)
        except FeatureUnavailable:
            # `metadata` is not enabled for this account: recorded, not fatal.
            session.metadata = "feature_unavailable"
            without = {key: value for key, value in body.items() if key != "metadata"}
            return await self._client.create_agent(without, idempotency_key=idempotency_key)

    async def reattach(self, request: ReattachRequest) -> SessionHandle:
        session = self._session(request)
        try:
            agent = await self._client.get_agent(request.native_session_ref)
            if request.native_turn_ref is not None:
                await self._client.get_run(request.native_session_ref, request.native_turn_ref)
        except (AgentNotFound, RunNotFound) as error:
            raise NativeTurnLost("the cloud agent or run no longer exists") from error
        session.agent_id = str(agent["id"])
        session.agent_url = agent.get("url")
        session.initial_run_id = session.initial_run_id or agent.get("latestRunId")
        if request.native_turn_ref is not None:
            # The last run the journal acknowledged: the anchor a lost send reconciles against.
            session.anchor_run = request.native_turn_ref
            session.known_runs.add(request.native_turn_ref)
        await self._rehydrate(session, request)
        return self._handle(request, session)

    async def _rehydrate(self, session: _CloudSession, request: HarnessRequest) -> None:
        """What a lost worker's process no longer holds: the run branch (resolved on the
        remote, never re-published), the projection (its paths are the patch exclusions) and
        the provider workspace lease; a missing branch loses the turn."""

        rehydrated: list[str] = []
        binding = session.binding
        if session.branch is None:
            branch = await self._publisher.resolve(
                repository=self._repository(binding),
                base_ref=binding.workspace.base_ref,
                branch=run_branch(binding, session.operation.identity.run_id),
            )
            if branch is None:
                raise NativeTurnLost("the run branch of this cloud session no longer exists")
            session.branch = branch
            rehydrated.append("branch")
        if session.projection is None:
            session.projection = await self._projections.project(
                session.operation, profile=PROFILE, packet_index=None
            )
            rehydrated.append("projection")
        if session.lease is None and self._workspaces is not None:
            await self._allocate_workspace(
                session,
                request,
                base_ref=binding.workspace.base_ref,
                branch=session.branch,
                run_id=session.operation.identity.run_id,
            )
            rehydrated.append("workspace_lease")
        if rehydrated:
            session.rehydrated = tuple(dict.fromkeys((*session.rehydrated, *rehydrated)))

    def rehydration_receipt(self, harness_execution_id: str) -> tuple[str, ...]:
        """Which non-persisted facts the last reattach re-applied (empty when nothing was)."""

        session = self._sessions.get(harness_execution_id)
        return () if session is None else session.rehydrated

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        session = self._session(request)
        agent_id = request.session.native_session_ref or session.agent_id
        if agent_id is None:
            raise ValueError("send_turn requires a created cloud agent")
        first_run = (session.first_runs or {}).get(agent_id)
        if first_run is not None and request.instruction_ref in (session.turn_texts or {}):
            # A continuation agent: its create request enqueued the continuation turn.
            session.known_runs.add(first_run)
            session.sent_keys[request.idempotency_key] = first_run
            return TurnHandle(
                session=request.session, turn_no=request.turn_no, native_turn_ref=first_run
            )
        if request.turn_no == 1:
            # The create request enqueued the first run.
            run_id = session.initial_run_id
            if run_id is None:
                agent = await self._client.get_agent(agent_id)
                run_id = agent.get("latestRunId")
            if run_id is None:
                raise NativeTurnLost("the cloud agent reports no first run")
            session.known_runs.add(str(run_id))
            session.sent_keys[request.idempotency_key] = str(run_id)
            return TurnHandle(
                session=request.session, turn_no=request.turn_no, native_turn_ref=str(run_id)
            )
        text = (session.turn_texts or {}).get(request.instruction_ref) or turn_text(
            session.operation
        )
        try:
            run = await self._client.create_run(agent_id, {"prompt": {"text": text}})
        except AgentBusy:
            # ADR-0030: this agent's own active run; `wait_then_send` at the boundary.
            return TurnHandle(session=request.session, turn_no=request.turn_no, status="busy")
        except CloudCapacityLimited as error:
            raise ProviderCapacityLimited(capacity_signal(error, now=self._clock())) from error
        run_id = str(run["id"])
        session.known_runs.add(run_id)
        session.sent_keys[request.idempotency_key] = run_id
        return TurnHandle(session=request.session, turn_no=request.turn_no, native_turn_ref=run_id)

    # --- MP-06: reconcile an ambiguous create/send by native identity ---------------------------

    async def reconcile_dispatch(
        self, record: DispatchRecord, *, session: SessionHandle | None
    ) -> DispatchLookup:
        parts = parse_dispatch_key(record.idempotency_key)
        if parts is None:
            return DispatchLookup(outcome="unknown", detail="the dispatch key names no execution")
        staged = self._sessions.get(parts.harness_execution_id)
        if record.kind == "create":
            return await self._reconcile_create(record, parts, staged)
        return await self._reconcile_send(record, parts, staged, session)

    async def _reconcile_create(
        self, record: DispatchRecord, parts: DispatchKeyParts, staged: _CloudSession | None
    ) -> DispatchLookup:
        if staged is not None and staged.binding.cloud is not None:
            if not staged.binding.cloud.client_agent_id:
                return DispatchLookup(
                    outcome="unknown",
                    detail="the create used an Idempotency-Key; the API has no read-back by key",
                )
        agent_ref = client_agent_id(parts.harness_execution_id, parts.generation)
        try:
            agent = await self._client.get_agent(agent_ref)
        except AgentNotFound:
            # The id is ours and deterministic: nothing exists under it, nothing was accepted.
            return DispatchLookup(
                outcome="not_received", detail="no agent exists under the client agent id"
            )
        except BridgeError as error:
            return DispatchLookup(outcome="unknown", detail=str(error)[:512])
        if staged is not None:
            staged.agent_id = str(agent["id"])
            staged.agent_url = agent.get("url")
            staged.initial_run_id = staged.initial_run_id or agent.get("latestRunId")
            if staged.initial_run_id:
                staged.known_runs.add(str(staged.initial_run_id))
        return DispatchLookup(outcome="found", native_ref=str(agent["id"]))

    async def _reconcile_send(
        self,
        record: DispatchRecord,
        parts: DispatchKeyParts,
        staged: _CloudSession | None,
        session: SessionHandle | None,
    ) -> DispatchLookup:
        agent_id = (session.native_session_ref if session is not None else None) or (
            staged.agent_id if staged is not None else None
        )
        if staged is not None:
            remembered = staged.sent_keys.get(record.idempotency_key)
            if remembered is not None:
                return DispatchLookup(outcome="found", native_ref=remembered)
            if parts.kind == "continuation" and agent_id is not None:
                # The hydrated agent's create enqueued its continuation turn (in-process).
                first = (staged.first_runs or {}).get(agent_id)
                if first is not None:
                    return DispatchLookup(outcome="found", native_ref=first)
        if agent_id is None:
            return DispatchLookup(outcome="unknown", detail="no cloud agent to query")
        try:
            agent = await self._client.get_agent(agent_id)
        except AgentNotFound:
            return DispatchLookup(outcome="unknown", detail="the cloud agent no longer exists")
        except BridgeError as error:
            return DispatchLookup(outcome="unknown", detail=str(error)[:512])
        if _status(agent.get("status")) == "archived":
            return DispatchLookup(outcome="unknown", detail="the cloud agent is archived")
        latest = agent.get("latestRunId")
        latest_ref = str(latest) if latest else None
        if parts.kind == "continuation":
            if latest_ref is not None:
                # A continuation agent exists only for its continuation turn: its latest
                # (and first) run is that turn.
                return DispatchLookup(outcome="found", native_ref=latest_ref)
            return DispatchLookup(outcome="unknown", detail="the continuation agent has no run")
        if parts.turn_no == 1:
            if latest_ref is None:
                return DispatchLookup(
                    outcome="unknown", detail="the agent reports no run for its create"
                )
            # The create enqueued turn 1: the agent's run is that turn.
            return DispatchLookup(outcome="found", native_ref=latest_ref)
        anchors: set[str] = set()
        if staged is not None:
            anchors |= staged.known_runs
            if staged.anchor_run is not None:
                anchors.add(staged.anchor_run)
        if not anchors:
            return DispatchLookup(
                outcome="unknown",
                detail="no acknowledged run anchors this agent; the lost send cannot be told "
                "apart from an earlier turn",
            )
        if latest_ref is None:
            return DispatchLookup(outcome="unknown", detail="the agent reports no latest run")
        if latest_ref in anchors:
            # One active run per agent and this lane is the agent's only sender: the newest
            # run is one Mission Control already acknowledged, so the lost send was refused.
            return DispatchLookup(
                outcome="not_received",
                detail="the agent's latest run is one this lane already acknowledged",
            )
        if staged is not None:
            staged.known_runs.add(latest_ref)
            staged.sent_keys[record.idempotency_key] = latest_ref
        return DispatchLookup(outcome="found", native_ref=latest_ref)

    # --- observe ------------------------------------------------------------------------------

    def _frame(
        self,
        request: ObserveRequest,
        *,
        key: str,
        cursor: str,
        raw_kind: str,
        body: dict[str, Any],
        run_id: str,
        terminal: bool = False,
    ) -> LaneFrame:
        return LaneFrame(
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            provider_key=key,
            cursor=cursor,
            kind=raw_kind,
            raw_kind=raw_kind,
            body=body,
            terminal=terminal,
            digest=sha256_digest(body),
            native_turn_ref=run_id,
            tool_call_ref=first(body, "callId", "call_id") if raw_kind == "tool_call" else None,
        )

    async def _attribution(self, agent_id: str, run_id: str, body: dict[str, Any]) -> None:
        """`git.branches[]` is agent-scoped: keep it only if this run is the latest."""

        try:
            agent = await self._client.get_agent(agent_id)
        except AgentNotFound:
            body["git_attributed"] = False
            return
        body["latestRunId"] = agent.get("latestRunId")
        body["git_attributed"] = agent.get("latestRunId") == run_id

    async def observe(self, request: ObserveRequest) -> AsyncIterator[LaneFrame]:
        session = self._session(request)
        agent_id = request.turn.session.native_session_ref or session.agent_id
        run_id = request.turn.native_turn_ref
        if agent_id is None or run_id is None:
            raise NativeTurnLost("no cloud run to observe")
        after = last_event_id(request.after, run_id)
        cursor = stream_cursor(run_id, after) if after is not None else STREAM_START
        expired = run_id in session.expired_streams
        if not expired:
            outcome = "ended"
            try:
                async for frame in self._stream_frames(request, session, agent_id, run_id, after):
                    cursor = frame.cursor
                    yield frame
                    if frame.terminal:
                        return
            except InvalidLastEventId:
                # Our cursor is foreign to this run: never replay with it. Reopen from the
                # start once; per-run keys make the replay store nothing twice.
                async for frame in self._stream_frames(request, session, agent_id, run_id, None):
                    cursor = frame.cursor
                    yield frame
                    if frame.terminal:
                        return
            except StreamExpired:
                # Retention passed: the backlog is gone, the run is not. Record the fact
                # separately from whatever the run record says and stop asking the stream.
                session.expired_streams[run_id] = cursor
                outcome = "expired"
                yield self._frame(
                    request,
                    key=bounded_key("sse-expired", run_id, after or "start"),
                    cursor=cursor,
                    raw_kind="status",
                    body={
                        "stream": "expired",
                        "retention_seconds": session.retention_seconds,
                        "last_event_id": after,
                        "observation": "run_record",
                    },
                    run_id=run_id,
                )
        else:
            outcome = "expired"
        async for frame in self._reconcile_run(request, agent_id, run_id, cursor, outcome):
            yield frame

    async def _stream_frames(
        self,
        request: ObserveRequest,
        session: _CloudSession,
        agent_id: str,
        run_id: str,
        after: str | None,
    ) -> AsyncIterator[LaneFrame]:
        cursor = stream_cursor(run_id, after) if after is not None else STREAM_START
        async for item in self._client.stream(agent_id, run_id, last_event_id=after):
            if isinstance(item, StreamOpened):
                session.retention_seconds = item.retention_seconds
                body = {
                    "stream": "opened",
                    "retention_seconds": item.retention_seconds,
                    "last_event_id": item.last_event_id,
                }
                yield self._frame(
                    request,
                    key=bounded_key("sse-open", run_id, after or "start"),
                    cursor=cursor,
                    raw_kind="status",
                    body=body,
                    run_id=run_id,
                )
                continue
            if item.event == "heartbeat":
                continue  # keep-alive only; never persisted
            if item.event == "done":
                break
            body = item.json()
            if item.id is not None:
                cursor = stream_cursor(run_id, item.id)
                key = sse_provider_key(item.id, run_id)
            else:
                # The leading status is re-sent on every reconnect without an id.
                key = _content_key(item.event, run_id, item.data)
            terminal = item.event == "result"
            if terminal:
                await self._attribution(agent_id, run_id, body)
            yield self._frame(
                request,
                key=key,
                cursor=cursor,
                raw_kind=item.event,
                body=body,
                run_id=run_id,
                terminal=terminal,
            )
            if terminal:
                return

    async def _reconcile_run(
        self,
        request: ObserveRequest,
        agent_id: str,
        run_id: str,
        cursor: str,
        outcome: str,
    ) -> AsyncIterator[LaneFrame]:
        """The run record is the terminal truth once the stream ended or expired: a terminal
        status becomes the one `run.final` frame; a running one is polled at a bounded
        cadence after expiry (a status frame per change), or left to the next segment."""

        polls = 0
        last_status: str | None = None
        while True:
            try:
                run = await self._client.get_run(agent_id, run_id)
            except (AgentNotFound, RunNotFound) as error:
                raise NativeTurnLost("the cloud run no longer exists") from error
            status = _status(run.get("status"))
            if status in _TERMINAL:
                body = dict(run)
                body["synthesized_from"] = "run_record"
                body["observation"] = {"source": "run_record", "stream": outcome}
                await self._attribution(agent_id, run_id, body)
                yield self._frame(
                    request,
                    key=cursor_cloud_final_key(run_id),
                    cursor=cursor,
                    raw_kind="run.final",
                    body=body,
                    run_id=run_id,
                    terminal=True,
                )
                return
            if outcome != "expired":
                return
            if status != last_status:
                last_status = status
                yield self._frame(
                    request,
                    key=bounded_key("run-status", run_id, status or "unknown"),
                    cursor=cursor,
                    raw_kind="status",
                    body={
                        "runId": run_id,
                        "status": run.get("status"),
                        "observation": "run_record",
                    },
                    run_id=run_id,
                )
            polls += 1
            if polls >= self._expired_poll_limit:
                return
            await asyncio.sleep(self._expired_poll_interval_s)

    def closing_facts(self, turn: TurnHandle, frame: LaneFrame) -> ClosingFacts:
        body = dict(frame.body) if isinstance(frame.body, dict) else {}
        if not body.get("git_attributed", False):
            body.pop("git", None)
        return body_closing_facts(body)

    def final_text(self, turn: TurnHandle, frame: LaneFrame) -> str | None:
        """`FinalTextLane` (MP-20): the run's whole final text from the terminal frame body
        (the closing facts keep only a bounded excerpt of it)."""

        del turn
        body = frame.body if isinstance(frame.body, Mapping) else {}
        value = first(body, "result", "text")
        return value if isinstance(value, str) else None

    def resume_cursor(self, provider_key: str) -> str | None:
        if not provider_key.startswith("sse:"):
            return None
        run, separator, event_id = provider_key.removeprefix("sse:").rpartition(":")
        return stream_cursor(run, event_id) if separator and run else event_id

    # --- FT-G6: later turns and continuation -----------------------------------------------------

    def stage_turn(self, harness_execution_id: str, instruction_ref: str, text: str) -> None:
        session = self._sessions.get(harness_execution_id)
        if session is None:
            raise NativeTurnLost(f"harness execution {harness_execution_id} is not staged")
        session.turn_texts = {**(session.turn_texts or {}), instruction_ref: text}

    async def pending_handover(self, harness_execution_id: str) -> SessionHandover | None:
        session = self._sessions.get(harness_execution_id)
        return None if session is None else session.handover

    async def complete_handover(self, harness_execution_id: str, transfer_id: str) -> None:
        session = self._sessions.get(harness_execution_id)
        if session is not None and session.handover is not None:
            if session.handover.transfer_id == transfer_id:
                session.agent_id = session.handover.session.native_session_ref
                session.handover = None

    def live_session(self, agent_id: str) -> str | None:
        for harness_execution_id, session in self._sessions.items():
            if session.agent_id == agent_id and session.branch is not None:
                return harness_execution_id
        return None

    def session_branch(self, harness_execution_id: str) -> tuple[str, str]:
        session = self._sessions[harness_execution_id]
        assert session.branch is not None
        return self._repository(session.binding), session.branch.branch

    def session_lease(self, harness_execution_id: str) -> WorkspaceLease | None:
        session = self._sessions.get(harness_execution_id)
        return None if session is None else session.lease

    async def read_branch(
        self, harness_execution_id: str, roots: tuple[str, ...]
    ) -> list[tuple[str, bytes]]:
        repository, branch = self.session_branch(harness_execution_id)
        head = await self._publisher.head(repository=repository, branch=branch)
        return await self._publisher.read_tree(repository=repository, ref=head, roots=roots)

    async def hydrate_agent(
        self,
        harness_execution_id: str,
        *,
        transfer_id: str,
        files: list[tuple[str, bytes]],
        prompt: str,
        source_session_ref: str,
    ) -> tuple[str, dict[str, str]]:
        """Commit the continuation's files to the run branch, then create a fresh agent on
        it whose first run is the continuation turn; offer the handover. Returns the agent id
        and the restored digests read back from the pushed branch."""

        session = self._sessions[harness_execution_id]
        binding = session.binding
        cloud = binding.cloud
        assert cloud is not None and session.branch is not None
        repository, branch = self.session_branch(harness_execution_id)
        head = await self._publisher.commit_files(
            repository=repository,
            branch=branch,
            files=files,
            message=f"mission control: continuation {transfer_id}",
        )
        agent_ref = client_agent_id(f"{harness_execution_id}:{transfer_id}", 1)
        body: dict[str, Any] = {
            "prompt": {"text": prompt},
            "model": {"id": binding.model_id},
            "name": f"mc-{session.identity.run_key}-continuation"[:100],
            "repos": [{"url": repository, "startingRef": branch}],
            "workOnCurrentBranch": True,
            "autoCreatePR": cloud.auto_create_pr,
            "mode": binding.mode,
            "agentId": agent_ref,
        }
        try:
            created = await self._client.create_agent(body, idempotency_key=None)
            agent = created.get("agent") or {}
            run = created.get("run") or {}
            first_run = str(run.get("id") or agent.get("latestRunId"))
        except AgentIdConflict:
            agent = await self._client.get_agent(agent_ref)
            first_run = str(agent.get("latestRunId"))
        except CloudCapacityLimited as error:
            raise ProviderCapacityLimited(capacity_signal(error, now=self._clock())) from error
        agent_id = str(agent["id"])
        instruction_ref = f"continuation:{transfer_id}"
        self.stage_turn(harness_execution_id, instruction_ref, prompt)
        session.first_runs = {**(session.first_runs or {}), agent_id: first_run}
        session.known_runs.add(first_run)
        session.handover = SessionHandover(
            transfer_id=transfer_id,
            session=SessionHandle(
                lane_profile=PROFILE,
                harness_execution_id=harness_execution_id,
                generation=session.identity.attempt_no,
                native_session_ref=agent_id,
                native_details={"cloud_branch": f"{branch}@{head}"},
            ),
            instruction_ref=instruction_ref,
            source_session_ref=source_session_ref,
        )
        restored = {
            f"/{safe_relative(path).as_posix()}": bytes_digest(content)
            for path, content in await self._publisher.read_tree(
                repository=repository, ref=head, roots=tuple(path for path, _ in files)
            )
        }
        return agent_id, restored

    # --- controls -----------------------------------------------------------------------------

    async def status(self, request: StatusRequest) -> ProviderStatus:
        session = self._session(request)
        agent_id = request.session.native_session_ref or session.agent_id
        if agent_id is None:
            return ProviderStatus(status="not_started", terminal=False, idle=True)
        try:
            agent = await self._client.get_agent(agent_id)
            run = (
                await self._client.get_run(agent_id, request.turn.native_turn_ref)
                if request.turn is not None and request.turn.native_turn_ref
                else None
            )
        except (AgentNotFound, RunNotFound) as error:
            raise NativeTurnLost("the cloud agent or run no longer exists") from error
        agent_idle = _status(agent.get("status")) == "idle"
        if run is None:
            return ProviderStatus(
                status=_status(agent.get("status")), terminal=False, idle=agent_idle
            )
        status = _status(run.get("status"))
        terminal = status in _TERMINAL
        return ProviderStatus(status=status, terminal=terminal, idle=agent_idle or terminal)

    async def cancel_turn(self, request: CancelTurnRequest) -> CancelReceipt:
        session = self._session(request)
        agent_id = request.turn.session.native_session_ref or session.agent_id
        run_id = request.turn.native_turn_ref
        if agent_id is None or run_id is None:
            return CancelReceipt(acknowledged=True, already_terminal=True, native_status="idle")
        already = False
        try:
            await self._client.cancel_run(agent_id, run_id)
        except RunNotCancellable:
            already = True
        except (AgentNotFound, RunNotFound):
            return CancelReceipt(acknowledged=False, native_status="unknown")
        run = await self._client.get_run(agent_id, run_id)
        return CancelReceipt(
            acknowledged=True, already_terminal=already, native_status=_status(run.get("status"))
        )

    async def usage(self, request: UsageRequest) -> UsageReport:
        """Tokens settle from `GET /v1/agents/{id}/usage?runId=` only when the API reports
        this run's usage (`runs[].usage`, or `totalUsage` when no run is asked for); cost
        settles only when the cost reader (the SDK's `get_usage().cost`) reports a non-null
        value. Anything the provider does not report stays `unknown` (never zero)."""

        session = self._session(request)
        agent_id = request.session.native_session_ref or session.agent_id
        run_id = request.turn.native_turn_ref if request.turn is not None else None
        if agent_id is None:
            return UsageReport(disposition="unknown")
        try:
            usage = await self._client.usage(agent_id, run_id)
        except (FeatureUnavailable, AgentNotFound, RunNotFound):
            return UsageReport(disposition="unknown")
        tokens = _run_tokens(usage, run_id)
        if tokens is None:
            return UsageReport(disposition="unknown")
        cost = (
            await self._cost_reader(agent_id, run_id)
            if self._cost_reader is not None and run_id is not None
            else None
        )
        input_tokens = int(tokens.get("inputTokens", 0) or 0)
        output_tokens = int(tokens.get("outputTokens", 0) or 0)
        return UsageReport(
            disposition="settled",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=int(tokens.get("totalTokens", 0) or input_tokens + output_tokens),
            cost_micros_usd=cost,
        )

    # --- snapshot, end ------------------------------------------------------------------------

    def _excluded(self, session: _CloudSession) -> tuple[str, ...]:
        return session.projection.paths() if session.projection is not None else ()

    async def _record_head(self, session: _CloudSession, head: str) -> CapturedSnapshot | None:
        """The branch head read back from the remote, recorded on the provider workspace
        lease (`branch:<branch>@<sha>`): the producer of the restore convention."""

        if self._workspaces is None or session.lease is None:
            return None
        try:
            captured = await self._workspaces.record_provider_snapshot(
                session.lease, head_commit=head
            )
        except WorkspaceError as error:
            raise NativeTurnLost(f"the provider workspace snapshot was refused: {error}") from error
        session.custody = captured
        return captured

    async def snapshot(self, request: SnapshotRequest) -> SnapshotManifest:
        session = self._session(request)
        if session.branch is None:
            raise ValueError("no published branch to snapshot")
        head = await self._publisher.head(
            repository=self._repository(session.binding), branch=session.branch.branch
        )
        refs = [branch_snapshot(session.branch.branch, head)]
        captured = await self._record_head(session, head)
        if captured is not None and session.lease is not None:
            refs.append(f"workspace-lease:{session.lease.lease_id}")
        if session.agent_id is not None:
            refs += [
                f"artifact:{item.get('path')}"
                for item in await self._client.list_artifacts(session.agent_id)
            ]
        return SnapshotManifest(
            lane_profile=PROFILE,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            kind="branch",
            refs=tuple(refs),
            emulated=True,
            digest=sha256_digest(refs),
        )

    async def end_session(self, request: EndSessionRequest) -> CleanupReceipt:
        session = self._session(request)
        agent_id = request.session.native_session_ref or session.agent_id
        if session.branch is None and agent_id is not None:
            # A session ended by a worker that never prepared it (reattached after a loss).
            try:
                await self._rehydrate(session, request)
            except NativeTurnLost:
                session.branch = None
        if session.branch is None or agent_id is None:
            self._sessions.pop(request.harness_execution_id, None)
            return CleanupReceipt(released=False)
        scope = session.identity.request_scope
        name = f"cursor-cloud/{request.harness_execution_id}/{request.generation}"
        diff, head = await self._publisher.diff(
            repository=self._repository(session.binding),
            base=session.branch.base_commit,
            branch=session.branch.branch,
            exclude=self._excluded(session),
        )
        patch_ref = await self._artifacts.stage(
            request_scope=scope, name=f"{name}/patch.diff", content=diff, media_type="text/x-diff"
        )
        registered: list[dict[str, Any]] = []
        refs: list[str] = []
        for item in await self._client.list_artifacts(agent_id):
            path = str(item.get("path", ""))
            if not path:
                continue
            content = await self._client.download_artifact(agent_id, path)
            candidate = (
                await self._outputs.register(session.operation, path, content)
                if self._outputs is not None
                else None
            )
            ref = candidate or await self._artifacts.stage(
                request_scope=scope,
                name=f"{name}/{path}",
                content=content,
                media_type="application/octet-stream",
            )
            refs.append(ref)
            registered.append(
                {"path": path, "digest": bytes_digest(content), "size": len(content), "ref": ref}
            )
        captured = await self._record_head(session, head)
        manifest = json.dumps(
            {
                "branch": session.branch.branch,
                "base_commit": session.branch.base_commit,
                "head": head,
                "artifacts": registered,
                "metadata": session.metadata,
                "stream_retention_seconds": session.retention_seconds,
                "expired_streams": dict(sorted(session.expired_streams.items())),
                "workspace_snapshot_ref": captured.snapshot_ref if captured else None,
                "workspace_lease_id": str(session.lease.lease_id) if session.lease else None,
            },
            sort_keys=True,
        ).encode("utf-8")
        manifest_ref = await self._artifacts.stage(
            request_scope=scope,
            name=f"{name}/session-manifest.json",
            content=manifest,
            media_type="application/json",
        )
        # The lease is released only after the patch, the artifacts and the head snapshot
        # are in custody (SPEC-01 step 8); the provider keeps the VM, nothing is deleted.
        if self._workspaces is not None and session.lease is not None:
            session.lease = await self._workspaces.release(session.lease, custody=captured)
        # Archived only after the patch and every artifact are registered; never deleted here.
        if self._archive:
            await self._client.archive(agent_id)
        self._sessions.pop(request.harness_execution_id, None)
        return CleanupReceipt(
            released=True, artifact_refs=(patch_ref, *refs, manifest_ref), patch_ref=patch_ref
        )


def _run_tokens(usage: Mapping[str, Any], run_id: str | None) -> Mapping[str, Any] | None:
    """This run's token usage from the `/usage` response, or the agent total when no run is
    asked for; None when the API reports nothing for it (agent totals are never attributed
    to one run)."""

    if run_id is not None:
        for entry in usage.get("runs") or ():
            if isinstance(entry, Mapping) and entry.get("id") == run_id:
                tokens = entry.get("usage")
                return tokens if isinstance(tokens, Mapping) and tokens else None
        return None
    tokens = usage.get("totalUsage")
    return tokens if isinstance(tokens, Mapping) and tokens else None


def _workspace_item(packet: list[tuple[str, bytes]]) -> str | None:
    """The snapshot ref of the packet's `workspace` item (`.mission/inputs.json`), if any."""

    raw = dict(packet).get(INPUTS_MANIFEST_PATH)
    if raw is None:
        return None
    try:
        workspace = json.loads(raw.decode("utf-8")).get("workspace")
    except (ValueError, AttributeError):
        return None
    if isinstance(workspace, dict) and workspace.get("snapshot_ref"):
        return str(workspace["snapshot_ref"])
    return None


__all__ = [
    "BRANCH_SNAPSHOT_PREFIX",
    "CURSOR_CLOUD_POLICY",
    "EXPIRED_POLL_INTERVAL_S",
    "EXPIRED_POLL_LIMIT",
    "PROFILE",
    "STREAM_START",
    "CursorCloudHarness",
    "DispatchKeyParts",
    "branch_snapshot",
    "capacity_signal",
    "client_agent_id",
    "last_event_id",
    "parse_branch_snapshot",
    "parse_dispatch_key",
    "run_branch",
    "sse_provider_key",
    "stream_cursor",
]
