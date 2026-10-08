"""The `cursor_cloud` Lane Profile: Cursor Cloud Agents API v1 (SPEC-07 section 6; FT-G5).

One Session Turn of a bound operation, driven by `lane.turn` (FT-G2):

- `prepare` pushes branch `mc/<run_id>` from the binding's base ref carrying the Host
  Projection (rules, agents, command hooks, MCP; cloud agents read project config from the
  repository) and the Context Packet files, and records the branch head;
- `start` creates the agent with a client-supplied `agentId` (`409 agent_id_conflict` means it
  already exists: reattach) or, when the binding needs `envVars`, with an `Idempotency-Key`;
  `metadata` that the account cannot use (`403 feature_unavailable`) is recorded, not fatal.
  The agent works on the pre-created branch (`startingRef` plus `workOnCurrentBranch`). The
  create request enqueues the first run, so turn 1 is that run;
- `send_turn` for later turns returns `busy` on `409 agent_busy` (`wait_then_send`);
- `observe` reads the run's SSE stream with `Last-Event-ID`: frames are keyed by the event id,
  the id-less leading `status` is deduplicated by content, heartbeats are not persisted, the
  retention header is recorded, and after `410 stream_expired` the terminal frame is
  synthesized from `GET .../runs/{runId}`;
- closing facts keep `git.branches[]` only when the agent's `latestRunId` is this run;
- `usage` settles tokens from `/usage?runId=` and cost only when a cost reader reports it;
- `end_session` fetches the branch diff from the SCM as the patch, downloads artifacts through
  their presigned URLs and registers them with digests, then archives the agent.

Kernel Hooks cannot call the worker's loopback listener from the cloud VM, so the cloud
projection carries catalog command hooks only; an immediate cancel reaches the agent through
`POST .../cancel`. Qualification stays false until FT-G6 records a real run.
"""

from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final
from uuid import UUID, uuid5

from mission_control.adapters.cursor.bridge import (
    AgentBusy,
    FeatureUnavailable,
    RunNotCancellable,
    RunNotFound,
)
from mission_control.adapters.cursor.cloud_api import (
    AgentIdConflict,
    AgentNotFound,
    CloudAgentsClient,
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
    turn_text,
    verify_projection,
)
from mission_control.adapters.cursor.scm import GitBranchPublisher, PublishedBranch
from mission_control.application.execution.harness.describe import CURSOR_CLOUD_DESCRIBE
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.execution.harness.protocol import NativeTurnLost
from mission_control.application.frames.kinds import bounded_key, cursor_cloud_final_key
from mission_control.domain.agentic_components.projection import HostProjection
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.context.render import bytes_digest
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

CloudEnvResolver = Callable[[str], Awaitable[Mapping[str, str]]]
CloudCostReader = Callable[[str, str], Awaitable[int | None]]


def client_agent_id(harness_execution_id: str, generation: int) -> str:
    """The deterministic `bc-<uuid>` a create sends, so a lost response never duplicates."""

    return f"bc-{uuid5(_AGENT_NAMESPACE, f'{harness_execution_id}:{generation}')}"


def run_branch(binding: CursorExecutionBinding, run_id: str) -> str:
    return binding.workspace.branch_prefix + _REF_CHARS.sub("-", run_id)


def sse_provider_key(event_id: str) -> str:
    return bounded_key("sse", event_id)


def _content_key(kind: str, run_id: str, data: str) -> str:
    return bounded_key("sse-content", kind, run_id, sha256_digest(data))


def _status(value: Any) -> str:
    return str(value or "").lower()


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
    ) -> None:
        if describe.lane_profile != PROFILE:
            raise ValueError("the Cursor cloud harness describes the cursor_cloud profile")
        self._client = client
        self._publisher = publisher
        self._projections = projections
        self._artifacts = artifacts
        self._inputs = inputs
        self._env_resolver = env_resolver
        self._cost_reader = cost_reader
        self._archive = archive_on_end
        self._describe = describe
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
        branch = await self._publisher.publish(
            repository=self._repository(binding),
            base_ref=binding.workspace.base_ref,
            branch=run_branch(binding, request.run_id),
            files=files,
        )
        session.projection = projection
        session.branch = branch
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

    # --- start, reattach, send ------------------------------------------------------------------

    def _handle(self, request: HarnessRequest, session: _CloudSession) -> SessionHandle:
        assert session.agent_id is not None
        details = {"cloud_branch": "", "cloud_agent_url": session.agent_url or ""}
        if session.branch is not None:
            details["cloud_branch"] = f"{session.branch.branch}@{session.branch.head}"
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
            return self._handle(request, session)
        agent = created.get("agent") or {}
        run = created.get("run") or {}
        session.agent_id = str(agent["id"])
        session.initial_run_id = str(run["id"]) if run.get("id") else agent.get("latestRunId")
        session.agent_url = agent.get("url")
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
        return self._handle(request, session)

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        session = self._session(request)
        agent_id = request.session.native_session_ref or session.agent_id
        if agent_id is None:
            raise ValueError("send_turn requires a created cloud agent")
        if request.turn_no == 1:
            # The create request enqueued the first run.
            run_id = session.initial_run_id
            if run_id is None:
                agent = await self._client.get_agent(agent_id)
                run_id = agent.get("latestRunId")
            if run_id is None:
                raise NativeTurnLost("the cloud agent reports no first run")
            return TurnHandle(
                session=request.session, turn_no=request.turn_no, native_turn_ref=str(run_id)
            )
        try:
            run = await self._client.create_run(
                agent_id, {"prompt": {"text": turn_text(session.operation)}}
            )
        except AgentBusy:
            return TurnHandle(session=request.session, turn_no=request.turn_no, status="busy")
        return TurnHandle(
            session=request.session, turn_no=request.turn_no, native_turn_ref=str(run["id"])
        )

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
        after = None if request.after in (None, STREAM_START) else request.after
        cursor = request.after or STREAM_START
        try:
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
                    cursor = item.id
                    key = sse_provider_key(item.id)
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
        except StreamExpired:
            pass
        try:
            run = await self._client.get_run(agent_id, run_id)
        except (AgentNotFound, RunNotFound) as error:
            raise NativeTurnLost("the cloud run no longer exists") from error
        if _status(run.get("status")) in _TERMINAL:
            body = dict(run)
            body["synthesized_from"] = "run_record"
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

    def closing_facts(self, turn: TurnHandle, frame: LaneFrame) -> ClosingFacts:
        body = dict(frame.body) if isinstance(frame.body, dict) else {}
        if not body.get("git_attributed", False):
            body.pop("git", None)
        return body_closing_facts(body)

    def resume_cursor(self, provider_key: str) -> str | None:
        return provider_key.removeprefix("sse:") if provider_key.startswith("sse:") else None

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
        """Tokens settle from `GET /v1/agents/{id}/usage?runId=`; cost settles only when the
        cost reader (the SDK's `get_usage().cost`) reports a non-null value."""

        session = self._session(request)
        agent_id = request.session.native_session_ref or session.agent_id
        run_id = request.turn.native_turn_ref if request.turn is not None else None
        if agent_id is None:
            return UsageReport(disposition="unknown")
        usage = await self._client.usage(agent_id, run_id)
        tokens: Mapping[str, Any] = usage.get("totalUsage") or {}
        for entry in usage.get("runs") or ():
            if isinstance(entry, Mapping) and entry.get("id") == run_id:
                tokens = entry.get("usage") or tokens
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

    async def snapshot(self, request: SnapshotRequest) -> SnapshotManifest:
        session = self._session(request)
        if session.branch is None:
            raise ValueError("no published branch to snapshot")
        refs = [f"branch:{session.branch.branch}@{session.branch.head}"]
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
            ref = await self._artifacts.stage(
                request_scope=scope,
                name=f"{name}/{path}",
                content=content,
                media_type="application/octet-stream",
            )
            refs.append(ref)
            registered.append(
                {"path": path, "digest": bytes_digest(content), "size": len(content), "ref": ref}
            )
        manifest = json.dumps(
            {
                "branch": session.branch.branch,
                "base_commit": session.branch.base_commit,
                "head": head,
                "artifacts": registered,
                "metadata": session.metadata,
                "stream_retention_seconds": session.retention_seconds,
            },
            sort_keys=True,
        ).encode("utf-8")
        manifest_ref = await self._artifacts.stage(
            request_scope=scope,
            name=f"{name}/session-manifest.json",
            content=manifest,
            media_type="application/json",
        )
        # Archived only after the patch and every artifact are registered; never deleted here.
        if self._archive:
            await self._client.archive(agent_id)
        self._sessions.pop(request.harness_execution_id, None)
        return CleanupReceipt(
            released=True, artifact_refs=(patch_ref, *refs, manifest_ref), patch_ref=patch_ref
        )


__all__ = [
    "PROFILE",
    "STREAM_START",
    "CursorCloudHarness",
    "client_agent_id",
    "run_branch",
    "sse_provider_key",
]
