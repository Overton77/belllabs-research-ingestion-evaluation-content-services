"""The Deep Agents lane behind the AgentHarness protocol (FT-G1).

`DeepAgentsHarness` wraps the existing runtime adapter (the `RuntimePort` family:
`DeploymentOperationRuntime(DeepAgentRuntimeAdapter(...))`) without changing its behavior:
`OperationExecutionService` still calls `execute` for a bound operation attempt, with the same
invocation, secrets and cancellation semantics, and the lane decides that a `deep_agent`
attempt needs checkpoint lineage composition (the branch lifted out of the service).

The protocol operations run the same adapter in process, one Session Turn per `execute`:
`prepare` takes the invocation the operation boundary staged for the harness execution,
`send_turn` starts cognition as a task, `observe` yields its closing frame, `cancel_turn`
cancels the task (the adapter sees `CancelledError`, as with an activity cancel), `reattach`
reads the latest durable checkpoint through `observe_latest`, `snapshot` is emulated from the
captured checkpoint, and `usage` reports the settled per-turn amounts. FT-G2's `lane.turn`
activity drives these operations; streamed per-step frames arrive with FT-C1's frame writer.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Protocol

from mission_control.application.execution.harness.describe import DEEP_AGENTS_DESCRIBE
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.authoring.contracts import SecretRef
from mission_control.domain.execution.checkpoint_lineage import CheckpointLineageInDoubt
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    RuntimeInvocation,
    RuntimeResult,
)
from mission_control.domain.execution.lanes import (
    CancelReceipt,
    CancelTurnRequest,
    CleanupReceipt,
    EndSessionRequest,
    LaneDescribe,
    LaneFrame,
    ObserveRequest,
    PreparedSession,
    PrepareRequest,
    ReattachRequest,
    SendTurnRequest,
    SessionHandle,
    SnapshotManifest,
    SnapshotRequest,
    StartRequest,
    TurnHandle,
    UsageReport,
    UsageRequest,
)

TERMINAL_CURSOR = "terminal"


class DeepAgentRuntime(Protocol):
    """The adapter surface the lane wraps (`RuntimePort`, optionally `observe_latest`)."""

    async def execute(
        self, invocation: RuntimeInvocation, resolved_secrets: Mapping[str, str]
    ) -> RuntimeResult: ...


class HarnessSecrets(Protocol):
    async def resolve(self, refs: tuple[SecretRef, ...]) -> Mapping[str, str]: ...


class HarnessSessionUnknown(LookupError):
    """The harness execution has no staged invocation or session in this process."""


@dataclass
class _Session:
    invocation: RuntimeInvocation
    generation: int
    turn: asyncio.Task[RuntimeResult] | None = None
    turn_no: int = 0
    result: RuntimeResult | None = None
    frames: list[LaneFrame] = field(default_factory=list)


def _thread_ref(invocation: RuntimeInvocation) -> str:
    plan = invocation.checkpoint_plan
    if plan is not None:
        return plan.namespace
    return f"binding:{invocation.binding.binding_id}"


class DeepAgentsHarness:
    """`deep_agents` lane profile; also an `OperationLane` for the bounded execute path."""

    def __init__(self, runtime: DeepAgentRuntime, secrets: HarnessSecrets | None = None) -> None:
        self._runtime = runtime
        self._secrets = secrets
        self._sessions: dict[str, _Session] = {}

    @property
    def runtime(self) -> DeepAgentRuntime:
        return self._runtime

    def describe(self) -> LaneDescribe:
        return DEEP_AGENTS_DESCRIBE

    # --- OperationLane: the existing bounded path, unchanged ----------------------------------

    def requires_checkpoint_lineage(self, request: OperationExecutionRequest) -> bool:
        # REQ-CP-DA-016: Deep Agent cognition is lineage-qualified; native placements are not.
        return request.execution_runtime == "deep_agent"

    async def execute(
        self, invocation: RuntimeInvocation, resolved_secrets: Mapping[str, str]
    ) -> RuntimeResult:
        return await self._runtime.execute(invocation, resolved_secrets)

    async def observe_latest(
        self, invocation: RuntimeInvocation, resolved_secrets: Mapping[str, str]
    ) -> RuntimeResult:
        observe = getattr(self._runtime, "observe_latest", None)
        if observe is None:
            # The same classification the operation boundary applied before FT-G1.
            raise CheckpointLineageInDoubt(
                "the runtime cannot report the latest durable checkpoint of a cancelled unit",
                reason="unclassifiable",
            )
        result: RuntimeResult = await observe(invocation, resolved_secrets)
        return result

    # --- AgentHarness ----------------------------------------------------------------------

    def stage(
        self, harness_execution_id: str, generation: int, invocation: RuntimeInvocation
    ) -> None:
        """The operation boundary hands the lane the bound invocation `prepare` materializes."""

        current = self._sessions.get(harness_execution_id)
        if current is not None and current.invocation != invocation:
            raise ValueError("harness execution is already staged with another invocation")
        if current is None:
            self._sessions[harness_execution_id] = _Session(invocation, generation)

    def _session(self, harness_execution_id: str) -> _Session:
        try:
            return self._sessions[harness_execution_id]
        except KeyError:
            raise HarnessSessionUnknown(harness_execution_id) from None

    async def prepare(self, request: PrepareRequest) -> PreparedSession:
        session = self._session(request.harness_execution_id)
        workspace = session.invocation.workspace
        return PreparedSession(
            lane_profile="deep_agents",
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            workspace_ref=f"workspace:{workspace.workspace_id}",
            projection_digests={"mount_manifest": workspace.mount_manifest_digest},
            materialization_ref=workspace.runtime_digest,
        )

    def _handle(self, harness_execution_id: str, generation: int) -> SessionHandle:
        session = self._session(harness_execution_id)
        return SessionHandle(
            lane_profile="deep_agents",
            harness_execution_id=harness_execution_id,
            generation=generation,
            native_session_ref=_thread_ref(session.invocation),
        )

    async def start(self, request: StartRequest) -> SessionHandle:
        return self._handle(request.harness_execution_id, request.generation)

    async def reattach(self, request: ReattachRequest) -> SessionHandle:
        """Native by checkpoint: the latest durable checkpoint of the thread is the state."""

        session = self._session(request.harness_execution_id)
        if request.native_session_ref != _thread_ref(session.invocation):
            raise ValueError("reattach names another thread than the staged invocation")
        if session.turn is None and session.result is None:
            session.result = await self.observe_latest(
                session.invocation, await self._resolve(session.invocation)
            )
        return self._handle(request.harness_execution_id, request.generation)

    async def _resolve(self, invocation: RuntimeInvocation) -> Mapping[str, str]:
        refs = invocation.binding.secret_refs
        if not refs:
            return {}
        if self._secrets is None:
            raise ValueError("the deep_agents harness has no secret resolution port")
        return await self._secrets.resolve(refs)

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        session = self._session(request.harness_execution_id)
        if session.turn is not None and not session.turn.done():
            return TurnHandle(
                session=request.session,
                turn_no=request.turn_no,
                native_turn_ref=f"turn:{session.turn_no}",
                status="busy",
            )
        if request.turn_no <= session.turn_no:
            # Idempotent re-send of an accepted turn.
            return TurnHandle(
                session=request.session,
                turn_no=request.turn_no,
                native_turn_ref=f"turn:{request.turn_no}",
            )
        secrets = await self._resolve(session.invocation)
        session.turn_no = request.turn_no
        session.result = None
        session.turn = asyncio.create_task(self._runtime.execute(session.invocation, secrets))
        return TurnHandle(
            session=request.session,
            turn_no=request.turn_no,
            native_turn_ref=f"turn:{request.turn_no}",
        )

    async def cancel_turn(self, request: CancelTurnRequest) -> CancelReceipt:
        session = self._session(request.harness_execution_id)
        task = session.turn
        if task is None or task.done():
            return CancelReceipt(acknowledged=True, already_terminal=True, native_status="idle")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return CancelReceipt(acknowledged=True, native_status="cancelled")
        except Exception:
            return CancelReceipt(acknowledged=True, already_terminal=True, native_status="error")
        return CancelReceipt(acknowledged=True, already_terminal=True, native_status="finished")

    async def observe(self, request: ObserveRequest) -> AsyncIterator[LaneFrame]:
        session = self._session(request.harness_execution_id)
        if request.after == TERMINAL_CURSOR:
            return
        if session.turn is not None:
            try:
                session.result = await asyncio.shield(session.turn)
                status, body = "finished", stable_json_dump(session.result)
            except asyncio.CancelledError:
                if not session.turn.cancelled():
                    raise
                status, body = "cancelled", {"status": "cancelled"}
            except Exception as error:
                status, body = "error", {"status": "error", "error_type": type(error).__name__}
        elif session.result is not None:
            status, body = "finished", stable_json_dump(session.result)
        else:
            return
        frame = LaneFrame(
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            provider_key=f"{_thread_ref(session.invocation)}:turn:{session.turn_no}:{status}",
            cursor=TERMINAL_CURSOR,
            kind="terminal",
            terminal=True,
            digest=sha256_digest(body),
            excerpt=status,
        )
        session.frames.append(frame)
        yield frame

    async def snapshot(self, request: SnapshotRequest) -> SnapshotManifest:
        """Emulated: the captured checkpoint plus the materialized workspace manifest."""

        session = self._session(request.harness_execution_id)
        result = session.result
        if result is None or result.checkpoint is None:
            raise ValueError("no captured checkpoint to snapshot yet")
        key = result.checkpoint.result_key
        refs = (
            f"checkpoint:{sha256_digest(stable_json_dump(key))}",
            f"workspace:{session.invocation.workspace.mount_manifest_digest}",
        )
        return SnapshotManifest(
            lane_profile="deep_agents",
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            kind="checkpoint",
            refs=refs,
            emulated=True,
            digest=sha256_digest(list(refs)),
        )

    async def usage(self, request: UsageRequest) -> UsageReport:
        session = self._session(request.harness_execution_id)
        if session.result is None:
            return UsageReport(disposition="unknown")
        amounts = session.result.usage.amounts
        return UsageReport(
            disposition="settled",
            input_tokens=amounts.get("tokens.input", 0),
            output_tokens=amounts.get("tokens.output", 0),
            total_tokens=amounts.get("tokens.total", 0),
        )

    async def end_session(self, request: EndSessionRequest) -> CleanupReceipt:
        session = self._sessions.pop(request.harness_execution_id, None)
        if session is None:
            return CleanupReceipt(released=False)
        if session.turn is not None and not session.turn.done():
            session.turn.cancel()
            with_result = await asyncio.gather(session.turn, return_exceptions=True)
            del with_result
        refs = session.result.output_refs if session.result is not None else ()
        return CleanupReceipt(released=True, artifact_refs=refs)


__all__ = ["TERMINAL_CURSOR", "DeepAgentsHarness", "HarnessSessionUnknown"]
