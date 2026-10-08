"""Compaction observation and continuation hydration on the Deep Agents lane (SPEC-02, B4).

Deep Agents compacts silently: its ``SummarizationMiddleware`` records a private
``_summarization_event {cutoff_index, summary_message, file_path}`` and offloads the evicted
history to ``/conversation_history/<thread>.md`` with no callback (research
deepagents-middleware section 2.3). :class:`ObservedSummarizationMiddleware` replaces the
stock middleware in place (same ``.name``) and makes compaction visible:

- ``before_compaction`` (cutoff index, pre-summary token estimate, evicted message count)
  is emitted when the summary is about to be generated;
- ``after_compaction`` (summary message digest, ``file_path``, post-summary token estimate)
  is emitted when a new ``_summarization_event`` appears; the ``compact_conversation`` tool
  path (``SummarizationToolMiddleware``) is caught in ``wrap_tool_call`` the same way.

Both are custom stream events (``mc.before_compaction`` / ``mc.after_compaction``) that the
C1 :class:`~mission_control.adapters.deep_agents.frames.DeepAgentFrameRecorder` persists as
Provider Frames through the FrameSink, so the reducer derives
``session.compaction_observed`` from the closing ``after_compaction`` frame. Each
observation is also kept on the middleware so the adapter can archive the offloaded history
as a ``conversation_history`` artifact (:func:`archive_conversation_history`): evidence for
the archive, never Loop State.

:class:`DeepAgentsSessionHydrator` is the Deep Agents
:class:`~mission_control.application.context.continuation.SessionHydrator`: it starts a new
thread seeded with the checkpoint packet (the ``admitted_input`` prompt with the checkpoint
fields inline as the first message, the restored ``/inputs/**`` and ``/outputs/**`` files and
the packet's ``.mission/`` files) through ``aupdate_state(..., as_node="__start__")`` on the
new ``thread_id``; the next ``ainvoke(None, config)`` is the fresh session's first turn.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from deepagents.backends import StateBackend
from deepagents.backends.protocol import BackendProtocol
from deepagents.middleware.summarization import (
    SummarizationMiddleware,
    create_summarization_middleware,
)
from langchain.agents.middleware.types import ExtendedModelResponse
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AnyMessage, HumanMessage
from langgraph.types import Command

from mission_control.adapters.deep_agents.hooks import (
    HookDispatcher,
    MissionSummarizationMiddleware,
)
from mission_control.application.context.continuation import (
    HydrationReceipt,
    HydrationRequest,
    WorkspaceSnapshot,
)
from mission_control.application.context.pack_service import MissionFileStagingPort
from mission_control.domain.capabilities.hooks import HookEvent

logger = logging.getLogger(__name__)

BEFORE_COMPACTION_EVENT = "mc.before_compaction"
AFTER_COMPACTION_EVENT = "mc.after_compaction"
COMPACT_TOOL_NAME = "compact_conversation"
CONVERSATION_HISTORY_KIND = "conversation_history"
CONTINUATION_MESSAGE_SOURCE = "mc_continuation"
SNAPSHOT_SCHEMA_VERSION = "mc.continuation_workspace_snapshot.v1"


def _digest_bytes(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _message_text(message: Any) -> str:
    content = getattr(message, "content", message)
    if isinstance(content, str):
        return content
    return json.dumps(content, sort_keys=True, default=str)


@dataclass(frozen=True, slots=True)
class CompactionObservation:
    """One observed compaction (the archive input; frames carry the same facts)."""

    session_ref: str
    cutoff_index: int | None
    summary_digest: str
    file_path: str | None
    pre_tokens: int | None
    post_tokens: int | None
    source: str
    """``summarization_middleware`` or ``compact_conversation``."""


@dataclass(slots=True)
class _Pending:
    writer: Callable[[Any], None] | None
    session_ref: str
    pre_tokens: int | None
    effective_cutoff: int | None = None
    emitted_before: bool = False


_CURRENT: contextvars.ContextVar[_Pending | None] = contextvars.ContextVar(
    "mc_compaction_pending", default=None
)


def _writer_of(request: Any) -> Callable[[Any], None] | None:
    runtime = getattr(request, "runtime", None)
    writer = getattr(runtime, "stream_writer", None)
    return writer if callable(writer) else None


def _emit(writer: Callable[[Any], None] | None, payload: Mapping[str, Any]) -> None:
    if writer is None:
        return
    try:
        writer(dict(payload))
    except Exception:
        logger.warning("compaction frame could not be written to the stream", exc_info=True)


class ObservedSummarizationMiddleware(MissionSummarizationMiddleware):
    """Deep Agents summarization that emits ``before_compaction`` / ``after_compaction``.

    Keeps ``.name == "SummarizationMiddleware"`` so ``create_deep_agent`` replaces its
    default summarizer in place; Hook Scripts subscribed to the compaction events still run
    through the optional :class:`HookDispatcher` (FT-A5).
    """

    dispatcher: HookDispatcher | None  # type: ignore[assignment]
    observations: list[CompactionObservation]

    @classmethod
    def observed(
        cls,
        model: BaseChatModel,
        backend: BackendProtocol,
        dispatcher: HookDispatcher | None = None,
        *,
        trigger: Any = None,
        keep: Any = None,
    ) -> ObservedSummarizationMiddleware:
        """Adopt the model-aware defaults, optionally overriding ``trigger``/``keep``.

        ``trigger=("messages", 3)`` forces summarization in demos and tests.
        """

        if trigger is None and keep is None:
            base: SummarizationMiddleware = create_summarization_middleware(model, backend)
        else:
            defaults = create_summarization_middleware(model, backend)
            base = SummarizationMiddleware(
                model,
                backend=backend,
                trigger=trigger if trigger is not None else defaults._lc_helper.trigger,
                keep=keep if keep is not None else defaults._lc_helper.keep,
            )
        instance = cls.__new__(cls)
        instance.__dict__.update(base.__dict__)
        instance.dispatcher = dispatcher
        instance.observations = []
        return instance

    # -- detection -----------------------------------------------------------------------

    def _determine_cutoff_index(self, messages: list[AnyMessage]) -> int:
        cutoff = SummarizationMiddleware._determine_cutoff_index(self, messages)
        pending = _CURRENT.get()
        if pending is not None:
            pending.effective_cutoff = cutoff
        return cutoff

    def _before(self, messages_to_summarize: list[AnyMessage]) -> dict[str, Any] | None:
        pending = _CURRENT.get()
        if pending is None or pending.emitted_before:
            return None
        pending.emitted_before = True
        details = {
            "cutoff_index": pending.effective_cutoff,
            "message_count": len(messages_to_summarize),
            "pre_estimate_tokens": pending.pre_tokens,
        }
        _emit(
            pending.writer,
            {
                "type": BEFORE_COMPACTION_EVENT,
                "id": f"{pending.session_ref}:before:{pending.effective_cutoff}:"
                f"{len(messages_to_summarize)}",
                "session_ref": pending.session_ref,
                **details,
            },
        )
        return details

    def _create_summary(self, messages_to_summarize: list[AnyMessage]) -> str:
        details = self._before(messages_to_summarize)
        if self.dispatcher is not None and details is not None:
            self.dispatcher.dispatch_sync(HookEvent.BEFORE_COMPACTION, details=details)
        return SummarizationMiddleware._create_summary(self, messages_to_summarize)

    async def _acreate_summary(self, messages_to_summarize: list[AnyMessage]) -> str:
        details = self._before(messages_to_summarize)
        if self.dispatcher is not None and details is not None:
            await self.dispatcher.dispatch(HookEvent.BEFORE_COMPACTION, details=details)
        return await SummarizationMiddleware._acreate_summary(self, messages_to_summarize)

    def _pending_for(self, request: Any) -> _Pending:
        try:
            pre = self._count_tokens(
                self._get_effective_messages(request),
                getattr(request, "system_message", None),
                getattr(request, "tools", None),
            )
        except Exception:
            pre = None
        return _Pending(
            writer=_writer_of(request), session_ref=self._get_thread_id(), pre_tokens=pre
        )

    def _after(
        self, request: Any, event: Mapping[str, Any], pending: _Pending, source: str
    ) -> dict[str, Any]:
        summary = event.get("summary_message")
        summary_digest = _digest_bytes(_message_text(summary).encode("utf-8"))
        try:
            effective = self._apply_event_to_messages(list(request.messages), event)  # type: ignore[arg-type]
            post = self._count_tokens(
                effective,
                getattr(request, "system_message", None),
                getattr(request, "tools", None),
            )
        except Exception:
            post = None
        cutoff = event.get("cutoff_index")
        file_path = event.get("file_path")
        observation = CompactionObservation(
            session_ref=pending.session_ref,
            cutoff_index=cutoff if isinstance(cutoff, int) else None,
            summary_digest=summary_digest,
            file_path=file_path if isinstance(file_path, str) else None,
            pre_tokens=pending.pre_tokens,
            post_tokens=post,
            source=source,
        )
        self.observations.append(observation)
        details = {
            "cutoff_index": observation.cutoff_index,
            "summary_digest": summary_digest,
            "file_path": observation.file_path,
            "pre_estimate_tokens": observation.pre_tokens,
            "post_estimate_tokens": post,
            "source": source,
        }
        _emit(
            pending.writer,
            {
                "type": AFTER_COMPACTION_EVENT,
                "id": f"{pending.session_ref}:after:{observation.cutoff_index}:{summary_digest}",
                "session_ref": pending.session_ref,
                **details,
            },
        )
        return details

    # -- model calls ---------------------------------------------------------------------

    def wrap_model_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        previous = request.state.get("_summarization_event")
        pending = self._pending_for(request)
        token = _CURRENT.set(pending)
        try:
            response = SummarizationMiddleware.wrap_model_call(self, request, handler)
        finally:
            _CURRENT.reset(token)
        event = _new_event(response, previous)
        if event is not None:
            details = self._after(request, event, pending, "summarization_middleware")
            if self.dispatcher is not None:
                self.dispatcher.dispatch_sync(HookEvent.AFTER_COMPACTION, details=details)
        return response

    async def awrap_model_call(self, request: Any, handler: Callable[[Any], Awaitable[Any]]) -> Any:
        previous = request.state.get("_summarization_event")
        pending = self._pending_for(request)
        token = _CURRENT.set(pending)
        try:
            response = await SummarizationMiddleware.awrap_model_call(self, request, handler)
        finally:
            _CURRENT.reset(token)
        event = _new_event(response, previous)
        if event is not None:
            details = self._after(request, event, pending, "summarization_middleware")
            if self.dispatcher is not None:
                await self.dispatcher.dispatch(HookEvent.AFTER_COMPACTION, details=details)
        return response

    # -- the compact_conversation tool (SummarizationToolMiddleware) -----------------------

    def wrap_tool_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        if _tool_name(request) != COMPACT_TOOL_NAME:
            return handler(request)
        pending = _Pending(
            writer=_tool_writer(request), session_ref=self._get_thread_id(), pre_tokens=None
        )
        self._tool_before(pending)
        result = handler(request)
        self._tool_after(request, result, pending)
        return result

    async def awrap_tool_call(self, request: Any, handler: Callable[[Any], Awaitable[Any]]) -> Any:
        if _tool_name(request) != COMPACT_TOOL_NAME:
            return await handler(request)
        pending = _Pending(
            writer=_tool_writer(request), session_ref=self._get_thread_id(), pre_tokens=None
        )
        self._tool_before(pending)
        result = await handler(request)
        self._tool_after(request, result, pending)
        return result

    def _tool_before(self, pending: _Pending) -> None:
        pending.emitted_before = True
        _emit(
            pending.writer,
            {
                "type": BEFORE_COMPACTION_EVENT,
                "id": f"{pending.session_ref}:before:tool",
                "session_ref": pending.session_ref,
                "source": COMPACT_TOOL_NAME,
            },
        )

    def _tool_after(self, request: Any, result: Any, pending: _Pending) -> None:
        if not isinstance(result, Command) or not isinstance(result.update, Mapping):
            return
        event = result.update.get("_summarization_event")
        if not isinstance(event, Mapping):
            return

        state = getattr(request, "state", None) or {}
        messages = list(state.get("messages", ())) if isinstance(state, Mapping) else []
        view = _RequestView(messages=messages)
        self._after(view, event, pending, COMPACT_TOOL_NAME)


@dataclass(frozen=True, slots=True)
class _RequestView:
    """The model-request fields a token estimate reads, for the tool path."""

    messages: list[Any]
    system_message: Any = None
    tools: Any = None


def _tool_name(request: Any) -> str | None:
    call = getattr(request, "tool_call", None)
    return call.get("name") if isinstance(call, Mapping) else None


def _tool_writer(request: Any) -> Callable[[Any], None] | None:
    runtime = getattr(request, "runtime", None)
    writer = getattr(runtime, "stream_writer", None)
    return writer if callable(writer) else None


def _new_event(response: Any, previous: Any) -> Mapping[str, Any] | None:
    if not isinstance(response, ExtendedModelResponse) or response.command is None:
        return None
    update = response.command.update
    if not isinstance(update, Mapping):
        return None
    event = update.get("_summarization_event")
    if not isinstance(event, Mapping) or event == previous:
        return None
    return event


# --------------------------------------------------------------------------------------
# Conversation history archive (artifact kind conversation_history)
# --------------------------------------------------------------------------------------


class ConversationHistoryRegistrar(Protocol):
    async def register_conversation_history(
        self,
        *,
        session_ref: str,
        file_path: str,
        content: bytes,
        content_digest: str,
        summary_digest: str,
    ) -> str:
        """Register the offloaded history as an artifact of kind ``conversation_history``;
        return its artifact ref. Idempotent by content digest."""
        ...


@dataclass(frozen=True, slots=True)
class ArchivedHistory:
    session_ref: str
    file_path: str
    content_digest: str
    artifact_ref: str
    kind: str = CONVERSATION_HISTORY_KIND


async def archive_conversation_history(
    observations: Sequence[CompactionObservation],
    registrar: ConversationHistoryRegistrar,
    *,
    backend: BackendProtocol | None = None,
    state_files: Mapping[str, Any] | None = None,
) -> tuple[ArchivedHistory, ...]:
    """Pull each offloaded ``/conversation_history/<session>.md`` and register it.

    A sandbox or filesystem backend is read with ``adownload_files``; ``StateBackend``
    keeps files in graph state, so the caller passes the final state's ``files`` instead.
    The newest observation per file wins (the file is append-only per session).
    """

    latest: dict[str, CompactionObservation] = {}
    for observation in observations:
        if observation.file_path:
            latest[observation.file_path] = observation
    archived: list[ArchivedHistory] = []
    for path, observation in sorted(latest.items()):
        content = await _history_bytes(path, backend=backend, state_files=state_files)
        if content is None:
            logger.warning("offloaded conversation history %s is not readable", path)
            continue
        digest = _digest_bytes(content)
        ref = await registrar.register_conversation_history(
            session_ref=observation.session_ref,
            file_path=path,
            content=content,
            content_digest=digest,
            summary_digest=observation.summary_digest,
        )
        archived.append(
            ArchivedHistory(
                session_ref=observation.session_ref,
                file_path=path,
                content_digest=digest,
                artifact_ref=ref,
            )
        )
    return tuple(archived)


async def _history_bytes(
    path: str,
    *,
    backend: BackendProtocol | None,
    state_files: Mapping[str, Any] | None,
) -> bytes | None:
    if state_files is not None and path in state_files:
        return _file_data_bytes(state_files[path])
    if backend is None or isinstance(backend, StateBackend):
        return None
    responses = await backend.adownload_files([path])
    for response in responses:
        if response.error is None and response.content is not None:
            return bytes(response.content)
    return None


def _file_data_bytes(file_data: Any) -> bytes:
    from deepagents.backends.utils import file_data_to_string

    if isinstance(file_data, Mapping):
        text = file_data_to_string(dict(file_data))  # type: ignore[arg-type]
        if file_data.get("encoding", "utf-8") != "utf-8":
            import base64

            return base64.standard_b64decode(text)
        return text.encode("utf-8")
    return str(file_data).encode("utf-8")


# --------------------------------------------------------------------------------------
# Hydration: a fresh thread seeded from the checkpoint packet
# --------------------------------------------------------------------------------------


class SnapshotBytes(Protocol):
    async def retrieve(self, durable_ref: str) -> bytes: ...


class HydratableGraph(Protocol):
    async def aupdate_state(
        self, config: Any, values: Any, as_node: str | None = None, task_id: str | None = None
    ) -> Any: ...

    async def aget_state(self, config: Any, *, subgraphs: bool = False) -> Any: ...


@dataclass
class DeepAgentsSessionHydrator:
    """Start the fresh Deep Agents session of a continuation on a new ``thread_id``.

    ``graph`` is the compiled agent with a checkpointer; ``backend`` its file backend.
    The new thread is ``<source thread>~continuation~<checkpoint digest prefix>`` so a
    replayed hydration addresses the same thread. Files are verified against the snapshot
    digests before seeding; the restored digests reported back are read from the new
    thread's state (``StateBackend``) or the backend (sandbox), never assumed.
    """

    graph: HydratableGraph
    backend: BackendProtocol
    bytes_source: SnapshotBytes
    seeded_threads: list[str] = field(default_factory=list)

    @staticmethod
    def target_thread(source_session_ref: str, checkpoint_digest: str) -> str:
        return f"{source_session_ref}~continuation~{checkpoint_digest.removeprefix('sha256:')[:16]}"

    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt:
        checkpoint = request.checkpoint
        thread = self.target_thread(request.source_session_ref, checkpoint.checkpoint_digest)
        files: list[tuple[str, bytes]] = []
        for path, durable_ref in sorted(request.snapshot.durable_refs.items()):
            content = await self.bytes_source.retrieve(durable_ref)
            expected = request.snapshot.manifest.get(path)
            if expected is not None and _digest_bytes(content) != expected:
                # The continuity check reports the mismatch with its typed reason.
                logger.warning("snapshot bytes for %s differ from the manifest digest", path)
            files.append((path, content))
        for name, text in sorted(request.mission_files.items()):
            files.append((f"/{name}" if not name.startswith("/") else name, text.encode("utf-8")))
        # Imported here: the materializer composes this module's middleware.
        from mission_control.adapters.deep_agents.materializer import seed_backend_files

        state_files = await seed_backend_files(self.backend, files)
        config = {"configurable": {"thread_id": thread}}
        values: dict[str, Any] = {
            "messages": [
                HumanMessage(
                    content=request.prompt_text,
                    additional_kwargs={
                        "lc_source": CONTINUATION_MESSAGE_SOURCE,
                        "checkpoint_id": checkpoint.checkpoint_id,
                        "context_packet_ref": checkpoint.context_packet_ref,
                    },
                )
            ]
        }
        if state_files:
            values["files"] = state_files
        await self.graph.aupdate_state(config, values, as_node="__start__")
        self.seeded_threads.append(thread)
        restored = await self._restored(config, [path for path, _ in files])
        return HydrationReceipt(
            target_session_ref=thread,
            restored=restored,
            native_identity={
                "thread_id": thread,
                "source_thread_id": request.source_session_ref,
                "checkpoint_id": checkpoint.checkpoint_id,
            },
        )

    async def _restored(self, config: Mapping[str, Any], paths: Sequence[str]) -> dict[str, str]:
        if isinstance(self.backend, StateBackend):
            snapshot = await self.graph.aget_state(config)
            values = getattr(snapshot, "values", {}) or {}
            files = values.get("files", {}) if isinstance(values, Mapping) else {}
            return {
                path: _digest_bytes(_file_data_bytes(files[path]))
                for path in paths
                if path in files
            }
        responses = await self.backend.adownload_files(list(paths))
        return {
            response.path: _digest_bytes(bytes(response.content))
            for response in responses
            if response.error is None and response.content is not None
        }


@dataclass
class StateBackendWorkspaceSnapshots:
    """A :class:`WorkspaceSnapshotPort` over a Deep Agents thread's ``StateBackend`` files.

    ``thread_values`` reads the source thread's state values. Every file and the snapshot
    manifest itself are staged content-addressed through ``staging``; the snapshot
    reference is the manifest's durable ref, so :meth:`load` re-reads and digest-verifies
    it from storage (nothing is kept in process memory).
    """

    thread_values: Callable[[str], Awaitable[Mapping[str, Any]]]
    staging: MissionFileStagingPort
    bytes_source: SnapshotBytes

    async def snapshot(
        self, *, request_scope: str, run_key: str, session_ref: str, roots: Sequence[str]
    ) -> WorkspaceSnapshot:
        values = await self.thread_values(session_ref)
        files = values.get("files", {}) if isinstance(values, Mapping) else {}
        manifest: dict[str, str] = {}
        durable: dict[str, str] = {}
        total = 0
        for path in sorted(files):
            if roots and not any(path == root or path.startswith(root + "/") for root in roots):
                continue
            content = _file_data_bytes(files[path])
            digest = _digest_bytes(content)
            manifest[path] = digest
            durable[path] = await self.staging.stage(
                request_scope=request_scope,
                name=f"continuation/files/{digest.removeprefix('sha256:')}",
                content=content,
                media_type="application/octet-stream",
            )
            total += len(content)
        document = json.dumps(
            {
                "schema_version": SNAPSHOT_SCHEMA_VERSION,
                "manifest": manifest,
                "durable_refs": durable,
                "total_bytes": total,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        ref = await self.staging.stage(
            request_scope=request_scope,
            name=f"continuation/snapshots/{_digest_bytes(document).removeprefix('sha256:')}.json",
            content=document,
            media_type="application/json",
        )
        return WorkspaceSnapshot(
            snapshot_ref=ref, manifest=manifest, durable_refs=durable, total_bytes=total
        )

    async def load(self, *, request_scope: str, snapshot_ref: str) -> WorkspaceSnapshot | None:
        try:
            document = json.loads(await self.bytes_source.retrieve(snapshot_ref))
        except (LookupError, ValueError):
            return None
        if document.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
            return None
        return WorkspaceSnapshot(
            snapshot_ref=snapshot_ref,
            manifest=document["manifest"],
            durable_refs=document["durable_refs"],
            total_bytes=document["total_bytes"],
        )
