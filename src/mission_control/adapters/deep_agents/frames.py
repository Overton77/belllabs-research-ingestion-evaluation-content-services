"""Deep Agents frame writer: LangGraph `astream` v2 parts -> provider frames (SPEC-03, C1).

One `DeepAgentFrameRecorder` serves one invocation (one turn) of one harness execution.
It consumes `astream(..., stream_mode=["updates", "messages", "custom"], subgraphs=True,
version="v2")` parts and persists, in arrival order and before anything is derived:

- `session_init` and `turn_started` before the first model call;
- `message_delta` / `thinking_delta` coalesced from `messages`-mode chunks (bounded, never
  one row per token) and `message` for each completed AI message in `updates`;
- `tool_call_started` for each tool call an AI message requests and `tool_call_completed`
  / `tool_call_failed` for each `ToolMessage` (keyed by `tool_call_id`);
- `usage` (closing) for each AI message that reports provider token usage;
- `approval_requested` for a LangGraph interrupt, `after_compaction` for a
  `_summarization_event`, and the Mission Control custom events (`mc.before_compaction`,
  `mc.after_compaction`, `mc.hook_invoked`, `mc.hook_result`, `mc.approval_resolved`)
  that middleware writes through the stream writer;
- `turn_ended` and `run_result` when the invocation finishes, or `error` / `run_result
  {error}` when it fails, plus `tool_call_completed` backfill (`raw_kind =
  "checkpoint_backfill"`) for any tool result the stream did not deliver.

Subagent frames (namespace `tools:<task_id>`) carry `subordinate_ref` from
`metadata.lc_agent_name` (or the namespace). The dedupe key is
`thread_id:checkpoint_ns:checkpoint_id:step:<message_id|tool_call_id>:<kind>`; message
ids and tool call ids are the stable discriminators, so re-running the writer over the
same stream adds no rows.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, ToolMessage

from mission_control.application.frames.kinds import (
    UNKNOWN_KINDS,
    UnknownKindCounter,
    classify,
    deep_agents_key,
)
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.frames.body import jsonable
from mission_control.domain.frames.contracts import (
    AppendReceipt,
    FrameObservation,
    HarnessExecutionHandle,
    LaneProfile,
)

logger = logging.getLogger(__name__)

LANE = LaneProfile.DEEP_AGENTS
DELTA_FLUSH_BYTES = 2_048
_THINKING_BLOCKS = {"reasoning", "thinking"}


def _digest(value: Any) -> str:
    payload = json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"), default=str)
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content
    parts: list[str] = []
    for block in content or ():
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, Mapping) and block.get("type") not in _THINKING_BLOCKS:
            text = block.get("text")
            if isinstance(text, str):
                parts.append(text)
    return "".join(parts)


def _thinking(message: BaseMessage) -> str:
    parts: list[str] = []
    content = message.content
    if not isinstance(content, str):
        for block in content or ():
            if isinstance(block, Mapping) and block.get("type") in _THINKING_BLOCKS:
                for key in ("reasoning", "thinking", "text"):
                    value = block.get(key)
                    if isinstance(value, str):
                        parts.append(value)
                        break
    extra = message.additional_kwargs.get("reasoning_content")
    if isinstance(extra, str):
        parts.append(extra)
    return "".join(parts)


def _usage_counts(metadata: Mapping[str, Any] | None) -> dict[str, int]:
    if not metadata:
        return {}
    counts = {
        "input_tokens": int(metadata.get("input_tokens", 0) or 0),
        "output_tokens": int(metadata.get("output_tokens", 0) or 0),
        "total_tokens": int(metadata.get("total_tokens", 0) or 0),
    }
    details = metadata.get("input_token_details") or {}
    if isinstance(details, Mapping) and details.get("cache_read") is not None:
        counts["cached_input_tokens"] = int(details.get("cache_read") or 0)
    output_details = metadata.get("output_token_details") or {}
    if isinstance(output_details, Mapping) and output_details.get("reasoning") is not None:
        counts["reasoning_tokens"] = int(output_details.get("reasoning") or 0)
    return counts


def message_body(message: BaseMessage) -> dict[str, Any]:
    """A JSON body for one LangChain message (content, tool calls, status, usage)."""

    body: dict[str, Any] = {
        "type": message.type,
        "id": message.id,
        "name": message.name,
        "content": jsonable(message.content),
    }
    if isinstance(message, AIMessage):
        body["tool_calls"] = [
            {"id": call.get("id"), "name": call.get("name"), "args": jsonable(call.get("args"))}
            for call in message.tool_calls
        ]
        if message.usage_metadata:
            body["usage"] = _usage_counts(message.usage_metadata)
        metadata = message.response_metadata or {}
        for key in ("model_name", "model", "finish_reason", "stop_reason"):
            if key in metadata:
                body[key] = jsonable(metadata[key])
    if isinstance(message, ToolMessage):
        body["tool_call_id"] = message.tool_call_id
        body["status"] = message.status
    return body


class DeepAgentFrameRecorder:
    """Observes one Deep Agents invocation and appends its frames through a FrameWriter."""

    def __init__(
        self,
        writer: FrameWriter,
        *,
        thread_id: str,
        invocation_id: str,
        counter: UnknownKindCounter = UNKNOWN_KINDS,
        delta_flush_bytes: int = DELTA_FLUSH_BYTES,
    ) -> None:
        self._writer = writer
        self._thread = thread_id
        self._turn = invocation_id
        self._counter = counter
        self._flush_bytes = delta_flush_bytes
        self._agent_names: dict[str, str] = {}
        self._deltas: dict[tuple[str, str, str], list[Any]] = {}
        self._delta_counts: dict[tuple[str, str, str], int] = {}
        self._closed_tool_calls: set[str] = set()
        self._started_tool_calls: set[str] = set()
        self.unknown = 0

    @property
    def receipt(self) -> AppendReceipt:
        return self._writer.receipt

    @property
    def handle(self) -> HarnessExecutionHandle:
        return self._writer.handle

    # --- observation building ---------------------------------------------------------

    def _observation(
        self,
        raw_kind: str,
        *,
        item_id: str,
        body: Any,
        ns: Sequence[str] = (),
        step: int | None = None,
        checkpoint_id: str | None = None,
        tool_call_ref: str | None = None,
        key_kind: str | None = None,
    ) -> FrameObservation:
        classification = classify(LANE, raw_kind, body, counter=self._counter)
        if not classification.known:
            self.unknown += 1
        namespace = "|".join(ns)
        return FrameObservation(
            provider_key=deep_agents_key(
                thread_id=self._thread,
                checkpoint_ns=namespace,
                checkpoint_id=checkpoint_id,
                step=step,
                item_id=item_id,
                kind=key_kind or classification.kind.value,
            ),
            raw_kind=raw_kind,
            kind=classification.kind,
            body=body,
            native_turn_ref=self._turn,
            subordinate_ref=self._subordinate(ns),
            tool_call_ref=tool_call_ref,
        )

    def _subordinate(self, ns: Sequence[str]) -> str | None:
        if not ns or not str(ns[0]).startswith("tools:"):
            return None
        return self._agent_names.get(str(ns[0])) or str(ns[0])

    async def _write(self, observations: Iterable[FrameObservation]) -> None:
        items = list(observations)
        if items:
            await self._writer.write(items)

    # --- lifecycle ------------------------------------------------------------------------

    async def begin(self, *, body: Mapping[str, Any]) -> None:
        await self._write(
            [
                self._observation(
                    "graph.session_init", item_id="session", body={"thread_id": self._thread}
                ),
                self._observation(
                    "graph.invocation_started",
                    item_id=self._turn,
                    body={"thread_id": self._thread, "invocation_id": self._turn, **body},
                ),
            ]
        )

    async def observe(self, part: Mapping[str, Any]) -> None:
        kind = part.get("type")
        ns = tuple(str(item) for item in part.get("ns", ()) or ())
        data = part.get("data")
        if kind == "messages":
            await self._write(self._messages(ns, data))
        elif kind == "updates":
            observations = list(self._flush_all())
            observations.extend(self._updates(ns, data))
            await self._write(observations)
        elif kind == "custom":
            await self._write(self._custom(ns, data))

    async def flush(self) -> None:
        await self._write(self._flush_all())

    async def finish(
        self,
        *,
        messages: Sequence[BaseMessage],
        own_messages: Sequence[BaseMessage],
        output_text: str,
        structured_keys: Sequence[str],
        checkpoint_id: str | None,
    ) -> None:
        observations = list(self._flush_all())
        observations.extend(self.backfill(messages))
        usage: dict[str, int] = {}
        for message in own_messages:
            if isinstance(message, AIMessage):
                for key, value in _usage_counts(message.usage_metadata).items():
                    usage[key] = usage.get(key, 0) + value
        final = next((item for item in reversed(messages) if isinstance(item, AIMessage)), None)
        metadata = final.response_metadata if final is not None else {}
        stop_reason = str(
            metadata.get("stop_reason") or metadata.get("finish_reason") or "end_turn"
        )
        observations.append(
            self._observation(
                "graph.invocation_ended",
                item_id=self._turn,
                body={
                    "invocation_id": self._turn,
                    "outcome": "succeeded",
                    "stop_reason": stop_reason,
                    "message_count": len(messages),
                    "final_message_id": final.id if final is not None else None,
                    **usage,
                },
            )
        )
        observations.append(
            self._observation(
                "graph.run_result",
                item_id=self._turn,
                body={
                    "status": "finished",
                    "invocation_id": self._turn,
                    "output_digest": _digest(output_text),
                    "output_bytes": len(output_text.encode("utf-8")),
                    "structured_output_keys": sorted(structured_keys),
                    "result_checkpoint_id": checkpoint_id,
                },
            )
        )
        await self._write(observations)

    async def fail(self, error: BaseException, *, unknown_state: bool) -> None:
        """Best effort: a failure to record the failure never masks the original error."""

        body = {
            "invocation_id": self._turn,
            "error_type": type(error).__name__,
            "unknown_state": unknown_state,
            "reason": getattr(error, "reason", None),
        }
        observations = [
            *self._flush_all(),
            self._observation("graph.error", item_id=self._turn, body=body),
        ]
        if not unknown_state:
            observations.append(
                self._observation(
                    "graph.run_result",
                    item_id=self._turn,
                    body={
                        "status": "error",
                        "invocation_id": self._turn,
                        "error_type": body["error_type"],
                    },
                )
            )
        try:
            await self._write(observations)
        except Exception:  # pragma: no cover - logged, original error re-raised by caller
            logger.exception("provider frames for a failed invocation were not recorded")

    def backfill(self, messages: Sequence[BaseMessage]) -> list[FrameObservation]:
        """Tool results present in the final checkpoint whose closing frame never arrived."""

        observations: list[FrameObservation] = []
        for message in messages:
            if not isinstance(message, ToolMessage):
                continue
            call_id = str(message.tool_call_id)
            if call_id in self._closed_tool_calls:
                continue
            self._closed_tool_calls.add(call_id)
            observations.append(
                self._observation(
                    "checkpoint_backfill",
                    item_id=call_id,
                    body=message_body(message),
                    tool_call_ref=call_id,
                )
            )
        return observations

    # --- stream modes -------------------------------------------------------------------

    def _messages(self, ns: tuple[str, ...], data: Any) -> list[FrameObservation]:
        if not isinstance(data, tuple | list) or len(data) != 2:
            return []
        message, metadata = data
        metadata = metadata if isinstance(metadata, Mapping) else {}
        if ns and metadata.get("lc_agent_name"):
            self._agent_names.setdefault(ns[0], str(metadata["lc_agent_name"]))
        if not isinstance(message, AIMessageChunk) or not message.id:
            return []  # whole messages are recorded from `updates`, once
        observations: list[FrameObservation] = []
        step = metadata.get("langgraph_step")
        for stream, text in (("text", _text(message.content)), ("thinking", _thinking(message))):
            if not text:
                continue
            key = (str(message.id), stream, "|".join(ns))
            buffer = self._deltas.setdefault(key, [ns, step, ""])
            buffer[2] += text
            if len(buffer[2].encode("utf-8")) >= self._flush_bytes:
                observations.append(self._flush(key))
        return observations

    def _flush(self, key: tuple[str, str, str]) -> FrameObservation:
        ns, step, text = self._deltas.pop(key)
        index = self._delta_counts.get(key, 0)
        self._delta_counts[key] = index + 1
        message_id, stream, _namespace = key
        raw = "messages.ai_chunk" if stream == "text" else "messages.reasoning_chunk"
        return self._observation(
            raw,
            item_id=f"{message_id}#{index}",
            body={"message_id": message_id, "chunk_index": index, "text": text},
            ns=ns,
            step=step if isinstance(step, int) else None,
        )

    def _flush_all(self) -> list[FrameObservation]:
        return [self._flush(key) for key in list(self._deltas)]

    def _updates(self, ns: tuple[str, ...], data: Any) -> list[FrameObservation]:
        observations: list[FrameObservation] = []
        if not isinstance(data, Mapping):
            return observations
        for node, value in data.items():
            if node == "__metadata__":
                continue
            if node == "__interrupt__":
                interrupts = [
                    {
                        "id": getattr(item, "id", None),
                        "value": jsonable(getattr(item, "value", item)),
                    }
                    for item in (value if isinstance(value, Sequence) else (value,))
                ]
                observations.append(
                    self._observation(
                        "updates.interrupt",
                        item_id=",".join(str(item["id"]) for item in interrupts)
                        or _digest(interrupts),
                        body={"interrupts": interrupts},
                        ns=ns,
                    )
                )
                # The closing evidence the reducer reads: the session now requires action.
                observations.append(
                    self._observation(
                        "updates.interrupt.session_state",
                        item_id=",".join(str(item["id"]) for item in interrupts)
                        or _digest(interrupts),
                        body={
                            "state": "requires_action",
                            "request_ref": ",".join(str(item["id"]) for item in interrupts),
                        },
                        ns=ns,
                    )
                )
                continue
            if not isinstance(value, Mapping):
                continue
            if value.get("_summarization_event") is not None:
                event = jsonable(value.get("_summarization_event"))
                digest = _digest(event)
                observations.append(
                    self._observation(
                        "updates.summarization_event",
                        item_id=f"{node}:{digest}",
                        body={"node": node, "summary_digest": digest, "event": event},
                        ns=ns,
                    )
                )
            messages = value.get("messages")
            if isinstance(messages, list | tuple):
                for message in messages:
                    observations.extend(self._message(ns, message))
        return observations

    def _message(self, ns: tuple[str, ...], message: Any) -> list[FrameObservation]:
        observations: list[FrameObservation] = []
        if isinstance(message, AIMessage) and message.id:
            body = message_body(message)
            observations.append(
                self._observation("updates.ai_message", item_id=str(message.id), body=body, ns=ns)
            )
            for call in message.tool_calls:
                call_id = str(call.get("id") or "")
                if not call_id or call_id in self._started_tool_calls:
                    continue
                self._started_tool_calls.add(call_id)
                observations.append(
                    self._observation(
                        "updates.ai_tool_call",
                        item_id=call_id,
                        body={
                            "tool_call_id": call_id,
                            "name": call.get("name"),
                            "args": jsonable(call.get("args")),
                            "args_digest": _digest(call.get("args")),
                            "message_id": message.id,
                        },
                        ns=ns,
                        tool_call_ref=call_id,
                    )
                )
            if message.usage_metadata:
                observations.append(
                    self._observation(
                        "model.usage",
                        item_id=str(message.id),
                        body={
                            "message_id": message.id,
                            "model": (message.response_metadata or {}).get("model_name"),
                            **_usage_counts(message.usage_metadata),
                        },
                        ns=ns,
                    )
                )
        elif isinstance(message, ToolMessage):
            call_id = str(message.tool_call_id)
            self._closed_tool_calls.add(call_id)
            body = message_body(message)
            body["result_digest"] = _digest(message.content)
            observations.append(
                self._observation(
                    "updates.tool_message",
                    item_id=call_id,
                    body=body,
                    ns=ns,
                    tool_call_ref=call_id,
                )
            )
        return observations

    def _custom(self, ns: tuple[str, ...], data: Any) -> list[FrameObservation]:
        event_type = data.get("type") if isinstance(data, Mapping) else None
        raw = (
            f"custom.{event_type}"
            if isinstance(event_type, str) and event_type.startswith("mc.")
            else "custom"
        )
        item = data.get("id") if isinstance(data, Mapping) else None
        tool_call_ref = data.get("tool_call_id") if isinstance(data, Mapping) else None
        return [
            self._observation(
                raw,
                item_id=str(item) if item else _digest(data),
                body=jsonable(data),
                ns=ns,
                tool_call_ref=str(tool_call_ref) if tool_call_ref else None,
                key_kind=raw,
            )
        ]
