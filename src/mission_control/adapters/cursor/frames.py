"""Cursor run events to Provider Frames and closing facts (SPEC-07 sections 5.3 and 8; FT-G3).

The local bridge's `ObserveRun` envelopes (`sdkMessage`, `interactionUpdate`, `step`,
`result`, `done`) map to the C1 raw kinds of `CURSOR_LOCAL_KINDS`, with the dedupe key
`cursor_local_key("<run_id>:<offset>")`, so a resume from a stored offset stores no frame
twice. Only the terminal `RunResult` becomes `ClosingFacts`; deltas and starts are evidence
for the transcript only. Field names are read in both the wire (camelCase) and the SDK
dataclass (snake_case) spelling, so recorded fixtures and the live SDK map identically.

`scrub` removes secret values and e-mail addresses from a recorded fixture before it is
committed (SPEC-07 Testing Decisions).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from mission_control.adapters.cursor.bridge import BridgeEvent, RunState
from mission_control.application.frames.kinds import cursor_local_key
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.lane_turns import (
    MAX_EXCERPT_CHARS,
    ClosingFacts,
    LaneGitBranch,
    NativeStatus,
)
from mission_control.domain.execution.lanes import LaneFrame, UsageReport

_BRIDGE_PREFIX = "bridge:"
_FINAL_SUFFIX = ":final"
_LIFECYCLE = "lifecycle."
_STATUS: dict[str, NativeStatus] = {
    "finished": "finished",
    "completed": "finished",
    "succeeded": "finished",
    "error": "error",
    "failed": "error",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "expired": "expired",
}
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def first(body: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = body.get(key)
        if value is not None and value != "":
            return value
    return None


def local_provider_key(run_id: str, offset: str) -> str:
    return cursor_local_key(f"{run_id}:{offset}")


def local_final_key(run_id: str) -> str:
    return cursor_local_key(f"{run_id}{_FINAL_SUFFIX}")


def local_lifecycle_key(run_id: str, name: str) -> str:
    """Synthetic lifecycle frames the lane writes itself (`agent.created`, `send.accepted`)."""

    return cursor_local_key(f"{run_id}:{_LIFECYCLE}{name}")


def offset_of(provider_key: str) -> str | None:
    """The bridge offset a persisted local frame names (None for synthetic frames)."""

    if not provider_key.startswith(_BRIDGE_PREFIX) or provider_key.endswith(_FINAL_SUFFIX):
        return None
    _run, _sep, offset = provider_key.removeprefix(_BRIDGE_PREFIX).rpartition(":")
    if not offset or offset.startswith(_LIFECYCLE):
        return None
    return offset


def run_of(provider_key: str) -> str | None:
    """The run a persisted local frame belongs to (None for a key of another shape)."""

    if not provider_key.startswith(_BRIDGE_PREFIX):
        return None
    run, separator, _rest = provider_key.removeprefix(_BRIDGE_PREFIX).rpartition(":")
    return run if separator and run else None


@dataclass(frozen=True)
class MappedEvent:
    raw_kind: str
    body: dict[str, Any]
    tool_call_ref: str | None
    terminal: bool


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def map_envelope(envelope: Mapping[str, Any]) -> MappedEvent:
    """One wire envelope to its C1 raw kind, JSON body and tool call reference."""

    if "sdkMessage" in envelope:
        message = _mapping(envelope["sdkMessage"])
        kind = str(message.get("type") or "unknown")
        return MappedEvent(
            raw_kind=kind,
            body=message,
            tool_call_ref=first(message, "call_id", "callId") if kind == "tool_call" else None,
            terminal=False,
        )
    if "interactionUpdate" in envelope:
        update = _mapping(envelope["interactionUpdate"])
        kind = str(update.get("type") or "unknown")
        return MappedEvent(
            raw_kind=kind,
            body=update,
            tool_call_ref=first(update, "call_id", "callId"),
            terminal=False,
        )
    if "step" in envelope:
        return MappedEvent("status", {"step": _mapping(envelope["step"])}, None, False)
    if "result" in envelope:
        wrapper = _mapping(envelope["result"])
        inner = wrapper.get("result")
        if isinstance(inner, Mapping):
            return MappedEvent("RunResult", dict(inner), None, True)
        return MappedEvent("status", wrapper, None, False)
    if "done" in envelope:
        return MappedEvent("status", {"done": True, **_mapping(envelope["done"])}, None, False)
    return MappedEvent("unknown", dict(envelope), None, False)


def local_lane_frame(
    event: BridgeEvent, *, run_id: str, harness_execution_id: str, generation: int
) -> LaneFrame:
    mapped = map_envelope(event.envelope)
    return LaneFrame(
        harness_execution_id=harness_execution_id,
        generation=generation,
        provider_key=local_provider_key(run_id, event.offset),
        cursor=event.offset,
        kind=mapped.raw_kind,
        raw_kind=mapped.raw_kind,
        body=mapped.body,
        terminal=mapped.terminal,
        digest=sha256_digest(mapped.body),
        native_turn_ref=run_id,
        tool_call_ref=mapped.tool_call_ref,
    )


def final_lane_frame(
    state: RunState, *, harness_execution_id: str, generation: int, cursor: str | None
) -> LaneFrame:
    """A terminal frame synthesized from the run record when the stream ended without one."""

    body: dict[str, Any] = {
        "runId": state.run_id,
        "agentId": state.agent_id,
        "status": state.status,
        "result": state.result,
        "durationMs": state.duration_ms,
        "git": {"branches": [dict(branch) for branch in state.git_branches]},
        "synthesized_from": "run_record",
    }
    if state.model:
        body["model"] = {"id": state.model}
    if state.usage:
        body["usage"] = dict(state.usage)
    return LaneFrame(
        harness_execution_id=harness_execution_id,
        generation=generation,
        provider_key=local_final_key(state.run_id),
        cursor=cursor or "final",
        kind="RunResult",
        raw_kind="RunResult",
        body=body,
        terminal=True,
        digest=sha256_digest(body),
        native_turn_ref=state.run_id,
    )


def native_status(value: Any) -> NativeStatus:
    return _STATUS.get(str(value or "").lower(), "error")


def _usage(body: Mapping[str, Any]) -> UsageReport:
    usage = _mapping(body.get("usage"))
    if not usage:
        return UsageReport(disposition="unknown")
    input_tokens = int(first(usage, "input_tokens", "inputTokens") or 0)
    output_tokens = int(first(usage, "output_tokens", "outputTokens") or 0)
    total = int(first(usage, "total_tokens", "totalTokens") or input_tokens + output_tokens)
    # Stream tokens are what the provider reported for the run; cost is not yet settled.
    return UsageReport(
        disposition="estimated",
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total,
    )


def git_branches(body: Mapping[str, Any]) -> tuple[LaneGitBranch, ...]:
    git = _mapping(body.get("git"))
    branches: list[LaneGitBranch] = []
    for item in git.get("branches") or ():
        entry = _mapping(item)
        repo = first(entry, "repoUrl", "repo_url")
        if not repo:
            continue
        branches.append(
            LaneGitBranch(
                repo_url=str(repo),
                branch=first(entry, "branch") or None,
                pr_url=first(entry, "prUrl", "pr_url") or None,
            )
        )
    return tuple(branches)


def closing_facts(body: Mapping[str, Any]) -> ClosingFacts:
    """`RunResult` (or a run record) to the closing facts the reducer and settlement read."""

    status = native_status(first(body, "status"))
    model = _mapping(body.get("model"))
    error = _mapping(body.get("error"))
    result = first(body, "result", "text") or ""
    usage = _usage(body)
    return ClosingFacts(
        native_status=status,
        result_excerpt=str(result)[:MAX_EXCERPT_CHARS],
        usage=usage,
        cost_disposition="estimated" if usage.disposition != "unknown" else "unknown",
        git_branches=git_branches(body),
        error_code=(
            str(first(error, "code") or "provider_error")[:128] if status == "error" else None
        ),
        duration_ms=int(first(body, "durationMs", "duration_ms") or 0) or None,
        model=str(first(model, "id")) if first(model, "id") else None,
    )


def scrub(value: Any, *, secrets: Iterable[str] = (), mask: str = "[redacted]") -> Any:
    """A recorded fixture without secret values or e-mail addresses (recursive)."""

    secret_values = tuple(item for item in secrets if item)
    if isinstance(value, Mapping):
        return {
            str(key): (
                None
                if str(key) in {"user_email", "userEmail"}
                else scrub(item, secrets=secret_values, mask=mask)
            )
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [scrub(item, secrets=secret_values, mask=mask) for item in value]
    if isinstance(value, str):
        text = value
        for secret in secret_values:
            text = text.replace(secret, mask)
        return _EMAIL.sub(mask, text)
    return value


__all__ = [
    "MappedEvent",
    "closing_facts",
    "final_lane_frame",
    "first",
    "git_branches",
    "local_final_key",
    "local_lane_frame",
    "local_lifecycle_key",
    "local_provider_key",
    "map_envelope",
    "native_status",
    "offset_of",
    "run_of",
    "scrub",
]
