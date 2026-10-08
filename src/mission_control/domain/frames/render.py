"""Pure Transcript renderers: JSONL for agents and Markdown for humans (SPEC-03, C3).

Both apply secret redaction again at render time (defence in depth) and never inline the
contents of a referenced body artifact; they render its `mc://` / `artifact://` reference.
Markdown: a heading per activation, a sub-heading per turn, one bullet per entry as
`<mark> HH:MM:SS · role · title` where `✓` marks canonical mission events and `·` marks
provider frames, tool calls as collapsed blocks with digest and excerpt, artifacts as
links, and expired frames as `(frame expired, digest …)`.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence

from mission_control.domain.frames.body import redact_text
from mission_control.domain.frames.transcript import TranscriptEntry
from mission_control.domain.frames.usage import render_dimension

CANONICAL_MARK = "✓"
FRAME_MARK = "·"
MARKDOWN_EXCERPT_CHARS = 1_200
TURN_START_KINDS = frozenset({"session.turn_started", "turn_started"})
TOOL_KINDS = frozenset(
    {"tool_call.completed", "tool_call_started", "tool_call_completed", "tool_call_failed"}
)


def _clean(value: str | None, secret_values: Sequence[str]) -> str | None:
    if value is None:
        return None
    return redact_text(value, secret_values)[0]


def redacted_entry(entry: TranscriptEntry, secret_values: Sequence[str] = ()) -> TranscriptEntry:
    """The entry with its title and excerpt redacted again."""

    return entry.model_copy(
        update={
            "title": _clean(entry.title, secret_values) or entry.title,
            "body_excerpt": _clean(entry.body_excerpt, secret_values),
        }
    )


def to_jsonl(entries: Iterable[TranscriptEntry], *, secret_values: Sequence[str] = ()) -> str:
    """One canonical JSON object per line (sorted keys, no whitespace, UTF-8)."""

    lines = [
        json.dumps(
            redacted_entry(entry, secret_values).model_dump(mode="json", exclude_none=True),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        for entry in entries
    ]
    return "".join(f"{line}\n" for line in lines)


def _artifact_link(ref: str) -> str:
    label = ref.rsplit("/", 1)[-1] or ref
    return f"[{label}]({ref})"


def _usage_line(entry: TranscriptEntry) -> str | None:
    if entry.usage is None:
        return None
    dims = entry.usage.dimensions
    return (
        f"usage: input {render_dimension(dims.get('input_tokens'))}, "
        f"output {render_dimension(dims.get('output_tokens'))}, "
        f"cost_micros {render_dimension(dims.get('cost_micros'))}"
    )


def _fence(text: str) -> str:
    fence = "```"
    while fence in text:
        fence += "`"
    return fence


def to_markdown(
    entries: Sequence[TranscriptEntry],
    *,
    run_id: str,
    secret_values: Sequence[str] = (),
) -> str:
    lines = [
        f"# Transcript · run {run_id}",
        "",
        f"_Legend: {CANONICAL_MARK} canonical mission event; {FRAME_MARK} provider frame "
        "(evidence, not state)_",
    ]
    # A turn sub-heading comes from the canonical `session.turn_started`; a provider
    # `turn_started` frame opens one only where no canonical turn event exists.
    canonical_turns = {
        entry.activation_id for entry in entries if entry.kind == "session.turn_started"
    }
    activation: str | None = "__none__"
    for raw in entries:
        entry = redacted_entry(raw, secret_values)
        if entry.activation_id != activation:
            activation = entry.activation_id
            heading = f"Activation {activation}" if activation else "Run"
            if entry.node_key:
                heading += f" · {entry.node_key}"
            lines.extend(["", f"## {heading}", ""])
        if entry.kind == "session.turn_started" or (
            entry.kind in TURN_START_KINDS and entry.activation_id not in canonical_turns
        ):
            lines.extend(["", f"### Turn {_turn_label(entry)}", ""])
        mark = CANONICAL_MARK if entry.canonical else FRAME_MARK
        clock = entry.at.strftime("%H:%M:%S")
        title = entry.title
        if entry.refs.artifact_ref and entry.source == "artifact":
            title = f"{title} {_artifact_link(entry.refs.artifact_ref)}"
        line = f"- {mark} {clock} · {entry.role or 'system'} · {title}"
        if entry.subordinate_ref:
            line += f" _(subagent {entry.subordinate_ref})_"
        lines.append(line)
        usage = _usage_line(entry)
        if usage:
            lines.append(f"  - {usage}")
        if entry.refs.artifact_ref and entry.source != "artifact":
            lines.append(f"  - body: {_artifact_link(entry.refs.artifact_ref)}")
        if entry.kind in TOOL_KINDS and entry.body_excerpt:
            excerpt = entry.body_excerpt[:MARKDOWN_EXCERPT_CHARS]
            digest = entry.refs.body_digest or ""
            fence = _fence(excerpt)
            lines.extend(
                [
                    "  <details>",
                    f"  <summary>{entry.refs.tool_call_ref or 'tool call'} · {digest}</summary>",
                    "",
                    f"  {fence}json",
                    *(f"  {item}" for item in excerpt.splitlines() or [""]),
                    f"  {fence}",
                    "  </details>",
                ]
            )
    lines.append("")
    return "\n".join(lines)


def _turn_label(entry: TranscriptEntry) -> str:
    if entry.body_excerpt:
        try:
            body = json.loads(entry.body_excerpt)
        except ValueError:
            body = None
        if isinstance(body, dict) and isinstance(body.get("turn_ordinal"), int):
            return str(body["turn_ordinal"])
    return entry.refs.native_event_ref or entry.cursor[-8:]
