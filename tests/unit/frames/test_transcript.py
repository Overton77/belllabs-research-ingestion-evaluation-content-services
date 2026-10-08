"""C3: transcript merge, cursor, retention placeholders, redaction, renderers and filters."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from mission_control.application.frames.transcript import (
    FullBodyUnavailable,
    TranscriptDenied,
    TranscriptRunNotFound,
    TranscriptService,
    summarize_sessions,
)
from mission_control.domain.frames.body import SECRET_ENV_NAMES
from mission_control.domain.frames.render import to_jsonl, to_markdown
from mission_control.domain.frames.transcript import (
    CursorExpired,
    TranscriptCursor,
    TranscriptEntry,
    TranscriptQuery,
)
from mission_control.domain.policies.contracts import ActorContext
from tests.fixtures.provider_frames import FIXTURE_ACTIVATION, SCOPE
from tests.fixtures.transcripts import (
    READER,
    RUN_KEY,
    SECRET_VALUE,
    InMemoryMissionEvents,
    StaticFrames,
    normalize,
    transcript_events,
    transcript_frames,
)

GOLDENS = Path(__file__).parent / "goldens"


def service(
    *, include_expired: bool = False, drop_frames: tuple[int, ...] = (), **kw: Any
) -> TranscriptService:
    frames = transcript_frames()
    events = transcript_events(frames, include_expired=include_expired)
    kept = [frame for frame in frames if frame.arrival_ordinal not in drop_frames]
    return TranscriptService(
        InMemoryMissionEvents(events),
        StaticFrames(kept),  # type: ignore[arg-type]
        request_scope=SCOPE,
        secret_values=(SECRET_VALUE,),
        **kw,
    )


def test_cursor_round_trips_and_orders() -> None:
    event = TranscriptCursor(seq=3)
    frame = TranscriptCursor(seq=3, observed_us=1_759_838_401_000_000, ordinal=2, tag="7d4f0c1e")
    assert TranscriptCursor.decode(event.encode()) == event
    assert TranscriptCursor.decode(frame.encode()) == frame
    assert event < frame < TranscriptCursor(seq=4)
    assert event.encode() < frame.encode() < TranscriptCursor(seq=4).encode()
    for bad in ("", "tc1:1:2:3:x", "tc2:000000000001:00000000000000000:000000000000:00000000"):
        with pytest.raises(CursorExpired):
            TranscriptCursor.decode(bad)


@pytest.mark.asyncio
async def test_merge_orders_events_and_frames_with_strictly_increasing_cursors() -> None:
    page = await service().materialize(RUN_KEY, actor=READER)
    entries = page.entries
    cursors = [entry.cursor for entry in entries]
    assert cursors == sorted(cursors) and len(set(cursors)) == len(cursors)
    sequence = [(entry.kind, entry.canonical) for entry in entries]
    assert sequence == [
        ("workflow_run.admit", True),
        ("workflow_run.start", True),
        ("session_init", False),
        ("session.turn_started", True),
        ("turn_started", False),
        ("message_delta", False),
        ("tool_call_started", False),
        ("tool_call_completed", False),
        ("tool_call.completed", True),
        ("usage", False),
        ("turn_ended", False),
        ("artifact.registered", True),
        ("human_task.resolved", True),
        ("session.turn_completed", True),
        ("run_result", False),
    ]
    by_kind = {entry.kind: entry for entry in entries}
    artifact = by_kind["artifact.registered"]
    assert artifact.source == "artifact" and artifact.refs.artifact_ref is not None
    assert artifact.refs.body_digest == "sha256:" + "a" * 64
    assert by_kind["human_task.resolved"].role == "human"
    assert by_kind["human_task.resolved"].source == "human"
    assert by_kind["workflow_run.start"].role == "mission_control"
    assert by_kind["workflow_run.start"].source == "command"
    assert by_kind["tool_call_completed"].role == "tool"
    assert by_kind["message_delta"].role == "agent"
    assert by_kind["session.turn_completed"].usage is not None
    assert by_kind["usage"].usage is not None and by_kind["turn_ended"].usage is None
    assert by_kind["tool_call_completed"].node_key == "collect", "node from the activation"
    assert page.next_cursor == entries[-1].cursor and not page.has_more


@pytest.mark.asyncio
async def test_since_resumes_and_retention_yields_placeholders_not_errors() -> None:
    full = await service().materialize(RUN_KEY, actor=READER)
    middle = full.entries[7].cursor
    tail = await service().materialize(RUN_KEY, actor=READER, query=TranscriptQuery(since=middle))
    assert [entry.cursor for entry in tail.entries] == [entry.cursor for entry in full.entries[8:]]
    # Frames 2..6 expired under retention; the events that cite them get placeholders.
    expired = await service(drop_frames=(2, 3, 4, 5, 6), include_expired=True).materialize(
        RUN_KEY, actor=READER, query=TranscriptQuery(since=full.entries[1].cursor)
    )
    placeholders = [entry for entry in expired.entries if entry.kind == "frame_expired"]
    assert len(placeholders) == 3
    assert any("sha256:" + "e" * 64 in entry.title for entry in placeholders)
    assert all(not entry.canonical for entry in placeholders)
    canonical = [entry.kind for entry in expired.entries if entry.canonical]
    assert "tool_call.completed" in canonical and "session.turn_completed" in canonical
    with pytest.raises(CursorExpired):
        await service().materialize(RUN_KEY, actor=READER, query=TranscriptQuery(since="nope"))


@pytest.mark.asyncio
async def test_filters_and_limits() -> None:
    svc = service()
    canonical = await svc.materialize(
        RUN_KEY, actor=READER, query=TranscriptQuery(canonical_only=True)
    )
    assert all(entry.canonical for entry in canonical.entries) and len(canonical.entries) == 7
    tools = await svc.materialize(
        RUN_KEY,
        actor=READER,
        query=TranscriptQuery(kinds=frozenset({"tool_call.completed", "tool_call_completed"})),
    )
    assert [entry.kind for entry in tools.entries] == ["tool_call_completed", "tool_call.completed"]
    activation = await svc.materialize(
        RUN_KEY, actor=READER, query=TranscriptQuery(activation=str(FIXTURE_ACTIVATION))
    )
    assert activation.entries and all(
        entry.activation_id == str(FIXTURE_ACTIVATION) for entry in activation.entries
    )
    node = await svc.materialize(RUN_KEY, actor=READER, query=TranscriptQuery(node="collect"))
    assert node.entries and all(entry.node_key == "collect" for entry in node.entries)
    limited = await svc.materialize(RUN_KEY, actor=READER, query=TranscriptQuery(limit=3))
    assert len(limited.entries) == 3 and limited.has_more
    assert limited.next_cursor == limited.entries[-1].cursor
    nobody = await svc.materialize(RUN_KEY, actor=READER, query=TranscriptQuery(subordinate="x"))
    assert nobody.entries == ()


@pytest.mark.asyncio
async def test_authorization_unknown_runs_and_full_bodies() -> None:
    stranger = ActorContext(actor_id="x", authority_refs=frozenset(), permissions=frozenset())
    with pytest.raises(TranscriptDenied):
        await service().materialize(RUN_KEY, actor=stranger)
    with pytest.raises(TranscriptRunNotFound):
        await service().materialize("other-run", actor=READER)
    with pytest.raises(FullBodyUnavailable):
        await service().materialize(RUN_KEY, actor=READER, full=True)

    class Bodies:
        async def read(self, request_scope: str, actor: ActorContext, artifact_ref: str) -> str:
            assert actor is READER and request_scope == SCOPE
            return f"full body of {artifact_ref} with {SECRET_VALUE}"

    page = await service(artifacts=Bodies()).materialize(RUN_KEY, actor=READER, full=True)
    artifact = next(entry for entry in page.entries if entry.kind == "artifact.registered")
    assert artifact.body_excerpt is not None and "full body of mc://" in artifact.body_excerpt
    assert SECRET_VALUE not in artifact.body_excerpt


def _scan_for_secrets(text: str) -> None:
    assert SECRET_VALUE not in text
    for name in SECRET_ENV_NAMES:
        assert f"{name}=" not in text
    assert "Bearer ey" not in text and "sk-live" not in text


@pytest.mark.asyncio
async def test_markdown_and_jsonl_goldens_with_render_time_redaction() -> None:
    page = await service().materialize(RUN_KEY, actor=READER)
    markdown = to_markdown(page.entries, run_id=RUN_KEY, secret_values=(SECRET_VALUE,))
    jsonl = to_jsonl(page.entries, secret_values=(SECRET_VALUE,))
    _scan_for_secrets(markdown)
    _scan_for_secrets(jsonl)
    lines = jsonl.splitlines()
    assert len(lines) == len(page.entries)
    for line in lines:
        parsed = json.loads(line)
        assert json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False) == line
        TranscriptEntry.model_validate(parsed)
    assert "## Activation" in markdown and "### Turn 1" in markdown
    assert "- ✓ " in markdown and "- · " in markdown
    assert "<details>" in markdown and "mc://artifacts/biotech" in markdown
    assert (GOLDENS / "transcript.md").read_text(encoding="utf-8") == normalize(markdown)
    assert (GOLDENS / "transcript.jsonl").read_text(encoding="utf-8") == normalize(jsonl)


@pytest.mark.asyncio
async def test_render_redacts_entries_that_bypassed_write_time_redaction() -> None:
    page = await service().materialize(RUN_KEY, actor=READER)
    leaked = page.entries[0].model_copy(
        update={"body_excerpt": "token=abcdef CURSOR_API_KEY=crsr_123 render-only-secret-4242"}
    )
    text = to_jsonl([leaked], secret_values=(SECRET_VALUE,)) + to_markdown(
        [leaked], run_id=RUN_KEY, secret_values=(SECRET_VALUE,)
    )
    assert "crsr_123" not in text and SECRET_VALUE not in text


def test_session_summaries_for_inspection() -> None:
    summaries = summarize_sessions(transcript_frames())
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.lane_profile == "deep_agents" and summary.turns_closed == 1
    assert summary.tool_calls == 1 and summary.run_result_status == "finished"
    assert summary.usage is not None and summary.usage.value("input_tokens") == 8
