"""MP-07: every pinned `claude_agent_sdk.types.Message` maps to Provider Frames; closing facts
come from the `result` frame, never from its text. Inputs are the labelled FIXTURES under
`tests/fixtures/provider_frames/claude/` parsed by the SDK's own `parse_message`."""

from __future__ import annotations

import json

import pytest
from claude_agent_sdk._internal.message_parser import parse_message
from claude_agent_sdk.types import (
    AssistantMessage,
    HookEventMessage,
    ResultMessage,
    SystemMessage,
    TaskStartedMessage,
    UserMessage,
)

from mission_control.adapters.claude.frames import (
    closing_facts,
    lane_frame,
    message_payload,
    native_status,
    observations,
    session_id_of,
    synthesized_result,
    usage_report,
)
from mission_control.application.frames.kinds import UnknownKindCounter
from mission_control.domain.frames.contracts import FrameKind
from tests.unit.claude.fixtures import FIXTURES, SESSION_ID, FixtureScript


def _messages(name: str) -> list[dict]:
    script = FixtureScript.load(name)
    found: list[dict] = []
    for record in script.records:
        if record.message is not None:
            found.append(record.message)
        found.extend(record.allowed)
        found.extend(record.denied)
    return found


def test_every_fixture_is_labelled_and_parses_with_the_sdk_parser() -> None:
    for path in sorted(FIXTURES.glob("*.jsonl")):
        marker = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
        assert marker["recorded"] is False and "FIXTURE" in marker["note"], path.name
        assert "0.2.165" in marker["note"]
        for raw in _messages(path.stem):
            parsed = parse_message(raw)
            assert parsed is not None, (path.name, raw["type"])
            payload = message_payload(parsed)
            assert payload["type"] in {
                "assistant",
                "user",
                "system",
                "result",
                "stream_event",
                "rate_limit_event",
                "conversation_reset",
            }
            assert observations(parsed, counter=None), (path.name, raw["type"])


def test_assistant_and_user_messages_split_into_message_and_tool_frames() -> None:
    raws = {raw["uuid"]: raw for raw in _messages("full_run") if "uuid" in raw}
    assistant = parse_message(raws["a-0001"])
    assert isinstance(assistant, AssistantMessage)
    payload = message_payload(assistant)
    assert [block["type"] for block in payload["content"]] == ["text", "tool_use"]
    kinds = [(item.raw_kind, item.kind, item.tool_call_ref) for item in observations(assistant)]
    assert kinds == [
        ("assistant", FrameKind.MESSAGE, None),
        ("assistant.tool_use", FrameKind.TOOL_CALL_STARTED, "toolu_bash_01"),
    ]
    user = parse_message(raws["u-0002d"])
    assert isinstance(user, UserMessage)
    (failed,) = observations(user)
    assert (failed.raw_kind, failed.kind, failed.tool_call_ref) == (
        "user.tool_result",
        FrameKind.TOOL_CALL_FAILED,
        "toolu_bash_01",
    )
    ok = parse_message(raws["u-0004"])
    assert isinstance(ok, UserMessage)
    assert observations(ok)[0].kind is FrameKind.TOOL_CALL_COMPLETED
    assert session_id_of(assistant) == SESSION_ID
    # The dedupe key is the MP-13 one: claude:<uuid>[:<block>]:<raw_kind>.
    assert observations(assistant)[1].provider_key == "claude:a-0001:1:assistant.tool_use"


def test_system_task_stream_rate_limit_and_reset_messages_map_or_stay_unknown() -> None:
    raws = {
        raw.get("uuid"): raw for raw in _messages("subagent_task") + _messages("unknown_frames")
    }
    init = parse_message(raws[f"init-{SESSION_ID}"])
    assert isinstance(init, SystemMessage) and session_id_of(init) == SESSION_ID
    assert observations(init)[0].kind is FrameKind.SESSION_INIT
    started = parse_message(raws["s-3002"])
    assert isinstance(started, TaskStartedMessage)
    (task,) = observations(started)
    assert task.kind is FrameKind.STATUS and task.subordinate_ref == "claude:task:toolu_task_31"
    sub = parse_message(raws["a-3003"])
    assert observations(sub)[0].subordinate_ref == "claude:task:toolu_task_31"
    for uuid, expected in (
        ("s-3005", FrameKind.STATUS),
        ("s-3006", FrameKind.STATUS),
        ("s-3007", FrameKind.STATUS),
        ("e-3009", FrameKind.MESSAGE_DELTA),
        ("l-4004", FrameKind.STATUS),
    ):
        parsed = parse_message(raws[uuid])
        assert parsed is not None
        assert observations(parsed)[0].kind is expected, uuid
    counter = UnknownKindCounter()
    # MP-07 lifecycle facts the kinds table now maps (non-closing STATUS, never UNKNOWN).
    for uuid in ("s-4001", "c-4002", "m-4003"):
        parsed = parse_message(raws[uuid])
        assert parsed is not None
        (frame,) = observations(parsed, counter=counter)
        assert frame.kind is FrameKind.STATUS, uuid
    # A subtype no row maps stays UNKNOWN and is counted (UNKNOWN visibility).
    parsed = parse_message(raws["x-4007"])
    assert parsed is not None
    (frame,) = observations(parsed, counter=counter)
    assert frame.kind is FrameKind.UNKNOWN
    assert sorted(raw for (_lane, raw) in counter.snapshot()) == ["system.fixture_unmapped_notice"]


def test_hook_event_messages_map_to_hook_frames() -> None:
    started = parse_message(
        {
            "type": "system",
            "subtype": "hook_started",
            "hook_event": "PreToolUse",
            "session_id": SESSION_ID,
            "uuid": "h-0001",
        }
    )
    assert isinstance(started, HookEventMessage)
    assert observations(started)[0].kind is FrameKind.HOOK_INVOKED
    finished = parse_message(
        {
            "type": "system",
            "subtype": "hook_response",
            "hook_event": "PreToolUse",
            "session_id": SESSION_ID,
            "uuid": "h-0002",
            "outcome": "success",
        }
    )
    assert isinstance(finished, HookEventMessage)
    assert observations(finished)[0].kind is FrameKind.HOOK_RESULT


def test_result_message_yields_turn_ended_then_run_result_and_lane_frames_mark_terminal() -> None:
    raw = next(raw for raw in _messages("full_run") if raw["type"] == "result")
    result = parse_message(raw)
    assert isinstance(result, ResultMessage)
    ended, final = observations(result)
    assert (ended.kind, final.kind) == (FrameKind.TURN_ENDED, FrameKind.RUN_RESULT)
    frames = [
        lane_frame(item, harness_execution_id="heid", generation=1, ordinal=n, turn_ref="turn-1")
        for n, item in enumerate((ended, final), start=7)
    ]
    assert [frame.terminal for frame in frames] == [False, True]
    assert [frame.cursor for frame in frames] == ["7", "8"]
    assert frames[1].raw_kind == "result" and frames[1].native_turn_ref == "turn-1"


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({}, ("finished", None)),
        ({"terminal_reason": "aborted_tools"}, ("cancelled", "cancelled_by_command")),
        ({"terminal_reason": "aborted_streaming"}, ("cancelled", "cancelled_by_command")),
        ({"is_error": True, "api_error_status": 429}, ("error", "capacity")),
        ({"is_error": True, "api_error_status": 529}, ("error", "capacity")),
        ({"is_error": True, "subtype": "error_max_turns"}, ("error", "max_turns")),
        (
            {"is_error": True, "subtype": "error_during_execution"},
            ("error", "error_during_execution"),
        ),
        ({"synthesized_from": "process_exit", "is_error": True}, ("error", "process_exit")),
    ],
)
def test_native_status_reads_subtype_flags_and_reason_never_the_text(
    changes: dict, expected: tuple
) -> None:
    raw = next(raw for raw in _messages("full_run") if raw["type"] == "result")
    body = {**raw, "result": "ERROR: this text must not decide the status", **changes}
    assert native_status(body) == expected


def test_closing_facts_settle_tokens_and_keep_cost_estimated() -> None:
    raw = next(raw for raw in _messages("full_run") if raw["type"] == "result")
    facts = closing_facts(raw, model="claude-sonnet-4-5")
    assert facts.native_status == "finished"
    assert facts.result_excerpt == "BINDING-OK: report written."
    assert facts.usage.disposition == "settled"
    assert (facts.usage.input_tokens, facts.usage.output_tokens, facts.usage.total_tokens) == (
        1500,
        240,
        1740,
    )
    # `total_cost_usd` is the SDK's client-side estimate: never a settled amount.
    assert facts.usage.cost_micros_usd is None and facts.cost_disposition == "estimated"
    assert facts.duration_ms == 4200 and facts.model == "claude-sonnet-4-5"
    error = closing_facts({**raw, "is_error": True, "api_error_status": 429, "result": "API Error"})
    assert (error.native_status, error.error_code, error.error_message) == (
        "error",
        "capacity",
        "API Error",
    )
    assert usage_report(None).disposition == "unknown"
    assert usage_report({"input_tokens": 0, "output_tokens": 0}).disposition == "unknown"


def test_synthesized_result_is_an_explicit_process_exit_record() -> None:
    ended, final = synthesized_result(
        session_id=SESSION_ID, turn_ref="turn-9", reason="process_exit", detail="exit 143"
    )
    assert (ended.kind, final.kind) == (FrameKind.TURN_ENDED, FrameKind.RUN_RESULT)
    assert final.body["synthesized_from"] == "process_exit"
    assert final.provider_key == "claude:mc-synth-turn-9::result"
    facts = closing_facts(final.body)
    assert (facts.native_status, facts.error_code, facts.error_message) == (
        "error",
        "process_exit",
        "exit 143",
    )
