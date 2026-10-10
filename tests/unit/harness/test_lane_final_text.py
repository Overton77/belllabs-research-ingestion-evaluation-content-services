"""Recovery 2026-10-09: Codex and Cursor lanes read the full final text (`FinalTextLane`).

The settlement parses the Completion Candidate from the lane's whole final answer, never from
the bounded closing-facts excerpt, so a long final JSON object is not truncated.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from mission_control.adapters.codex.harness import CodexLocalHarness
from mission_control.adapters.cursor.cloud import CursorCloudHarness
from mission_control.adapters.cursor.local import CursorLocalHarness

LONG = '{"answer": "' + "x" * 10_000 + '"}'


def _frame(body: dict[str, Any]) -> Any:
    return SimpleNamespace(body=body)


def _turn(native: str | None = "turn-1") -> Any:
    session = SimpleNamespace(harness_execution_id="heid")
    return SimpleNamespace(session=session, native_turn_ref=native)


def test_cursor_lanes_return_the_whole_result_text() -> None:
    for lane in (CursorLocalHarness, CursorCloudHarness):
        assert lane.final_text(None, _turn(), _frame({"result": LONG})) == LONG  # type: ignore[arg-type]
        assert lane.final_text(None, _turn(), _frame({"status": "FINISHED"})) is None  # type: ignore[arg-type]


def test_codex_prefers_the_recorded_agent_message_and_needs_a_session() -> None:
    state = SimpleNamespace(last_agent_text=LONG)
    holder = SimpleNamespace(_sessions={"heid": SimpleNamespace(turns={"turn-1": state})})
    assert CodexLocalHarness.final_text(holder, _turn(), _frame({})) == LONG  # type: ignore[arg-type]
    empty = SimpleNamespace(_sessions={})
    assert CodexLocalHarness.final_text(empty, _turn(), _frame({})) is None  # type: ignore[arg-type]
