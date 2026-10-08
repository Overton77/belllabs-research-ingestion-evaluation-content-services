"""FT-G3 opt-in live smoke of the pinned `cursor-sdk` bridge (paid; skipped by default).

Runs only with `MC_CURSOR_LOCAL_LIVE=1`, a `CURSOR_API_KEY` and a finite `MC_PAID_BUDGET_USD`
approved in Linear (SPEC-07 section 12; FT-G6 owns the recorded qualification). It launches
the real bridge in a throwaway git repository, creates one local agent, sends one tiny turn,
observes it to the end through `SdkLocalBridge`, maps every envelope through the lane's frame
mapper, and closes the agent. CI and the fast-track agents never run it.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from pydantic import SecretStr

from mission_control.adapters.cursor.bridge import LocalAgentSpec, SdkBridgeLauncher
from mission_control.adapters.cursor.frames import map_envelope
from tests.fixtures.cursor_local import make_repository

LIVE = (
    os.environ.get("MC_CURSOR_LOCAL_LIVE") == "1"
    and bool(os.environ.get("CURSOR_API_KEY"))
    and float(os.environ.get("MC_PAID_BUDGET_USD", "0") or 0) > 0
)

pytestmark = pytest.mark.skipif(
    not LIVE,
    reason="paid live drill: set MC_CURSOR_LOCAL_LIVE=1, CURSOR_API_KEY, MC_PAID_BUDGET_USD",
)


async def test_one_local_turn_through_the_pinned_bridge(tmp_path: Path) -> None:
    repository = make_repository(tmp_path / "repo")
    launcher = SdkBridgeLauncher(SecretStr(os.environ["CURSOR_API_KEY"]))
    bridge = await launcher.launch(workspace=repository, state_root=tmp_path / "state")
    try:
        agent_id = await bridge.create_agent(
            LocalAgentSpec(model="composer-2", name="mc-live-smoke", cwd=str(repository))
        )
        run_id = await bridge.send(
            agent_id, "Reply with the single word: ready.", idempotency_key="mc-live-smoke:1"
        )
        kinds = [
            map_envelope(event.envelope).raw_kind
            async for event in bridge.observe(run_id, after_offset=None)
        ]
        state = await bridge.run_state(run_id)
        assert state.terminal and "RunResult" in kinds
        await bridge.close_agent(agent_id)
    finally:
        await bridge.aclose()
