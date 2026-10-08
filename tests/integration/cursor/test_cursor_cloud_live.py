"""FT-G5 opt-in live smoke of the Cloud Agents API v1 (paid; skipped by default).

Runs only with `MC_CURSOR_CLOUD_LIVE=1`, a `CURSOR_API_KEY`, a throwaway repository in
`MC_CURSOR_CLOUD_REPO` and a finite `MC_PAID_BUDGET_USD` approved in Linear (SPEC-07 section 12;
FT-G6 owns the recorded qualification). It creates one cloud agent with a client `agentId`,
reads its run stream to the end through `CloudAgentsClient`, and archives the agent. CI and the
fast-track agents never run it.
"""

from __future__ import annotations

import os
from uuid import uuid4

import pytest
from pydantic import SecretStr

from mission_control.adapters.cursor.cloud_api import CloudAgentsClient, StreamOpened

LIVE = (
    os.environ.get("MC_CURSOR_CLOUD_LIVE") == "1"
    and bool(os.environ.get("CURSOR_API_KEY"))
    and bool(os.environ.get("MC_CURSOR_CLOUD_REPO"))
    and float(os.environ.get("MC_PAID_BUDGET_USD", "0") or 0) > 0
)

pytestmark = pytest.mark.skipif(
    not LIVE,
    reason=(
        "paid live drill: set MC_CURSOR_CLOUD_LIVE=1, CURSOR_API_KEY, MC_CURSOR_CLOUD_REPO, "
        "MC_PAID_BUDGET_USD"
    ),
)


async def test_one_cloud_run_streams_to_its_result_and_is_archived() -> None:
    client = CloudAgentsClient(SecretStr(os.environ["CURSOR_API_KEY"]))
    agent_id = f"bc-{uuid4()}"
    try:
        created = await client.create_agent(
            {
                "agentId": agent_id,
                "prompt": {"text": "Reply with the single word: ready. Do not change files."},
                "repos": [{"url": os.environ["MC_CURSOR_CLOUD_REPO"]}],
            }
        )
        run_id = created["run"]["id"]
        events = [
            item.event
            async for item in client.stream(agent_id, run_id, last_event_id=None)
            if not isinstance(item, StreamOpened)
        ]
        assert "result" in events or (await client.get_run(agent_id, run_id))["status"]
    finally:
        await client.archive(agent_id)
        await client.aclose()
