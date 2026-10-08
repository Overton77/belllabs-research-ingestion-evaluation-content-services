"""FT-G3: `.mission/hooks/kernel.py` -> loopback HTTP callback -> Stop Fence and intents.

The real kernel hook script runs as Cursor runs it (a subprocess with the native payload on
stdin, from the workspace) against a real uvicorn listener on 127.0.0.1 serving the callback
app. An allowed `preToolUse` exits 0 with `permission: allow` and writes its Operation Intent;
after a Stop Fence the same hook exits 2 with `permission: deny`; an unreachable callback denies
(fail-closed). No Cursor agent is involved.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import uvicorn

from mission_control.adapters.cursor.hooks_callback import kernel_hook_script
from mission_control.domain.policies.stop_fence import StopFence
from mission_control.interfaces.http.hook_callback import create_hook_callback_app
from tests.fixtures.lane_turns import SCOPE
from tests.unit.harness.test_hook_callback import HEID, RUN, SCOPE_BLOCK, Stack, _context


def _workspace(root: Path, url: str, token: str) -> Path:
    hooks = root / ".mission" / "hooks"
    hooks.mkdir(parents=True)
    (hooks / "kernel.py").write_bytes(kernel_hook_script())
    (root / ".mission" / "bin").mkdir(parents=True)
    (root / ".mission" / "bin" / ".token").write_text(token, encoding="utf-8")
    (hooks / "context.json").write_text(
        json.dumps(
            {
                "scope": SCOPE_BLOCK,
                "harness_execution_id": str(HEID),
                "generation": 1,
                "run_id": RUN,
                "callback": {"url": url, "token_path": ".mission/bin/.token"},
            }
        ),
        encoding="utf-8",
    )
    return hooks / "kernel.py"


def _run(
    script: Path, event: str, hook: str, payload: dict[str, Any]
) -> tuple[int, dict[str, Any]]:
    completed = subprocess.run(
        [sys.executable, str(script), event, hook],
        input=json.dumps(payload).encode("utf-8"),
        capture_output=True,
        cwd=script.parents[2],
        timeout=60,
        check=False,
    )
    return completed.returncode, json.loads(completed.stdout.decode("utf-8"))


PAYLOAD = {
    "conversation_id": "conv-1",
    "generation_id": "gen-1",
    "hook_event_name": "preToolUse",
    "tool_name": "Shell",
    "tool_input": {"command": "make deploy"},
    "tool_use_id": "call-77",
}


async def test_the_kernel_script_round_trips_through_the_loopback_callback(tmp_path: Path) -> None:
    stack = Stack()
    token = await stack.service.issue(_context(), ttl=timedelta(minutes=10))
    config = uvicorn.Config(
        create_hook_callback_app(stack.service), host="127.0.0.1", port=0, log_level="warning"
    )
    server = uvicorn.Server(config)
    serving = asyncio.create_task(server.serve())
    try:
        for _ in range(200):
            if server.started:
                break
            await asyncio.sleep(0.05)
        assert server.started
        port = server.servers[0].sockets[0].getsockname()[1]
        url = f"http://127.0.0.1:{port}/v1/applications/biotech/internal/hook-callback"
        script = await asyncio.to_thread(_workspace, tmp_path / "lease", url, token)

        code, out = await asyncio.to_thread(
            _run, script, "before_tool", "mc.operation_intent", PAYLOAD
        )
        assert (code, out) == (0, {"permission": "allow"})
        (intent,) = stack.intents.intents()
        assert intent.effect_ref == "tool_use:call-77"

        await stack.fences.persist(
            StopFence(
                request_scope=SCOPE,
                run_id=RUN,
                generation=1,
                command_id="cancel-now",
                reason="operator stop",
                requested_at=stack.clock.now,
            )
        )
        fenced = {**PAYLOAD, "tool_use_id": "call-78"}
        code, out = await asyncio.to_thread(_run, script, "before_tool", "mc.stop_fence", fenced)
        assert code == 2 and out["permission"] == "deny"
        assert "stop-fenced" in out["agent_message"]
    finally:
        server.should_exit = True
        await serving

    # The listener is gone: the fail-closed hook denies.
    code, out = await asyncio.to_thread(
        _run, script, "before_shell", "mc.stop_fence", {"command": "ls"}
    )
    assert code == 2 and out["permission"] == "deny"
    assert "callback failed" in out["agent_message"]
    assert token not in json.dumps(out)


@pytest.mark.parametrize("event", ["after_tool", "stop"])
async def test_an_unreachable_callback_never_blocks_observation_events(
    tmp_path: Path, event: str
) -> None:
    script = await asyncio.to_thread(
        _workspace, tmp_path / "lease", "http://127.0.0.1:9/unreachable", "token-x"
    )
    code, _out = await asyncio.to_thread(_run, script, event, "mc.frame_capture", {})
    assert code == 0
