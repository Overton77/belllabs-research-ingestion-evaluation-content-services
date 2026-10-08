"""FT-A5 live proof (opt-in): a real model's shell call is denied by the policy hook.

Skipped unless ``MC_LIVE_HOOK_MODEL=1`` and ``OPENAI_API_KEY`` are set: it spends one small
model call. The deterministic equivalent runs in tests/unit/deep_agents/test_hook_graph.py.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from deepagents import create_deep_agent
from deepagents.backends import StateBackend
from langchain_core.messages import HumanMessage, ToolMessage
from langchain_core.tools import tool

from mission_control.adapters.deep_agents.hooks import (
    HookContext,
    HookDispatcher,
    HookScriptMiddleware,
    KernelHookPorts,
    RecordingFrameSink,
    ResolvedHookScript,
)
from mission_control.contracts.hooks import HookScope
from mission_control.domain.capabilities.hooks import HookEvent

pytestmark = pytest.mark.skipif(
    os.environ.get("MC_LIVE_HOOK_MODEL") != "1" or not os.environ.get("OPENAI_API_KEY"),
    reason="live model hook proof is opt-in (MC_LIVE_HOOK_MODEL=1, OPENAI_API_KEY)",
)

POLICY = Path(__file__).resolve().parents[3] / "scripts" / "hooks" / "policy_template" / "policy.py"
RAN: list[str] = []


@tool
def shell(command: str) -> str:
    """Run a shell command (fixture: records instead of executing)."""
    RAN.append(command)
    return "ok"


@pytest.mark.asyncio
async def test_live_model_shell_call_is_denied(tmp_path: Path) -> None:
    from langchain_openai import ChatOpenAI

    frames = RecordingFrameSink()
    dispatcher = HookDispatcher(
        context=HookContext(scope=HookScope(installation_id="live", application_id="biotech")),
        kernel_hooks=KernelHookPorts(frames=frames).build(),
        scripts=(
            ResolvedHookScript(
                hook_id="hook.mc-policy-template",
                events=frozenset({HookEvent.BEFORE_SHELL}),
                interpreter="python",
                entrypoint=POLICY,
                fail_closed=True,
            ),
        ),
        frames=frames,
        cwd=tmp_path,
    )
    agent = create_deep_agent(
        model=ChatOpenAI(model="gpt-4.1-mini", temperature=0),
        tools=[shell],
        middleware=[HookScriptMiddleware(dispatcher)],
        backend=StateBackend(),
        system_prompt="Use the shell tool exactly once with the command the user gives.",
    )
    result = await agent.ainvoke({"messages": [HumanMessage(content="Run: rm -rf /")]})
    denied = [m for m in result["messages"] if isinstance(m, ToolMessage) and m.status == "error"]
    assert RAN == [] and denied
