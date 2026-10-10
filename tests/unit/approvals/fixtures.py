"""FIXTURE builders for MP-11 approval tests; no provider, MCP server or database is contacted.

The native identities below are fixture strings shaped like the provider fields they stand
for (Claude ``ToolPermissionContext.tool_use_id``, a Codex JSON-RPC request id); they prove
nothing about live provider behaviour.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from mission_control.application.execution.approvals import (
    ApprovalResolutionRequest,
    ApprovalTaskView,
    ElicitationPrompt,
    QuestionPrompt,
)
from mission_control.application.execution.approvals_broker import NativeApprovalRequest
from mission_control.application.execution.approvals_governed import (
    GovernedEffectResult,
    GovernedIntent,
)
from mission_control.domain.execution.approvals import NativeApprovalCorrelation
from tests.fixtures.provider_frames import SCOPE

OPENED_AT = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
POLICY = "sha256:" + "a" * 64
OTHER_POLICY = "sha256:" + "b" * 64
RUN = "run-1"
EXECUTION = "harness-exec-1"
REVIEWER = "owner"


class Clock:
    """A settable FIXTURE clock."""

    def __init__(self, at: datetime = OPENED_AT) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at

    def advance(self, seconds: float) -> None:
        self.at = self.at + timedelta(seconds=seconds)


def permission_request(**updates: Any) -> NativeApprovalRequest:
    values: dict[str, Any] = {
        "request_scope": SCOPE,
        "run_id": RUN,
        "lane_profile": "claude_agent_sdk",
        "origin": "provider_permission",
        "harness_execution_id": EXECUTION,
        "generation": 1,
        "native": NativeApprovalCorrelation(
            native_session_ref="fixture-session",
            native_turn_ref="fixture-turn",
            tool_call_ref="toolu_fixture_1",
        ),
        "tool_name": "Bash",
        "arguments": {"command": "git push origin main", "timeout": 30},
        "effect_kind": "shell",
        "policy_digest": POLICY,
        "reviewers": (REVIEWER,),
        "prompt": "Allow the agent to push?",
        "replay_strategy": "reissue_native_request",
    }
    values.update(updates)
    return NativeApprovalRequest(**values)


def connection_scoped_request(request_ref: str = "7", **updates: Any) -> NativeApprovalRequest:
    """A Codex-shaped request: only a connection-scoped JSON-RPC id, no stable call id."""

    values: dict[str, Any] = {
        "lane_profile": "codex",
        "native": NativeApprovalCorrelation(
            native_session_ref="fixture-thread", native_request_ref=request_ref
        ),
        "replay_strategy": "restart_at_safe_boundary",
    }
    values.update(updates)
    return permission_request(**values)


def question_request(**updates: Any) -> NativeApprovalRequest:
    values: dict[str, Any] = {
        "origin": "provider_question",
        "tool_name": "AskUserQuestion",
        "arguments": {"question": "Which branch?"},
        "effect_kind": "other",
        "question": QuestionPrompt(question="Which branch?", options=("main", "dev")),
        "native": NativeApprovalCorrelation(tool_call_ref="toolu_question_1"),
    }
    values.update(updates)
    return permission_request(**values)


def elicitation_request(**updates: Any) -> NativeApprovalRequest:
    values: dict[str, Any] = {
        "origin": "mcp_elicitation",
        "tool_name": None,
        "arguments": {"server": "fixture-mcp", "message": "Pick an environment"},
        "effect_kind": "mcp",
        "elicitation": ElicitationPrompt(
            mode="form",
            message="Pick an environment",
            requested_schema={
                "type": "object",
                "properties": {"environment": {"type": "string"}},
                "required": ["environment"],
            },
            server_name="fixture-mcp",
        ),
        "native": NativeApprovalCorrelation(native_request_ref="elicit-1", connection_scoped=True),
        "replay_strategy": "deny_and_park",
    }
    values.update(updates)
    return permission_request(**values)


def answer(task: ApprovalTaskView, **updates: Any) -> ApprovalResolutionRequest:
    values: dict[str, Any] = {
        "request_id": "req-1",
        "expected_task_version": task.version,
        "decision": "approve",
        "reviewed_digest": task.packet.review_digest,
    }
    values.update(updates)
    return ApprovalResolutionRequest(**values)


class RecordingExecutor:
    """FIXTURE governed executor: records every call; optionally slow or failing."""

    def __init__(self, *, delay: float = 0.0, fail: bool = False) -> None:
        self.calls: list[str] = []
        self.delay = delay
        self.fail = fail

    async def execute(self, intent: GovernedIntent) -> GovernedEffectResult:
        import asyncio

        self.calls.append(intent.intent_id)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail:
            raise RuntimeError("fixture executor failure")
        return GovernedEffectResult(
            outcome="applied",
            output={"echo": intent.arguments},
            external_ref=f"fixture:{intent.intent_id}",
        )
