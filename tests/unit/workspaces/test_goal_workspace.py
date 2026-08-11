from __future__ import annotations

import json
import stat
from hashlib import sha256
from pathlib import Path

import pytest

from app.application.workspaces.goal_workspace import (
    GoalScopeViolation,
    GoalWorkspaceService,
    GoalWorkspaceSpec,
)
from app.domain.operation_execution.errors import WorkspaceSlotConflict
from app.domain.run_control.errors import IdempotencyConflict


def digest(value: str) -> str:
    return f"sha256:{sha256(value.encode()).hexdigest()}"


def spec() -> GoalWorkspaceSpec:
    return GoalWorkspaceSpec(
        namespace_id="tenant-1",
        run_id="run-goal-1",
        objective="Produce a source-grounded company inventory.",
        acceptance_contract="An independent verifier accepts every current-company claim.",
        protected_scope_digests={
            "inputs": digest("admitted-inputs"),
            "authority": digest("bounded-authority"),
            "budget": digest("token-budget"),
        },
    )


def test_goal_truth_is_read_only_and_regenerated_before_prompt_projection(
    tmp_path: Path,
) -> None:
    service = GoalWorkspaceService(tmp_path)
    workspace = service.initialize(spec())
    expected_markdown = workspace.goal_markdown_path.read_bytes()
    expected_state = workspace.goal_state_path.read_bytes()

    assert {
        path.name
        for path in (
            workspace.goal_directory,
            workspace.work_directory,
            workspace.checkpoints_directory,
            workspace.handoffs_directory,
            workspace.agents_directory,
        )
    } == {"goal", "work", "checkpoints", "handoffs", "agents"}
    assert not workspace.goal_markdown_path.stat().st_mode & stat.S_IWUSR
    assert not workspace.goal_state_path.stat().st_mode & stat.S_IWUSR

    workspace.goal_markdown_path.chmod(stat.S_IWRITE | stat.S_IREAD)
    workspace.goal_markdown_path.write_text("broadened objective", encoding="utf-8")
    workspace.goal_state_path.chmod(stat.S_IWRITE | stat.S_IREAD)
    workspace.goal_state_path.write_text('{"tampered":true}', encoding="utf-8")

    projection = service.project_prompt(workspace)

    assert workspace.goal_markdown_path.read_bytes() == expected_markdown
    assert workspace.goal_state_path.read_bytes() == expected_state
    assert not workspace.goal_markdown_path.stat().st_mode & stat.S_IWUSR
    assert not workspace.goal_state_path.stat().st_mode & stat.S_IWUSR
    assert expected_markdown.decode() in projection.prompt
    assert expected_state.decode() in projection.prompt


def test_prompt_injects_exact_goal_view_and_latest_accepted_handoff(tmp_path: Path) -> None:
    service = GoalWorkspaceService(tmp_path)
    workspace = service.initialize(spec())
    checkpoint = service.write_checkpoint(
        workspace,
        agent_run_id="agent-1",
        iteration=1,
        content="Inventory drafted; ownership evidence remains.",
        idempotency_key="checkpoint-agent-1",
    )
    first = service.accept_handoff(
        workspace,
        from_agent_run_id="agent-1",
        iteration=1,
        summary="Draft inventory exists.",
        instructions="Verify present ownership and operating status.",
        checkpoint_id=checkpoint.checkpoint_id,
        proposed_scope_digests=workspace.spec.scope_map,
        idempotency_key="handoff-agent-1",
    )
    second = service.accept_handoff(
        workspace,
        from_agent_run_id="agent-2",
        iteration=2,
        summary="Ownership evidence collected.",
        instructions="Resolve the final ambiguous company.",
        checkpoint_id=checkpoint.checkpoint_id,
        proposed_scope_digests=workspace.spec.scope_map,
        idempotency_key="handoff-agent-2",
    )

    projection = service.project_prompt(workspace)

    assert first.handoff_id not in projection.prompt
    assert second.handoff_id in projection.prompt
    assert second.summary in projection.prompt
    assert second.instructions in projection.prompt
    assert projection.latest_handoff_id == second.handoff_id
    assert projection.goal_view_digest == workspace.goal_view_digest
    assert projection.content_digest == digest(projection.prompt)


def test_sequential_agent_runs_reuse_workspace_and_shared_work_directory(
    tmp_path: Path,
) -> None:
    service = GoalWorkspaceService(tmp_path)
    first_view = service.initialize(spec())
    first_run = service.begin_agent_run(
        first_view,
        agent_run_id="agent-run-1",
        lease_owner_id="worker-1",
    )
    shared_file = first_run.workspace.work_directory / "research.md"
    shared_file.write_text("durable work", encoding="utf-8")
    service.end_agent_run(first_run)

    reopened = service.initialize(spec())
    second_run = service.begin_agent_run(
        reopened,
        agent_run_id="agent-run-2",
        lease_owner_id="worker-2",
    )

    assert reopened.workspace_id == first_view.workspace_id
    assert second_run.workspace.workspace_id == first_run.workspace.workspace_id
    assert second_run.workspace.work_directory == first_run.workspace.work_directory
    assert shared_file.read_text(encoding="utf-8") == "durable work"
    assert second_run.agent_directory != first_run.agent_directory
    service.end_agent_run(second_run)


def test_handoff_is_immutable_idempotent_and_cannot_change_scope(tmp_path: Path) -> None:
    service = GoalWorkspaceService(tmp_path)
    workspace = service.initialize(spec())
    checkpoint = service.write_checkpoint(
        workspace,
        agent_run_id="agent-1",
        iteration=1,
        content="checkpoint",
        idempotency_key="checkpoint-1",
    )
    values = {
        "from_agent_run_id": "agent-1",
        "iteration": 1,
        "summary": "summary",
        "instructions": "continue",
        "checkpoint_id": checkpoint.checkpoint_id,
        "proposed_scope_digests": workspace.spec.scope_map,
        "idempotency_key": "handoff-1",
    }

    first = service.accept_handoff(workspace, **values)
    replay = service.accept_handoff(workspace, **values)

    assert replay == first
    path = workspace.handoffs_directory / f"{first.handoff_id}.json"
    assert not path.stat().st_mode & stat.S_IWUSR
    assert json.loads(path.read_text(encoding="utf-8"))["content_digest"] == first.content_digest

    with pytest.raises(IdempotencyConflict, match="different content"):
        service.accept_handoff(workspace, **{**values, "summary": "conflicting summary"})

    broadened = {**workspace.spec.scope_map, "budget": digest("larger-budget")}
    with pytest.raises(GoalScopeViolation, match="frozen protected scope"):
        service.accept_handoff(
            workspace,
            **{
                **values,
                "proposed_scope_digests": broadened,
                "idempotency_key": "handoff-broadened",
            },
        )


def test_handoff_failure_produces_stable_fallback_checkpoint(tmp_path: Path) -> None:
    service = GoalWorkspaceService(tmp_path)
    workspace = service.initialize(spec())

    first = service.record_handoff_failure(
        workspace,
        agent_run_id="agent-1",
        iteration=1,
        idempotency_key="handoff-failure-1",
        failure_reason="agent exceeded its handoff turn allowance",
        last_agent_output="partial evidence list",
    )
    replay = service.record_handoff_failure(
        workspace,
        agent_run_id="agent-1",
        iteration=1,
        idempotency_key="handoff-failure-1",
        failure_reason="agent exceeded its handoff turn allowance",
        last_agent_output="partial evidence list",
    )

    assert first == replay
    assert first.kind == "fallback"
    assert first.failure_reason == "agent exceeded its handoff turn allowance"
    assert "Resume from the frozen goal" in first.content
    assert "partial evidence list" in first.content
    assert (workspace.checkpoints_directory / f"{first.checkpoint_id}.json").is_file()


def test_concurrent_writable_lease_owners_are_rejected(tmp_path: Path) -> None:
    service = GoalWorkspaceService(tmp_path)
    workspace = service.initialize(spec())
    first = service.begin_agent_run(
        workspace,
        agent_run_id="agent-run-1",
        lease_owner_id="worker-1",
    )

    with pytest.raises(WorkspaceSlotConflict, match="another agent run"):
        service.begin_agent_run(
            workspace,
            agent_run_id="agent-run-2",
            lease_owner_id="worker-2",
        )

    service.end_agent_run(first)
    second = service.begin_agent_run(
        workspace,
        agent_run_id="agent-run-2",
        lease_owner_id="worker-2",
    )
    service.end_agent_run(second)
