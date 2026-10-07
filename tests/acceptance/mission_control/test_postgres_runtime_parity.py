"""PostgreSQL-only parity of the existing governed API and production Temporal workers.

The exact deterministic local model still runs through real DeepAgents, sync children,
MCP, persistent saver/store, operation journals and family settlement. No Mongo connection,
provider experiment, schema reset, or prerequisite skip occurs in this qualification.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import asyncpg
import pytest
from tests.fixtures.mission_control_common_db import legacy_poison_intact
from tests.fixtures.mission_control_production_stack import (
    open_postgres_production_stack,
)
from tests.fixtures.mission_control_production_stack import (
    start_local as _start_local,
)
from tests.fixtures.rrm009_production_harness import (
    STAGE_OBJECTIVE,
    ProductionStack,
    _admit,
    _calls,
    _command,
    _holds_wait,
    _launch,
    _lineages,
    _operation_payloads,
    _pinned_summary,
    _receipt_states,
    _release_wait,
    _replay,
    _run,
    _saver_checkpoints,
    _send,
    _terminal,
    _until,
    _visible,
    _wait_for,
    canonical_evidence,
    record_parity_trace,
)
from tests.fixtures.rrm009_production_stack import (
    AGENT_COGNITIVE_QUEUE,
    SCOPE,
    goal_input,
    goal_templates,
    publish_technical_catalog,
    stage_input,
    stage_templates,
)
from tests.fixtures.run_authority_digest import run_authority_digest

from mission_control.adapters.postgres.orchestration.goal_directed_repository import (
    PostgresGoalDirectedDocumentRepository,
)
from mission_control.adapters.postgres.orchestration.stagegraph_repository import (
    PostgresStageGraphOperationTemplateRepository,
)
from mission_control.application.execution.run_launch import fork_semantic_input_binding_ref
from mission_control.application.recovery.run_forks import ForkPatchPolicyRegistry
from mission_control.bootstrap.technical_api import api
from mission_control.domain.policies.contracts import (
    CancelAction,
    PauseAction,
    PauseDecision,
    ResumeAction,
    ResumeDecision,
    RunOutcome,
)
from mission_control.domain.policies.forks import (
    ForkPatchPolicy,
    PatchablePath,
    stage_objective_path,
)

pytestmark = pytest.mark.common_db


@pytest.fixture
async def stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[ProductionStack]:
    async with open_postgres_production_stack(root=tmp_path, monkeypatch=monkeypatch) as production:
        yield production


# --- StageGraph --------------------------------------------------------------------------------


async def _assert_completed_with_baseline_released(stack: ProductionStack, run_id: str) -> None:
    """RRM-021: a run admitted with a non-empty baseline completed, with nothing reserved."""

    run = await _run(stack, run_id)
    assert run["terminal_outcome"] == "completed", run
    budget = await stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    assert budget.status_code == 200, budget.text
    body = budget.json()
    assert not any(body["reserved"].values()), body
    assert "baseline" not in body["reservations"], body


@pytest.mark.asyncio
async def test_stagegraph_runs_through_the_production_composition_with_fork_relay_and_inspection(
    stack: ProductionStack,
) -> None:
    catalog = await publish_technical_catalog(
        stack.control_plane, family="StageGraph", now=datetime.now(UTC)
    )
    templates = PostgresStageGraphOperationTemplateRepository(stack.worker_pool)
    policies = cast(ForkPatchPolicyRegistry, api.state.fork_patch_policies)
    policies.register(
        catalog.workflow_ref.digest,
        ForkPatchPolicy(
            policy_id="fork-patch-policy:rrm009-technical-stagegraph",
            family="stage_graph",
            patchable=(
                PatchablePath(path=stage_objective_path("review"), invalidates=("review",)),
            ),
        ),
    )
    source_run = await _admit(stack, catalog, "rrm009-stagegraph-source")
    source_binding = f"semantic-input:rrm009:{source_run}"
    await templates.persist_templates(
        request_scope=SCOPE,
        semantic_input_binding_ref=source_binding,
        templates=stage_templates(stack.technical, catalog),
        recorded_at=datetime.now(UTC),
    )
    persisted = await templates.get_template(
        semantic_input_binding_ref=source_binding,
        operation_request_key="draft/execute/default",
        request_scope=SCOPE,
        run_id=source_run,
    )
    assert persisted.deep_agent_binding is not None
    assert persisted.deep_agent_binding.task_queue == AGENT_COGNITIVE_QUEUE
    assert AGENT_COGNITIVE_QUEUE in stack.worker_queues, stack.worker_queues
    # A launch that does not bind the admitted authority is refused before Temporal.
    stale = await stack.http.post(
        f"/run-control/v1/runs/{source_run}/launch",
        json={
            "request_scope": SCOPE,
            "run_id": source_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, source_run, source_binding, 7)),
        },
    )
    assert stale.status_code == 422 and stale.json()["detail"]["code"] == "stale_run_version"
    receipt = await _launch(
        stack,
        source_run,
        {
            "request_scope": SCOPE,
            "run_id": source_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, source_run, source_binding, 1)),
        },
    )
    assert receipt["workflow_id"] == f"belllabs-run/{source_run}"
    assert receipt["parent_run_id"] is None
    # A repeated launch of the same pending run is idempotent at the facade.
    again = await stack.http.post(
        f"/run-control/v1/runs/{source_run}/launch",
        json={
            "request_scope": SCOPE,
            "run_id": source_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, source_run, source_binding, 1)),
        },
    )
    # While the run is still pending the duplicate start resolves to the same execution;
    # once the family has started it the launch is refused, never started twice.
    if again.status_code == 202:
        assert (again.json()["workflow_id"], again.json()["temporal_run_id"]) == (
            receipt["workflow_id"],
            receipt["temporal_run_id"],
        ), again.text
    else:
        assert again.status_code == 409, again.text
        assert again.json()["detail"]["code"] == "run_not_pending", again.text

    # `draft` settles (sync subagent, report written, MCP called); the run holds its wait.
    await _wait_for(stack, source_run, lambda: _holds_wait(stack, source_run), 150)
    snapshot = await stack.http.post(
        f"/run-control/v1/runs/{source_run}/snapshots", json={"request_scope": SCOPE}
    )
    assert snapshot.status_code == 201, snapshot.text
    manifest = snapshot.json()
    assert manifest["boundary_kind"] == "stage_settled"
    fork = await stack.http.post(
        f"/run-control/v1/runs/{source_run}/forks",
        json={
            "request_scope": SCOPE,
            "request_id": f"fork-{source_run[:8]}",
            "idempotency_key": f"fork-{source_run[:8]}",
            "snapshot_id": manifest["snapshot_id"],
            "snapshot_digest": manifest["snapshot_digest"],
            "changes": [{"path": stage_objective_path("review"), "value": STAGE_OBJECTIVE}],
            "invalidation_frontier": ["review"],
            "baseline_reservations": {"tokens.total": 20},
            "sponsorship_ref": "sponsorship:test",
            "approval_refs": ["approval:test"],
            "reason": "RRM-009 technical fork",
        },
    )
    assert fork.status_code == 201, fork.text
    derived_run = fork.json()["target_run_id"]
    fork_request_id = fork.json()["request_id"]
    derived_binding = fork_semantic_input_binding_ref(fork_request_id)
    # The governed launch of the derived run: parent from the receipt, patched templates.
    # RRM-021: the governed launch still refuses a family input whose baseline differs from
    # the admitted one (RRM-009), now that the StageGraph settles the baseline it carries.
    wrong_baseline = await stack.http.post(
        f"/run-control/v1/runs/{derived_run}/launch",
        json={
            "request_scope": SCOPE,
            "run_id": derived_run,
            "family": "StageGraph",
            "stagegraph": {
                **asdict(stage_input(catalog, derived_run, derived_binding, 1)),
                "baseline_reservation": {"tokens.total": 21},
            },
            "source_semantic_input_binding_ref": source_binding,
        },
    )
    assert wrong_baseline.status_code == 422, wrong_baseline.text
    assert wrong_baseline.json()["detail"]["code"] == "budget_mismatch"
    wrong_binding = await stack.http.post(
        f"/run-control/v1/runs/{derived_run}/launch",
        json={
            "request_scope": SCOPE,
            "run_id": derived_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, derived_run, "semantic-input:other", 1)),
            "source_semantic_input_binding_ref": source_binding,
        },
    )
    assert wrong_binding.status_code == 422
    assert wrong_binding.json()["detail"]["code"] == "fork_binding_mismatch"
    derived_receipt = await _launch(
        stack,
        derived_run,
        {
            "request_scope": SCOPE,
            "run_id": derived_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, derived_run, derived_binding, 1)),
            "source_semantic_input_binding_ref": source_binding,
        },
    )
    assert derived_receipt["parent_run_id"] == source_run
    assert derived_receipt["fork_request_id"] == fork_request_id
    derived_templates = await templates.list_templates(
        request_scope=SCOPE, semantic_input_binding_ref=derived_binding
    )
    assert (
        derived_templates["review/execute/default"].prompt_segments[-1].content == STAGE_OBJECTIVE
    )
    assert (
        derived_templates["draft/execute/default"]
        == (
            await templates.list_templates(
                request_scope=SCOPE, semantic_input_binding_ref=source_binding
            )
        )["draft/execute/default"]
    )
    assert await _visible(stack.client, f"BellLabsParentRunId = '{source_run}'", 1) == 1
    await _release_wait(stack, derived_run)
    await _wait_for(stack, derived_run, lambda: _terminal(stack, derived_run), 240)
    await _assert_completed_with_baseline_released(stack, derived_run)

    # RRM-007 relay drill on a persistent namespace: the pause is accepted while the Temporal
    # transport is down, and delivered and applied once the server is back and the relay runs.
    await stack.env.shutdown()
    run = await _run(stack, source_run)
    pause_id = f"pause:{source_run[:8]}"
    paused = await _send(
        stack,
        source_run,
        _command(
            source_run,
            run["version"],
            pause_id,
            PauseAction(
                decision=PauseDecision(
                    decision_id=pause_id,
                    scope=frozenset({"run"}),
                    reason="RRM-009 relay drill: operator hold while the transport is down",
                    authority_ref="authority:lifecycle",
                ),
                # The source holds its declared wait, so no admissible work remains.
                runnable_work_remains=False,
            ),
            "workflow_run.pause",
        ),
    )
    assert (
        paused["status"] == "accepted" and paused["reason_code"] == "accepted_pending_application"
    )
    assert await _receipt_states(stack, source_run, pause_id) == ["accepted"]
    stack.env = await _start_local(stack.temporal_db)
    relay = api.state.boundary_command_relay

    async def pause_applied() -> bool:
        await relay.run_once()
        return (await _receipt_states(stack, source_run, pause_id))[-1:] == ["applied"]

    await _until(pause_applied, seconds=240)
    assert await _receipt_states(stack, source_run, pause_id) == [
        "accepted",
        "delivered",
        "applied",
    ]
    assert (await _run(stack, source_run))["phase"] == "paused"
    run = await _run(stack, source_run)
    resume_id = f"resume:{source_run[:8]}"
    await _send(
        stack,
        source_run,
        _command(
            source_run,
            run["version"],
            resume_id,
            ResumeAction(
                decision=ResumeDecision(
                    decision_id=resume_id,
                    pause_decision_id=pause_id,
                    reason="RRM-009 relay drill: operator release",
                    authority_ref="authority:lifecycle",
                )
            ),
            "workflow_run.resume",
        ),
    )

    async def resumed() -> bool:
        return (await _receipt_states(stack, source_run, resume_id))[-1:] == ["applied"]

    await _until(resumed)
    await _release_wait(stack, source_run)
    await _wait_for(stack, source_run, lambda: _terminal(stack, source_run), 240)
    await _assert_completed_with_baseline_released(stack, source_run)

    # Visibility (REQ-CP-EXEC-015) on the persistent namespace: root, family and operations.
    assert await _visible(stack.client, f"BellLabsRunId = '{source_run}'", 4) == 4
    # The derived run's `draft` unit also runs its operation workflow, which reuses the
    # source's settled result by immutable ref (RRM-006) without any cognition.
    assert await _visible(stack.client, f"BellLabsRunId = '{derived_run}'", 4) == 4
    derived_ids = sorted(
        [
            execution.id
            async for execution in stack.client.list_workflows(f"BellLabsRunId = '{derived_run}'")
        ]
    )
    assert derived_ids == sorted(
        [
            f"belllabs-run/{derived_run}",
            f"family/{derived_run}/1",
            *(
                f"operation/{derived_run}:operation:execution-epoch:1:stage:{stage}:mapped:none:"
                "workflow-cycle:0:stage-cycle:0:slot:execute:attempt:1"
                for stage in ("draft", "review")
            ),
        ]
    )
    source = await _run(stack, source_run)
    derived = await _run(stack, derived_run)
    assert (source["phase"], source["terminal_outcome"]) == ("terminal", "completed")
    assert (derived["phase"], derived["terminal_outcome"]) == ("terminal", "completed")
    source_outputs = sorted(item["output_ref"] for item in source["accepted_output_evidence"])
    derived_outputs = sorted(item["output_ref"] for item in derived["accepted_output_evidence"])
    by_stage = {
        run_id: {ref.split(":")[2]: ref for ref in outputs}
        for run_id, outputs in ((source_run, source_outputs), (derived_run, derived_outputs))
    }
    assert by_stage[derived_run]["draft"] == by_stage[source_run]["draft"]  # reused by ref
    assert by_stage[derived_run]["review"] != by_stage[source_run]["review"]  # patched

    # Sync subagent, writable-slot capture and the exact MCP tool, per operation.
    calls = _calls(stack, source_run)
    assert sorted(calls) and all(value == {"parent": 4, "child": 1} for value in calls.values())
    assert next(iter(_calls(stack, derived_run).values())) == {"parent": 4, "child": 1}
    payloads = [
        *await _operation_payloads(stack, source_run),
        *await _operation_payloads(stack, derived_run),
    ]
    facts = [(payload["structured_output"] or {}).get("facts") for payload in payloads]
    # Source draft and review, the derived draft's reused result (the source draft's output,
    # with no cognition of its own) and the derived review.
    assert len(facts) == 4 and all(item == {"child": True, "mcp": True} for item in facts), facts
    lineages = _lineages(payloads)
    assert len(lineages) == 3  # a reused result carries no lineage of its own
    for lineage in lineages:
        # The sync subagent's model call is charged to the parent operation (REQ-CP-DA-007).
        assert lineage["usage"]["amounts"] == {"model.turns": 5, "tokens.total": 25}
        assert [call["scope"] for call in lineage["usage"]["model_calls"]].count("subordinate") == 1
        assert lineage["invoked"] == {
            "framework": ["write_file"],
            "mcp": ["lookup_binding_marker"],
            "sync_subagent": ["task"],
        }
        assert lineage["credential_refs"] == ["environment:OPENAI_API_KEY"]
        assert lineage["placement"]["task_queue"] == AGENT_COGNITIVE_QUEUE
    async with stack.owner_pool.acquire() as connection:
        candidates = await connection.fetch(
            """SELECT DISTINCT entry->>'candidate_id' AS candidate_id
               FROM mission_control.workspace_manifest,
                    jsonb_array_elements(payload->'entries') entry
               WHERE entry->>'kind' = 'local_candidate'"""
        )
    assert len(candidates) >= 3

    # Inspection (REQ-CP-RUN-011/012) over the composed sources: Temporal current, the
    # persistent saver's history served, async-child detail composed.
    read = await stack.http.get(
        f"/run-control/v1/inspection/runs/{source_run}", params={"request_scope": SCOPE}
    )
    assert read.status_code == 200, read.text
    sections = read.json()["sections"]
    assert sections["temporal"]["freshness"] == "current", sections["temporal"]
    assert sections["async_children_detail"]["freshness"] == "current", sections
    unit_key = read.json()["data"]["units"][0]["unit_key"]
    # REQ-CP-EXEC-015: the unit's operation execution is listed by its unit key, and the
    # run's root, family and operations by their kind, on the persistent namespace.
    assert await _visible(stack.client, f"BellLabsUnitKey = '{unit_key}'", 1) == 1
    kinds = sorted(
        [
            str(execution.search_attributes.get("BellLabsWorkflowKind", ["?"])[0])
            async for execution in stack.client.list_workflows(f"BellLabsRunId = '{source_run}'")
        ]
    )
    assert kinds == ["family", "operation", "operation", "root"], kinds
    history = await stack.http.get(
        f"/run-control/v1/inspection/runs/{source_run}/units/{unit_key}/checkpoints",
        params={"request_scope": SCOPE},
    )
    assert history.status_code == 200, history.text
    history_sections = history.json()["sections"]
    assert history_sections["checkpoints"]["freshness"] == "current", history_sections
    assert len(history.json()["data"]["entries"]) >= 4
    assert await _saver_checkpoints(stack.owner_pool) > 0
    replayed = await _replay(
        stack.client,
        [
            f"belllabs-run/{source_run}",
            f"family/{source_run}/1",
            f"belllabs-run/{derived_run}",
            f"family/{derived_run}/1",
        ],
    )
    # The canonical rows behind both runs exist and link (not only the HTTP projections).
    source_rows = await canonical_evidence(stack, source_run)
    derived_rows = await canonical_evidence(stack, derived_run)
    assert source_rows["counts"]["settled_claims"] == 2, source_rows
    assert source_rows["terminal_outcome"] == derived_rows["terminal_outcome"] == "succeeded"
    record_parity_trace(
        "stagegraph_fork_relay_inspection",
        {
            "database": stack.database.name if stack.database else None,
            "source": source_rows,
            "derived": derived_rows,
            "fork_request_id": fork_request_id,
        },
    )
    ready = await stack.http.get("/health/ready")
    # RRM-009 review: the unauthenticated probe discloses status and mode only.
    assert ready.status_code == 200 and ready.json() == {
        "status": "ready",
        "mode": "runtime-control",
    }, ready.text
    print(
        "RRM-009 EVIDENCE stagegraph:",
        json.dumps(
            {
                "source_run": source_run,
                "derived_run": derived_run,
                "fork_request_id": fork_request_id,
                "pause_receipts": await _receipt_states(stack, source_run, pause_id),
                "outputs": {"source": source_outputs, "derived": derived_outputs},
                "model_calls": calls,
                "facts": facts,
                "candidates": len(candidates),
                "saver_checkpoints": await _saver_checkpoints(stack.owner_pool),
                "inspection": {name: item["freshness"] for name, item in sections.items()},
                "replayed_events": replayed,
                "readiness": api.state.runtime_control.readiness,
                "capabilities": _pinned_summary(stack),
                "lineage": lineages[0],
            },
            sort_keys=True,
            default=str,
        ),
    )


@pytest.mark.asyncio
async def test_stagegraph_cancel_at_declared_wait_closes_real_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The legacy-poison parity run: legacy namespaces exist with sentinel rows that no
    # restricted role may touch; the whole production stack must never fall back to them.
    async with open_postgres_production_stack(
        root=tmp_path, monkeypatch=monkeypatch, legacy_poison=True
    ) as stack:
        await _cancel_at_declared_wait(stack)
        assert stack.database is not None
        assert await legacy_poison_intact(stack.database.owner_dsn)
        async with stack.worker_pool.acquire() as connection:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.fetch("SELECT * FROM belllabs_control.workflow_runs")


async def _cancel_at_declared_wait(stack: ProductionStack) -> None:
    catalog = await publish_technical_catalog(
        stack.control_plane, family="StageGraph", now=datetime.now(UTC)
    )
    run_id = await _admit(stack, catalog, "mission-control-stage-cancel")
    binding_ref = f"semantic-input:mission-control:{run_id}"
    await PostgresStageGraphOperationTemplateRepository(stack.worker_pool).persist_templates(
        request_scope=SCOPE,
        semantic_input_binding_ref=binding_ref,
        templates=stage_templates(stack.technical, catalog),
        recorded_at=datetime.now(UTC),
    )
    await _launch(
        stack,
        run_id,
        {
            "request_scope": SCOPE,
            "run_id": run_id,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, run_id, binding_ref, 1)),
        },
    )
    await _wait_for(stack, run_id, lambda: _holds_wait(stack, run_id), 180)
    run = await _run(stack, run_id)
    decision = await _send(
        stack,
        run_id,
        _command(run_id, run["version"], f"cancel:{run_id}", CancelAction(), "workflow_run.cancel"),
    )
    # Cancel is the existing immediate lifecycle transition; pause/resume above use
    # queued boundary application. Preserve and prove that distinction.
    assert decision["reason_code"] == "accepted" and decision["phase"] == "cancelling", decision
    await _wait_for(stack, run_id, lambda: _terminal(stack, run_id), 120)
    terminal = await _run(stack, run_id)
    assert terminal["terminal_outcome"] == "cancelled", terminal
    # StageGraph records both the journal receipt and the family acceptance receipt.
    # Cancellation must preserve that settled draft without executing the waiting review.
    assert (
        terminal["accepted_operation_settlement_evidence"]
        == run["accepted_operation_settlement_evidence"]
    )
    assert len(_calls(stack, run_id)) == 1
    budget = await stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    assert budget.status_code == 200, budget.text
    assert not any(budget.json()["reserved"].values()), budget.text
    rows = await canonical_evidence(stack, run_id)
    assert rows["terminal_outcome"] == "cancelled", rows
    assert rows["counts"]["settled_claims"] == 1, rows
    # A terminal run's authority rows and saver checkpoints no longer change.
    first = await run_authority_digest(stack.owner_pool, run_id, request_scope=SCOPE)
    second = await run_authority_digest(stack.owner_pool, run_id, request_scope=SCOPE)
    assert first == second, (first, second)
    empty = "d41d8cd98f00b204e9800998ecf8427e"
    assert first["mission_run"] != empty and first["activation"] != empty, first
    assert first["checkpoint_transition"] != empty and first["saver_checkpoints"] != empty, first
    record_parity_trace(
        "stagegraph_cancel_at_declared_wait_legacy_poison",
        {**rows, "database": stack.database.name if stack.database else None, "poison": True},
    )


# --- GoalDirected ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_goal_directed_runs_two_iterations_through_the_production_composition(
    stack: ProductionStack,
) -> None:
    catalog = await publish_technical_catalog(
        stack.control_plane, family="GoalDirected", now=datetime.now(UTC)
    )
    run_id = await _admit(stack, catalog, "rrm009-goal-source")
    binding_ref = f"semantic-input:rrm009:{run_id}"
    templates = goal_templates(stack.technical, catalog)
    await PostgresGoalDirectedDocumentRepository(stack.worker_pool).persist_templates(
        request_scope=SCOPE,
        semantic_input_binding_ref=binding_ref,
        executor=templates["executor"],
        verifier=templates["verifier"],
        recorded_at=datetime.now(UTC),
    )
    objective = "Produce one independently verified technical record."
    receipt = await _launch(
        stack,
        run_id,
        {
            "request_scope": SCOPE,
            "run_id": run_id,
            "family": "GoalDirected",
            "goal_directed": asdict(goal_input(catalog, run_id, binding_ref, 1, objective)),
        },
    )
    assert receipt["workflow_id"] == f"belllabs-run/{run_id}"
    await _wait_for(stack, run_id, lambda: _terminal(stack, run_id), 300)
    run = await _run(stack, run_id)
    assert (run["phase"], run["terminal_outcome"]) == ("terminal", RunOutcome.COMPLETED.value)
    # RRM-019: only the final executor's verified outputs are promoted.
    assert [item["output_ref"] for item in run["accepted_output_evidence"]] == [
        "artifact:rrm009-goal:2"
    ]
    assert len(run["accepted_operation_settlement_evidence"]) == 4
    calls = _calls(stack, run_id)
    assert len(calls) == 4 and all(value == {"parent": 4, "child": 1} for value in calls.values())
    assert await _visible(stack.client, f"BellLabsRunId = '{run_id}'", 6) == 6
    budget = await stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    assert budget.status_code == 200
    replayed = await _replay(stack.client, [f"belllabs-run/{run_id}", f"family/{run_id}/1"])
    rows = await canonical_evidence(stack, run_id)
    assert rows["terminal_outcome"] == "succeeded", rows
    assert rows["counts"]["settled_claims"] == 4, rows
    assert rows["counts"]["operation_receipt"] >= 4, rows
    record_parity_trace("goal_directed_two_iterations", rows)
    print(
        "RRM-009 EVIDENCE goal_directed:",
        json.dumps(
            {
                "run": run_id,
                "accepted_outputs": [
                    item["output_ref"] for item in run["accepted_output_evidence"]
                ],
                "settlements": len(run["accepted_operation_settlement_evidence"]),
                "model_calls": calls,
                "consumed": budget.json()["consumed"],
                "replayed_events": replayed,
            },
            sort_keys=True,
        ),
    )
