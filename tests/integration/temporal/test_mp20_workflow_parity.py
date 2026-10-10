"""MP-20 / OVE-83: local workflow parity through the production path (G2/G3 evidence).

Every test compiles, submits and starts a mission/v2 manifest through the public router and
the production launch author, runs it on a real Temporal dev server against a disposable
PostgreSQL 17 common component, and compares receipts and records (settlements, accepted
evidence, workspace candidates, lane turns), never only final text. Provider behaviour comes
from deterministic FIXTURE clients injected at the client/launcher seam
(`tests/fixtures/mp20_parity.py` lists exactly what is production and what is fixture). No
live provider call is made and nothing here qualifies a profile.

Cursor (`cursor_local`, `cursor_cloud`) runs through the same production path: the launch
author seals `mc.cursor_binding.v1` from `providers.cursor_*` of the deployment file, the real
Cursor harnesses re-render and verify that binding's projection at `prepare`, and only the
bridge (local) and the Cloud Agents API (cloud) are FIXTURE responders.
"""

from __future__ import annotations

import dataclasses
import json
import os
from collections.abc import AsyncIterator
from hashlib import sha256
from pathlib import Path
from typing import Any, cast, get_args
from uuid import uuid4

import pytest

from mission_control.adapters.cursor.scm import GitBranchPublisher
from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.run_control.mailbox import PostgresCommandMailbox
from mission_control.adapters.temporal.human_gate_wake import TemporalHumanGateWake
from mission_control.adapters.temporal.workflows.stagegraph import (
    CONCLUDED_FAILURE_PATCH,
    StageGraphWorkflow,
)
from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.harness.describe import declared_matrix
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.execution.service import RunControlService
from mission_control.application.human_tasks.service import HumanTaskRejected, HumanTaskService
from mission_control.application.missions.service import MissionControlService
from mission_control.bootstrap.technical_api import api
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.domain.authoring.manifest_v2 import RequiredControl
from mission_control.domain.context.refs import parse_workspace_candidate_ref
from mission_control.domain.execution.lane_requirements import RequirementSet, admit_requirements
from mission_control.domain.policies.contracts import ActorContext, CancelAction
from mission_control.domain.policies.mailbox import MailboxState
from mission_control.domain.programs.human_gate import HumanResolutionRequest
from tests.fixtures.cursor_cloud import make_remote
from tests.fixtures.manifest_runtime import GoalScript
from tests.fixtures.mp20_parity import (
    CURSOR_PROFILES,
    LOCAL_PROFILES,
    SECRET_MARKERS,
    SESSION_PROFILES,
    ParityStack,
    StagePlan,
    gated_stage_graph,
    goal_chain,
    goal_loop,
    handoff_stage_graph,
    manifest_text,
    open_parity_stack,
)
from tests.fixtures.rrm009_production_harness import _command, _diagnose, _run, _send, _until
from tests.fixtures.rrm009_production_stack import SCOPE
from tests.fixtures.temporal_history import patch_ids
from tests.integration.temporal.test_manifest_launch_production import (
    OWNER_KEY,
    SCOPED,
    CountingAuthor,
    delivered,
    fetch,
    post,
    pumping,
    relay_pump,
    root_executions,
    submit,
)
from tests.integration.temporal.test_rrm_007_boundary_interventions import replay

pytestmark = pytest.mark.common_db

OPERATION_WORKFLOW = "belllabs.operation.v2"
OWNER = ActorContext(actor_id="owner")
CONTROLLER = ActorContext(
    actor_id="operator",
    permissions=frozenset({"workflow_run.read", "workflow_run.control"}),
)


@pytest.fixture
async def parity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[ParityStack]:
    async with open_parity_stack(tmp_path, monkeypatch) as stack:
        yield stack


async def _settled(parity: ParityStack, run_id: str, seconds: float = 300) -> dict[str, Any]:
    async def terminal() -> bool:
        return (await _run(parity.stack, run_id))["terminal_outcome"] is not None

    try:
        await _until(terminal, seconds)
    except (TimeoutError, AssertionError) as error:
        raise AssertionError(f"{error}: {await _diagnose(parity.stack, run_id)}") from error
    return await _run(parity.stack, run_id)


async def _lane_turn_queues(parity: ParityStack, run_id: str) -> dict[str, list[str]]:
    """Per operation workflow: the task queues its `lane.turn` activities ran on."""

    found: dict[str, list[str]] = {}
    query = f"BellLabsRunId = '{run_id}' AND WorkflowType = '{OPERATION_WORKFLOW}'"
    async for execution in parity.stack.client.list_workflows(query):
        history = await parity.stack.client.get_workflow_handle(execution.id).fetch_history()
        for event in history.events:
            if event.HasField("activity_task_scheduled_event_attributes"):
                attributes = event.activity_task_scheduled_event_attributes
                if attributes.activity_type.name == "lane.turn":
                    found.setdefault(execution.id, []).append(attributes.task_queue.name)
    return found


async def _operation_results(parity: ParityStack, run_id: str) -> dict[str, dict[str, Any]]:
    """Each closed operation workflow's public settlement (the receipt the family consumed)."""

    found: dict[str, dict[str, Any]] = {}
    query = f"BellLabsRunId = '{run_id}' AND WorkflowType = '{OPERATION_WORKFLOW}'"
    async for execution in parity.stack.client.list_workflows(query):
        if execution.status is None or execution.status.name != "COMPLETED":
            continue
        result = await parity.stack.client.get_workflow_handle(execution.id).result()
        found[execution.id] = {"disposition": result["disposition"], **result["result"]}
    return found


async def _candidates(parity: ParityStack, refs: list[str]) -> list[dict[str, Any]]:
    ids = [parse_workspace_candidate_ref(ref) for ref in refs]
    async with parity.stack.owner_pool.acquire() as connection:
        rows = await connection.fetch(
            "SELECT candidate_key, logical_path, content_digest, size_bytes "
            "FROM mission_control.workspace_candidate_descriptor WHERE candidate_key = ANY($1)",
            [item for item in ids if item],
        )
    return [dict(row) for row in rows]


# --- V14: Stage Graph handoff across providers ----------------------------------------------------


HANDOFFS = [
    ("claude_agent_sdk", "codex"),
    ("codex", "deep_agents"),
    ("deep_agents", "claude_agent_sdk"),
    ("cursor_local", "cursor_cloud"),
    ("cursor_cloud", "claude_agent_sdk"),
    ("codex", "cursor_local"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("producer", "consumer"), HANDOFFS, ids=lambda item: item)
async def test_v14_an_accepted_stage_output_feeds_the_next_stage_on_another_provider(
    parity: ParityStack, producer: str, consumer: str
) -> None:
    key = f"mp20-handoff-{producer}-{consumer}".replace("_", "-")
    script = parity.script
    script.stages[(key, "produce")] = StagePlan(output="draft", padded=True)
    script.stages[(key, "consume")] = StagePlan(output="report", obligations=("handed_off",))
    # A cursor_cloud stage beside a worker-hosted one inherits from a Deep Agents mission.
    mixed = "cursor_cloud" in (producer, consumer) and producer != consumer
    mission_lane = "deep_agents" if mixed else None
    document = handoff_stage_graph(key, producer, consumer, mission_lane=mission_lane)
    receipt = await submit(parity.app, manifest_text(document))
    (mission,) = receipt["missions"]
    run_id = mission["run_id"]
    started = await post(parity.app, "/missions:start", {"run_id": run_id})
    assert started.status_code == 202, started.text
    assert started.json()["family"] == "StageGraph"

    run = await _settled(parity, run_id)

    # Acceptance is the family's: the consumer's typed obligation evidence, never completion.
    assert run["terminal_outcome"] == "completed", run
    assert {item["obligation_ref"] for item in run["accepted_obligation_evidence"]} == {
        "handed_off"
    }
    outputs = [item["output_ref"] for item in run["accepted_output_evidence"]]
    assert outputs and all(parse_workspace_candidate_ref(ref) for ref in outputs), outputs
    # Every accepted output is a registered, digest-bound workspace candidate.
    candidates = await _candidates(parity, outputs)
    assert len(candidates) == len(outputs)
    assert {Path(item["logical_path"]).name for item in candidates} >= {
        "draft.json",
        "report.json",
    }

    # Each provider ran its own stage, on its own lane (Session Lanes on their binding's queue).
    assert [turn[0] for turn in script.lane_turns if turn[1] == key] == [producer, consumer]
    queues = [
        queue for found in (await _lane_turn_queues(parity, run_id)).values() for queue in found
    ]
    for profile in (producer, consumer):
        if profile in parity.lane_queues:
            assert parity.lane_queues[profile] in queues
    assert set(queues) <= set(parity.lane_queues.values())

    # Receipts, not text: both units settled `completed`; a Session Lane's settlement carries
    # its Completion Candidate with `output_refs` reconciled to the candidates it registered.
    results = list((await _operation_results(parity, run_id)).values())
    assert len(results) == 2 and {item["status"] for item in results} == {"completed"}
    for item in results:
        structured = item["structured_output"]
        assert structured is not None, item
        # Deep Agents leaves `output_refs` to its registered captures (top level).
        assert set(structured.get("output_refs", item["output_refs"])) <= set(outputs), item

    # The accepted candidates are exactly the bytes each provider wrote (custody receipts).
    draft = script.written[(key, "produce", producer)]
    report = script.written[(key, "consume", consumer)]
    assert {item["content_digest"] for item in candidates} >= {_digest(draft), _digest(report)}

    # V14: the consumer received the producer's accepted bytes as a digest-checked, read-only
    # materialized input (above the packer's `auto` floor), and neither a provider-local path
    # nor a credential crossed over.
    (seen,) = [
        item for item in script.observations if (item.mission_key, item.node) == (key, "consume")
    ]
    manifest = json.loads(seen.inputs_text)
    (entry,) = manifest["inputs"]
    assert entry["content_digest"] == _digest(draft)
    assert parse_workspace_candidate_ref(entry["artifact_ref"])
    (materialized,) = seen.inputs
    if consumer == "deep_agents":
        # The Deep Agents workspace materializer verified the digest before mounting it (a
        # mismatch raises `WorkspaceDigestMismatch`); the agent read the mounted file.
        assert materialized["path"] == entry["path"] and materialized["content"]
    else:
        assert materialized["exists"] and materialized["digest_ok"], materialized
        # The lanes chmod packet files 0o444 on their POSIX worker hosts (Linux/WSL); this
        # Windows test host skips the chmod by design (`cursor/projection._write`). A
        # cursor_cloud packet is committed to the run branch: the cloud agent's checkout is the
        # provider's, so there is no lease file mode to assert there.
        assert materialized["read_only"] or os.name == "nt" or consumer == "cursor_cloud", (
            materialized
        )
    for text in (seen.context_text, seen.inputs_text):
        assert str(parity.leases) not in text
        assert str(parity.leases).replace("\\", "/") not in text
        assert not any(marker in text for marker in SECRET_MARKERS)


def _digest(content: bytes) -> str:
    return f"sha256:{sha256(content).hexdigest()}"


# --- V12/V15 counters: a Goal Loop converging at its second iteration ---------------------------

GOAL_LANES = [
    ("deep_agents", "deep_agents"),
    ("claude_agent_sdk", "claude_agent_sdk"),
    ("codex", "codex"),
    # Mixed: the independent verifier runs on another provider under the same admission.
    ("claude_agent_sdk", "codex"),
    ("cursor_local", "cursor_local"),
    ("cursor_cloud", "cursor_cloud"),
]


def _plan(name: str = "finding", accept_at: int = 2) -> GoalScript:
    return GoalScript(
        obligation="converged",
        output_contract=f"output:{name}",
        output_name=name,
        accept_at=accept_at,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(("executor", "verifier"), GOAL_LANES, ids=lambda item: item)
async def test_v12_a_goal_loop_converges_at_its_second_iteration_on_each_lane(
    parity: ParityStack, executor: str, verifier: str
) -> None:
    key = f"mp20-goal-{executor}-{verifier}".replace("_", "-")
    receipt = await submit(parity.app, manifest_text(goal_loop(key, executor, verifier)))
    (mission,) = receipt["missions"]
    parity.script.goal(mission, _plan())
    run_id = mission["run_id"]
    started = await post(parity.app, "/missions:start", {"run_id": run_id})
    assert started.status_code == 202, started.text
    assert started.json()["family"] == "GoalDirected"

    run = await _settled(parity, run_id, 420)

    assert run["terminal_outcome"] == "completed", run
    assert {item["obligation_ref"] for item in run["accepted_obligation_evidence"]} == {"converged"}
    # Two iterations, each its own executor and independent verifier unit: the iteration
    # counter advanced once per verified rejection and never reset.
    units = _goal_units(parity, key, run_id)
    assert units == [
        (executor, "executor", 1),
        (verifier, "verifier", 1),
        (executor, "executor", 2),
        (verifier, "verifier", 2),
    ], units
    # Each provider unit read its own packet: the activation its context index names is that
    # unit's iteration and role (a stale workspace shows an earlier activation, or none).
    seen = [
        (item.profile, item.node, item.activation)
        for item in parity.script.observations
        if item.mission_key == key
    ]
    counts: dict[str, int] = {}
    for _profile, node, activation in seen:
        counts[node] = counts.get(node, 0) + 1
        assert activation == f"goal-iteration/{counts[node]}/{node}", seen
    # The accepted output is the registered candidate of the second executor's file.
    outputs = [item["output_ref"] for item in run["accepted_output_evidence"]]
    assert outputs and all(parse_workspace_candidate_ref(ref) for ref in outputs), outputs
    candidates = await _candidates(parity, outputs)
    assert len(candidates) == len(outputs)
    if executor != "deep_agents":
        written = parity.script.written[(key, "executor:2", executor)]
        assert {item["content_digest"] for item in candidates} == {_digest(written)}
    # Every unit's usage reservation was released by its authoritative settlement.
    budget = await parity.stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    assert budget.status_code == 200, budget.text
    assert not budget.json()["reservations"], budget.text
    assert len(budget.json()["usage_settlements"]) >= 4, budget.text


def _goal_units(parity: ParityStack, key: str, run_id: str) -> list[tuple[str, str, int]]:
    """Each Goal Loop unit, ordered by iteration then role: (lane, role, iteration)."""

    units: set[tuple[str, str, int]] = set()
    for item in parity.stack.model_log:
        operation = str(item.get("operation", ""))
        if item.get("run_id") == run_id and operation.startswith("goal-iteration/"):
            _prefix, iteration, role = operation.split("/")[:3]
            units.add(("deep_agents", role.split(":")[0], int(iteration)))
    counts: dict[str, int] = {}
    for observation in parity.script.observations:
        if observation.mission_key != key:
            continue
        counts[observation.node] = counts.get(observation.node, 0) + 1
        units.add((observation.profile, observation.node, counts[observation.node]))
    return sorted(units, key=lambda unit: (unit[2], unit[1] == "verifier"))


# --- V15: two linked Goal Loops, released once under replayed release intents ---------------------

CHAINS = [
    ("codex", "claude_agent_sdk"),
    ("claude_agent_sdk", "deep_agents"),
    ("cursor_local", "cursor_cloud"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("supplier", "consumer"), CHAINS, ids=lambda item: item)
async def test_v15_a_linked_consumer_starts_once_with_the_suppliers_accepted_output(
    parity: ParityStack, supplier: str, consumer: str
) -> None:
    supplier_key = f"mp20-supplier-{supplier}".replace("_", "-")
    consumer_key = f"mp20-consumer-{consumer}".replace("_", "-")
    stack = parity.stack
    receipt = await submit(
        parity.app, manifest_text(goal_chain(supplier_key, supplier, consumer_key, consumer))
    )
    members = {item["mission_key"]: item for item in receipt["missions"]}
    first, second = members[supplier_key], members[consumer_key]
    assert first["run_id"] is not None and second["run_id"] is None
    parity.script.goal(first, _plan("finding"))
    parity.script.goal(second, _plan("summary", accept_at=1))

    started = await post(parity.app, "/missions:start", {"run_id": first["run_id"]})
    assert started.status_code == 202, started.text
    author = CountingAuthor(parity.author)
    pump = relay_pump(stack, author)  # type: ignore[arg-type]
    async with pumping(pump):

        async def consumer_started() -> bool:
            rows = await fetch(
                stack,
                f"SELECT run_key FROM mission_control.mission_run "
                f"WHERE {SCOPED} AND mission_id = $4::uuid",
                second["mission_id"],
            )
            return bool(rows) and bool(await root_executions(stack, rows[0]["run_key"]))

        try:
            await _until(consumer_started, 420)
        except (TimeoutError, AssertionError) as error:
            raise AssertionError(f"{error}: {await _diagnose(stack, first['run_id'])}") from error

    supplier_run = await _run(stack, first["run_id"])
    assert supplier_run["terminal_outcome"] == "completed", supplier_run
    (row,) = await fetch(
        stack,
        f"SELECT run_key, created_by_actor_ref FROM mission_control.mission_run "
        f"WHERE {SCOPED} AND mission_id = $4::uuid",
        second["mission_id"],
    )
    consumer_run_id = row["run_key"]
    assert row["created_by_actor_ref"].startswith("chain:")
    assert author.calls == [consumer_run_id]
    (intent,) = await fetch(
        stack,
        f"SELECT delivery_key, delivery_state FROM mission_control.outbox "
        f"WHERE {SCOPED} AND destination_kind = $4",
        "mc.chain.start_run",
    )
    # The acknowledgement is lost and the release intent is delivered again: still one root.
    async with stack.owner_pool.acquire() as connection:
        await connection.execute(
            f"UPDATE mission_control.outbox SET delivery_state = 'pending', delivered_at = NULL "
            f"WHERE {SCOPED} AND delivery_key = $4",
            *OWNER_KEY,
            intent["delivery_key"],
        )
    reports = await pump.run_once()
    assert delivered(reports) == [intent["delivery_key"]], reports
    assert len(await root_executions(stack, consumer_run_id)) == 1

    consumer_run = await _settled(parity, consumer_run_id, 420)
    assert consumer_run["terminal_outcome"] == "completed", consumer_run
    # The consumer has its own budget account, never the supplier's.
    accounts = []
    for run_key in (first["run_id"], consumer_run_id):
        response = await stack.http.get(
            f"/run-control/v1/runs/{run_key}/budget", params={"request_scope": SCOPE}
        )
        accounts.append(response.json()["account_id"])
    assert accounts[0] != accounts[1]
    # Only the declared accepted output crossed the link, digest-bound.
    supplied = [item["output_ref"] for item in supplier_run["accepted_output_evidence"]]
    (candidate,) = await _candidates(parity, supplied)
    if consumer == "deep_agents":
        reads = [
            text
            for (run_key, operation), texts in parity.script.reads.items()
            if run_key == consumer_run_id and operation.endswith("executor")
            for text in texts
        ]
        assert any(candidate["content_digest"] in text for text in reads), reads
    else:
        (seen, *_rest) = [
            item
            for item in parity.script.observations
            if (item.mission_key, item.node) == (consumer_key, "executor")
        ]
        assert candidate["content_digest"] in seen.inputs_text + seen.context_text, (
            seen.inputs_text,
            seen.context_text[:2000],
        )


# --- V01: every control a profile's declared matrix refuses is a typed refusal at submit ----------


def _refused_controls(profile: str) -> dict[str, str]:
    """The controls the profile's declared describe matrix refuses (never hard-coded)."""

    describe = declared_matrix(profile)
    refused: dict[str, str] = {}
    for control in get_args(RequiredControl):
        issues = admit_requirements(describe, RequirementSet(controls=(control,)))
        if issues:
            refused[control] = issues[0].code
    return refused


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", [*LOCAL_PROFILES, *CURSOR_PROFILES])
async def test_v01_a_control_the_lane_refuses_is_a_pointed_blocker_before_any_run(
    parity: ParityStack, profile: str
) -> None:
    refused = _refused_controls(profile)
    assert refused, f"{profile} declares every control; nothing to refuse"
    key = f"mp20-refusal-{profile}".replace("_", "-")
    document = handoff_stage_graph(key, profile, profile)
    document["mission"]["environment"]["requires"] = {
        **document["mission"]["environment"]["requires"],
        "controls": ["cancel", *refused],
    }
    response = await post(
        parity.app,
        "/missions:submit",
        {"manifest_yaml": manifest_text(document), "request_id": str(uuid4())},
    )
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "manifest_blocked"
    found = {item["pointer"]: (item["code"], item.get("reason")) for item in detail["blockers"]}
    for index, (control, reason) in enumerate(refused.items(), start=1):
        pointer = f"/mission/environment/requires/controls/{index}"
        assert found.get(pointer) == ("UNSUPPORTED_BEHAVIOR", reason), (control, found)
    # Nothing was admitted, so nothing can be dispatched to a provider.
    assert not parity.script.lane_turns


# --- Cursor cloud: every unit of a run reaches the per-run branch -------------------------------


@pytest.mark.asyncio
async def test_a_later_cloud_unit_of_the_run_publishes_its_own_packet(tmp_path: Path) -> None:
    """MP-20 Cursor finding, fixed in production `GitBranchPublisher.publish`."""

    remote = make_remote(tmp_path / "remote")
    target = GitBranchPublisher(tmp_path / "mirrors")
    first = await target.publish(
        repository=str(remote),
        base_ref="main",
        branch="mc/run-1",
        files=[("goal/1/executor/.mission/context.md", b"executor 1")],
    )
    second = await target.publish(
        repository=str(remote),
        base_ref="main",
        branch="mc/run-1",
        files=[("goal/1/verifier/.mission/context.md", b"verifier 1")],
    )
    retried = await target.publish(
        repository=str(remote),
        base_ref="main",
        branch="mc/run-1",
        files=[("goal/1/verifier/.mission/context.md", b"verifier 1")],
    )
    tree = dict(await target.read_tree(repository=str(remote), ref=second.head, roots=("goal",)))
    assert tree.get("goal/1/verifier/.mission/context.md") == b"verifier 1", sorted(tree)
    assert second.head != first.head and retried.head == second.head
    assert first.base_commit == second.base_commit


# --- V09: a Human Gate between provider stages (approve / deny / request_changes) -----------------


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", ["approve", "deny"])
async def test_v09_a_human_gate_between_provider_stages_resolves_once(
    parity: ParityStack, decision: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = f"mp20-gate-{decision}"
    script = parity.script
    script.stages[(key, "produce")] = StagePlan(output="draft")
    script.stages[(key, "consume")] = StagePlan(output="report", obligations=("handed_off",))
    document = gated_stage_graph(key, "claude_agent_sdk", "codex")
    receipt = await submit(parity.app, manifest_text(document))
    (mission,) = receipt["missions"]
    run_id = mission["run_id"]
    started = await post(parity.app, "/missions:start", {"run_id": run_id})
    assert started.status_code == 202, started.text

    repository = PostgresHumanTaskRepository(parity.stack.worker_pool)
    tasks = HumanTaskService(
        repository, request_scope=SCOPE, wake=TemporalHumanGateWake(parity.stack.client)
    )

    async def opened() -> bool:
        return bool(await repository.list(SCOPE, run_id=run_id, lifecycle="open"))

    try:
        await _until(opened, 240)
    except (TimeoutError, AssertionError) as error:
        raise AssertionError(f"{error}: {await _diagnose(parity.stack, run_id)}") from error
    (task,) = await repository.list(SCOPE, run_id=run_id, lifecycle="open")
    # The provider stage ran; the consumer waits on the durable gate, not on a worker.
    assert [turn[2] for turn in script.lane_turns if turn[1] == key] == ["produce"]
    # The manifest lowering declares no remediation route, so `request_changes` is not a
    # permitted decision here: a typed refusal, never a silent approval.
    assert task.activation.permitted_decisions == ("approve", "deny")
    with pytest.raises(HumanTaskRejected) as refused:
        await tasks.resolve(task.human_task_id, _answer(task, "changes", "request_changes"), OWNER)
    assert refused.value.code
    resolved = await tasks.resolve(task.human_task_id, _answer(task, "decide", decision), OWNER)
    # A replayed resolution (same request) returns the same receipt; a second decision cannot.
    again = await tasks.resolve(task.human_task_id, _answer(task, "decide", decision), OWNER)
    assert (resolved.status, again.status) == ("accepted", "duplicate")
    assert dataclasses.replace(again, status="accepted") == resolved
    with pytest.raises(HumanTaskRejected):
        await tasks.resolve(task.human_task_id, _answer(task, "second", "approve"), OWNER)

    if decision == "approve":
        run = await _settled(parity, run_id)
        assert run["terminal_outcome"] == "completed", run
        assert [turn[2] for turn in script.lane_turns if turn[1] == key] == ["produce", "consume"]
        return
    # Deny (SPEC-03): the gate closes as not accepted, the consumer never runs and no
    # obligation is accepted; the family concludes and the reducer records the typed `failed`
    # outcome, attributed to the gate stage (the run's only failure) and the owner's decision.
    run = await _settled(parity, run_id)
    assert run["terminal_outcome"] == "failed", run
    assert [turn[2] for turn in script.lane_turns if turn[1] == key] == ["produce"]
    assert not run["accepted_obligation_evidence"]
    family_handle = parity.stack.client.get_workflow_handle(f"family/{run_id}/1")
    family = await family_handle.describe()
    assert family.status is not None and family.status.name == "COMPLETED"
    # The concluded-failure proposal is a patched branch; its history replays deterministically.
    assert CONCLUDED_FAILURE_PATCH in patch_ids(await family_handle.fetch_history())
    assert await replay(family_handle, [StageGraphWorkflow]) == 1
    (closed,) = await repository.list(SCOPE, run_id=run_id)
    assert closed.lifecycle == "resolved", closed
    assert closed.resolution is not None
    assert (closed.resolution.actor_ref, closed.resolution.decision) == ("owner", "deny")
    assert not await repository.list(SCOPE, run_id=run_id, lifecycle="open")


def _answer(task: Any, request_id: str, decision: str) -> HumanResolutionRequest:
    return HumanResolutionRequest.model_validate(
        {
            "request_id": request_id,
            "expected_task_version": task.version,
            "decision": decision,
            "reviewed_packet_digest": task.activation.packet_digest,
            **({"comment": "Tighten the draft."} if decision == "request_changes" else {}),
        }
    )


# --- V04: a queued instruction reaches the declared boundary once, on each Session Lane ----------

INSTRUCTION = "MP-20 operator instruction: cite the source digest in the finding."
DELIVERY_SEMANTICS = {
    "claude_agent_sdk": "turn_boundary_guaranteed",
    "codex": "wait_then_send",
}


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", SESSION_PROFILES)
async def test_v04_a_queued_instruction_is_applied_once_at_the_next_iteration_boundary(
    parity: ParityStack, profile: str
) -> None:
    key = f"mp20-queue-{profile}".replace("_", "-")
    receipt = await submit(parity.app, manifest_text(goal_loop(key, profile, profile)))
    (mission,) = receipt["missions"]
    parity.script.goal(mission, _plan(accept_at=2))
    run_id = mission["run_id"]
    run_control = cast(RunControlService, api.state.run_control_service)
    mailbox_store = PostgresCommandMailbox(parity.stack.worker_pool)
    facade = MissionControlService(
        run_control,
        BoundaryInterventionService(run_control),
        request_scope=SCOPE,
        mailbox=MailboxDeliveryService(mailbox_store, run_control),
    )
    projection = await run_control.get_run(SCOPE, run_id)
    queued = await facade.command(
        run_id,
        MissionCommandRequest.model_validate(
            {
                "request_id": str(uuid4()),
                "expected_version": projection.version,
                "expected_generation": 1,
                "target": {"kind": "run", "id": run_id},
                "kind": "queue_instruction",
                "payload": {"boundary": "next_iteration", "content": {"text": INSTRUCTION}},
                "reason": "MP-20 operator steering",
            }
        ),
        CONTROLLER,
    )
    assert queued.delivery is not None
    assert [item.state.value for item in queued.delivery.receipts] == ["accepted", "queued"]
    started = await post(parity.app, "/missions:start", {"run_id": run_id})
    assert started.status_code == 202, started.text

    run = await _settled(parity, run_id, 420)
    assert run["terminal_outcome"] == "completed", run
    # Exactly one provider turn carried it: the first executor's (the declared boundary).
    carried = [
        (item.node, item.activation)
        for item in parity.script.observations
        if item.mission_key == key and INSTRUCTION in item.context_text
    ]
    assert carried == [("executor", "goal-iteration/1/executor")], carried
    (entry,) = await mailbox_store.list_entries(SCOPE, run_id)
    assert entry.state == MailboxState.CONSUMED
    status = await run_control.get_boundary_command(
        SCOPE, run_id, entry.command_issuer, entry.command_id
    )
    assert status is not None
    # Receipts distinguish acceptance, queueing, delivery, observation and application.
    assert [item.state.value for item in status.receipts] == [
        "accepted",
        "queued",
        "delivered",
        "observed",
        "applied",
    ]
    assert {
        item.delivery_report.delivered_semantics
        for item in status.receipts
        if item.delivery_report is not None
    } == {DELIVERY_SEMANTICS[profile]}


# --- V06: an accepted cancel reaches the held provider turn on each Session Lane ------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", SESSION_PROFILES)
async def test_v06_an_accepted_cancel_interrupts_the_provider_turn_and_settles_cancelled(
    parity: ParityStack, profile: str
) -> None:
    key = f"mp20-cancel-{profile}".replace("_", "-")
    script = parity.script
    script.stages[(key, "produce")] = StagePlan(output="draft", hold=True)
    script.stages[(key, "consume")] = StagePlan(output="report", obligations=("handed_off",))
    receipt = await submit(parity.app, manifest_text(handoff_stage_graph(key, profile, profile)))
    (mission,) = receipt["missions"]
    run_id = mission["run_id"]
    started = await post(parity.app, "/missions:start", {"run_id": run_id})
    assert started.status_code == 202, started.text

    async def held() -> bool:
        if profile == "claude_agent_sdk":
            return bool(parity.claude.clients) and parity.claude.last.held.is_set()
        return bool(parity.codex.launches) and parity.codex.server.held.is_set()

    try:
        await _until(held, 180)
    except (TimeoutError, AssertionError) as error:
        raise AssertionError(f"{error}: {await _diagnose(parity.stack, run_id)}") from error
    running = await _run(parity.stack, run_id)
    decision = await _send(
        parity.stack,
        run_id,
        _command(
            run_id, running["version"], f"cancel:{run_id}", CancelAction(), "workflow_run.cancel"
        ),
    )
    assert decision["reason_code"] == "accepted", decision

    run = await _settled(parity, run_id)
    assert run["terminal_outcome"] == "cancelled", run
    # The native turn was interrupted (not left running) and the unit settled once,
    # `cancelled` by the command; the dependent stage never ran; no reservation stays open.
    if profile == "claude_agent_sdk":
        assert parity.claude.last.interrupts >= 1
    else:
        assert parity.codex.server.interrupts, parity.codex.server.interrupts
    results = list((await _operation_results(parity, run_id)).values())
    assert [(item["status"], item["failure_code"]) for item in results] == [
        ("cancelled", "cancelled")
    ], results
    assert [turn[2] for turn in script.lane_turns if turn[1] == key] == ["produce"]
    budget = await parity.stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    assert not budget.json()["reservations"], budget.text


# --- V09 (GoalDirected): a denied acceptance review closes the loop not accepted ---------------


@pytest.mark.asyncio
async def test_v09_a_denied_goal_review_fails_the_loop_without_another_iteration(
    parity: ParityStack,
) -> None:
    """SPEC-03: `acceptance.human` needs an attributable resolution; a deny closes the review
    as not accepted, the loop never runs another iteration on it, nothing is accepted as
    output, and the reducer records the typed `failed` outcome."""

    key = "mp20-goal-review-deny"
    profile = "claude_agent_sdk"
    document = goal_loop(key, profile, profile)
    document["mission"]["goals"][0]["criteria"].append(
        {
            "key": "reviewed",
            "description": "The owner approved the finding",
            "evidence": ["finding"],
            "acceptance": {"human": "approved"},
        }
    )
    receipt = await submit(parity.app, manifest_text(document))
    (mission,) = receipt["missions"]
    parity.script.goal(mission, _plan(accept_at=1))
    run_id = mission["run_id"]
    started = await post(parity.app, "/missions:start", {"run_id": run_id})
    assert started.status_code == 202, started.text
    assert started.json()["family"] == "GoalDirected"

    repository = PostgresHumanTaskRepository(parity.stack.worker_pool)
    tasks = HumanTaskService(
        repository, request_scope=SCOPE, wake=TemporalHumanGateWake(parity.stack.client)
    )

    async def opened() -> bool:
        return bool(await repository.list(SCOPE, run_id=run_id, lifecycle="open"))

    try:
        await _until(opened, 240)
    except (TimeoutError, AssertionError) as error:
        raise AssertionError(f"{error}: {await _diagnose(parity.stack, run_id)}") from error
    (task,) = await repository.list(SCOPE, run_id=run_id, lifecycle="open")
    # The verified completion waits on the review; the reviewer is the `owner` role.
    assert task.activation.family == "GoalDirected"
    assert "deny" in task.activation.permitted_decisions
    turns = [turn[2] for turn in parity.script.lane_turns if turn[1] == key]
    assert turns == ["executor", "verifier"], turns
    resolved = await tasks.resolve(task.human_task_id, _answer(task, "decide", "deny"), OWNER)
    assert resolved.status == "accepted"

    run = await _settled(parity, run_id)
    assert run["terminal_outcome"] == "failed", run
    assert not run["accepted_output_evidence"], run
    # No remediation iteration: a deny is not `request_changes`.
    assert [turn[2] for turn in parity.script.lane_turns if turn[1] == key] == turns
    (closed,) = await repository.list(SCOPE, run_id=run_id)
    assert closed.lifecycle == "resolved" and closed.resolution is not None
    assert (closed.resolution.actor_ref, closed.resolution.decision) == ("owner", "deny")


# --- V14 (SPEC-02): the authored input name and expansion tier reach the consumer's packet ------


@pytest.mark.asyncio
@pytest.mark.parametrize("expand", ["materialize", "reference"])
async def test_v14_the_authored_input_name_and_tier_reach_the_consumers_packet(
    parity: ParityStack, monkeypatch: pytest.MonkeyPatch, expand: str
) -> None:
    key = f"mp20-authored-{expand}"
    script = parity.script
    # A small draft: under `auto` it would be inlined, so the tier is the author's.
    script.stages[(key, "produce")] = StagePlan(output="draft")
    script.stages[(key, "consume")] = StagePlan(output="report", obligations=("handed_off",))
    document = handoff_stage_graph(key, "claude_agent_sdk", "codex")
    _produce, consume = document["mission"]["program"]["nodes"]
    consume["inputs"] = [{"name": "source-draft", "from": "produce.draft", "expand": expand}]
    receipt = await submit(parity.app, manifest_text(document))
    (mission,) = receipt["missions"]
    run_id = mission["run_id"]
    started = await post(parity.app, "/missions:start", {"run_id": run_id})
    assert started.status_code == 202, started.text

    run = await _settled(parity, run_id)
    assert run["terminal_outcome"] == "completed", run
    (seen,) = [
        item for item in script.observations if (item.mission_key, item.node) == (key, "consume")
    ]
    rows = [line for line in seen.context_text.splitlines() if line.startswith("| source-draft |")]
    assert rows and all(f"| {expand} |" in row for row in rows), seen.context_text
    assert "from-produce" not in seen.context_text
    manifest = json.loads(seen.inputs_text) if seen.inputs_text else {}
    draft = script.written[(key, "produce", "claude_agent_sdk")]
    if expand == "materialize":
        (entry,) = manifest["inputs"]
        assert entry["path"].lstrip("/").startswith("inputs/source-draft/"), entry
        assert entry["content_digest"] == _digest(draft)
        (materialized,) = seen.inputs
        assert materialized["exists"] and materialized["digest_ok"], materialized
    else:
        assert not manifest.get("inputs"), manifest
        assert draft.decode("utf-8") not in seen.context_text


# --- MP-20 follow-up: Session Lane units charge their settled tokens to the run budget ----------
# (Found by MP-20: a manifest unit reserves `operation.attempts`/`concurrency.slots` or
# `goal.iterations` only, so lane settlements charged no tokens to the run on any lane.)


async def _run_budget(parity: ParityStack, run_id: str) -> dict[str, Any]:
    response = await parity.stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    assert response.status_code == 200, response.text
    return cast(dict[str, Any], response.json())


def _assert_lane_tokens_charged_once(
    results: list[dict[str, Any]], budget: dict[str, Any], session_units: int
) -> None:
    tokens = ("tokens.input", "tokens.output", "tokens.total")
    declared = {item["dimension"] for item in budget["limits"]} & set(tokens)
    assert "tokens.total" in declared, budget["limits"]
    amounts = [item["usage"]["amounts"] for item in results]
    # Every Session Lane unit charged its settled turn usage on exactly the token dimensions
    # the run declares (no invented dimension), once, and never as a pending amount.
    assert sum(1 for item in amounts if item.get("tokens.total", 0) > 0) == session_units, results
    for item in amounts:
        assert set(item) <= declared, results
        if {"tokens.input", "tokens.output"} <= set(item):
            assert item["tokens.total"] == item["tokens.input"] + item["tokens.output"], item
    assert all(not item["usage"]["pending_external_amounts"] for item in results), results
    # Recorded once per settlement: the run's consumption is exactly the settlements' sum.
    for dimension in declared:
        charged = sum(item.get(dimension, 0) for item in amounts)
        assert budget["consumed"].get(dimension, 0) == charged, (dimension, budget)
        recorded = sum(
            record["actual_amounts"].get(dimension, 0)
            for record in budget["usage_records"].values()
        )
        assert recorded == charged, (dimension, budget)
    assert not budget["reservations"], budget


@pytest.mark.asyncio
async def test_mp20_stage_graph_session_lane_units_charge_their_tokens_to_the_run_budget(
    parity: ParityStack,
) -> None:
    key = "mp20-budget-stages"
    script = parity.script
    script.stages[(key, "produce")] = StagePlan(output="draft")
    script.stages[(key, "consume")] = StagePlan(output="report", obligations=("handed_off",))
    receipt = await submit(
        parity.app, manifest_text(handoff_stage_graph(key, "claude_agent_sdk", "codex"))
    )
    (mission,) = receipt["missions"]
    run_id = mission["run_id"]
    started = await post(parity.app, "/missions:start", {"run_id": run_id})
    assert started.status_code == 202, started.text

    run = await _settled(parity, run_id)

    assert run["terminal_outcome"] == "completed", run
    results = list((await _operation_results(parity, run_id)).values())
    assert len(results) == 2 and {item["status"] for item in results} == {"completed"}
    _assert_lane_tokens_charged_once(results, await _run_budget(parity, run_id), 2)


@pytest.mark.asyncio
async def test_mp20_goal_loop_session_lane_units_charge_their_tokens_to_the_run_budget(
    parity: ParityStack,
) -> None:
    key = "mp20-budget-goal"
    receipt = await submit(parity.app, manifest_text(goal_loop(key, "claude_agent_sdk", "codex")))
    (mission,) = receipt["missions"]
    parity.script.goal(mission, _plan())
    run_id = mission["run_id"]
    started = await post(parity.app, "/missions:start", {"run_id": run_id})
    assert started.status_code == 202, started.text

    run = await _settled(parity, run_id, 420)

    assert run["terminal_outcome"] == "completed", run
    results = list((await _operation_results(parity, run_id)).values())
    # Two iterations: executor (Claude) and independent verifier (Codex) each.
    assert len(results) == 4, results
    _assert_lane_tokens_charged_once(results, await _run_budget(parity, run_id), 4)
