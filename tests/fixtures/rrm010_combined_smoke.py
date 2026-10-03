"""RRM-010 combined technical smoke: one production stack, both families, one qualification.

Everything runs over `open_production_stack` (the deployment's API composition, the workers
built by `ProductionWorkerActivityCompositionFactory`, a persistent `start_local` namespace, the
disposable application PostgreSQL and MongoDB) and enters through the governed facade. Parent
cognition is deterministic (`TechnicalModel` and the cancellation drill's `SpawningModel`); the
only live model is the hosted async child on the `rrm009-agent-server` Agent Server.

Phases (each asserts its own saga; the test joins them):

1. `cancel_with_active_async_child`: a StageGraph run spawns a real async child; while it runs,
   scoped inspection (list, run, unit, checkpoint history) shows it and a snapshot is refused
   `snapshot_not_quiescent` (`async_child_active:<child>`); then the running cancellation
   (`run_cancellation_drill`) cancels the child at the provider, reconciles its usage and
   terminalizes the run `cancelled` with `applied` receipts.
2. `stagegraph_fork_and_wait_release`: a StageGraph run holds its declared wait; inspection and a
   historical checkpoint read; a `stage_settled` snapshot and a patched fork, independently
   admitted and launched; the derived run's and then the source's wait are released through
   the facade (`applied`); the source's authority is unchanged by the fork and the derived run.
3. `goal_directed_pause_resume_and_fork`: a GoalDirected run is paused while its first
   executor runs (applied at the iteration boundary) and resumed; it completes; a
   `goal_verifier_settled` snapshot of the terminal run is forked with a patched objective and
   the derived run completes; the parent's authority is unchanged.

No company input, no company fixture (RRM-011 stays held).
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import httpx
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langchain_core.outputs import ChatResult

from app.agent_server.async_subagents.auth import mint_scope_claim
from app.application.orchestration.mongo_goal_directed_repository import (
    MongoGoalDirectedDocumentRepository,
)
from app.application.orchestration.mongo_stagegraph_repository import (
    MongoStageGraphOperationTemplateRepository,
)
from app.application.run_control.run_launch import fork_semantic_input_binding_ref
from app.application.runtime.run_forks import ForkPatchPolicyRegistry
from app.domain.operation_execution.async_subagent_reconciliation import (
    AsyncServedGraphIdentity,
)
from app.domain.operation_execution.contracts import AsyncSubagentContract
from app.domain.run_control.contracts import (
    PauseAction,
    PauseDecision,
    ResumeAction,
    ResumeDecision,
)
from app.domain.run_control.forks import ForkPatchPolicy, PatchablePath, stage_objective_path
from app.server import api
from app.temporal.deployment_composition import DeploymentCapabilityComponents
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from tests.fixtures.rrm009_cancellation import (
    CANCELLATION_ENVIRONMENT,
    TOKEN_ENV,
    CancellationGate,
    SpawningModel,
    run_cancellation_drill,
)
from tests.fixtures.rrm009_production_harness import (
    STAGE_OBJECTIVE,
    ProductionStack,
    _admit,
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
    _send,
    _terminal,
    _visible,
    _wait_for,
)
from tests.fixtures.rrm009_production_stack import (
    LANGGRAPH_SCHEMA,
    NODE_EXECUTABLE,
    SCOPE,
    ChildModel,
    TechnicalBinding,
    TechnicalCatalog,
    TechnicalModel,
    goal_input,
    goal_templates,
    stage_input,
    stage_templates,
)
from tests.fixtures.run_authority_digest import run_authority_digest

LIVE_FLAG = "BELLABS_RUN_RRM_010_LIVE"
GOAL_OBJECTIVE = "Produce one independently verified technical record."
PATCHED_GOAL_OBJECTIVE = "Produce one independently verified technical record, checked twice."
INSPECTION = "/run-control/v1/inspection/runs"
# The child's immutable detail lifecycle while it works; the authority shows its latest
# recorded run-control lifecycle fact as recorded (`pending` until a terminal fact).
ACTIVE_CHILD_LIFECYCLES = {"admitted", "submitted", "running", "waiting"}
TERMINAL_CHILD_LIFECYCLES = {"completed", "failed", "cancelled", "timed_out"}
# The deployment settings the smoke runs with, on top of `runtime_environment`: the
# cancellation drills' heartbeat classes and drain, async spawning on, tracing off.
SMOKE_ENVIRONMENT = {
    **CANCELLATION_ENVIRONMENT,
    "ASYNC_SUBAGENT_SPAWNING_ENABLED": "true",
    "ASYNC_SUBAGENT_COMPLETION_WAIT_SECONDS": "240",
    "LANGSMITH_TRACING": "false",
}


def smoke_opt_in() -> tuple[bool, str]:
    if os.getenv(LIVE_FLAG) != "1":
        return False, f"{LIVE_FLAG}=1 is required for the RRM-010 combined smoke"
    for name in (
        "OPENAI_API_KEY",
        "AGENT_SERVER_ENDPOINT",
        TOKEN_ENV,
        "TEST_APPLICATION_POSTGRES_DSN",
        "TEST_MONGODB_URI",
    ):
        if not os.getenv(name, "").strip():
            return False, f"{name} is required for the RRM-010 combined smoke"
    if not NODE_EXECUTABLE.exists():
        return False, f"the pinned MCP servers and browser tool require node at {NODE_EXECUTABLE}"
    return True, ""


# --- Deterministic parent cognition ---------------------------------------------------------


@dataclass
class GoalHold:
    """Holds the first GoalDirected executor's first model call until `released` is set."""

    held: asyncio.Event = field(default_factory=asyncio.Event)
    released: asyncio.Event = field(default_factory=asyncio.Event)


class HeldGoalModel(TechnicalModel):
    """`TechnicalModel`, except that the first iteration's executor waits for `hold`."""

    hold: Any

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        first_executor = "goal-iteration/1/" in self.operation_id and (
            self.operation_id.rsplit("/", 1)[-1].startswith("executor")
        )
        if first_executor and not self.hold.released.is_set():
            self.hold.held.set()
            await self.hold.released.wait()
        return self._reply(messages)


def smoke_components(
    technical: TechnicalBinding,
    gate: CancellationGate,
    hold: GoalHold,
    model_log: list[dict[str, Any]],
) -> DeploymentCapabilityComponents:
    """One parent model per binding: a binding with an async child spawns it and is held by
    the cancellation gate; every other binding runs the technical cognition."""

    def parent(bound: Any, _secrets: Any) -> BaseChatModel:
        if bound.async_subagents:
            return SpawningModel(
                run_id=bound.run_id, operation_id=bound.operation_id, log=model_log, gate=gate
            )
        return HeldGoalModel(
            run_id=bound.run_id, operation_id=bound.operation_id, log=model_log, hold=hold
        )

    def child(bound: Any, _secrets: Any) -> BaseChatModel:
        return ChildModel(run_id=bound.run_id, operation_id=bound.operation_id, log=model_log)

    return DeploymentCapabilityComponents(
        model_factories={
            technical.binding.model.ref.digest: parent,
            technical.child_model_ref.digest: child,
        },
        prompts={
            technical.child_prompt_ref.digest: (
                "You are the technical child. Reply with the marker you are asked for."
            )
        },
        skill_bundles={technical.bundle.bundle_digest: technical.bundle},
        mcp_servers={server.ref.digest: server for server in technical.binding.mcp_servers},
    )


# --- Facade reads ---------------------------------------------------------------------------


async def get_json(stack: ProductionStack, path: str, **params: Any) -> dict[str, Any]:
    response = await stack.http.get(path, params={"request_scope": SCOPE, **params})
    assert response.status_code == 200, (path, response.text)
    return cast(dict[str, Any], response.json())


def _units_by_stage(read: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        item["semantic_operation_id"].split(":stage:", 1)[1].split(":", 1)[0]: item
        for item in read["data"]["units"]
    }


def _freshness(read: dict[str, Any]) -> dict[str, str]:
    return {name: section["freshness"] for name, section in read["sections"].items()}


async def parent_digest(stack: ProductionStack, run_id: str) -> dict[str, str]:
    return await run_authority_digest(stack.owner_pool, run_id, saver_schema=LANGGRAPH_SCHEMA)


async def assert_settled(stack: ProductionStack, run_id: str, outcome: str) -> dict[str, Any]:
    """Terminal with `outcome`; nothing reserved or pending; every effect claim settled."""

    run = await _run(stack, run_id)
    assert (run["phase"], run["terminal_outcome"]) == ("terminal", outcome), run
    budget = await get_json(stack, f"/run-control/v1/runs/{run_id}/budget")
    assert budget["reservations"] == {}, budget["reservations"]
    assert not any(budget["reserved"].values()), budget["reserved"]
    assert not any(budget["pending_settlement"].values()), budget["pending_settlement"]
    effects = await get_json(stack, f"/run-control/v1/runs/{run_id}/effects")
    claims = effects["claims"]
    assert claims and all(claim["settlement"] is not None for claim in claims.values()), claims
    if outcome == "completed":
        assert run["accepted_operation_settlement_evidence"], run
        assert run["accepted_output_evidence"], run
    return {
        "outcome": run["terminal_outcome"],
        "consumed": budget["consumed"],
        "effects": sorted(
            {(claim["effect_kind"], claim["disposition"]) for claim in claims.values()}
        ),
        "accepted_settlements": len(run["accepted_operation_settlement_evidence"]),
        "accepted_outputs": sorted(item["output_ref"] for item in run["accepted_output_evidence"]),
    }


async def wait_applied(stack: ProductionStack, run_id: str, command_id: str) -> list[str]:
    async def applied() -> bool:
        return (await _receipt_states(stack, run_id, command_id))[-1:] == ["applied"]

    await _wait_for(stack, run_id, applied, 180)
    receipts = await _receipt_states(stack, run_id, command_id)
    assert receipts == ["accepted", "delivered", "applied"], receipts
    return receipts


async def _fork(
    stack: ProductionStack,
    source_run: str,
    manifest: dict[str, Any],
    changes: list[dict[str, Any]],
    frontier: list[str],
) -> dict[str, Any]:
    """The fork view (`receipt` plus `reuse_decisions`); the request is idempotent."""

    body = {
        "request_scope": SCOPE,
        "request_id": f"fork-{source_run[:8]}",
        "idempotency_key": f"fork-{source_run[:8]}",
        "snapshot_id": manifest["snapshot_id"],
        "snapshot_digest": manifest["snapshot_digest"],
        "changes": changes,
        "invalidation_frontier": frontier,
        "baseline_reservations": {"tokens.total": 20},
        "sponsorship_ref": "sponsorship:test",
        "approval_refs": ["approval:test"],
        "reason": "RRM-010 combined technical smoke",
    }
    response = await stack.http.post(f"/run-control/v1/runs/{source_run}/forks", json=body)
    assert response.status_code == 201, response.text
    replay = await stack.http.post(f"/run-control/v1/runs/{source_run}/forks", json=body)
    assert replay.status_code == 201 and replay.json() == response.json(), replay.text
    view = await get_json(stack, f"/run-control/v1/forks/{body['request_id']}")
    assert view["receipt"] == response.json(), view
    return view


async def _independently_admitted(
    stack: ProductionStack, source_run: str, view: dict[str, Any]
) -> dict[str, Any]:
    """The derived run is its own admitted run (pending, epoch 1) with a recorded lineage."""

    receipt = view["receipt"]
    derived_run = receipt["target_run_id"]
    assert derived_run != source_run
    assert receipt["source_run_id"] == source_run, receipt
    assert receipt["target_execution_epoch"] == 1, receipt
    assert receipt["lineage"]["derived_execution_epoch"] == 1, receipt["lineage"]
    derived = await _run(stack, derived_run)
    assert derived["phase"] == "pending", derived
    assert derived["run_id"] == derived_run, derived
    return {
        "derived_run": derived_run,
        "derived_version": derived["version"],
        "request_id": receipt["request_id"],
        "admission_ref": receipt["admission_ref"],
        "patch_digest": receipt["patch_digest"],
        "lineage_digest": receipt["lineage"].get("lineage_digest"),
        "reused_unit_keys": receipt["lineage"]["reused_unit_keys"],
        "reuse": sorted({(item["decision"], item["reason"]) for item in view["reuse_decisions"]}),
    }


# --- Phase 1: an active real async child, inspected, refused a snapshot, then cancelled -------


def active_child_inspection(stack: ProductionStack) -> Any:
    """The drill's `before_cancel` hook: inspection shows the running child, the fork
    admission's snapshot classifies it as not quiescent."""

    async def inspect(
        run_id: str, child_id: str | None, provider_run_id: str | None
    ) -> dict[str, Any]:
        assert child_id is not None and provider_run_id is not None
        listed = await get_json(stack, INSPECTION, phase="active")
        assert run_id in [item["run_id"] for item in listed["data"]["items"]], listed
        read = await get_json(stack, f"{INSPECTION}/{run_id}")
        assert read["sections"]["async_children"]["freshness"] == "current", read["sections"]
        assert read["sections"]["temporal"]["freshness"] == "current", read["sections"]
        (child,) = read["data"]["async_children"]
        assert child["child_execution_id"] == child_id, child
        assert child["provider_run_id"] == provider_run_id, child
        assert child["detail_lifecycle"] in ACTIVE_CHILD_LIFECYCLES, child
        assert child["lifecycle"] not in TERMINAL_CHILD_LIFECYCLES, child
        assert child["cancellation_requested"] is False, child
        assert child["result_decision"] is None and child["settlement_ref"] is None, child
        units = _units_by_stage(read)
        # `draft` was accepted (the run has a stage boundary); `review` holds the child.
        assert units["draft"]["status"] == "settled", units
        assert units["review"]["status"] == "active", units
        unit_key = units["review"]["unit_key"]
        unit = await get_json(stack, f"{INSPECTION}/{run_id}/units/{unit_key}")
        assert unit["data"]["status"] == "active", unit["data"]["status"]
        assert [item["child_execution_id"] for item in unit["data"]["async_children"]] == [child_id]
        history = await get_json(stack, f"{INSPECTION}/{run_id}/units/{unit_key}/checkpoints")
        # The history reader anchors on recorded checkpoint keys (a transition's source,
        # result or namespace head). A first-generation unit still in cognition has recorded
        # none, so its history is reported unavailable rather than read from unrecorded saver
        # state; after the cancel's recorded transition it is current (below).
        checkpoints = history["sections"]["checkpoints"]
        assert (checkpoints["freshness"], checkpoints["reason"]) == (
            "unavailable",
            "no_recorded_checkpoint_key",
        ), history["sections"]
        assert history["data"]["entries"] == [], history["data"]
        snapshot = await stack.http.post(
            f"/run-control/v1/runs/{run_id}/snapshots", json={"request_scope": SCOPE}
        )
        assert snapshot.status_code == 409, snapshot.text
        detail = snapshot.json()["detail"]
        assert detail["code"] == "snapshot_not_quiescent", detail
        assert f"async_child_active:{child_id}" in detail["reasons"], detail
        return {
            "listed_active": True,
            "run_sections": _freshness(read),
            "child": {
                "lifecycle": child["lifecycle"],
                "detail_lifecycle": child["detail_lifecycle"],
                "dependency_class": child["dependency_class"],
                "provider_run_id": child["provider_run_id"],
            },
            "unit": {
                "status": unit["data"]["status"],
                "lease_state": unit["data"]["generations"][0]["lease_state"],
            },
            "checkpoint_history": [checkpoints["freshness"], checkpoints["reason"]],
            "snapshot_refused": {"code": detail["code"], "reasons": detail["reasons"]},
        }

    return inspect


async def served_child_identity(contract: AsyncSubagentContract) -> dict[str, Any]:
    """The Agent Server's served graph identity equals the contract (capability availability
    on the rebuilt server)."""

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(
            f"{os.environ['AGENT_SERVER_ENDPOINT'].rstrip('/')}"
            "/belllabs/async-subagents/served-graphs",
            headers={"Authorization": f"Bearer {mint_scope_claim(os.environ[TOKEN_ENV], SCOPE)}"},
        )
    assert response.status_code == 200, response.text
    served = [AsyncServedGraphIdentity.model_validate(item) for item in response.json()["graphs"]]
    matching = [item for item in served if item.matches(contract)]
    assert len(matching) == 1, (served, contract.graph_id)
    return matching[0].model_dump(mode="json")


async def cancel_with_active_async_child(
    stack: ProductionStack,
    technical: TechnicalBinding,
    async_technical: TechnicalBinding,
    gate: CancellationGate,
    catalog: TechnicalCatalog,
) -> dict[str, Any]:
    """`draft` settles, its wait is released (`applied`), `review` spawns a real async child
    and is held; inspection and the snapshot refusal run before the cancel."""

    evidence = await run_cancellation_drill(
        stack,
        async_technical,
        gate,
        catalog=catalog,
        before_cancel=active_child_inspection(stack),
        settled_draft=technical,
    )
    assert evidence["wait_release_receipts"] == ["accepted", "delivered", "applied"], evidence
    run_id = evidence["run_id"]
    read = await get_json(stack, f"{INSPECTION}/{run_id}")
    (child,) = read["data"]["async_children"]
    assert child["detail_lifecycle"] == "cancelled", child
    assert child["cancellation_requested"] is True, child
    assert child["result_decision"] == "reject" and child["settlement_ref"], child
    units = _units_by_stage(read)
    assert {stage: unit["status"] for stage, unit in units.items()} == {
        "draft": "settled",
        "review": "settled",
    }, units
    unit_key = units["review"]["unit_key"]
    history = await get_json(stack, f"{INSPECTION}/{run_id}/units/{unit_key}/checkpoints")
    assert history["sections"]["checkpoints"]["freshness"] == "current", history["sections"]
    entries = history["data"]["entries"]
    assert len(entries) >= 2 and all(entry["stamped"] for entry in entries), entries
    evidence["terminal_inspection"] = {
        "sections": _freshness(read),
        "child": {
            key: child[key]
            for key in ("lifecycle", "detail_lifecycle", "result_decision", "settlement_ref")
        },
        "reconciliation_state": read["data"]["reconciliation_state"],
        "checkpoint_history": [
            {"step": entry["step"], "roles": entry["roles"], "pending": entry["pending_task_names"]}
            for entry in entries
        ],
    }
    evidence["settlement"] = await assert_settled(stack, run_id, "cancelled")
    return evidence


# --- Phase 2: StageGraph inspection, historical read, safe fork and wait release ------------


async def stagegraph_fork_and_wait_release(
    stack: ProductionStack, technical: TechnicalBinding, catalog: TechnicalCatalog
) -> dict[str, Any]:
    policies = cast(ForkPatchPolicyRegistry, api.state.fork_patch_policies)
    policies.register(
        catalog.workflow_ref.digest,
        ForkPatchPolicy(
            policy_id="fork-patch-policy:rrm010-technical-stagegraph",
            family="stage_graph",
            patchable=(
                PatchablePath(path=stage_objective_path("review"), invalidates=("review",)),
            ),
        ),
    )
    templates = MongoStageGraphOperationTemplateRepository()
    source_run = await _admit(stack, catalog, f"rrm010-stagegraph-{uuid4().hex[:12]}")
    source_binding = f"semantic-input:rrm010:{source_run}"
    await templates.persist_templates(
        request_scope=SCOPE,
        semantic_input_binding_ref=source_binding,
        templates=stage_templates(technical, catalog),
        recorded_at=datetime.now(UTC),
    )
    await _launch(
        stack,
        source_run,
        {
            "request_scope": SCOPE,
            "run_id": source_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, source_run, source_binding, 1)),
        },
    )
    # `draft` settles (sync subagent, report written, MCP called); the run holds its wait.
    await _wait_for(stack, source_run, lambda: _holds_wait(stack, source_run), 150)

    # Inspection and a historical checkpoint read; reads never mutate authority.
    before_reads = await parent_digest(stack, source_run)
    read = await get_json(stack, f"{INSPECTION}/{source_run}")
    assert set(_freshness(read).values()) == {"current"}, read["sections"]
    assert read["data"]["projection"]["phase"] == "waiting", read["data"]["projection"]
    (draft,) = read["data"]["units"]
    assert draft["status"] == "settled", draft
    unit_key = draft["unit_key"]
    unit = await get_json(stack, f"{INSPECTION}/{source_run}/units/{unit_key}")
    assert unit["data"]["status"] == "settled"
    settlement = unit["data"]["journal"][0]["settlements"][0]
    assert settlement["status"] == "completed", settlement
    history = await get_json(stack, f"{INSPECTION}/{source_run}/units/{unit_key}/checkpoints")
    entries = history["data"]["entries"]
    assert history["sections"]["checkpoints"]["freshness"] == "current"
    assert len(entries) >= 4, entries
    assert all(entry["stamped"] and entry["binding_compatible"] for entry in entries), entries
    assert set(entries[-1]["roles"]) >= {"result"}, entries[-1]
    parents = [entry["key"]["parent_checkpoint_id"] for entry in entries[1:]]
    assert parents == [entry["key"]["checkpoint_id"] for entry in entries[:-1]]
    # An earlier checkpoint: the model's sync-subagent call, pending its `tools` step.
    historical = next(entry for entry in entries if "tools" in entry["pending_task_names"])
    assert historical["key"]["checkpoint_id"] != entries[-1]["key"]["checkpoint_id"]
    summary = await get_json(
        stack,
        f"{INSPECTION}/{source_run}/units/{unit_key}/checkpoints/"
        f"{historical['key']['checkpoint_id']}/summary",
    )
    assert summary["data"]["stamped_digests"]["belllabs_unit_key"] == unit_key, summary["data"]
    assert summary["data"]["pending_task_names"] == historical["pending_task_names"]
    assert summary["sections"]["checkpoint"]["redaction"]["withheld_field_count"] >= 1
    assert await parent_digest(stack, source_run) == before_reads

    # A safe snapshot at the declared boundary and a patched, independently admitted fork.
    snapshot = await stack.http.post(
        f"/run-control/v1/runs/{source_run}/snapshots", json={"request_scope": SCOPE}
    )
    assert snapshot.status_code == 201, snapshot.text
    manifest = snapshot.json()
    assert manifest["boundary_kind"] == "stage_settled", manifest["boundary_kind"]
    source_before = await parent_digest(stack, source_run)
    source_projection = await _run(stack, source_run)
    fork = await _fork(
        stack,
        source_run,
        manifest,
        [{"path": stage_objective_path("review"), "value": STAGE_OBJECTIVE}],
        ["review"],
    )
    admitted = await _independently_admitted(stack, source_run, fork)
    derived_run = admitted["derived_run"]
    derived_binding = fork_semantic_input_binding_ref(admitted["request_id"])
    derived_receipt = await _launch(
        stack,
        derived_run,
        {
            "request_scope": SCOPE,
            "run_id": derived_run,
            "family": "StageGraph",
            "stagegraph": asdict(
                stage_input(catalog, derived_run, derived_binding, admitted["derived_version"])
            ),
            "source_semantic_input_binding_ref": source_binding,
        },
    )
    assert derived_receipt["parent_run_id"] == source_run
    assert derived_receipt["fork_request_id"] == admitted["request_id"]
    assert await _visible(stack.client, f"BellLabsParentRunId = '{source_run}'", 1) == 1

    # Boundary intervention on the derived run: release its declared wait.
    await _release_wait(stack, derived_run)
    derived_release = await wait_applied(stack, derived_run, f"release:{derived_run[:8]}")
    await _wait_for(stack, derived_run, lambda: _terminal(stack, derived_run), 240)
    derived_settlement = await assert_settled(stack, derived_run, "completed")
    # The parent is untouched by the snapshot, the fork and the whole derived run.
    assert await parent_digest(stack, source_run) == source_before
    assert await _run(stack, source_run) == source_projection

    # Boundary intervention on the source: release its wait; it completes as before.
    await _release_wait(stack, source_run)
    source_release = await wait_applied(stack, source_run, f"release:{source_run[:8]}")
    await _wait_for(stack, source_run, lambda: _terminal(stack, source_run), 240)
    source_settlement = await assert_settled(stack, source_run, "completed")
    by_stage = {
        run_id: {ref.split(":")[2]: ref for ref in settled["accepted_outputs"]}
        for run_id, settled in (
            (source_run, source_settlement),
            (derived_run, derived_settlement),
        )
    }
    assert by_stage[derived_run]["draft"] == by_stage[source_run]["draft"]  # reused by ref
    assert by_stage[derived_run]["review"] != by_stage[source_run]["review"]  # patched
    lineages = _lineages(
        [
            *await _operation_payloads(stack, source_run),
            *await _operation_payloads(stack, derived_run),
        ]
    )
    assert len(lineages) == 3, lineages  # a reused result carries no lineage of its own
    for lineage in lineages:
        assert lineage["invoked"] == {
            "framework": ["write_file"],
            "mcp": ["lookup_binding_marker"],
            "sync_subagent": ["task"],
        }, lineage["invoked"]
    replayed = await _replay(
        stack.client,
        [
            f"belllabs-run/{source_run}",
            f"family/{source_run}/1",
            f"belllabs-run/{derived_run}",
            f"family/{derived_run}/1",
        ],
    )
    return {
        "source_run": source_run,
        "derived_run": derived_run,
        "inspection": {
            "run_sections": _freshness(read),
            "unit_status": unit["data"]["status"],
            "checkpoint_entries": len(entries),
            "historical_checkpoint": {
                "step": historical["step"],
                "pending": historical["pending_task_names"],
                "summary_digest": summary["data"]["summary_digest"],
            },
            "reads_left_authority_unchanged": True,
        },
        "snapshot": {
            "boundary_kind": manifest["boundary_kind"],
            "snapshot_digest": manifest["snapshot_digest"],
            "reuse_candidates": len(manifest["reuse_candidates"]),
        },
        "fork": admitted,
        "parent_unchanged": True,
        "wait_release_receipts": {"derived": derived_release, "source": source_release},
        "settlement": {"source": source_settlement, "derived": derived_settlement},
        "lineage_invoked": lineages[0]["invoked"],
        "replayed_events": replayed,
    }


# --- Phase 3: GoalDirected pause and resume, then a fork of the terminal run ----------------


async def goal_directed_pause_resume_and_fork(
    stack: ProductionStack,
    technical: TechnicalBinding,
    catalog: TechnicalCatalog,
    hold: GoalHold,
) -> dict[str, Any]:
    documents = MongoGoalDirectedDocumentRepository()

    async def persist(binding_ref: str) -> None:
        templates = goal_templates(technical, catalog)
        await documents.persist_templates(
            request_scope=SCOPE,
            semantic_input_binding_ref=binding_ref,
            executor=templates["executor"],
            verifier=templates["verifier"],
            recorded_at=datetime.now(UTC),
        )

    source_run = await _admit(stack, catalog, f"rrm010-goal-{uuid4().hex[:12]}")
    binding_ref = f"semantic-input:rrm010:{source_run}"
    await persist(binding_ref)
    await _launch(
        stack,
        source_run,
        {
            "request_scope": SCOPE,
            "run_id": source_run,
            "family": "GoalDirected",
            "goal_directed": asdict(
                goal_input(catalog, source_run, binding_ref, 1, GOAL_OBJECTIVE)
            ),
        },
    )
    family_id = f"family/{source_run}/1"
    await asyncio.wait_for(hold.held.wait(), timeout=180)

    # Pause while the first executor runs: delivered at once, applied at the iteration boundary.
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
                    reason="RRM-010 smoke: operator hold at the iteration boundary",
                    authority_ref="authority:lifecycle",
                ),
                runnable_work_remains=False,
            ),
            "workflow_run.pause",
        ),
    )
    assert paused["status"] == "accepted", paused

    async def delivered() -> bool:
        return "delivered" in await _receipt_states(stack, source_run, pause_id)

    await _wait_for(stack, source_run, delivered, 120)
    assert (await _run(stack, source_run))["phase"] == "active", "delivered is not applied"
    hold.released.set()
    pause_receipts = await wait_applied(stack, source_run, pause_id)

    async def family_recorded_pause() -> bool:
        state = await stack.client.get_workflow_handle(family_id).query(
            GoalDirectedWorkflow.boundary_state
        )
        return state["paused"] is not None

    await _wait_for(stack, source_run, family_recorded_pause, 120)
    projection = await _run(stack, source_run)
    assert projection["phase"] == "paused", projection["phase"]
    assert [item["decision_id"] for item in projection["active_pauses"]] == [pause_id]
    boundary = await stack.client.get_workflow_handle(family_id).query(
        GoalDirectedWorkflow.boundary_state
    )
    assert boundary["paused"]["next_goal_iteration"] == 2, boundary
    paused_read = await get_json(stack, f"{INSPECTION}/{source_run}")
    paused_units = [item["status"] for item in paused_read["data"]["units"]]
    assert paused_units == ["settled", "settled"], paused_units

    resume_id = f"resume:{source_run[:8]}"
    await _send(
        stack,
        source_run,
        _command(
            source_run,
            projection["version"],
            resume_id,
            ResumeAction(
                decision=ResumeDecision(
                    decision_id=resume_id,
                    pause_decision_id=pause_id,
                    reason="RRM-010 smoke: operator release",
                    authority_ref="authority:lifecycle",
                )
            ),
            "workflow_run.resume",
        ),
    )
    resume_receipts = await wait_applied(stack, source_run, resume_id)
    await _wait_for(stack, source_run, lambda: _terminal(stack, source_run), 300)
    source_settlement = await assert_settled(stack, source_run, "completed")
    # RRM-019: only the final executor's verified outputs are promoted.
    assert source_settlement["accepted_outputs"] == ["artifact:rrm009-goal:2"], source_settlement
    assert source_settlement["accepted_settlements"] == 4, source_settlement

    # A fork of the terminal run at the verifier boundary with a patched objective.
    snapshot = await stack.http.post(
        f"/run-control/v1/runs/{source_run}/snapshots", json={"request_scope": SCOPE}
    )
    assert snapshot.status_code == 201, snapshot.text
    manifest = snapshot.json()
    assert manifest["boundary_kind"] == "goal_verifier_settled", manifest["boundary_kind"]
    assert manifest["family_position"]["head_operation_role"] == "verifier"
    source_before = await parent_digest(stack, source_run)
    source_projection = await _run(stack, source_run)
    fork = await _fork(
        stack,
        source_run,
        manifest,
        [{"path": "goal.objective", "value": PATCHED_GOAL_OBJECTIVE}],
        ["*"],
    )
    admitted = await _independently_admitted(stack, source_run, fork)
    assert admitted["reused_unit_keys"] == []  # GoalDirected units are never reused
    assert set(admitted["reuse"]) == {("not_reusable", "goal_revision_identity_is_run_bound")}, (
        admitted["reuse"]
    )
    derived_run = admitted["derived_run"]
    derived_binding = fork_semantic_input_binding_ref(admitted["request_id"])
    await persist(derived_binding)
    derived_receipt = await _launch(
        stack,
        derived_run,
        {
            "request_scope": SCOPE,
            "run_id": derived_run,
            "family": "GoalDirected",
            "goal_directed": asdict(
                goal_input(
                    catalog,
                    derived_run,
                    derived_binding,
                    admitted["derived_version"],
                    PATCHED_GOAL_OBJECTIVE,
                )
            ),
        },
    )
    assert derived_receipt["parent_run_id"] == source_run
    assert await _visible(stack.client, f"BellLabsParentRunId = '{source_run}'", 1) == 1
    await _wait_for(stack, derived_run, lambda: _terminal(stack, derived_run), 300)
    derived_settlement = await assert_settled(stack, derived_run, "completed")
    assert derived_settlement["accepted_settlements"] == 4, derived_settlement
    assert await parent_digest(stack, source_run) == source_before
    assert await _run(stack, source_run) == source_projection
    source_ids = {
        item["settlement_id"]
        for item in source_projection["accepted_operation_settlement_evidence"]
    }
    derived_ids = {
        item["settlement_id"]
        for item in (await _run(stack, derived_run))["accepted_operation_settlement_evidence"]
    }
    assert source_ids and derived_ids and not source_ids & derived_ids
    replayed = await _replay(
        stack.client,
        [
            f"belllabs-run/{source_run}",
            family_id,
            f"belllabs-run/{derived_run}",
            f"family/{derived_run}/1",
        ],
    )
    return {
        "source_run": source_run,
        "derived_run": derived_run,
        "pause": {
            "receipts": pause_receipts,
            "next_goal_iteration": boundary["paused"]["next_goal_iteration"],
            "units_settled_while_paused": len(paused_units),
        },
        "resume_receipts": resume_receipts,
        "snapshot": {
            "boundary_kind": manifest["boundary_kind"],
            "snapshot_digest": manifest["snapshot_digest"],
            "goal_iteration": manifest["family_position"]["goal_iteration"],
        },
        "fork": admitted,
        "parent_unchanged": True,
        "settlement": {"source": source_settlement, "derived": derived_settlement},
        "replayed_events": replayed,
    }


# --- Capability availability ----------------------------------------------------------------


async def capability_availability(
    stack: ProductionStack, contract: AsyncSubagentContract
) -> dict[str, Any]:
    pinned = _pinned_summary(stack)
    assert pinned["mcp_servers"] and pinned["skills"] and pinned["tools"], pinned
    assert pinned["checkpointer_digests"] and pinned["store_digests"], pinned
    ready = await stack.http.get("/health/ready")
    assert ready.status_code == 200 and ready.json() == {
        "status": "ready",
        "mode": "runtime-control",
    }, ready.text
    return {
        "pinned": pinned,
        "readiness": api.state.runtime_control.readiness,
        "served_child_graph": await served_child_identity(contract),
    }


def print_evidence(label: str, evidence: dict[str, Any]) -> None:
    print(f"RRM-010 SMOKE EVIDENCE {label}:", json.dumps(evidence, sort_keys=True, default=str))


__all__ = [
    "GoalHold",
    "SMOKE_ENVIRONMENT",
    "cancel_with_active_async_child",
    "capability_availability",
    "goal_directed_pause_resume_and_fork",
    "print_evidence",
    "smoke_components",
    "smoke_opt_in",
    "stagegraph_fork_and_wait_release",
]
