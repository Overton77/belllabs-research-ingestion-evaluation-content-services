from __future__ import annotations

import json
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send, interrupt
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.experiments.langgraph_temporal_stagegraph.contracts import (
    TemporalStageInput,
    workflow_identity,
)
from app.experiments.langgraph_temporal_stagegraph.graph_state import (
    DispatchItem,
    LaunchReceipt,
    merge_events,
    merge_receipts,
)
from app.experiments.langgraph_temporal_stagegraph.repository import ExperimentRepository

from .config import SwarmSettings
from .contracts import (
    AcceptedClaim,
    FinalSynthesis,
    MissionPlan,
    ResearchUnitResult,
    SourceBundle,
)
from .evaluators import evaluate_unit
from .repository import SwarmEvidenceRepository
from .temporal_workflows import SwarmStageWorkflow

TERMINAL = {"ADMITTED", "FAILED", "CANCELLED"}


class SwarmState(TypedDict, total=False):
    run_id: str
    thread_id: str
    objective: str
    dispatch_item: DispatchItem
    dispatch_batch: tuple[DispatchItem, ...]
    launch_receipts: Annotated[tuple[LaunchReceipt, ...], merge_receipts]
    waiting_attempt_ids: tuple[str, ...]
    planned_unit_ids: tuple[str, ...]
    accepted_claim_ids: tuple[str, ...]
    final_output_ref: str | None
    event_log: Annotated[tuple[dict[str, object], ...], merge_events]


def _item(row: dict[str, Any]) -> DispatchItem:
    return DispatchItem(
        attempt_id=row["attempt_id"],
        stage_id=row["stage_id"],
        prompt=row["prompt"],
        delay_seconds=0.0,
    )


def _sends(state: SwarmState) -> list[Send]:
    return [
        Send(
            "launch_stage",
            {
                "run_id": state["run_id"],
                "thread_id": state["thread_id"],
                "dispatch_item": item,
            },
        )
        for item in state.get("dispatch_batch", ())
    ]


async def launch_swarm_stage(
    state: SwarmState,
    *,
    repository: ExperimentRepository,
    temporal: Client,
    settings: SwarmSettings,
) -> dict[str, Any]:
    item = state["dispatch_item"]
    workflow_id = workflow_identity(item["attempt_id"])
    request = TemporalStageInput(
        run_id=state["run_id"],
        thread_id=state["thread_id"],
        attempt_id=item["attempt_id"],
        stage_id=item["stage_id"],
        prompt=item["prompt"],
        delay_seconds=0,
        model=settings.openai_model,
        trace_headers={},
    )
    try:
        handle = await temporal.start_workflow(
            SwarmStageWorkflow.run,
            request,
            id=workflow_id,
            task_queue=settings.temporal_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
        temporal_run_id = handle.result_run_id
    except WorkflowAlreadyStartedError:
        handle = temporal.get_workflow_handle(workflow_id)
        temporal_run_id = handle.run_id
    await repository.bind_temporal_execution(item["attempt_id"], workflow_id, temporal_run_id)
    return {
        "launch_receipts": (
            {
                "attempt_id": item["attempt_id"],
                "temporal_workflow_id": workflow_id,
                "temporal_run_id": temporal_run_id,
            },
        ),
        "event_log": (
            {
                "event_id": f"launch:{item['attempt_id']}",
                "event": "temporal_launch_acknowledged",
                "stage_id": item["stage_id"],
            },
        ),
    }


def compile_swarm_graph(
    *,
    repository: ExperimentRepository,
    evidence_repository: SwarmEvidenceRepository,
    temporal: Client,
    settings: SwarmSettings,
    checkpointer: Any,
):
    async def initialize(state: SwarmState) -> dict[str, Any]:
        payload = json.dumps(
            {
                "kind": "bootstrap",
                "objective": state["objective"],
                "max_sources": settings.max_sources_per_unit,
            },
            sort_keys=True,
        )
        row = await repository.reserve_attempt(state["run_id"], "bootstrap_search", payload, 0)
        return {
            "dispatch_batch": (_item(row),),
            "waiting_attempt_ids": (row["attempt_id"],),
            "planned_unit_ids": (),
            "accepted_claim_ids": (),
            "final_output_ref": None,
        }

    async def launch_stage(state: SwarmState) -> dict[str, Any]:
        return await launch_swarm_stage(
            state, repository=repository, temporal=temporal, settings=settings
        )

    async def after_launch(state: SwarmState) -> dict[str, Any]:
        del state
        return {"dispatch_batch": ()}

    async def reconcile(state: SwarmState) -> dict[str, Any]:
        attempts = await repository.load_attempts(state["run_id"])
        failed = [item for item in attempts if item["status"] in {"FAILED", "CANCELLED"}]
        if failed:
            raise RuntimeError(
                "swarm stage failed: "
                + ", ".join(f"{item['stage_id']}:{item['error_type']}" for item in failed)
            )
        for attempt in attempts:
            if attempt["status"] == "READY_TO_RECONCILE":
                await repository.admit_success_idempotently(attempt["attempt_id"])
        attempts = await repository.load_attempts(state["run_id"])
        by_stage = {item["stage_id"]: item for item in attempts}
        batch: list[DispatchItem] = []
        planned_ids = state.get("planned_unit_ids", ())
        accepted_ids = state.get("accepted_claim_ids", ())

        bootstrap = by_stage.get("bootstrap_search")
        planner = by_stage.get("mission_planner")
        if bootstrap and bootstrap["status"] == "ADMITTED" and planner is None:
            bundle = SourceBundle.model_validate_json(bootstrap["output_text"])
            payload = json.dumps(
                {
                    "kind": "plan",
                    "objective": state["objective"],
                    "bootstrap": bundle.model_dump(mode="json"),
                },
                sort_keys=True,
            )
            batch.append(
                _item(
                    await repository.reserve_attempt(state["run_id"], "mission_planner", payload, 0)
                )
            )
        elif planner and planner["status"] == "ADMITTED":
            plan = MissionPlan.model_validate_json(planner["output_text"])
            bounded_units = plan.units[: settings.max_units]
            planned_ids = tuple(unit.unit_id for unit in bounded_units)
            await evidence_repository.save_plan(
                state["run_id"], plan.model_copy(update={"units": bounded_units})
            )
            missing_units = [
                unit for unit in bounded_units if f"research__{unit.unit_id}" not in by_stage
            ]
            for unit in missing_units:
                payload = json.dumps(
                    {
                        "kind": "research",
                        "objective": state["objective"],
                        "unit": unit.model_dump(mode="json"),
                        "max_sources": settings.max_sources_per_unit,
                    },
                    sort_keys=True,
                )
                batch.append(
                    _item(
                        await repository.reserve_attempt(
                            state["run_id"], f"research__{unit.unit_id}", payload, 0
                        )
                    )
                )

            research_attempts = [by_stage.get(f"research__{unit_id}") for unit_id in planned_ids]
            all_research_admitted = bool(research_attempts) and all(
                item is not None and item["status"] == "ADMITTED" for item in research_attempts
            )
            synthesis = by_stage.get("synthesize")
            if all_research_admitted and synthesis is None:
                accepted: list[AcceptedClaim] = []
                for item in research_attempts:
                    if item is None:
                        raise RuntimeError("admitted research attempt is missing")
                    result = ResearchUnitResult.model_validate_json(item["output_text"])
                    evaluations = evaluate_unit(state["run_id"], result)
                    await evidence_repository.save_unit_evidence(
                        state["run_id"], result, evaluations
                    )
                    sources = {source.source_id: source for source in result.sources}
                    for claim, evaluation in zip(result.analysis.claims, evaluations, strict=True):
                        if evaluation.disposition == "ACCEPT":
                            source = sources[claim.source_id]
                            accepted.append(
                                AcceptedClaim(
                                    claim_id=evaluation.claim_id,
                                    unit_id=result.unit.unit_id,
                                    claim_text=claim.claim_text,
                                    source_url=source.url,
                                    source_quote=claim.source_quote,
                                )
                            )
                if not accepted:
                    raise RuntimeError("deterministic evaluators rejected every research claim")
                accepted_ids = tuple(item.claim_id for item in accepted)
                payload = json.dumps(
                    {
                        "kind": "synthesize",
                        "objective": state["objective"],
                        "claims": [item.model_dump(mode="json") for item in accepted],
                    },
                    sort_keys=True,
                )
                batch.append(
                    _item(
                        await repository.reserve_attempt(state["run_id"], "synthesize", payload, 0)
                    )
                )

        attempts = await repository.load_attempts(state["run_id"])
        by_stage = {item["stage_id"]: item for item in attempts}
        synthesis = by_stage.get("synthesize")
        final_ref = (
            synthesis["output_ref"]
            if synthesis is not None and synthesis["status"] == "ADMITTED"
            else None
        )
        waiting = tuple(item["attempt_id"] for item in attempts if item["status"] not in TERMINAL)
        return {
            "dispatch_batch": tuple(batch),
            "waiting_attempt_ids": waiting,
            "planned_unit_ids": planned_ids,
            "accepted_claim_ids": accepted_ids,
            "final_output_ref": final_ref,
        }

    def route(state: SwarmState):
        if state.get("dispatch_batch"):
            return _sends(state)
        if state.get("waiting_attempt_ids"):
            return "wait"
        if state.get("final_output_ref"):
            return "finish"
        raise RuntimeError("swarm graph has no runnable or terminal condition")

    def wait(state: SwarmState) -> dict[str, Any]:
        wake = interrupt(
            {
                "kind": "swarm_stage_completion",
                "run_id": state["run_id"],
                "waiting_attempt_ids": state["waiting_attempt_ids"],
            }
        )
        return {
            "event_log": (
                {
                    "event_id": f"wake:{wake['wake_event_id']}",
                    "event": "graph_resumed",
                },
            )
        }

    async def finish(state: SwarmState) -> dict[str, Any]:
        final = next(
            item
            for item in await repository.load_attempts(state["run_id"])
            if item["stage_id"] == "synthesize"
        )
        FinalSynthesis.model_validate_json(final["output_text"])
        await repository.finish_run(state["run_id"])
        return {"event_log": ({"event_id": f"finish:{state['run_id']}", "event": "finish"},)}

    builder = StateGraph(SwarmState)
    builder.add_node("initialize", initialize)
    builder.add_node("launch_stage", launch_stage)
    builder.add_node("after_launch", after_launch)
    builder.add_node("reconcile", reconcile)
    builder.add_node("wait", wait)
    builder.add_node("finish", finish)
    builder.add_edge(START, "initialize")
    builder.add_conditional_edges("initialize", _sends, ["launch_stage"])
    builder.add_edge("launch_stage", "after_launch")
    builder.add_edge("after_launch", "reconcile")
    builder.add_conditional_edges("reconcile", route, ["launch_stage", "wait", "finish"])
    builder.add_edge("wait", "reconcile")
    builder.add_edge("finish", END)
    return builder.compile(checkpointer=checkpointer)
