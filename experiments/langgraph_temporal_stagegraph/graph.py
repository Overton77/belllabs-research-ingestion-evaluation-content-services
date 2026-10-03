from __future__ import annotations

from typing import Any

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send, interrupt
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from .config import ExperimentSettings
from .contracts import TemporalStageInput, workflow_identity
from .graph_state import DispatchItem, ExperimentState
from .repository import ExperimentRepository
from .temporal_workflows import TemporalStageWorkflow

FAST_PROMPT = (
    "In at most 120 words, identify two plausible mechanisms by which cellular senescence "
    "can affect tissue aging. This is research discussion, not medical advice."
)
SLOW_PROMPT = (
    "In at most 120 words, identify two evidence-quality concerns when interpreting "
    "preclinical longevity studies. This is research discussion, not medical advice."
)
TERMINAL_STATUSES = {"ADMITTED", "FAILED", "CANCELLED"}


def synthesis_ready(admitted_outputs: dict[str, str]) -> bool:
    return any(stage in admitted_outputs for stage in ("fast_research", "slow_research"))


def choose_synthesis_inputs(attempts: list[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
    candidates = [
        item
        for item in attempts
        if item["stage_id"] in {"fast_research", "slow_research"} and item["status"] == "ADMITTED"
    ]
    candidates.sort(key=lambda item: (item["admitted_at"], item["stage_id"]))
    return tuple(candidates[:1])


def _send_batch(state: ExperimentState) -> list[Send]:
    return [
        Send(
            "launch_temporal_stage",
            {
                "run_id": state["run_id"],
                "thread_id": state["thread_id"],
                "dispatch_item": item,
            },
        )
        for item in state.get("dispatch_batch", ())
    ]


async def launch_stage(
    state: ExperimentState,
    *,
    repository: ExperimentRepository,
    temporal: Client,
    settings: ExperimentSettings,
) -> dict[str, Any]:
    """Start-or-reconnect only; deliberately never awaits the workflow result."""
    item = state["dispatch_item"]
    workflow_id = workflow_identity(item["attempt_id"])
    request = TemporalStageInput(
        run_id=state["run_id"],
        thread_id=state["thread_id"],
        attempt_id=item["attempt_id"],
        stage_id=item["stage_id"],
        prompt=item["prompt"],
        delay_seconds=item["delay_seconds"],
        model=settings.openai_model,
        trace_headers={},
    )
    try:
        handle = await temporal.start_workflow(
            TemporalStageWorkflow.run,
            request,
            id=workflow_id,
            task_queue=settings.temporal_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
        run_id: str | None = handle.result_run_id
    except WorkflowAlreadyStartedError:
        handle = temporal.get_workflow_handle(workflow_id)
        run_id = handle.run_id
    await repository.bind_temporal_execution(item["attempt_id"], workflow_id, run_id)
    return {
        "launch_receipts": (
            {
                "attempt_id": item["attempt_id"],
                "temporal_workflow_id": workflow_id,
                "temporal_run_id": run_id,
            },
        ),
        "event_log": (
            {
                "event_id": f"launched:{item['attempt_id']}",
                "event": "temporal_launch_acknowledged",
                "stage_id": item["stage_id"],
            },
        ),
    }


def compile_experiment_graph(
    *,
    repository: ExperimentRepository,
    temporal: Client,
    settings: ExperimentSettings,
    checkpointer: Any,
):
    async def initialize(state: ExperimentState) -> dict[str, Any]:
        fast = await repository.reserve_attempt(state["run_id"], "fast_research", FAST_PROMPT, 2.0)
        # Current model latency can exceed 20 seconds. A 45-second controlled activity delay
        # leaves enough deterministic headroom for the any(1) timing assertion.
        slow = await repository.reserve_attempt(state["run_id"], "slow_research", SLOW_PROMPT, 45.0)
        batch = tuple(
            DispatchItem(
                attempt_id=item["attempt_id"],
                stage_id=item["stage_id"],
                prompt=item["prompt"],
                delay_seconds=item["delay_seconds"],
            )
            for item in (fast, slow)
        )
        return {
            "dispatch_batch": batch,
            "waiting_attempt_ids": tuple(item["attempt_id"] for item in batch),
            "admitted_outputs": {},
            "synthesized_output": None,
            "frozen_synthesis_stages": (),
            "event_log": ({"event_id": f"prepared:{state['run_id']}", "event": "prepared"},),
        }

    async def launch_temporal_stage(state: ExperimentState) -> dict[str, Any]:
        return await launch_stage(
            state, repository=repository, temporal=temporal, settings=settings
        )

    async def after_launch(state: ExperimentState) -> dict[str, Any]:
        return {"dispatch_batch": ()}

    async def reconcile(state: ExperimentState) -> dict[str, Any]:
        attempts = await repository.load_attempts(state["run_id"])
        failed = [a for a in attempts if a["status"] in {"FAILED", "CANCELLED"}]
        if failed:
            raise RuntimeError(
                "stage execution failed: "
                + ", ".join(f"{a['stage_id']}:{a['error_type']}" for a in failed)
            )
        for item in attempts:
            if item["status"] == "READY_TO_RECONCILE":
                await repository.admit_success_idempotently(item["attempt_id"])
        attempts = await repository.load_attempts(state["run_id"])
        admitted = {
            item["stage_id"]: item["output_ref"]
            for item in attempts
            if item["status"] == "ADMITTED"
        }
        synthesis = next((a for a in attempts if a["stage_id"] == "synthesize"), None)
        batch: tuple[DispatchItem, ...] = ()
        frozen = state.get("frozen_synthesis_stages", ())
        if synthesis is None and synthesis_ready(admitted):
            chosen = choose_synthesis_inputs(attempts)
            frozen = tuple(item["stage_id"] for item in chosen)
            blocks = []
            for item in chosen:
                text = await repository.load_result_text(item["output_ref"])
                blocks.append(f"{item['stage_id']}: {text}")
            prompt = (
                "In at most 150 words, synthesize the following admitted research result into "
                "a cautious architecture-experiment summary. Do not add unprovided evidence.\n\n"
                + "\n\n".join(blocks)
            )
            reserved = await repository.reserve_attempt(state["run_id"], "synthesize", prompt, 0.0)
            batch = (
                DispatchItem(
                    attempt_id=reserved["attempt_id"],
                    stage_id=reserved["stage_id"],
                    prompt=reserved["prompt"],
                    delay_seconds=reserved["delay_seconds"],
                ),
            )
            attempts = await repository.load_attempts(state["run_id"])
        waiting = tuple(
            item["attempt_id"] for item in attempts if item["status"] not in TERMINAL_STATUSES
        )
        return {
            "dispatch_batch": batch,
            "admitted_outputs": admitted,
            "waiting_attempt_ids": waiting,
            "synthesized_output": admitted.get("synthesize"),
            "frozen_synthesis_stages": frozen,
        }

    def route_after_reconcile(state: ExperimentState):
        if state.get("dispatch_batch"):
            return _send_batch(state)
        if state.get("waiting_attempt_ids"):
            return "wait_for_completion"
        if state.get("synthesized_output"):
            return "finish"
        raise RuntimeError("graph has no dispatch, wait, or successful terminal state")

    def wait_for_completion(state: ExperimentState) -> dict[str, Any]:
        wake = interrupt(
            {
                "kind": "temporal_stage_completion",
                "run_id": state["run_id"],
                "waiting_attempt_ids": state["waiting_attempt_ids"],
            }
        )
        return {
            "event_log": (
                {
                    "event_id": f"wake:{wake['wake_event_id']}",
                    "event": "graph_resumed",
                    "wake_event_id": wake["wake_event_id"],
                },
            )
        }

    async def finish(state: ExperimentState) -> dict[str, Any]:
        await repository.finish_run(state["run_id"])
        return {"event_log": ({"event_id": f"finished:{state['run_id']}", "event": "finished"},)}

    builder = StateGraph(ExperimentState)
    builder.add_node("initialize", initialize)
    builder.add_node("launch_temporal_stage", launch_temporal_stage)
    builder.add_node("after_launch", after_launch)
    builder.add_node("reconcile", reconcile)
    builder.add_node("wait_for_completion", wait_for_completion)
    builder.add_node("finish", finish)
    builder.add_edge(START, "initialize")
    builder.add_conditional_edges("initialize", _send_batch, ["launch_temporal_stage"])
    builder.add_edge("launch_temporal_stage", "after_launch")
    builder.add_edge("after_launch", "reconcile")
    builder.add_conditional_edges(
        "reconcile",
        route_after_reconcile,
        ["launch_temporal_stage", "wait_for_completion", "finish"],
    )
    builder.add_edge("wait_for_completion", "reconcile")
    builder.add_edge("finish", END)
    return builder.compile(checkpointer=checkpointer)
