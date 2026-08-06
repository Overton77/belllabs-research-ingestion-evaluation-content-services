from __future__ import annotations

from typing import Annotated, TypedDict

from app.agent_server.reducers import merge_unique_events


class GoalDirectedInput(TypedDict):
    request_scope: str
    belllabs_run_id: str
    execution_epoch: int
    graph_assembly_digest: str
    run_plan_digest: str
    goal_ref: str
    verifier_ref: str


class GoalDirectedState(GoalDirectedInput):
    event_refs: Annotated[tuple[str, ...], merge_unique_events]


class GoalDirectedOutput(TypedDict):
    request_scope: str
    belllabs_run_id: str
    execution_epoch: int
    event_refs: tuple[str, ...]
