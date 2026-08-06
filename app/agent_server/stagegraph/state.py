from __future__ import annotations

from typing import Annotated, TypedDict

from app.agent_server.reducers import merge_unique_events


class StageGraphInput(TypedDict):
    request_scope: str
    belllabs_run_id: str
    execution_epoch: int
    graph_assembly_digest: str
    run_plan_digest: str
    next_stage_ref: str


class StageGraphState(StageGraphInput):
    event_refs: Annotated[tuple[str, ...], merge_unique_events]


class StageGraphOutput(TypedDict):
    request_scope: str
    belllabs_run_id: str
    execution_epoch: int
    event_refs: tuple[str, ...]
