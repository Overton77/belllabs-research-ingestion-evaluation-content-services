from __future__ import annotations

from typing import Annotated, NotRequired, TypedDict

from app.agent_server.reducers import merge_unique_events


class StageGraphInput(TypedDict):
    request_scope: str
    belllabs_run_id: str
    execution_epoch: int
    graph_assembly_digest: str
    run_plan_digest: str
    next_stage_ref: str
    runtime_binding_ref: NotRequired[str]
    state_schema_digest: NotRequired[str]
    checkpoint_binding_version: NotRequired[int]
    lifecycle_projection_ref: NotRequired[str]
    lifecycle_projection_version: NotRequired[int]
    lifecycle_projection_digest: NotRequired[str]
    deployment_endpoint_id: NotRequired[str]
    deployment_revision: NotRequired[str]
    graph_id: NotRequired[str]


class StageGraphState(StageGraphInput):
    event_refs: Annotated[tuple[str, ...], merge_unique_events]
    runtime_reconciliation: NotRequired[dict[str, object]]


class StageGraphOutput(TypedDict):
    request_scope: str
    belllabs_run_id: str
    execution_epoch: int
    event_refs: tuple[str, ...]
