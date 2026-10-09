"""MP-06 shared setup: a Cursor-shaped lane unit whose frames and lane state live in a real
PostgreSQL 17 (runtime role, forced RLS), behind the in-memory governed boundary of
`tests.fixtures.lane_turns.lane_stack`. The provider is a labelled fixture lane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import asyncpg

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.lanes.execution_state import PostgresLaneExecutionStateStore
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    LaneTurnService,
)
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.domain.execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionRequest,
)
from tests.fixtures.lane_turns import LaneStack, cursor_binding, cursor_operation, lane_stack
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.frames_common import admit_unit_attempt


@dataclass
class PgLaneUnit:
    operation: OperationExecutionRequest
    stack: LaneStack
    frames: PostgresFrameRepository
    states: PostgresLaneExecutionStateStore

    @property
    def identity(self) -> LaneExecutionIdentity:
        return LaneExecutionIdentity.of(self.operation, "cursor_local", 1)

    @property
    def send_key(self) -> str:
        return f"{self.identity.harness_execution_id}:1:turn:1"

    def service(self, owner: str, *, min_lease_s: float = 1.0, **extra: Any) -> LaneTurnService:
        return LaneTurnService(
            lanes=self.stack.service._lanes,
            boundary=self.stack.boundary,
            frames=self.frames,
            states=self.states,
            frame_reader=self.frames,
            sessions=WorkerSessionManager(
                owner_ref=owner, min_lease=timedelta(seconds=min_lease_s)
            ),
            **extra,
        )


async def pg_lane_unit(
    pool: asyncpg.Pool, db: CommonDatabase, lane: Any, *, task_queue: str | None = None
) -> PgLaneUnit:
    admitted = await admit_unit_attempt(pool, db)
    unit = admitted.unit
    binding = cursor_binding(task_queue=task_queue) if task_queue else None
    payload = cursor_operation(binding=binding).model_dump(mode="python")
    payload.update(
        request_scope=unit.request_scope,
        identity=OperationAttemptIdentity(
            run_id=admitted.run_key,
            operation_id=unit.semantic_operation_id,
            operation_attempt=unit.semantic_attempt,
        ),
        runtime_unit=unit,
        idempotency_key=f"mp06:{unit.unit_key}",
    )
    operation = OperationExecutionRequest.model_validate(payload)
    stack = lane_stack(lane, operation=operation)
    return PgLaneUnit(
        operation=operation,
        stack=stack,
        frames=PostgresFrameRepository(pool),
        states=PostgresLaneExecutionStateStore(pool),
    )


__all__ = ["PgLaneUnit", "pg_lane_unit"]
