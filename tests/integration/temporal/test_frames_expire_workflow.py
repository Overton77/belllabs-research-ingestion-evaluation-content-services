"""`mc.frames_expire.v1` runs `frames.expire` per tenant scope (SPEC-03 retention, C1)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pytest
from temporalio.client import ScheduleActionStartWorkflow, ScheduleAlreadyRunningError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from mission_control.adapters.temporal.activities.frames_expire import (
    FRAMES_EXPIRE_WORKFLOW,
    FramesExpireActivities,
    FramesExpireInput,
    ensure_frames_expire_schedule,
)
from mission_control.adapters.temporal.workflows.frames_expire import FramesExpireWorkflow


@dataclass(frozen=True)
class _Policy:
    retain_days: int = 30
    keep_closing_frames: bool = True


@dataclass(frozen=True)
class _Report:
    deleted_non_closing: int
    deleted_closing: int
    policy: _Policy


class _Retention:
    def __init__(self) -> None:
        self.calls: list[tuple[str, datetime]] = []

    async def expire(self, request_scope: str, *, now: datetime) -> _Report:
        self.calls.append((request_scope, now))
        return _Report(
            deleted_non_closing=len(request_scope) % 5, deleted_closing=0, policy=_Policy()
        )


@pytest.mark.asyncio
async def test_frames_expire_workflow_applies_retention_to_every_scope() -> None:
    retention = _Retention()
    scopes = ["mc/a/biotech/t1", "mc/a/biotech/t2"]
    async with await WorkflowEnvironment.start_time_skipping() as env:
        queue = f"frames-expire-{uuid.uuid4()}"
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[FramesExpireWorkflow],
            activities=[FramesExpireActivities(retention).expire],
        ):
            result = await env.client.execute_workflow(
                FRAMES_EXPIRE_WORKFLOW,
                FramesExpireInput(request_scopes=scopes),
                id=f"frames-expire-{uuid.uuid4()}",
                task_queue=queue,
                result_type=None,
            )
    assert [call[0] for call in retention.calls] == scopes
    assert all(call[1].tzinfo is not None for call in retention.calls)
    assert [item["request_scope"] for item in result["scopes"]] == scopes
    assert result["scopes"][0]["retain_days"] == 30


class _Handle:
    def __init__(self, client: _Client, schedule_id: str) -> None:
        self._client = client
        self._id = schedule_id

    async def update(self, updater: Any) -> None:
        self._client.updated.append((self._id, updater(None).schedule))


class _Client:
    def __init__(self, *, exists: bool) -> None:
        self.exists = exists
        self.created: list[tuple[str, Any]] = []
        self.updated: list[tuple[str, Any]] = []

    async def create_schedule(self, schedule_id: str, schedule: Any) -> None:
        if self.exists:
            raise ScheduleAlreadyRunningError
        self.created.append((schedule_id, schedule))

    def get_schedule_handle(self, schedule_id: str) -> _Handle:
        return _Handle(self, schedule_id)


@pytest.mark.asyncio
async def test_schedule_is_created_once_and_updated_in_place() -> None:
    fresh = _Client(exists=False)
    schedule_id = await ensure_frames_expire_schedule(
        fresh,  # type: ignore[arg-type]
        application_id="biotech",
        request_scopes=["mc/a/biotech/t1"],
        task_queue="mission-control-generic",
        every=timedelta(hours=6),
    )
    assert schedule_id == "mc-frames-expire:biotech"
    created = fresh.created[0][1]
    assert isinstance(created.action, ScheduleActionStartWorkflow)
    assert created.action.workflow == FRAMES_EXPIRE_WORKFLOW
    assert created.spec.intervals[0].every == timedelta(hours=6)
    existing = _Client(exists=True)
    await ensure_frames_expire_schedule(
        existing,  # type: ignore[arg-type]
        application_id="biotech",
        request_scopes=["mc/a/biotech/t1", "mc/a/biotech/t2"],
        task_queue="mission-control-generic",
    )
    assert existing.created == [] and existing.updated[0][0] == "mc-frames-expire:biotech"
