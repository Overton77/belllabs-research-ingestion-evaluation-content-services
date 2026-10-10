"""Recovery 2026-10-09: a worker serves the lane task queues its launch bindings route to.

Cursor, Claude and Codex bindings carry the `task_queue` their `lane.*` activities run on;
production polled only `<base>.agent-cognitive`, so a binding routed to a dedicated lane queue
was never served. `MISSION_CONTROL_LANE_TASK_QUEUES` names the extra queues of this worker.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from mission_control.adapters.temporal import worker as module
from tests.fixtures.isolated_settings import isolated_settings


def _record(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    queues: list[str] = []

    def cognitive(_client: Any, *, task_queue: str, **_values: Any) -> str:
        queues.append(task_queue)
        return task_queue

    monkeypatch.setattr(module, "create_agent_cognitive_worker", cognitive)
    monkeypatch.setattr(
        module, "create_coordinator_workers", lambda *_a, **_k: SimpleNamespace(workers=())
    )
    return queues


def test_lane_queues_are_polled_with_the_cognitive_activities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues = _record(monkeypatch)
    settings = isolated_settings(
        temporal_task_queue="mc",
        mission_control_lane_task_queues=("mc-lane-claude", "mc-lane-codex", "mc-lane-claude"),
    )
    composition = SimpleNamespace(coordinator=None, operation=object(), artifacts=None)
    workers = module.create_production_workers(None, settings, composition)  # type: ignore[arg-type]
    cognitive = module.BellLabsTaskQueues.from_base("mc").agent_cognitive
    assert queues == [cognitive, "mc-lane-claude", "mc-lane-codex"]
    assert workers.lanes == ("mc-lane-claude", "mc-lane-codex")
    assert workers.workers[-2:] == ("mc-lane-claude", "mc-lane-codex")


def test_no_lane_queue_and_the_cognitive_queue_itself_add_no_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues = _record(monkeypatch)
    cognitive = module.BellLabsTaskQueues.from_base("mc").agent_cognitive
    settings = isolated_settings(
        temporal_task_queue="mc", mission_control_lane_task_queues=(cognitive,)
    )
    composition = SimpleNamespace(coordinator=None, operation=object(), artifacts=None)
    workers = module.create_production_workers(None, settings, composition)  # type: ignore[arg-type]
    assert queues == [cognitive] and workers.lanes == ()
