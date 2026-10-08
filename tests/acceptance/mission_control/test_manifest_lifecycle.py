"""FT-E3 acceptance: compile, submit and start Mission 1 on the real local stack.

`missions/01-research-ingestion-deep-agents.yml` compiles against the seeded catalog fixture,
submit commits the revision and admits the run through the API's own admission path, and
start launches it through the governed launch service on the local Temporal dev server with
the production workers and deterministic local cognition (no provider, no paid effect). The
Stage Graph reaches `active` (running) and its first released stage is `collect`; the run is then
cancelled so nothing outlives the proof.
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import pytest
from tests.fixtures.manifest_runtime import AUTHOR, compose_lifecycle
from tests.fixtures.mission_control_production_stack import open_postgres_production_stack
from tests.fixtures.rrm009_production_harness import (
    ProductionStack,
    _command,
    _run,
    _send,
    _terminal,
    _wait_for,
)
from tests.fixtures.rrm009_production_stack import SCOPE

from mission_control.application.authoring.manifest_submit import SubmitRequest
from mission_control.domain.policies.contracts import CancelAction

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
MISSION_1 = ROOT / "docs/specs/fast-track-2026-10/missions/01-research-ingestion-deep-agents.yml"
PROOF = ROOT / ".scratch/fast-track-2026-10-07/E3"


@pytest.fixture
async def stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[ProductionStack]:
    async with open_postgres_production_stack(root=tmp_path, monkeypatch=monkeypatch) as production:
        yield production


def _operations(stack: ProductionStack, run_id: str) -> list[str]:
    seen: list[str] = []
    for item in stack.model_log:
        if item["run_id"] == run_id and item["operation"] not in seen:
            seen.append(item["operation"])
    return seen


@pytest.mark.asyncio
async def test_mission_1_compiles_submits_and_starts_a_stage_graph_at_collect(
    stack: ProductionStack,
) -> None:
    service, inputs = await compose_lifecycle(stack, SCOPE)
    manifest = MISSION_1.read_text(encoding="utf-8")
    receipt, replayed = await service.submit(
        SubmitRequest(
            manifest_yaml=manifest,
            request_id=uuid4(),
            actor=AUTHOR,
            sponsorship_refs=frozenset({"sponsorship:test"}),
        )
    )
    assert not replayed and not receipt.unchanged
    (mission,) = receipt.missions
    assert mission.family == "StageGraph" and mission.run_id is not None
    run_id = mission.run_id
    admitted = await _run(stack, run_id)
    assert admitted["phase"] == "pending", admitted

    started = await service.start(run_id, AUTHOR)
    assert started.family == "StageGraph"
    assert inputs.staged == [run_id]
    # controls.subscriptions: one stream subscription registered before the launch.
    assert [item.channel for item in started.subscriptions] == ["stream_ticket"]

    async def collect_released() -> bool:
        return bool(_operations(stack, run_id))

    await _wait_for(stack, run_id, collect_released, 180)
    running = await _run(stack, run_id)
    assert running["phase"] == "active", running
    operations = _operations(stack, run_id)
    assert "collect" in operations[0], operations

    decision = await _send(
        stack,
        run_id,
        _command(
            run_id,
            running["version"],
            f"cancel:{run_id}",
            CancelAction(),
            "workflow_run.cancel",
        ),
    )
    assert decision["reason_code"] == "accepted", decision
    await _wait_for(stack, run_id, lambda: _terminal(stack, run_id), 180)
    terminal = await _run(stack, run_id)
    assert terminal["terminal_outcome"] == "cancelled", terminal
    evidence = {
        "manifest": MISSION_1.name,
        "mission_id": str(mission.mission_id),
        "revision_id": str(mission.revision_id),
        "run_id": run_id,
        "workflow_id": started.workflow_id,
        "first_operations": operations,
        "phase_after_start": running["phase"],
        "terminal_outcome": terminal["terminal_outcome"],
        "subscriptions": [item.model_dump(mode="json") for item in started.subscriptions],
    }
    if os.environ.get("MISSION_CONTROL_RECORD_PROOF") == "1":
        PROOF.mkdir(parents=True, exist_ok=True)
        (PROOF / "mission-1-lifecycle.json").write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    print("FT-E3 EVIDENCE:", json.dumps(evidence, sort_keys=True))
