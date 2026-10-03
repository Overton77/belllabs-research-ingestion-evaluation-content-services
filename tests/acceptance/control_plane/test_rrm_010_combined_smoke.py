"""RRM-010 combined technical smoke on the production composition (live opt-in).

One qualification joins, through the governed facade on one production stack:

* scoped inspection (list, run, unit, checkpoint history) and a historical checkpoint read;
* a safe derived fork per family, independently admitted, with the parent's authority unchanged;
* boundary interventions with `applied` receipts: StageGraph wait releases and a GoalDirected
  pause and resume;
* running cancellation with an active real async subagent on the Agent Server: inspection
  shows the child, the fork admission's snapshot classifies it as not quiescent, the cancel
  reaches it at the provider, its usage is reconciled and the run is `cancelled`;
* full result, usage and effect settlement of every run and capability availability.

Opt-in: `BELLABS_RUN_RRM_010_LIVE=1` with the disposable DSNs, the `rrm009-agent-server`
endpoint and its signing secret. Parent cognition is deterministic; the hosted child is the
only live model (a few thousand tokens). See `tests/fixtures/rrm010_combined_smoke.py`.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tests.fixtures.rrm009_cancellation import (
    CancellationGate,
    async_child_binding,
    publish_cancellation_catalog,
)
from tests.fixtures.rrm009_production_harness import (
    mongo_database,  # noqa: F401 - the per-test Mongo database fixture
    open_production_stack,
)
from tests.fixtures.rrm009_production_stack import publish_technical_catalog, technical_binding
from tests.fixtures.rrm010_combined_smoke import (
    SMOKE_ENVIRONMENT,
    GoalHold,
    cancel_with_active_async_child,
    capability_availability,
    goal_directed_pause_resume_and_fork,
    print_evidence,
    smoke_components,
    smoke_opt_in,
    stagegraph_fork_and_wait_release,
)


@pytest.mark.asyncio
async def test_combined_technical_smoke_on_the_production_composition(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,  # noqa: F811
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    enabled, reason = smoke_opt_in()
    if not enabled:
        pytest.skip(reason)
    endpoint = os.environ["AGENT_SERVER_ENDPOINT"]
    technical = technical_binding()
    async_technical, contract = async_child_binding(endpoint)
    gate = CancellationGate("cognition")
    hold = GoalHold()
    model_log: list[dict[str, Any]] = []
    async with open_production_stack(
        dsn=test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical=technical,
        components=smoke_components(technical, gate, hold, model_log),
        model_log=model_log,
        extra_environment={**SMOKE_ENVIRONMENT, "AGENT_SERVER_ENDPOINT": endpoint},
    ) as stack:
        capabilities = await capability_availability(stack, contract)
        stage_catalog = await publish_cancellation_catalog(stack)
        cancellation = await cancel_with_active_async_child(
            stack, technical, async_technical, gate, stage_catalog
        )
        stagegraph = await stagegraph_fork_and_wait_release(stack, technical, stage_catalog)
        goal_catalog = await publish_technical_catalog(
            stack.control_plane, family="GoalDirected", now=datetime.now(UTC)
        )
        goal_directed = await goal_directed_pause_resume_and_fork(
            stack, technical, goal_catalog, hold
        )
    assert cancellation["child"]["cancellation_receipt"] == "provider_acknowledged"
    print_evidence("capabilities", capabilities)
    print_evidence("cancellation_with_active_async_child", cancellation)
    print_evidence("stagegraph_fork_and_wait_release", stagegraph)
    print_evidence("goal_directed_pause_resume_and_fork", goal_directed)
