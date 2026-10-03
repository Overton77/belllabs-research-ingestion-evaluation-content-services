"""RRM-009 ticket item 6: subagent cancellation inside the production composition.

RRM-008's running cancellation, composed by the deployment (`compose_runtime_control`,
`ProductionWorkerActivityCompositionFactory`, `create_production_workers`) and driven only
through the facade. See `tests/fixtures/rrm009_cancellation.py` for the drill.

1. Sync subagent (deterministic, DSN opt-in): the cancel lands while the in-process child's
   model call is in flight; the unit settles `cancelled` with its interrupted lineage, nothing
   resumes and the run terminalizes `cancelled` with its cancel `applied`.
2. Async subagent (live opt-in `BELLABS_RUN_RRM_009_LIVE=1`, the RRM-009 Agent Server with
   signed scope claims, a real hosted child model): the cancel lands while the child is
   running, either during the parent's cognition or while the operation boundary waits for the
   child. The child is cancelled at the provider and acknowledged, rejected, its usage stays
   pending and keeps the run `cancelling` until the privileged usage reconciliation route
   settles it and hints the family; the run terminalizes `cancelled`, receipts `applied`.
   A third case replaces the worker set while the child runs: the retried attempt reconnects
   to the same child (no second spawn or provider run; the worker shutdown settled nothing)
   and the cancel then reaches it. This is the core of RRM-010's combined smoke.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from tests.fixtures.rrm009_cancellation import (
    CANCELLATION_ENVIRONMENT,
    CancellationGate,
    CancellationWindow,
    async_child_binding,
    cancellation_components,
    run_cancellation_drill,
)
from tests.fixtures.rrm009_live_capabilities import live_opt_in
from tests.fixtures.rrm009_production_harness import (
    mongo_database,  # noqa: F401 - the per-test Mongo database fixture
    open_production_stack,
)
from tests.fixtures.rrm009_production_stack import technical_binding


def _evidence(label: str, evidence: dict[str, Any]) -> None:
    print(f"RRM-009 CANCEL EVIDENCE {label}:", json.dumps(evidence, sort_keys=True, default=str))


@pytest.mark.asyncio
async def test_sync_subagent_cancellation_in_the_production_composition(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,  # noqa: F811
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    technical = technical_binding()
    gate = CancellationGate("sync_child")
    model_log: list[dict[str, Any]] = []
    async with open_production_stack(
        dsn=test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical=technical,
        components=cancellation_components(technical, gate, model_log),
        model_log=model_log,
        extra_environment=dict(CANCELLATION_ENVIRONMENT),
    ) as stack:
        evidence = await run_cancellation_drill(stack, technical, gate)
    assert evidence["model_calls"] == {"parent": 1, "child": 1}
    _evidence("sync_child", evidence)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("window", "restart"),
    [("cognition", False), ("completion_wait", False), ("cognition", True)],
    ids=["cognition", "completion_wait", "restart_then_cancel"],
)
async def test_async_subagent_cancellation_in_the_production_composition(
    window: CancellationWindow,
    restart: bool,
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,  # noqa: F811
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    enabled, reason = live_opt_in()
    if not enabled:
        pytest.skip(reason)
    endpoint = os.environ["AGENT_SERVER_ENDPOINT"]
    technical, _contract = async_child_binding(endpoint)
    gate = CancellationGate(window, restart_workers=restart)
    model_log: list[dict[str, Any]] = []
    async with open_production_stack(
        dsn=test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical=technical,
        components=cancellation_components(technical, gate, model_log),
        model_log=model_log,
        extra_environment={
            **CANCELLATION_ENVIRONMENT,
            "ASYNC_SUBAGENT_SPAWNING_ENABLED": "true",
            "AGENT_SERVER_ENDPOINT": endpoint,
            "ASYNC_SUBAGENT_COMPLETION_WAIT_SECONDS": "240",
            "LANGSMITH_TRACING": "false",
        },
    ) as stack:
        evidence = await run_cancellation_drill(stack, technical, gate)
    assert evidence["child"]["cancellation_receipt"] == "provider_acknowledged"
    _evidence(f"{window}{':restart' if restart else ''}", evidence)
