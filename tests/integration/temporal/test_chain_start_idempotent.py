"""FT-D2: the relay starts a released chain consumer idempotently (SPEC-04, ADR-0031).

Time-skipping Temporal environment: the chain relay delivers the same ``mc.chain.start_run``
intent twice (the acknowledgement of the first delivery is lost). The production starter goes
through the governed ``RunLaunchService`` and the mission-scoped ``TemporalWorkflowSubmitter``,
whose root id is derived from the admitted run and started with ``USE_EXISTING`` and
``REJECT_DUPLICATE``: both deliveries resolve to one workflow execution.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from temporalio.testing import WorkflowEnvironment

from mission_control.adapters.temporal.submission import TemporalWorkflowSubmitter
from mission_control.application.chains.relay import (
    ChainIntent,
    ChainIntentRelay,
    ChainStartReceipt,
    LaunchServiceChainStarter,
)
from mission_control.application.execution.run_launch import RunLaunchService
from mission_control.application.execution.service import RunControlService
from mission_control.contracts.identities import mission_root_id, parse_request_scope
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.fixtures import GENERIC_GOAL_DIRECTED
from mission_control.domain.programs.contracts import GoalDirectedRunInput, GoalRevision
from tests.fixtures.mission_control_common_db import canonical_scope
from tests.unit.run_control.test_run_control import request, service

SCOPE = canonical_scope("tenant-1")
NOW = datetime(2026, 10, 8, 12, tzinfo=UTC)


def revision(run_id: str) -> GoalRevision:
    values = {
        "schema_version": "belllabs.goal-revision.v1",
        "revision_id": "goal-revision:1",
        "revision": 1,
        "parent_revision_id": None,
        "envelope_digest": sha256_digest({"chain": "consumer-envelope", "run": run_id}),
        "objective": "Ingest the supplied evidence map",
        "tactical_changes": (),
        "evidence_refs": ("input:chain",),
        "unmet_obligations": (),
        "proposer": "application:chain",
        "deciding_authority": "authority:chain",
        "applicability": "remaining_run",
        "tactics": (),
        "subgoals": (),
        "coverage_emphasis": (),
    }
    return GoalRevision(canonical_digest=sha256_digest(values), **values)  # type: ignore[arg-type]


class AdmittedRunInputs:
    """``ChainLaunchInputPort`` for the test: the admitted run's GoalDirected input."""

    def __init__(self, run_control: RunControlService) -> None:
        self._run_control = run_control
        self.calls = 0

    async def family_input(self, intent: ChainIntent) -> dict[str, Any]:
        self.calls += 1
        run = await self._run_control.get_run(intent.request_scope, intent.run_key)
        budget = await self._run_control.get_budget(intent.request_scope, intent.run_key)
        blueprint = GENERIC_GOAL_DIRECTED
        goal = revision(run.run_id)
        return asdict(
            GoalDirectedRunInput(
                run_id=run.run_id,
                request_scope=run.request_scope,
                effective_configuration_digest=run.effective_configuration_digest,
                blueprint_digest=sha256_digest(blueprint),
                blueprint=blueprint.model_dump(mode="json"),
                envelope_digest=goal.envelope_digest,
                initial_revision=goal,
                initial_run_version=run.version,
                baseline_reservation=dict(budget.reservations.get("baseline", {})),
                semantic_input_binding_ref="semantic-input:chain-consumer",
            )
        )


class OneShotStore:
    """An intent store whose acknowledgement is lost once, so the intent is delivered twice."""

    def __init__(self, intent: ChainIntent) -> None:
        self.intent = intent
        self.deliveries = 0
        self.delivered = False

    async def lease(self, *_args: Any, **_kwargs: Any) -> tuple[ChainIntent, ...]:
        return () if self.delivered else (self.intent,)

    async def mark_delivered(self, *_args: Any, **_kwargs: Any) -> None:
        self.deliveries += 1
        if self.deliveries >= 2:
            self.delivered = True

    async def mark_failed(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("delivery must not fail")


@pytest.mark.asyncio
async def test_delivering_the_same_start_intent_twice_yields_one_workflow() -> None:
    run_service, _ = service()
    admitted = await run_service.admit(request(request_scope=SCOPE, request_id=str(uuid4())))
    assert admitted.run_id is not None
    parsed = parse_request_scope(SCOPE)
    intent = ChainIntent(
        intent_kind="start_run",
        delivery_key=f"chain-start:{admitted.run_id}",
        request_scope=SCOPE,
        chain_id=uuid4(),
        mission_id=uuid4(),
        run_id=UUID(int=1),
        run_key=admitted.run_id,
        family="GoalDirected",
        initial_goal="Ingest the supplied evidence map",
        actor_ref="chain:test",
    )
    async with await WorkflowEnvironment.start_time_skipping() as env:
        submitter = TemporalWorkflowSubmitter(
            env.client,
            stagegraph_task_queue="chain-test-stagegraph",
            goal_directed_task_queue="chain-test-goal",
            mission_installation_id=parsed.installation_id,
            mission_application_id=parsed.application_id,
        )
        inputs = AdmittedRunInputs(run_service)
        starter = LaunchServiceChainStarter(
            inputs=inputs,
            launches=RunLaunchService(run_control=run_service, submitter=submitter),
        )
        store = OneShotStore(intent)
        relay = ChainIntentRelay(store=store, starter=starter)
        first = await relay.relay_once(SCOPE, now=NOW)
        second = await relay.relay_once(SCOPE, now=NOW)
        assert first.delivered == second.delivered == (intent.delivery_key,)
        receipts: list[ChainStartReceipt] = [
            first.receipts[intent.delivery_key],
            second.receipts[intent.delivery_key],
        ]
        workflow_id = mission_root_id(SCOPE, admitted.run_id)
        assert {receipt.workflow_id for receipt in receipts} == {workflow_id}
        assert receipts[0].temporal_run_id == receipts[1].temporal_run_id
        description = await env.client.get_workflow_handle(workflow_id).describe()
        assert description.run_id == receipts[0].temporal_run_id
        assert inputs.calls == 2
        # A third delivery after acknowledgement finds nothing to deliver.
        assert (await relay.relay_once(SCOPE, now=NOW)).delivered == ()
