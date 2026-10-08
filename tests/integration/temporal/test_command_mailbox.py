"""FT-F1: a queued instruction is consumed exactly once across a worker restart.

The production GoalDirected activities and `operation.execute` activity run a two-iteration
run on the time-skipping Temporal server, over in-memory run control, journal and lineage, a
real `create_deep_agent` graph with the deterministic RRM-016 model, and sealed Context
Packets (FT-B3). An operator queues an instruction before the run starts; the iteration-1
executor boundary delivers it into its packet; the worker is lost after that delivery and
before the turn starts (the first `operation.execute` attempt dies); the retried attempt
starts the turn and consumes the entry. The entry is delivered and observed once, completes
`applied`, and appears in no later packet.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from mission_control.adapters.storage.artifact_payloads import InMemoryArtifactPayloadStore
from mission_control.adapters.temporal.operation_activities import OperationExecutionActivities
from mission_control.adapters.temporal.registration.activities import (
    agent_cognitive_activities,
    coordinator_activities,
)
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.goal_directed import GoalDirectedWorkflow
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.application.context.pack_service import ContextPackService
from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from mission_control.application.execution.operations.operation_execution import (
    InMemoryOperationBindingRepository,
)
from mission_control.application.execution.run_control_repository import (
    InMemoryRunControlRepository,
)
from mission_control.application.missions.service import MissionControlService
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.domain.context.packet import ContextSourceKind
from mission_control.domain.policies.contracts import ActorContext, RunOutcome
from mission_control.domain.policies.mailbox import MailboxState
from tests.fixtures.checkpoint_recovery import MemoryOperationJournal
from tests.fixtures.goal_directed_journaled import (
    SCOPE,
    GoalScriptedModel,
    admit_goal_run,
    compose_goal_directed,
    goal_blueprint,
    goal_run_control,
    goal_run_input,
)
from tests.unit.operations.test_ft_b2_stage_handoff import (
    FakeArtifacts,
    FakeSelections,
    FakeStaging,
)
from tests.unit.run_control.test_run_control import actor as control_actor

QUEUE = "ft-f1-command-mailbox"
INSTRUCTION = "Also cite the 2024 meta-analysis in the record."


class LostOnce(OperationExecutionActivities):
    """The first `operation.execute` delivery dies before it reaches the turn (worker loss)."""

    lost: int = 0

    @activity.defn(name="operation.execute")
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        if LostOnce.lost == 0:
            LostOnce.lost += 1
            raise RuntimeError("worker lost between delivery and turn start")
        return await super().execute(payload)


def operator() -> ActorContext:
    source = control_actor()
    return source.model_copy(
        update={"permissions": source.permissions | {"workflow_run.read", "workflow_run.control"}}
    )


@pytest.mark.asyncio
async def test_queued_instruction_is_consumed_once_across_a_worker_restart() -> None:
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        repository = InMemoryRunControlRepository()
        run_control = goal_run_control(repository)
        selections, staging = FakeSelections(), FakeStaging()
        packs = ContextPackService(
            artifacts=FakeArtifacts({}, scope=None), selections=selections, staging=staging
        )
        mailbox = MailboxDeliveryService(repository.mailbox, run_control)
        composition = await compose_goal_directed(
            run_control=run_control,
            journal=MemoryOperationJournal(),
            lineage=CheckpointLineageService(InMemoryCheckpointLineageRepository()),
            results=InMemoryArtifactPayloadStore(),
            bindings=InMemoryOperationBindingRepository(),
            saver=InMemorySaver(),
            model=GoalScriptedModel(),
            blueprint=goal_blueprint(),
            context_packs=packs,
            context_inputs=staging,
            mailbox=mailbox,
        )
        run_id = await admit_goal_run(run_control, "ft-f1-mailbox")
        facade = MissionControlService(
            run_control,
            BoundaryInterventionService(run_control),
            request_scope=SCOPE,
            mailbox=mailbox,
        )
        projection = await run_control.get_run(SCOPE, run_id)
        receipt = await facade.command(
            run_id,
            MissionCommandRequest.model_validate(
                {
                    "request_id": str(uuid4()),
                    "expected_version": projection.version,
                    "expected_generation": 1,
                    "target": {"kind": "run", "id": run_id},
                    "kind": "queue_instruction",
                    "payload": {"boundary": "next_iteration", "content": {"text": INSTRUCTION}},
                    "reason": "operator steering",
                }
            ),
            operator(),
        )
        assert receipt.delivery is not None
        assert [item.state.value for item in receipt.delivery.receipts] == ["accepted", "queued"]
        LostOnce.lost = 0
        async with (
            Worker(
                environment.client,
                task_queue=QUEUE,
                workflows=[GoalDirectedWorkflow, OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
                activities=coordinator_activities("GoalDirected", composition.family),
            ),
            Worker(
                environment.client,
                task_queue=composition.binding.task_queue,
                activities=agent_cognitive_activities(
                    LostOnce(composition.service, worker_identity="ft-f1")
                ),
            ),
        ):
            result = await environment.client.execute_workflow(
                GoalDirectedWorkflow.run,
                goal_run_input(run_id, goal_blueprint(), baseline={"tokens.total": 20}),
                id=f"family/{run_id}/1",
                task_queue=QUEUE,
            )

        assert LostOnce.lost == 1
        assert result.goal_iterations == 2
        assert (await run_control.get_run(SCOPE, run_id)).terminal_outcome == RunOutcome.COMPLETED
        (entry,) = await repository.mailbox.list_entries(SCOPE, run_id)
        assert entry.state == MailboxState.CONSUMED
        assert entry.delivery_key is not None and "goal-iteration/1/executor" in entry.delivery_key
        status = await run_control.get_boundary_command(
            SCOPE, run_id, entry.command_issuer, entry.command_id
        )
        assert status is not None
        assert [item.state.value for item in status.receipts] == [
            "accepted",
            "queued",
            "delivered",
            "observed",
            "applied",
        ]
        assert {
            item.delivery_report.delivered_semantics
            for item in status.receipts
            if item.delivery_report is not None
        } == {"turn_boundary_guaranteed"}

        # Exactly one packet carries the instruction: the first executor's, first in line
        # after the operating contract and goals.
        carrying = [
            packet
            for packet, _selection in selections.rows.values()
            if any(
                item.source_kind == ContextSourceKind.QUEUED_INSTRUCTION for item in packet.items
            )
        ]
        assert [packet.target.activation_id for packet in carrying] == ["goal-iteration/1/executor"]
        (item,) = [
            item
            for item in carrying[0].items
            if item.source_kind == ContextSourceKind.QUEUED_INSTRUCTION
        ]
        assert item.inline is not None and item.inline.text == INSTRUCTION
        assert item.mandatory and item.trust.value == "admitted_input"
        kinds = [item.source_kind for item in carrying[0].items]
        assert kinds.index(ContextSourceKind.QUEUED_INSTRUCTION) < kinds.index(
            ContextSourceKind.WORKSPACE_MAP
        )
        events = [record.envelope.event_type for record in repository._outbox.values()]
        assert events.count("command.queued") == 1
        assert events.count("command.delivered") == 1
        assert events.count("command.completed") == 1
