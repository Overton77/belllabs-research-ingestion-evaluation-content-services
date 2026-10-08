"""FT-F2 on the time-skipping Temporal server: interrupt_and_inject through a running family.

The production GoalDirected activities and `operation.execute` activity run a two-iteration
run over in-memory run control, journal and lineage, a real `create_deep_agent` graph with the
deterministic RRM-016 model, sealed Context Packets and the command mailbox. While the first
executor turn is in its first model call, an operator sends `interrupt_and_inject` (twice, with
the same request id). The Deep Agents lane (`cancel_and_replace`) cancels the turn, finds no
uncertain effect, and runs the replacement turn on the same session with the injected item
sealed in a `follow_up_turn` packet; the run completes and the Command completes `applied`
exactly once.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from langchain_core.messages import BaseMessage
from langgraph.checkpoint.memory import InMemorySaver
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
from mission_control.application.execution.harness.inject import (
    InjectionSettings,
    InterruptAndInjectService,
)
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
from mission_control.domain.context.packet import ContextPurpose, ContextSourceKind
from mission_control.domain.policies.contracts import ActorContext, RunOutcome
from mission_control.domain.policies.mailbox import MailboxState
from tests.fixtures.checkpoint_recovery import MemoryOperationJournal
from tests.fixtures.goal_directed_journaled import (
    _ROLE,
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

QUEUE = "ft-f2-inject"
INJECTED = "Redirect: cite the 2024 meta-analysis first."


class SteerableModel(GoalScriptedModel):
    """The RRM-016 script reading its role from the operation's own input; an injected
    follow-up message (no role marker) is data the script records and otherwise skips."""

    def _observe(self, messages: list[BaseMessage]) -> tuple[int, int]:
        injected = [
            str(item.content)
            for item in messages
            if item.type == "human" and not _ROLE.search(str(item.content))
        ]
        self.__dict__.setdefault("injected", []).append(injected)
        kept = [
            item
            for item in messages
            if not (item.type == "human" and not _ROLE.search(str(item.content)))
        ]
        return super()._observe(kept)


def operator() -> ActorContext:
    source = control_actor()
    return source.model_copy(
        update={"permissions": source.permissions | {"workflow_run.read", "workflow_run.control"}}
    )


@pytest.mark.asyncio
async def test_interrupt_and_inject_replaces_the_running_executor_turn_on_temporal() -> None:
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
        model = SteerableModel()
        composition = await compose_goal_directed(
            run_control=run_control,
            journal=MemoryOperationJournal(),
            lineage=CheckpointLineageService(InMemoryCheckpointLineageRepository()),
            results=InMemoryArtifactPayloadStore(),
            bindings=InMemoryOperationBindingRepository(),
            saver=InMemorySaver(),
            model=model,
            blueprint=goal_blueprint(),
            context_packs=packs,
            context_inputs=staging,
            mailbox=mailbox,
            injections=InterruptAndInjectService(
                mailbox,
                packs=packs,
                settings=InjectionSettings(poll_seconds=0.05, settle_grace_seconds=1.0),
            ),
        )
        run_id = await admit_goal_run(run_control, "ft-f2-inject")
        facade = MissionControlService(
            run_control,
            BoundaryInterventionService(run_control),
            request_scope=SCOPE,
            mailbox=mailbox,
        )
        entered, gate = model.gate_on(1)  # the first executor call never returns by itself
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
                    OperationExecutionActivities(composition.service, worker_identity="ft-f2")
                ),
            ),
        ):
            handle = await environment.client.start_workflow(
                GoalDirectedWorkflow.run,
                goal_run_input(run_id, goal_blueprint(), baseline={"tokens.total": 20}),
                id=f"family/{run_id}/1",
                task_queue=QUEUE,
            )
            await asyncio.wait_for(entered.wait(), timeout=60)
            projection = await run_control.get_run(SCOPE, run_id)
            request = MissionCommandRequest.model_validate(
                {
                    "request_id": str(uuid4()),
                    "expected_version": projection.version,
                    "expected_generation": 1,
                    "target": {"kind": "run", "id": run_id},
                    "kind": "interrupt_and_inject",
                    "payload": {"content": {"text": INJECTED}},
                    "reason": "redirect the executor",
                }
            )
            first = await facade.command(run_id, request, operator())
            duplicate = await facade.command(run_id, request, operator())
            assert duplicate.replay and duplicate.delivery == first.delivery
            result = await handle.result()
            gate.set()

        assert result.goal_iterations == 2
        assert (await run_control.get_run(SCOPE, run_id)).terminal_outcome == RunOutcome.COMPLETED
        (entry,) = await repository.mailbox.list_entries(SCOPE, run_id)
        assert entry.state == MailboxState.CONSUMED
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
        observed = status.receipts[3].delivery_report
        assert observed.delivered_semantics == "cancel_and_replace"
        assert observed.native_refs.cancelled_turn_ref == "turn:1"
        assert observed.native_refs.replacement_turn_ref == "turn:2"
        # The replacement turn carried the follow_up_turn packet with the injected item.
        follow_ups = [
            packet
            for packet, _selection in selections.rows.values()
            if packet.target.purpose == ContextPurpose.FOLLOW_UP_TURN
        ]
        (packet,) = follow_ups
        (item,) = [i for i in packet.items if i.source_kind == ContextSourceKind.QUEUED_INSTRUCTION]
        assert item.inline is not None and item.inline.text == INJECTED
        seen: list[Any] = model.__dict__["injected"]
        assert any(any(INJECTED in text for text in call) for call in seen)
        # Exactly one interrupt for the duplicated request.
        events = [record.envelope.event_type for record in repository._outbox.values()]
        assert events.count("command.delivered") == 1
        assert events.count("command.completed") == 1
