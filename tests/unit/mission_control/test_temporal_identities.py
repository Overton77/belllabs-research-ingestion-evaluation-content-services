from dataclasses import asdict, replace
from datetime import UTC, datetime
from uuid import UUID

import pytest
from temporalio import workflow
from temporalio.api.common.v1 import Payloads, WorkflowType
from temporalio.api.history.v1 import HistoryEvent, WorkflowExecutionStartedEventAttributes
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError

from mission_control.adapters.temporal.linked_run_workflow import linked_root_input
from mission_control.adapters.temporal.submission import TemporalWorkflowSubmitter
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.mission_run import MissionRunWorkflow
from mission_control.adapters.temporal.workflows.operation import MissionOperationWorkflow
from mission_control.application.execution.run_launch import RunLaunchRequest, RunLaunchService
from mission_control.contracts.identities import (
    mission_linked_observer_id,
    mission_operation_id,
    mission_root_id,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef
from mission_control.domain.authoring.fixtures import GENERIC_STAGE_GRAPH
from mission_control.domain.composition.contracts import RunCompositionLink
from mission_control.domain.coordinator.launch import BlueprintFamily, LaunchIdempotencyConflict
from mission_control.domain.programs.contracts import GoalDirectedRunInput, StageGraphRunInput
from tests.fixtures.rrm009_production_stack import goal_blueprint, goal_revision
from tests.unit.run_control.test_run_control import actor, request, service

INSTALLATION = UUID("00000000-0000-0000-0000-000000000001")
SCOPE = f"mc/{INSTALLATION}/biotech/00000000-0000-0000-0000-000000000002"
DIGEST = "sha256:" + "a" * 64


class RecordingTemporalClient:
    def __init__(self):
        self.submissions = []
        self.data_converter = DataConverter.default
        self.started = None
        self.workflow_type = "mc.mission_run.v1"

    async def start_workflow(self, run, payload, **options):
        self.submissions.append((run, payload, options))
        if self.started is None:
            self.started = payload
        owner = self

        class Handle:
            id = options["id"]
            first_execution_run_id = "native-temporal-run"

            async def fetch_history_events(self, **_kwargs):
                yield HistoryEvent(
                    event_id=1,
                    workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                        workflow_type=WorkflowType(name=owner.workflow_type),
                        input=Payloads(payloads=await owner.data_converter.encode([owner.started])),
                    ),
                )

        return Handle()


def run_input(scope=SCOPE):
    return StageGraphRunInput(
        run_id="run-1",
        request_scope=scope,
        effective_configuration_digest=DIGEST,
        workflow_type_digest=DIGEST,
        blueprint_digest=sha256_digest(GENERIC_STAGE_GRAPH),
        blueprint=GENERIC_STAGE_GRAPH.model_dump(mode="json"),
    )


@pytest.mark.asyncio
async def test_new_submission_uses_scoped_root_and_retains_scope_after_continuation():
    client = RecordingTemporalClient()
    submitter = TemporalWorkflowSubmitter(
        client,
        stagegraph_task_queue="stage",
        goal_directed_task_queue="goal",
        mission_installation_id=INSTALLATION,
        mission_application_id="biotech",
    )
    result = await submitter.submit(
        run_input(),
        workflow_id="untrusted-id",
        blueprint_family=BlueprintFamily.STAGE_GRAPH,
    )
    run, payload, options = client.submissions[0]
    expected = f"mc/{INSTALLATION}/biotech/run/run-1"
    assert result.workflow_id == expected
    assert options["id"] == expected
    assert run is MissionRunWorkflow.run
    assert payload.schema_version == "mc.mission_run.v1"
    assert payload.workflow_type_digest == DIGEST
    assert payload.workflow_type_digest != run_input().blueprint_digest
    assert payload.family_workflow_id == expected + "/family/1"
    continued = replace(payload, continuity=payload.continuity.next_technical_segment())
    assert continued.workflow_id == expected
    assert continued.family_workflow_id == payload.family_workflow_id
    assert mission_operation_id(SCOPE, "run-1", "attempt:1") == expected + "/operation/attempt:1"


@pytest.mark.asyncio
async def test_cross_application_cannot_start_on_another_bound_submitter():
    client = RecordingTemporalClient()
    submitter = TemporalWorkflowSubmitter(
        client,
        stagegraph_task_queue="stage",
        goal_directed_task_queue="goal",
        mission_installation_id=INSTALLATION,
        mission_application_id="biotech",
    )
    with pytest.raises(ValueError, match="trusted application"):
        await submitter.submit(
            run_input(SCOPE.replace("/biotech/", "/ai-engineer/")),
            workflow_id="anything",
            blueprint_family=BlueprintFamily.STAGE_GRAPH,
        )
    assert client.submissions == []


@pytest.mark.parametrize("scope", ["tenant", SCOPE + "/extra", SCOPE.replace("biotech", "..")])
def test_invalid_scope_cannot_become_a_temporal_identity(scope):
    with pytest.raises(ValueError):
        mission_root_id(scope, "run-1")


def test_mission_registrations_reuse_governed_control_handlers():
    root = workflow._Definition.must_from_class(MissionRunWorkflow)
    operation = workflow._Definition.must_from_class(MissionOperationWorkflow)
    assert root.name == "mc.mission_run.v1"
    assert "deliver_cancel" in root.updates
    assert "signal_message" in root.signals
    assert operation.name == "mc.operation.v1"
    assert "unit_reconciliation_recorded" in operation.signals


@pytest.mark.asyncio
async def test_scoped_duplicate_start_verifies_stored_immutable_input():
    client = RecordingTemporalClient()
    submitter = TemporalWorkflowSubmitter(
        client,
        stagegraph_task_queue="stage",
        goal_directed_task_queue="goal",
        mission_installation_id=INSTALLATION,
        mission_application_id="biotech",
    )
    original = run_input()
    await submitter.submit(
        original, workflow_id="ignored", blueprint_family=BlueprintFamily.STAGE_GRAPH
    )
    await submitter.submit(
        original, workflow_id="ignored", blueprint_family=BlueprintFamily.STAGE_GRAPH
    )
    with pytest.raises(LaunchIdempotencyConflict, match="immutable binding"):
        await submitter.submit(
            replace(original, semantic_input_binding_ref="different-semantic-input"),
            workflow_id="ignored",
            blueprint_family=BlueprintFamily.STAGE_GRAPH,
        )
    client.workflow_type = "unrelated-workflow"
    with pytest.raises(LaunchIdempotencyConflict, match="registration"):
        await submitter.submit(
            original,
            workflow_id="ignored",
            blueprint_family=BlueprintFamily.STAGE_GRAPH,
        )


@pytest.mark.asyncio
async def test_new_root_rejects_cross_scope_family_as_nonretryable_failure():
    client = RecordingTemporalClient()
    submitter = TemporalWorkflowSubmitter(
        client,
        stagegraph_task_queue="stage",
        goal_directed_task_queue="goal",
        mission_installation_id=INSTALLATION,
        mission_application_id="biotech",
    )
    await submitter.submit(
        run_input(), workflow_id="ignored", blueprint_family=BlueprintFamily.STAGE_GRAPH
    )
    root = client.started
    invalid = replace(root, family_input={**root.family_input, "request_scope": "other-tenant"})
    with pytest.raises(ApplicationError) as rejected:
        await MissionRunWorkflow().run(invalid)
    assert rejected.value.non_retryable


def test_semantic_path_segments_are_escaped_without_collision():
    literal = mission_operation_id(SCOPE, "run-1", "operation/a")
    encoded = mission_operation_id(SCOPE, "run-1", "operation%2Fa")
    assert literal.endswith("operation%2Fa")
    assert literal != encoded
    other = SCOPE.replace("biotech", "ai-engineer")
    assert mission_linked_observer_id(SCOPE, "parent", "link") != (
        mission_linked_observer_id(other, "parent", "link")
    )


def test_linked_root_preserves_parent_and_enforces_child_scope_and_binding():
    child = run_input()
    link = RunCompositionLink(
        link_id="link",
        request_identity="request",
        request_fingerprint=DIGEST,
        request_scope=SCOPE,
        parent_run_id="parent",
        child_run_id=child.run_id,
        slot_id="child",
        request_revision=1,
        target_workflow_type_ref=ExactDefinitionRef(
            kind=DefinitionKind.WORKFLOW_TYPE,
            logical_id="fixture",
            revision=1,
            digest=DIGEST,
        ),
        child_effective_configuration_digest=DIGEST,
        dependency_class="required_blocking",
        linked_budget_account_id="account",
        result_admission_policy="exact",
        cancellation_policy="request_cancel",
        created_at=datetime.now(UTC),
    )
    payload = {"child_input": asdict(child), "child_task_queue": "stage"}
    root = linked_root_input(link, payload)
    assert root.parent_run_id == "parent"
    assert root.workflow_id == mission_root_id(SCOPE, child.run_id)
    with pytest.raises(ValueError, match="child authority"):
        linked_root_input(
            link, {**payload, "child_input": asdict(replace(child, request_scope="other"))}
        )


@pytest.mark.asyncio
async def test_new_workflow_subclasses_prepare_in_the_production_sandbox():
    runner = coordinator_workflow_runner()
    for implementation in (MissionRunWorkflow, MissionOperationWorkflow):
        runner.prepare_workflow(workflow._Definition.must_from_class(implementation))


@pytest.mark.asyncio
async def test_goal_launch_derives_missing_workflow_type_from_admitted_authority():
    authority, _ = service()
    admitted = await authority.admit(request(request_scope=SCOPE, request_id="goal-mission"))
    assert admitted.run_id is not None
    projection = await authority.get_run(SCOPE, admitted.run_id)
    blueprint = goal_blueprint()
    revision = goal_revision(admitted.run_id, "bounded goal")
    family = GoalDirectedRunInput(
        run_id=admitted.run_id,
        request_scope=SCOPE,
        effective_configuration_digest=projection.effective_configuration_digest,
        blueprint_digest=sha256_digest(blueprint),
        blueprint=blueprint.model_dump(mode="json"),
        envelope_digest=revision.envelope_digest,
        initial_revision=revision,
        semantic_input_binding_ref="input:goal",
        baseline_reservation={"tokens.total": 20},
    )
    client = RecordingTemporalClient()
    submitter = TemporalWorkflowSubmitter(
        client,
        stagegraph_task_queue="stage",
        goal_directed_task_queue="goal",
        mission_installation_id=INSTALLATION,
        mission_application_id="biotech",
    )
    launcher = RunLaunchService(run_control=authority, submitter=submitter)
    await launcher.launch(
        RunLaunchRequest(
            request_scope=SCOPE,
            run_id=admitted.run_id,
            family="GoalDirected",
            goal_directed=asdict(family),
        ),
        actor(),
    )
    root = client.started
    assert root.workflow_type_digest == projection.workflow_type_ref.digest
    assert root.family_input["workflow_type_digest"] == root.workflow_type_digest
