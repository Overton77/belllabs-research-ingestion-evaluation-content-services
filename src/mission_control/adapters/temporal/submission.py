from __future__ import annotations

from dataclasses import asdict, replace
from datetime import timedelta
from typing import Any
from uuid import UUID

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from mission_control.adapters.temporal.search_attributes import (
    child_search_attributes,
    merged_search_attributes,
    mission_visibility_attributes,
)
from mission_control.adapters.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from mission_control.adapters.temporal.workflows.mission_run import MissionRunWorkflow
from mission_control.contracts.canonical import canonical_digest
from mission_control.contracts.identities import mission_root_id
from mission_control.domain.coordinator.launch import (
    BlueprintFamily,
    LaunchIdempotencyConflict,
    WorkflowSubmission,
)
from mission_control.domain.programs.contracts import (
    BellLabsRunInput,
    GoalDirectedRunInput,
    StageGraphRunInput,
)
from mission_control.domain.programs.search_attributes import (
    SEARCH_ATTRIBUTES_REQUIRED,
    MissionVisibilityValues,
    SearchAttributePolicy,
    require_production_search_attribute_policy,
    run_search_attributes,
)


class TemporalWorkflowSubmitter:
    """Root-only Temporal submission adapter with stable duplicate-start identity."""

    def __init__(
        self,
        client: Client,
        *,
        stagegraph_task_queue: str,
        goal_directed_task_queue: str,
        root_task_queue: str | None = None,
        search_attribute_policy: SearchAttributePolicy = "disabled",
        mission_installation_id: UUID | None = None,
        mission_application_id: str | None = None,
    ) -> None:
        if not stagegraph_task_queue or not goal_directed_task_queue:
            raise ValueError("coordinator Temporal task queues must be non-empty")
        if stagegraph_task_queue == goal_directed_task_queue:
            raise ValueError("StageGraph and GoalDirected require distinct task queues")
        self._client = client
        self._stagegraph_task_queue = stagegraph_task_queue
        self._goal_directed_task_queue = goal_directed_task_queue
        self._root_task_queue = root_task_queue
        self._search_attribute_policy = search_attribute_policy
        if (mission_installation_id is None) != (mission_application_id is None):
            raise ValueError("both installation and application are required for scoped submission")
        self._mission_scope = (
            f"mc/{mission_installation_id}/{mission_application_id}/"
            if mission_installation_id is not None
            else None
        )

    @classmethod
    def for_production(
        cls,
        client: Client,
        *,
        stagegraph_task_queue: str,
        goal_directed_task_queue: str,
        root_task_queue: str | None = None,
        search_attribute_policy: SearchAttributePolicy,
        mission_installation_id: UUID | None = None,
        mission_application_id: str | None = None,
    ) -> TemporalWorkflowSubmitter:
        """Production composition: roots start with their Search Attributes (EXEC-015)."""

        return cls(
            client,
            stagegraph_task_queue=stagegraph_task_queue,
            goal_directed_task_queue=goal_directed_task_queue,
            root_task_queue=root_task_queue,
            mission_installation_id=mission_installation_id,
            mission_application_id=mission_application_id,
            search_attribute_policy=require_production_search_attribute_policy(
                search_attribute_policy
            ),
        )

    async def submit(
        self,
        workflow_input: object,
        *,
        workflow_id: str,
        blueprint_family: BlueprintFamily,
        parent_run_id: str | None = None,
        mission_id: str | None = None,
    ) -> WorkflowSubmission:
        """Start the admitted run's root; `parent_run_id` marks a fork's source run.

        FT-G7: under the `required` policy the root also starts with `mc_run_id`,
        `mc_mission_id` (when the caller knows the mission) and `ForkedFromRunId`.
        """

        del workflow_id  # Callers cannot override the admitted BellLabs root identity.
        if blueprint_family == BlueprintFamily.STAGE_GRAPH:
            if not isinstance(workflow_input, StageGraphRunInput):
                raise ValueError("StageGraph submission requires an immutable StageGraphRunInput")
            family_queue = self._stagegraph_task_queue
        elif blueprint_family == BlueprintFamily.GOAL_DIRECTED:
            if not isinstance(workflow_input, GoalDirectedRunInput):
                raise ValueError(
                    "GoalDirected submission requires an immutable GoalDirectedRunInput"
                )
            family_queue = self._goal_directed_task_queue
        else:
            raise ValueError(f"unsupported Temporal blueprint family: {blueprint_family}")

        if self._mission_scope is not None:
            if not workflow_input.request_scope.startswith(self._mission_scope):
                raise ValueError("workflow scope differs from trusted application binding")
            mission_root_id(workflow_input.request_scope, workflow_input.run_id)
        root_input = BellLabsRunInput(
            schema_version=(
                "mc.mission_run.v1"
                if self._mission_scope is not None
                else "belllabs.temporal-root.v1"
            ),
            run_id=workflow_input.run_id,
            request_scope=workflow_input.request_scope,
            effective_configuration_digest=workflow_input.effective_configuration_digest,
            workflow_type_digest=(
                workflow_input.workflow_type_digest
                if self._mission_scope is not None
                else workflow_input.blueprint_digest
            ),
            family=blueprint_family.value,
            family_input=asdict(replace(workflow_input, durable_operation_children=True)),
            family_task_queue=family_queue,
            search_attribute_policy=self._search_attribute_policy,
            parent_run_id=parent_run_id,
        )
        if self._mission_scope is not None:
            root_input.validate_mission_binding()
        root_attributes = child_search_attributes(
            self._search_attribute_policy,
            run_search_attributes(
                workflow_kind="root",
                run_id=root_input.run_id,
                request_scope=root_input.request_scope,
                family=root_input.family,
                execution_epoch=root_input.continuity.execution_epoch,
                parent_run_id=parent_run_id,
            ),
        )
        if self._search_attribute_policy == SEARCH_ATTRIBUTES_REQUIRED:
            root_attributes = merged_search_attributes(
                root_attributes,
                mission_visibility_attributes(
                    MissionVisibilityValues(
                        run_id=root_input.run_id,
                        mission_id=mission_id,
                        forked_from_run_ids=(parent_run_id,) if parent_run_id else (),
                    )
                ),
            )
        root_queue = self._root_task_queue or family_queue
        try:
            handle = await self._client.start_workflow(
                (
                    MissionRunWorkflow.run
                    if self._mission_scope is not None
                    else BellLabsRunWorkflow.run
                ),
                root_input,
                id=root_input.workflow_id,
                task_queue=root_queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                search_attributes=root_attributes,
            )
        except WorkflowAlreadyStartedError as error:
            if self._mission_scope is not None:
                existing = self._client.get_workflow_handle(
                    root_input.workflow_id, run_id=error.run_id
                )
                await self._verify_root_binding(existing, root_input)
            return WorkflowSubmission(
                workflow_id=root_input.workflow_id,
                temporal_run_id=error.run_id or None,
            )
        if self._mission_scope is not None:
            await self._verify_root_binding(handle, root_input)
        return WorkflowSubmission(
            workflow_id=handle.id,
            temporal_run_id=handle.first_execution_run_id or None,
        )

    async def _verify_root_binding(self, handle: Any, expected: BellLabsRunInput) -> None:
        """USE_EXISTING is identity reuse only after the admitted immutable input matches.

        Fetch just the first event, not an unbounded history. Failure to read/decode
        leaves launch reconciliation visible instead of claiming a verified receipt.
        """
        events = handle.fetch_history_events(page_size=1, rpc_timeout=timedelta(seconds=10))
        try:
            first = await anext(events)
        except StopAsyncIteration as error:
            raise LaunchIdempotencyConflict(
                "existing workflow has no readable start binding"
            ) from error
        if not first.HasField("workflow_execution_started_event_attributes"):
            raise LaunchIdempotencyConflict(
                "workflow history does not begin with an admitted start"
            )
        started = first.workflow_execution_started_event_attributes
        if started.workflow_type.name != "mc.mission_run.v1":
            raise LaunchIdempotencyConflict(
                "existing workflow registration differs from admitted Mission Control root"
            )
        decoded = await self._client.data_converter.decode(
            started.input.payloads, [BellLabsRunInput]
        )
        if len(decoded) != 1 or not isinstance(decoded[0], BellLabsRunInput):
            raise LaunchIdempotencyConflict(
                "existing workflow has no typed Mission Control binding"
            )
        actual = decoded[0]
        actual.validate_mission_binding()
        # Root Continue-As-New changes recovery mechanics, never these admitted values.
        immutable_fields = (
            "schema_version",
            "run_id",
            "request_scope",
            "effective_configuration_digest",
            "workflow_type_digest",
            "family",
            "family_input",
            "family_task_queue",
            "search_attribute_policy",
            "parent_run_id",
        )
        if canonical_digest({name: getattr(actual, name) for name in immutable_fields}) != (
            canonical_digest({name: getattr(expected, name) for name in immutable_fields})
        ):
            raise LaunchIdempotencyConflict(
                "existing workflow immutable binding differs from admitted launch"
            )
