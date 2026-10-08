"""Mission Manifest lifecycle on the real local stack (FT-E3, FT-D3).

``ProductionStack`` (``tests.fixtures.mission_control_production_stack``) runs the governed
API, the production Temporal workers and the deterministic local cognition on a disposable
common-component database. This module composes the manifest lifecycle over it: compile
resolves against the seeded catalog fixture (in memory, no provider), submit publishes the
lowered definitions into the installation catalog in PostgreSQL and admits through the API's
own ``RunControlService``, start launches through the API's governed ``RunLaunchService``
(the real ``TemporalWorkflowSubmitter``).

There is no production author of a manifest run's semantic input binding yet (the lane
binding a lowered stage or Goal Loop role executes with). ``StagedLaunchInputs`` is the test
author: it persists the qualification's deterministic DeepAgents templates under a binding
for the admitted run, bound to the lowered workspace template, and returns the family input
the production ``WorkflowLaunchDispatcher`` prepares from the admitted state.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any, cast

from tests.fixtures.catalog.fast_track_catalog import fast_track_catalog
from tests.fixtures.rrm009_production_harness import ProductionStack
from tests.fixtures.rrm009_production_stack import _template, _workspace

from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.control_plane.manifest_submission import (
    PostgresManifestSubmissionRepository,
)
from mission_control.adapters.postgres.orchestration.goal_directed_repository import (
    PostgresGoalDirectedDocumentRepository,
)
from mission_control.adapters.postgres.orchestration.stagegraph_repository import (
    PostgresStageGraphOperationTemplateRepository,
)
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.application.authoring.manifest_service import (
    ManifestCompileService,
    ManifestProgramCompiler,
)
from mission_control.application.authoring.manifest_submit import (
    ManifestSubmitService,
    register_manifest_admission_policies,
)
from mission_control.application.execution.run_launch import RunLaunchService
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    RunControlService,
)
from mission_control.application.programs.service import (
    GoalDirectedLaunchService,
    StageGraphLaunchService,
    WorkflowLaunchDispatcher,
)
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.bootstrap.technical_api import api
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    GoalDirectedBlueprint,
    StageGraphBlueprint,
)
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.domain.authoring.manifest_lowering import GOAL_WORKSPACE, STAGE_WORKSPACE
from mission_control.domain.execution.contracts import (
    StructuredOutputBinding,
    WorkspaceOwner,
    WorkspaceOwnerKind,
)
from mission_control.domain.policies.contracts import ActorContext

AUTHOR = ActorContext(
    actor_id="operator",
    authority_refs=frozenset({"authority:lifecycle"}),
    permissions=frozenset(
        {"mission.author", "mission.start", "workflow_run.read", "workflow_run.admit"}
    ),
)


def binding_ref(run_id: str) -> str:
    return f"semantic-input:manifest:{run_id}"


class StagedLaunchInputs:
    """Test author of a manifest run's semantic binding and family input (see module doc)."""

    def __init__(self, stack: ProductionStack, run_control: RunControlService) -> None:
        self._stack = stack
        self._run_control = run_control
        control_plane = stack.control_plane
        self._dispatcher = WorkflowLaunchDispatcher(
            stagegraph=StageGraphLaunchService(run_control, control_plane),
            goal_directed=GoalDirectedLaunchService(run_control, control_plane),
            run_control=run_control,
            control_plane=control_plane,
        )
        self.staged: list[str] = []

    async def _configuration(self, request_scope: str, run_id: str) -> Any:
        projection = await self._run_control.get_run(request_scope, run_id)
        return await self._stack.control_plane.retrieve_for_admission(
            projection.effective_configuration_digest
        )

    async def stage(self, request_scope: str, run_id: str) -> str:
        configuration = await self._configuration(request_scope, run_id)
        workspace_ref = next(
            ref
            for ref in configuration.source_refs
            if ref.kind == DefinitionKind.WORKSPACE_TEMPLATE
        )
        blueprint = configuration.selected_blueprint
        reference = binding_ref(run_id)
        if isinstance(blueprint, StageGraphBlueprint):
            await PostgresStageGraphOperationTemplateRepository(
                self._stack.worker_pool
            ).persist_templates(
                request_scope=request_scope,
                semantic_input_binding_ref=reference,
                templates=stage_templates(self._stack, blueprint, workspace_ref),
                recorded_at=datetime.now(UTC),
            )
        else:
            assert isinstance(blueprint, GoalDirectedBlueprint)
            templates = goal_templates(self._stack, blueprint, workspace_ref)
            await PostgresGoalDirectedDocumentRepository(self._stack.worker_pool).persist_templates(
                request_scope=request_scope,
                semantic_input_binding_ref=reference,
                executor=templates["executor"],
                verifier=templates["verifier"],
                recorded_at=datetime.now(UTC),
            )
        self.staged.append(run_id)
        return reference

    async def family_input(
        self, *, request_scope: str, run_id: str, family: str, initial_goal: str | None
    ) -> dict[str, Any]:
        reference = await self.stage(request_scope, run_id)
        prepared = await self._dispatcher.prepare(
            request_scope,
            run_id,
            initial_goal=initial_goal if family == "GoalDirected" else None,
            task_timeout_seconds=180,
            semantic_input_binding_ref=reference,
        )
        return cast(dict[str, Any], asdict(prepared))


def stage_templates(
    stack: ProductionStack, blueprint: StageGraphBlueprint, workspace_ref: ExactDefinitionRef
) -> dict[str, Any]:
    templates: dict[str, Any] = {}
    for stage in blueprint.stages:
        for slot in stage.operation_slots:
            for variant in slot.allowed_variants:
                key = f"{stage.stage_id}/{slot.operation_slot_id}/{variant.operation_variant_id}"
                templates[key] = _template(
                    stack.technical,
                    objective=f"Manifest stage {stage.stage_id}. Produce the stage record.",
                    workspace=_workspace(
                        workspace_ref,
                        namespace=f"workspace-namespace:{{run_id}}:stage:{stage.stage_id}",
                        workspace_id=f"workspace:{{run_id}}:stage:{stage.stage_id}",
                        contract=STAGE_WORKSPACE,
                        owner=WorkspaceOwner(
                            kind=WorkspaceOwnerKind.STAGE, owner_id=f"stage:{stage.stage_id}"
                        ),
                    ),
                )
    return templates


def goal_templates(
    stack: ProductionStack, blueprint: GoalDirectedBlueprint, workspace_ref: ExactDefinitionRef
) -> dict[str, Any]:
    templates: dict[str, Any] = {}
    for role in ("executor", "verifier"):
        templates[role] = _template(
            stack.technical,
            objective=f"Manifest Goal Loop {role} for {blueprint.logical_id}.",
            workspace=_workspace(
                workspace_ref,
                namespace="workspace-namespace:{run_id}",
                workspace_id=f"workspace:{{run_id}}:{role}",
                contract=GOAL_WORKSPACE,
                owner=WorkspaceOwner(kind=WorkspaceOwnerKind.RUN, owner_id="goal-template"),
            ),
            output_schema=StructuredOutputBinding(
                schema_id=f"goal-{role}-observation",
                revision=1,
                schema_digest=sha256_digest(f"goal-{role}-observation-schema"),
            ),
        )
    return templates


async def compose_lifecycle(
    stack: ProductionStack, request_scope: str
) -> tuple[ManifestSubmitService, StagedLaunchInputs]:
    """The manifest lifecycle over the stack's API services and installation catalog."""

    run_control = cast(RunControlService, api.state.run_control_service)
    policies = cast(AdmissionPolicyRegistry, api.state.admission_policy_registry)
    register_manifest_admission_policies(policies)
    launches = cast(RunLaunchService, api.state.run_launch_service)
    definitions, search = await fast_track_catalog()
    catalog = PostgresDefinitionRepository(
        stack.worker_pool, catalog_scope=stack.settings.mission_control_catalog_scope or ""
    )
    programs = ManifestProgramCompiler(catalog, ExtensionRegistry(), InMemoryPayloadStore())
    inputs = StagedLaunchInputs(stack, run_control)
    service = ManifestSubmitService(
        compiler=ManifestCompileService(definitions=definitions, search=search, programs=programs),
        programs=programs,
        run_control=run_control,
        submissions=PostgresManifestSubmissionRepository(stack.worker_pool),
        request_scope=request_scope,
        launches=launches,
        launch_inputs=inputs,
        subscriptions=SubscriptionService(
            PostgresSubscriptionStore(stack.worker_pool, request_scope)
        ),
    )
    return service, inputs
