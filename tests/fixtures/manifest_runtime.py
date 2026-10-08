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

import json
import re
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any, cast

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from tests.fixtures.catalog.fast_track_catalog import fast_track_catalog
from tests.fixtures.rrm009_production_harness import ProductionStack
from tests.fixtures.rrm009_production_stack import (
    TechnicalBinding,
    _LoggedModel,
    _template,
    _workspace,
    call_usage,
)

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
from mission_control.adapters.temporal.deployment_composition import (
    DeploymentCapabilityComponents,
)
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
from mission_control.domain.authoring.identity import stable_id
from mission_control.domain.authoring.manifest_lowering import GOAL_WORKSPACE, STAGE_WORKSPACE
from mission_control.domain.context.refs import workspace_candidate_ref
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


# --- Deterministic chain cognition (FT-D3) ----------------------------------------------------


@dataclass
class GoalScript:
    """What one admitted Goal Loop (by effective configuration digest) is scripted to do."""

    obligation: str
    output_contract: str
    output_name: str
    accept_at: int | None
    """The iteration whose verifier accepts; ``None`` never accepts (a cancel scenario)."""


@dataclass
class ChainScript:
    """Shared, mutable script of the chain scenarios; models read it at call time."""

    request_scope: str
    by_configuration: dict[str, GoalScript] = field(default_factory=dict)
    reads: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    """(run, operation) -> the text of each file the executor read before writing."""
    outputs: dict[tuple[str, str], str] = field(default_factory=dict)
    """(run, operation) -> the workspace candidate ref the executor reported."""


def candidate_ref(
    request_scope: str,
    *,
    run_id: str,
    operation_id: str,
    attempt: int,
    slot_name: str,
    logical_path: str,
    content: bytes,
) -> str:
    """The ``workspace-candidate://`` ref the runtime registers for a captured slot file.

    Mirrors ``bind_operation_execution_request`` (binding id) and
    ``WorkspaceCandidateCaptureService.capture`` (candidate id); the chain release resolves
    the supplier's accepted output through exactly this registered descriptor.
    """

    semantic_key = f"{run_id}:operation:{operation_id}:attempt:{attempt}"
    binding_id = stable_id("operation-binding", f"{request_scope}:{semantic_key}")
    digest = f"sha256:{sha256(content).hexdigest()}"
    return workspace_candidate_ref(
        stable_id("workspace-candidate", binding_id, slot_name, logical_path, digest)
    )


_INDEX = re.compile(r"This index: (\S+?)/\.mission/context\.md")


class ChainModel(_LoggedModel):
    """A Goal Loop role scripted per admitted configuration.

    Executor, by tool messages since its latest input: read ``.mission/context.md`` and
    ``.mission/inputs.json`` of its role root (when the packet names them), write one output
    file into its writable slot, then answer with that file's registered candidate ref.
    Verifier: accept the scripted obligation at the scripted iteration, otherwise reject.
    """

    script: Any
    configuration_digest: str
    attempt: int
    slots: Any  # ((slot_name, logical_path), ...)

    @property
    def _llm_type(self) -> str:
        return "ft-d3-chain"

    def _reply(self, messages: list[BaseMessage]) -> ChatResult:
        human_indexes = [index for index, item in enumerate(messages) if item.type == "human"]
        since_input = messages[human_indexes[-1] :] if human_indexes else messages
        tools = [item for item in since_input if isinstance(item, ToolMessage)]
        self._record("parent", messages)
        goal = self.script.by_configuration.get(self.configuration_digest)
        if goal is None or "goal-iteration/" not in self.operation_id:
            return self._final({"answer": "unscripted", "facts": {}, "output_refs": []})
        iteration = int(self.operation_id.split("goal-iteration/", 1)[1].split("/", 1)[0])
        role = self.operation_id.rsplit("/", 1)[-1].split(":", 1)[0]
        if role != "executor":
            return self._final(self._verdict(goal, iteration))
        text = "\n".join(str(item.content) for item in messages if isinstance(item, HumanMessage))
        index = _INDEX.search(text)
        reads: tuple[str, ...] = (
            (f"{index.group(1)}/.mission/context.md", f"{index.group(1)}/.mission/inputs.json")
            if index
            else ()
        )
        path, content = self._output(goal, iteration)
        if len(tools) < len(reads):
            return self._call("read_file", {"file_path": reads[len(tools)]})
        if len(tools) == len(reads):
            return self._call("write_file", {"file_path": path, "content": content})
        key = (self.run_id, self.operation_id)
        self.script.reads[key] = [str(item.content) for item in tools[: len(reads)]]
        ref = candidate_ref(
            self.script.request_scope,
            run_id=self.run_id,
            operation_id=self.operation_id,
            attempt=self.attempt,
            slot_name=self._slot(path),
            logical_path=path,
            content=content.encode("utf-8"),
        )
        self.script.outputs[key] = ref
        accepted = goal.accept_at is not None and iteration >= goal.accept_at
        return self._final(
            {
                "schema_version": "belllabs.goal-executor-observation.v1",
                "disposition": "completed",
                "output_refs": [ref],
                "completion_claim": accepted,
                "accepted_fact_refs": [f"fact:{goal.obligation}:{iteration}"],
                "evidence_refs": [f"evidence:{goal.obligation}:executor:{iteration}"],
                "handoff": None,
                "output_contract_ref": goal.output_contract,
            }
        )

    def _slot(self, path: str) -> str:
        for name, root in self.slots:
            if path == root or path.startswith(str(root).rstrip("/") + "/"):
                return str(name)
        return str(self.slots[0][0])

    def _output(self, goal: GoalScript, iteration: int) -> tuple[str, str]:
        root = next(
            (str(root) for _name, root in self.slots if str(root).rstrip("/").endswith("/work")),
            f"/goal/{iteration}/executor/work",
        )
        document = {
            "schema": f"{goal.output_name}@1",
            "obligation": goal.obligation,
            "iteration": iteration,
            "rows": [{"claim": f"{goal.obligation} claim {iteration}", "citation": "pmid:0"}],
        }
        return f"{root.rstrip('/')}/{goal.output_name}.json", json.dumps(document, sort_keys=True)

    def _verdict(self, goal: GoalScript, iteration: int) -> dict[str, Any]:
        accepted = goal.accept_at is not None and iteration >= goal.accept_at
        return {
            "schema_version": "belllabs.goal-verifier-observation.v1",
            "decision": "accepted" if accepted else "rejected",
            "progress_made": True,
            "accepted_obligation_refs": [goal.obligation] if accepted else [],
            "findings": [],
            "evidence_refs": [f"evidence:{goal.obligation}:verifier:{iteration}"],
            "unmet_obligations": [] if accepted else [goal.obligation],
            "obligation_applicability": [[goal.obligation, True]],
            "output_contract_ref": goal.output_contract,
        }

    def _call(self, name: str, args: dict[str, Any]) -> ChatResult:
        call_id = sha256(f"{self.operation_id}:{name}".encode()).hexdigest()[:12]
        message = AIMessage(
            content="",
            tool_calls=[{"name": name, "args": args, "id": f"ftd3-{call_id}", "type": "tool_call"}],
            usage_metadata=call_usage(),
        )
        return ChatResult(generations=[ChatGeneration(message=message)])

    def _final(self, body: dict[str, Any]) -> ChatResult:
        message = AIMessage(content=json.dumps(body, sort_keys=True), usage_metadata=call_usage())
        return ChatResult(generations=[ChatGeneration(message=message)])


def chain_components(
    technical: TechnicalBinding, script: ChainScript, model_log: list[dict[str, Any]]
) -> DeploymentCapabilityComponents:
    """The qualification's components with the parent model replaced by ``ChainModel``."""

    base = technical.components(model_log)

    def parent(bound: Any, _secrets: Any) -> ChainModel:
        return ChainModel(
            run_id=bound.run_id,
            operation_id=bound.operation_id,
            log=model_log,
            script=script,
            configuration_digest=bound.erc_digest,
            attempt=bound.operation_attempt,
            slots=tuple(
                (slot.slot_name, slot.logical_path)
                for slot in bound.workspace.slot_bindings
                if slot.access == "exclusive_write"
            ),
        )

    return replace(
        base,
        model_factories={**base.model_factories, technical.binding.model.ref.digest: parent},
    )
