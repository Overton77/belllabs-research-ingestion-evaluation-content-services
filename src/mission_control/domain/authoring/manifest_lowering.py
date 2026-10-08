"""Lowering ``MissionDefinition@1`` onto the existing compiler inputs (SPEC-05 "Mapping").

The kernel compiles Workflow Types, blueprints and profiles into an Effective Run Configuration
(the Compiled Program); it has no compiler for ``MissionDefinition@1`` yet. This module is the
one deterministic bridge, pure and digest-stable: a ``goal_loop`` root lowers to a
``GoalDirectedBlueprint`` (obligations = the required goals), a ``stage_graph`` root to a
``StageGraphBlueprint`` through ``build_stagegraph_v2`` (one stage per child node; a human gate
becomes a stage carrying a declared wait; a nested Goal Loop is lowered as a stage with a
warning, since the kernel has no nested-loop family), any other root to a one-stage graph. The
Runtime Profile pins every resolved capability by exact ref (``operation_binding_refs``); the
lane execution bindings themselves are produced by the semantic input binding at launch, never
named by the manifest. Logical ids are content-addressed by the definition digest, so the same
definition always lowers to the same published rows.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal

from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import (
    AuthorityCeiling,
    BudgetCeiling,
    ControlProfileDefinition,
    EvaluationProfileDefinition,
    ExactDefinitionRef,
    GoalConvergencePolicy,
    GoalDirectedBlueprint,
    GoalHandoffPolicy,
    GoalSessionRolloverPolicy,
    GoalVerifierPolicy,
    RunInputManifestRef,
    RuntimeProfileDefinition,
    StageGraphBlueprint,
    StageGraphWait,
    WorkflowTypeDefinition,
    WorkflowWorkspaceContract,
    WorkspaceSlot,
    WorkspaceTemplateDefinition,
)
from mission_control.domain.authoring.manifest import (
    Behavior,
    ManifestErrorCode,
    ManifestIssue,
)
from mission_control.domain.authoring.mission_definition import DefinitionNode, MissionDefinition
from mission_control.domain.authoring.stagegraph_builder import (
    StageGraphStageSpec,
    build_stagegraph_v2,
)

LOWERING_VERSION = "mc.manifest-lowering/1"
MANIFEST_INPUT_CONTRACT = "contract:mc.manifest-input@1"
MANIFEST_INVARIANT = "contract:mc.manifest-invariant@1"
MANIFEST_RUNTIME_BINDING = "python-3.12"
MANIFEST_RUN_CAPABILITIES = frozenset(
    {"model.invoke", "sandbox.execute", "mcp.call", "subagent.task", "artifact.promote"}
)
GOAL_WORKSPACE = WorkflowWorkspaceContract(
    slots=(WorkspaceSlot(name="work", path="/work", access="exclusive_write", purpose="role work"),)
)
STAGE_WORKSPACE = WorkflowWorkspaceContract(
    slots=(
        WorkspaceSlot(
            name="output",
            path="/workspace/output",
            access="exclusive_write",
            purpose="stage outputs",
        ),
    )
)
Family = Literal["StageGraph", "GoalDirected"]


def lowering_base_id(definition: MissionDefinition) -> str:
    """``mc.manifest.<mission key>.<12 hex of the definition digest>``."""

    return f"mc.manifest.{definition.mission_key}.{definition.digest.removeprefix('sha256:')[:12]}"


def required_goals(definition: MissionDefinition) -> frozenset[str]:
    """The goals whose acceptance the run must prove (the run's required obligations)."""

    required = frozenset(
        item.goal_key for item in definition.completion_contract.goals if item.required
    )
    return required or frozenset(goal.key for goal in definition.goals)


def budget_dimensions(
    definition: MissionDefinition, *, goal_iterations: int | None
) -> dict[str, int]:
    """The manifest budget as run-control budget dimensions (integers only, no floats)."""

    budget = definition.budget
    dimensions: dict[str, int] = {}
    if budget.tokens is not None:
        dimensions["tokens.total"] = budget.tokens
    if budget.tool_calls is not None:
        dimensions["tool.calls.total"] = budget.tool_calls
    if budget.usd is not None:
        dimensions["currency.estimated_micros"] = int(
            (Decimal(budget.usd) * Decimal(1_000_000)).to_integral_value()
        )
    if budget.wall_clock_seconds is not None:
        dimensions["time.elapsed_ms"] = budget.wall_clock_seconds * 1000
    if goal_iterations is not None:
        dimensions["goal.iterations"] = goal_iterations
    return dimensions


@dataclass(frozen=True)
class LoweredMission:
    """The compiler inputs of one mission, before publication (refs are assigned on publish)."""

    mission_key: str
    family: Family
    base_id: str
    blueprint: StageGraphBlueprint | GoalDirectedBlueprint
    runtime_profile: RuntimeProfileDefinition
    workspace_template: WorkspaceTemplateDefinition
    evaluation_profile: EvaluationProfileDefinition
    authority: AuthorityCeiling
    obligations: frozenset[str]
    output_contracts: frozenset[str]
    workspace_contract: WorkflowWorkspaceContract
    input_manifest: RunInputManifestRef
    initial_goal: str | None
    warnings: tuple[ManifestIssue, ...] = field(default=())

    @property
    def blueprint_digest(self) -> str:
        return sha256_digest(self.blueprint)

    @property
    def runtime_profile_digest(self) -> str:
        return sha256_digest(self.runtime_profile)

    def control_profile(self, blueprint_ref: ExactDefinitionRef) -> ControlProfileDefinition:
        return ControlProfileDefinition(
            logical_id=f"{self.base_id}.control",
            title=f"{self.mission_key} control",
            description=f"Control profile lowered from mission {self.mission_key}.",
            blueprint_ref=blueprint_ref,
            selected_variants=frozenset({"default"} if self.family == "StageGraph" else ()),
            authority_ceiling=self.authority,
        )

    def workflow_type(
        self,
        *,
        blueprint_ref: ExactDefinitionRef,
        control_ref: ExactDefinitionRef,
        runtime_ref: ExactDefinitionRef,
        workspace_ref: ExactDefinitionRef,
        evaluation_ref: ExactDefinitionRef,
    ) -> WorkflowTypeDefinition:
        return WorkflowTypeDefinition(
            logical_id=f"{self.base_id}.workflow",
            title=f"{self.mission_key} workflow",
            description=f"Workflow Type lowered from mission {self.mission_key}.",
            purpose=f"Run mission {self.mission_key} as authored in its Mission Manifest",
            input_admission_contract=MANIFEST_INPUT_CONTRACT,
            invariants=frozenset({MANIFEST_INVARIANT}),
            obligations=self.obligations,
            output_contracts=self.output_contracts,
            allowed_blueprints=frozenset({blueprint_ref}),
            allowed_control_profiles=frozenset({control_ref}),
            allowed_runtime_profiles=frozenset({runtime_ref}),
            allowed_workspace_templates=frozenset({workspace_ref}),
            allowed_evaluation_profiles=frozenset({evaluation_ref}),
            authority_ceiling=self.authority,
            workspace_contract=self.workspace_contract,
        )


def _goal_text(definition: MissionDefinition, key: str) -> str:
    for goal in definition.goals:
        if goal.key == key:
            return goal.description
    for objective in definition.objectives:
        if objective.key == key:
            return objective.description
    return key


def _owning_goal(definition: MissionDefinition, key: str) -> str | None:
    if any(goal.key == key for goal in definition.goals):
        return key
    return next((item.goal_key for item in definition.objectives if item.key == key), None)


def _goal_blueprint(
    definition: MissionDefinition, root: DefinitionNode, base_id: str
) -> tuple[GoalDirectedBlueprint, frozenset[str], frozenset[str], str]:
    objective_key = str(root.body.get("objective") or definition.goals[0].key)
    iterations = root.environment.governors.iterations if root.environment.governors else None
    patience = root.environment.governors.patience if root.environment.governors else None
    max_iterations = iterations or 1
    obligations = required_goals(definition)
    outputs = frozenset(f"output:{item.name}" for item in root.outputs) or frozenset(
        {f"output:{base_id}:result"}
    )
    authority = AuthorityCeiling(
        capabilities=MANIFEST_RUN_CAPABILITIES,
        budgets=BudgetCeiling(
            dimensions=budget_dimensions(definition, goal_iterations=max_iterations)
        ),
    )
    criteria = ", ".join(sorted(item.key for item in definition.criteria))
    blueprint = GoalDirectedBlueprint(
        logical_id=f"{base_id}.blueprint",
        title=f"{definition.title} (Goal Loop)",
        description=f"Goal Loop lowered from mission {definition.mission_key}.",
        objective_contract=f"manifest:{definition.mission_key}:objective:{objective_key}",
        acceptance_contract=f"manifest:{definition.mission_key}:criteria:{criteria}",
        admitted_input_classes=frozenset({"mc.manifest-input"}),
        authority_ceiling=authority,
        prohibited_work=frozenset({"work outside the mission manifest"}),
        required_output_contracts=outputs,
        required_obligation_refs=obligations,
        verifier_policy=GoalVerifierPolicy(
            operation_class="manifest_verifier",
            binding_ref=f"verifier:{base_id}@1",
            rubric_ref=f"rubric:{base_id}@1",
            rubric_version=1,
            acceptance_version=1,
            output_contract_ref=sorted(outputs)[0],
        ),
        allowed_operation_classes=frozenset({"manifest_operation"}),
        allowed_async_subgoal_classes=frozenset({"manifest_subgoal"}),
        allowed_linked_run_slot_ids=frozenset({"manifest_link"}),
        session_policy=GoalSessionRolloverPolicy(
            context_selection_policy_ref="context-selection:mc.manifest@1",
            context_compaction_policy_ref="context-compaction:mc.manifest@1",
            protected_fact_classes=frozenset({"objective"}),
            max_rollovers=0,
            compaction_failure_action="pause",
        ),
        handoff_policy=GoalHandoffPolicy(
            max_instruction_bytes=8192,
            allowed_workspace_ref_classes=frozenset({"mc.workspace"}),
            allowed_snapshot_ref_classes=frozenset({"mc.snapshot"}),
        ),
        convergence_policy=GoalConvergencePolicy(
            max_no_progress_iterations=patience or 3,
            authority_breach_action="fail",
            no_progress_action="partial_or_fail",
            repeated_blocker_action="partial_or_fail",
            soft_budget_action="continue",
        ),
        max_iterations=max_iterations,
    )
    return blueprint, obligations, outputs, _goal_text(definition, objective_key)


def _stage_children(root: DefinitionNode) -> tuple[DefinitionNode, ...]:
    if root.behavior is Behavior.STAGE_GRAPH:
        return root.nodes
    return (root,)


def _stage_blueprint(
    definition: MissionDefinition, root: DefinitionNode, base_id: str
) -> tuple[StageGraphBlueprint, frozenset[str], tuple[ManifestIssue, ...]]:
    children = _stage_children(root)
    warnings: list[ManifestIssue] = []
    keys = {child.key for child in children}
    # Each required goal is proven by the last stage (document order) that serves it.
    owners: dict[str, str] = {}
    for child in children:
        for key in child.objectives:
            goal = _owning_goal(definition, key)
            if goal is not None:
                owners[goal] = child.key
    goals = required_goals(definition)
    last = children[-1].key
    for goal in sorted(goals):
        owners.setdefault(goal, last)
    specs = []
    waits = []
    for child in children:
        if child.behavior is Behavior.GOAL_LOOP:
            warnings.append(
                ManifestIssue(
                    code=ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                    pointer=child.pointer,
                    message=(
                        f"nested Goal Loop {child.key} is lowered as one Stage Graph stage "
                        "(the kernel has no nested-loop family yet)"
                    ),
                    reason="nested_goal_loop_lowered_as_stage",
                )
            )
        if child.behavior is Behavior.HUMAN_GATE:
            waits.append(StageGraphWait(scope_kind="stage", scope_id=child.key, wait_id=child.key))
        outputs = tuple(item.name for item in child.outputs) or ("result",)
        specs.append(
            StageGraphStageSpec(
                stage_id=child.key,
                depends_on=tuple(item for item in child.depends_on if item in keys),
                output_slots=outputs,
                obligation_refs=tuple(
                    sorted(goal for goal, owner in owners.items() if owner == child.key)
                ),
                operation_contract_ref=f"operation:{base_id}:{child.key}@1",
            )
        )
    concurrency = 1
    if root.behavior is Behavior.STAGE_GRAPH:
        concurrency = int(root.body.get("concurrency") or 1)
    blueprint = build_stagegraph_v2(
        logical_id=f"{base_id}.blueprint",
        title=f"{definition.title} (Stage Graph)",
        description=f"Stage Graph lowered from mission {definition.mission_key}.",
        stages=tuple(specs),
        max_concurrency=concurrency,
    )
    if waits:
        blueprint = StageGraphBlueprint.model_validate(
            {**blueprint.model_dump(mode="python"), "waits": tuple(waits)}
        )
    return blueprint, frozenset(owners), tuple(warnings)


def lower_mission(
    definition: MissionDefinition, *, capability_refs: Iterable[ExactDefinitionRef] = ()
) -> LoweredMission:
    """Lower one resolved definition. Pure: the same definition and refs give the same rows."""

    base_id = lowering_base_id(definition)
    root = definition.program
    warnings: tuple[ManifestIssue, ...] = ()
    family: Family
    initial_goal: str | None = None
    if root.behavior is Behavior.GOAL_LOOP:
        family = "GoalDirected"
        blueprint: StageGraphBlueprint | GoalDirectedBlueprint
        blueprint, obligations, outputs, initial_goal = _goal_blueprint(definition, root, base_id)
        authority = blueprint.authority_ceiling
        workspace = GOAL_WORKSPACE
    else:
        family = "StageGraph"
        blueprint, obligations, warnings = _stage_blueprint(definition, root, base_id)
        outputs = frozenset()
        authority = AuthorityCeiling(
            capabilities=MANIFEST_RUN_CAPABILITIES,
            budgets=BudgetCeiling(dimensions=budget_dimensions(definition, goal_iterations=None)),
            max_concurrency=max(1, int(root.body.get("concurrency") or 1))
            if root.behavior is Behavior.STAGE_GRAPH
            else 1,
        )
        workspace = STAGE_WORKSPACE
    refs = tuple(
        sorted(set(capability_refs), key=lambda ref: (ref.kind.value, ref.logical_id, ref.revision))
    )
    runtime = RuntimeProfileDefinition(
        logical_id=f"{base_id}.runtime",
        title=f"{definition.mission_key} runtime",
        description=(
            f"Runtime profile lowered from mission {definition.mission_key}; the lane "
            "execution bindings come from the semantic input binding at launch."
        ),
        binding=MANIFEST_RUNTIME_BINDING,
        operation_binding_refs=frozenset(refs),
    )
    workspace_template = WorkspaceTemplateDefinition(
        logical_id=f"{base_id}.workspace",
        title=f"{definition.mission_key} workspace",
        description=f"Workspace template lowered from mission {definition.mission_key}.",
        slots=workspace.slots,
    )
    evaluation = EvaluationProfileDefinition(
        logical_id=f"{base_id}.evaluation",
        title=f"{definition.mission_key} evaluation",
        description=f"Evaluation gates lowered from mission {definition.mission_key} criteria.",
        gate_contract_refs=frozenset(
            f"criterion:{item.goal_key}.{item.key}" for item in definition.criteria
        ),
    )
    inputs_digest = sha256_digest({"inputs": list(definition.inputs)})
    return LoweredMission(
        mission_key=definition.mission_key,
        family=family,
        base_id=base_id,
        blueprint=blueprint,
        runtime_profile=runtime,
        workspace_template=workspace_template,
        evaluation_profile=evaluation,
        authority=authority,
        obligations=obligations,
        output_contracts=outputs,
        workspace_contract=workspace,
        input_manifest=RunInputManifestRef(
            manifest_id=f"{base_id}.inputs", revision=1, digest=inputs_digest
        ),
        initial_goal=initial_goal,
        warnings=warnings,
    )


def lowering_digests(lowered: LoweredMission) -> dict[str, str]:
    return {
        "lowering_version": LOWERING_VERSION,
        "blueprint_digest": lowered.blueprint_digest,
        "runtime_profile_digest": lowered.runtime_profile_digest,
    }
