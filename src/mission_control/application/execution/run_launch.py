"""The governed API-to-Temporal launch of an admitted run (RRM-009).

`POST /run-control/v1/run-requests` only admits (REQ-CP-RUN-001). `RunLaunchService` is the
production step that starts the admitted run's `BellLabsRunWorkflow`: it binds the caller's
family input to the admitted authority (scope, run, effective configuration, Workflow Type,
run version, blueprint digest), derives a fork's parent and patched templates from the fork
receipt (REQ-CP-EXEC-012, RRM-006), and submits through the production submitter, whose
root start carries the Search Attributes (REQ-CP-EXEC-015) and whose duplicate-start policy
makes a second launch of the same pending run idempotent.

Nothing here is company- or fixture-specific.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from mission_control.application.execution.service import RunControlService
from mission_control.application.recovery.run_forks import ForkMaterializationStore
from mission_control.application.recovery.runtime_recovery import ForkRepository
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import GoalDirectedBlueprint, StageGraphBlueprint
from mission_control.domain.coordinator.launch import (
    BlueprintFamily,
    LaunchIdempotencyConflict,
    WorkflowSubmission,
)
from mission_control.domain.policies.contracts import ActorContext, RunPhase, RunProjection
from mission_control.domain.policies.forks import RunForkPatch, RunForkReceipt, RunForkRequest
from mission_control.domain.programs.contracts import GoalDirectedRunInput, StageGraphRunInput

LAUNCH_PERMISSION = "workflow_run.start"
GOAL_OBJECTIVE_PATH = "goal.objective"
STAGE_OBJECTIVE_PREFIX = "stage_objectives."
STAGE_INPUT_ADAPTER: TypeAdapter[StageGraphRunInput] = TypeAdapter(StageGraphRunInput)
GOAL_INPUT_ADAPTER: TypeAdapter[GoalDirectedRunInput] = TypeAdapter(GoalDirectedRunInput)


def fork_semantic_input_binding_ref(fork_request_id: str) -> str:
    """The derived run's semantic input binding reference, bound to the fork request."""

    return f"semantic-input:fork:{fork_request_id}"


class RunLaunchRejected(ValueError):
    """The launch does not bind the admitted authority, or the run cannot start yet."""

    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


class RunLaunchRequest(BaseModel):
    """The caller's family input for one admitted run; every identity is verified."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1, max_length=256)
    run_id: str = Field(min_length=1, max_length=512)
    family: Literal["StageGraph", "GoalDirected"]
    stagegraph: dict[str, Any] | None = None
    goal_directed: dict[str, Any] | None = None
    # A fork launch names the source run's templates, which the patch is applied to.
    source_semantic_input_binding_ref: str | None = Field(default=None, min_length=1)


class RunLaunchReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str
    request_scope: str
    family: Literal["StageGraph", "GoalDirected"]
    workflow_id: str
    temporal_run_id: str | None
    semantic_input_binding_ref: str
    parent_run_id: str | None = None
    fork_request_id: str | None = None
    accepted_run_version: int


class RunWorkflowSubmitter(Protocol):
    async def submit(
        self,
        workflow_input: object,
        *,
        workflow_id: str,
        blueprint_family: BlueprintFamily,
        parent_run_id: str | None = None,
    ) -> WorkflowSubmission: ...


class ForkTemplateDerivationPort(Protocol):
    """Applies a fork patch's semantic-input changes to the derived run's templates."""

    async def derive_stagegraph(
        self,
        *,
        request_scope: str,
        source_semantic_input_binding_ref: str,
        derived_semantic_input_binding_ref: str,
        patch: RunForkPatch,
    ) -> tuple[str, ...]: ...


class RunLaunchService:
    def __init__(
        self,
        *,
        run_control: RunControlService,
        submitter: RunWorkflowSubmitter,
        forks: ForkRepository | None = None,
        materializations: ForkMaterializationStore | None = None,
        fork_templates: ForkTemplateDerivationPort | None = None,
    ) -> None:
        self._run_control = run_control
        self._submitter = submitter
        self._forks = forks
        self._materializations = materializations
        self._fork_templates = fork_templates

    async def launch(self, request: RunLaunchRequest, actor: ActorContext) -> RunLaunchReceipt:
        if LAUNCH_PERMISSION not in actor.permissions:
            raise RunLaunchRejected("unauthorized", f"{LAUNCH_PERMISSION} is required")
        run = await self._run_control.get_run(request.request_scope, request.run_id)
        if run.phase != RunPhase.PENDING:
            raise RunLaunchRejected(
                "run_not_pending",
                f"the run is {run.phase.value}; only a pending admitted run can be launched",
            )
        family, workflow_input = self._validated_input(request, run)
        # The family settles exactly the baseline it is told about (GoalDirected at closing),
        # so the input must carry the admitted baseline reservation, never another amount.
        budget = await self._run_control.get_budget(request.request_scope, request.run_id)
        if dict(workflow_input.baseline_reservation) != dict(
            budget.reservations.get("baseline", {})
        ):
            raise RunLaunchRejected(
                "budget_mismatch",
                "family input does not carry the admitted baseline reservation",
            )
        fork = await self._fork_of(request)
        parent_run_id: str | None = None
        fork_request_id: str | None = None
        if fork is not None:
            fork_request, receipt = fork
            fork_request_id = fork_request.request_id
            parent_run_id = receipt.source_run_id
            await self._apply_fork(request, fork_request, workflow_input)
        try:
            submission = await self._submitter.submit(
                workflow_input,
                workflow_id=f"belllabs-run/{run.run_id}",
                blueprint_family=family,
                parent_run_id=parent_run_id,
            )
        except LaunchIdempotencyConflict as error:
            raise RunLaunchRejected(error.code, str(error)) from error
        return RunLaunchReceipt(
            run_id=run.run_id,
            request_scope=run.request_scope,
            family=request.family,
            workflow_id=submission.workflow_id,
            temporal_run_id=submission.temporal_run_id,
            semantic_input_binding_ref=workflow_input.semantic_input_binding_ref,
            parent_run_id=parent_run_id,
            fork_request_id=fork_request_id,
            accepted_run_version=run.version,
        )

    def _validated_input(
        self, request: RunLaunchRequest, run: RunProjection
    ) -> tuple[BlueprintFamily, StageGraphRunInput | GoalDirectedRunInput]:
        payload = request.stagegraph if request.family == "StageGraph" else request.goal_directed
        if payload is None or (
            (request.stagegraph is not None) == (request.goal_directed is not None)
        ):
            raise RunLaunchRejected(
                "invalid_family_input", "exactly one family input must match the declared family"
            )
        try:
            workflow_input: StageGraphRunInput | GoalDirectedRunInput = (
                STAGE_INPUT_ADAPTER.validate_python(payload)
                if request.family == "StageGraph"
                else GOAL_INPUT_ADAPTER.validate_python(payload)
            )
        except (ValueError, TypeError) as error:
            raise RunLaunchRejected("invalid_family_input", str(error)) from error
        if workflow_input.run_id != run.run_id or workflow_input.request_scope != run.request_scope:
            raise RunLaunchRejected("identity_mismatch", "family input names another run or scope")
        if workflow_input.effective_configuration_digest != run.effective_configuration_digest:
            raise RunLaunchRejected(
                "configuration_mismatch",
                "family input is not bound to the admitted effective configuration",
            )
        if workflow_input.initial_run_version != run.version:
            raise RunLaunchRejected(
                "stale_run_version",
                f"family input binds run version {workflow_input.initial_run_version}, "
                f"the admitted run is at {run.version}",
            )
        if workflow_input.execution_epoch != 1:
            raise RunLaunchRejected("invalid_family_input", "a launch starts execution epoch 1")
        if not workflow_input.semantic_input_binding_ref:
            raise RunLaunchRejected(
                "invalid_family_input", "family input requires its semantic input binding ref"
            )
        if isinstance(workflow_input, StageGraphRunInput):
            if workflow_input.workflow_type_digest != run.workflow_type_ref.digest:
                raise RunLaunchRejected(
                    "configuration_mismatch", "family input names another Workflow Type"
                )
            try:
                blueprint: StageGraphBlueprint | GoalDirectedBlueprint = (
                    StageGraphBlueprint.model_validate(workflow_input.blueprint)
                )
            except ValueError as error:
                raise RunLaunchRejected("invalid_family_input", str(error)) from error
            family = BlueprintFamily.STAGE_GRAPH
        else:
            if run.request_scope.startswith("mc/"):
                if workflow_input.workflow_type_digest not in {"", run.workflow_type_ref.digest}:
                    raise RunLaunchRejected(
                        "configuration_mismatch", "family input names another Workflow Type"
                    )
                workflow_input = replace(
                    workflow_input, workflow_type_digest=run.workflow_type_ref.digest
                )
            try:
                blueprint = GoalDirectedBlueprint.model_validate(workflow_input.blueprint)
            except ValueError as error:
                raise RunLaunchRejected("invalid_family_input", str(error)) from error
            family = BlueprintFamily.GOAL_DIRECTED
        if sha256_digest(blueprint) != workflow_input.blueprint_digest:
            raise RunLaunchRejected(
                "configuration_mismatch", "blueprint digest does not match the frozen blueprint"
            )
        return family, workflow_input

    async def _fork_of(
        self, request: RunLaunchRequest
    ) -> tuple[RunForkRequest, RunForkReceipt] | None:
        if self._materializations is None or self._forks is None:
            return None
        fork = await self._materializations.fork_of_run(request.request_scope, request.run_id)
        if fork is None:
            return None
        if not fork.materialized:
            raise RunLaunchRejected(
                "fork_not_materialized",
                "a fork-derived run starts only once its fork is materialized",
                retryable=True,
            )
        fork_request = await self._forks.get_request(request.request_scope, fork.fork_request_id)
        receipt = await self._forks.get(request.request_scope, fork.fork_request_id)
        if fork_request is None or receipt is None or receipt.target_run_id != request.run_id:
            raise RunLaunchRejected(
                "fork_lineage_missing", "the fork request or receipt of this run is unavailable"
            )
        return fork_request, receipt

    async def _apply_fork(
        self,
        request: RunLaunchRequest,
        fork_request: RunForkRequest,
        workflow_input: StageGraphRunInput | GoalDirectedRunInput,
    ) -> None:
        expected_ref = fork_semantic_input_binding_ref(fork_request.request_id)
        if workflow_input.semantic_input_binding_ref != expected_ref:
            raise RunLaunchRejected(
                "fork_binding_mismatch",
                f"a fork-derived run binds its semantic input as {expected_ref}",
            )
        patch = fork_request.patch
        if isinstance(workflow_input, StageGraphRunInput):
            if request.source_semantic_input_binding_ref is None:
                raise RunLaunchRejected(
                    "fork_source_templates_required",
                    "a StageGraph fork launch names the source run's semantic input binding",
                )
            if self._fork_templates is None:
                raise RunLaunchRejected(
                    "fork_templates_unavailable", "fork template derivation is not composed"
                )
            await self._fork_templates.derive_stagegraph(
                request_scope=request.request_scope,
                source_semantic_input_binding_ref=request.source_semantic_input_binding_ref,
                derived_semantic_input_binding_ref=expected_ref,
                patch=patch,
            )
            return
        change = patch.change(GOAL_OBJECTIVE_PATH)
        if change is not None and workflow_input.initial_revision.objective != change.value:
            raise RunLaunchRejected(
                "fork_patch_mismatch",
                "the derived GoalDirected input does not carry the patched goal objective",
            )


__all__ = [
    "LAUNCH_PERMISSION",
    "ForkTemplateDerivationPort",
    "RunLaunchReceipt",
    "RunLaunchRejected",
    "RunLaunchRequest",
    "RunLaunchService",
    "RunWorkflowSubmitter",
    "fork_semantic_input_binding_ref",
]
