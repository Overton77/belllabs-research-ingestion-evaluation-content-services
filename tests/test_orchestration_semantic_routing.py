from __future__ import annotations

import pytest

pytest.skip(
    "legacy BoundGoal*/direct GoalDirected semantic routing deleted by WP-BP-020 atomic switch; canonical coverage is tests/test_wp_bp_020_*.py",
    allow_module_level=True,
)

from datetime import UTC, datetime

import pytest
from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError

from app.application.orchestration_binding_repository import (
    InMemoryRunSemanticInputBindingRepository,
    RunSemanticInputBindingService,
    SemanticInputBindingConflict,
    SemanticInputBindingNotFound,
)
from app.application.orchestration_routing import (
    BoundGoalHandoffPreparer,
    BoundGoalIndependentVerifier,
    BoundGoalIterationExecutor,
    BoundStageOperationExecutor,
    BoundWorkflowEvaluator,
    SemanticHandlerRegistry,
    SemanticRoutingError,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    GoalDirectedBlueprint,
    StageGraphBlueprint,
    StageNode,
)
from app.domain.orchestration.bindings import (
    GoalOperationHandlerBinding,
    RunSemanticInputBinding,
    SemanticHandlerBinding,
    SemanticInputPayload,
    StageHandlerBinding,
)
from app.domain.orchestration.contracts import (
    ExecutionIdentity,
    GoalDirectedRunInput,
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoffCheckpoint,
    GoalHandoffRequest,
    GoalHandoffResult,
    GoalRevision,
    GoalVerificationRequest,
    GoalVerificationResult,
    StageExecutionIdentity,
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationRequest,
    WorkflowEvaluationResult,
)
from app.domain.orchestration.goal_directed import GoalDirectedInterpreter
from app.domain.orchestration.interpreter import StageGraphInterpreter

DIGEST = "sha256:" + "a" * 64
BLUEPRINT_DIGEST = "sha256:" + "b" * 64
SCOPE_DIGEST = "sha256:" + "c" * 64


class ClaimSetInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    claims: tuple[str, ...]


class AcceptanceInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    minimum_outputs: int


CLAIM_SET_ADAPTER = TypeAdapter(ClaimSetInput)
ACCEPTANCE_ADAPTER = TypeAdapter(AcceptanceInput)


class CanonicalClaimStageHandler:
    """A real semantic transform: normalize, deduplicate, and content-address claims."""

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        value = binding.input.decode(CLAIM_SET_ADAPTER)
        normalized = tuple(
            sorted(
                {
                    " ".join(claim.casefold().split())
                    for claim in value.claims
                    if claim.strip()
                }
            )
        )
        output_ref = f"evidence:normalized:{sha256_digest(normalized).removeprefix('sha256:')}"
        return StageOperationResult(
            identity=request.identity,
            disposition="completed",
            output_refs=(output_ref,),
            evaluation_ref=f"evaluation:{output_ref}",
            actual_usage={"operation.attempts": 1},
            output_contract_ref=binding.output_contract_ref,
        )


class ExactOutputEvaluator:
    async def evaluate(
        self,
        request: WorkflowEvaluationRequest,
        binding: SemanticHandlerBinding,
    ) -> WorkflowEvaluationResult:
        acceptance = binding.input.decode(ACCEPTANCE_ADAPTER)
        outputs = tuple(
            output
            for stage_outputs in request.current_output_refs.values()
            for output in stage_outputs
        )
        accepted = len(outputs) >= acceptance.minimum_outputs and all(
            output.startswith("evidence:normalized:") for output in outputs
        )
        return WorkflowEvaluationResult(
            action="accept" if accepted else "fail",
            evaluation_ref=f"evaluation:output-count:{len(outputs)}",
            evaluation_contract_ref=request.evaluation_contract_ref,
            objective_contract_ref=request.objective_contract_ref,
            output_contract_ref=binding.output_contract_ref,
        )


class EvidenceGoalHandler:
    """Produces independently addressable evidence from frozen run input."""

    async def execute(
        self,
        claim: GoalExecutionClaim,
        binding: SemanticHandlerBinding,
    ) -> GoalExecutionResult:
        value = binding.input.decode(CLAIM_SET_ADAPTER)
        evidence = tuple(
            f"evidence:fact:{sha256_digest(item).removeprefix('sha256:')}"
            for item in sorted(set(value.claims))
        )
        return GoalExecutionResult(
            identity=claim.identity,
            disposition="completed",
            output_refs=evidence,
            completion_claim=True,
            actual_usage={"goal.iterations": 1, "tokens.total": len(evidence)},
            output_contract_ref=binding.output_contract_ref,
        )


class EvidenceAcceptanceVerifier:
    async def verify(
        self,
        request: GoalVerificationRequest,
        binding: SemanticHandlerBinding,
    ) -> GoalVerificationResult:
        acceptance = binding.input.decode(ACCEPTANCE_ADAPTER)
        evidence = request.execution_result.output_refs
        complete = (
            request.execution_result.completion_claim
            and len(evidence) >= acceptance.minimum_outputs
            and all(item.startswith("evidence:fact:") for item in evidence)
        )
        return GoalVerificationResult(
            identity=request.claim.identity,
            action="verified_completion" if complete else "continue",
            verification_ref=f"verification:{sha256_digest(evidence).removeprefix('sha256:')}",
            verifier_ref=request.verifier_ref,
            acceptance_contract_ref=request.acceptance_contract_ref,
            progress_made=bool(evidence),
            evidence_refs=evidence,
            unmet_obligations=() if complete else ("minimum-evidence",),
            output_contract_ref=binding.output_contract_ref,
        )


class EvidenceHandoffHandler:
    async def prepare(
        self,
        request: GoalHandoffRequest,
        binding: SemanticHandlerBinding,
    ) -> GoalHandoffResult:
        acceptance = binding.input.decode(ACCEPTANCE_ADAPTER)
        checkpoint = GoalHandoffCheckpoint(
            checkpoint_id=(
                "checkpoint:"
                + sha256_digest(
                    {
                        "identity": request.claim.identity.semantic_key,
                        "minimum_outputs": acceptance.minimum_outputs,
                    }
                ).removeprefix("sha256:")
            ),
            agent_run_identity=request.claim.identity,
            goal_revision_id=request.claim.identity.iteration.goal_revision_id,
            protected_scope_digest=request.protected_scope_digest,
            instructions=(
                f"Continue until at least {acceptance.minimum_outputs} independent "
                "evidence references satisfy the frozen acceptance contract."
            ),
            state_refs=request.execution_result.output_refs,
            artifact_refs=request.execution_result.output_refs,
            workspace_ref=request.claim.workspace_namespace,
        )
        return GoalHandoffResult(
            checkpoint=checkpoint,
            fallback_used=request.fallback,
            output_contract_ref=binding.output_contract_ref,
        )


def _handler(
    handler_id: str,
    schema_ref: str,
    value: object,
    output_contract_ref: str,
) -> SemanticHandlerBinding:
    return SemanticHandlerBinding(
        handler_id=handler_id,
        handler_revision=1,
        input=SemanticInputPayload.from_value(schema_ref=schema_ref, value=value),
        output_contract_ref=output_contract_ref,
    )


def _stage_binding(*, handler_id: str = "claims.normalize") -> RunSemanticInputBinding:
    stage = _handler(
        handler_id,
        "schema:claim-set:v1",
        {"claims": ["  NAD   therapy ", "nad therapy", "Cryotherapy"]},
        "schema:normalized-evidence:v1",
    )
    evaluator = _handler(
        "claims.evaluate",
        "schema:acceptance:v1",
        {"minimum_outputs": 1},
        "schema:evaluation:v1",
    )
    return RunSemanticInputBinding.create(
        request_scope="tenant-1",
        run_id="run-stage",
        blueprint_family="StageGraph",
        effective_configuration_digest=DIGEST,
        blueprint_digest=BLUEPRINT_DIGEST,
        stage_handlers=(StageHandlerBinding(stage_id="normalize", handler=stage),),
        workflow_evaluator=evaluator,
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
    )


def _goal_binding() -> RunSemanticInputBinding:
    iteration = _handler(
        "evidence.collect",
        "schema:claim-set:v1",
        {"claims": ["red-light-therapy", "cryotherapy"]},
        "schema:evidence-set:v1",
    )
    verifier = _handler(
        "evidence.verify",
        "schema:acceptance:v1",
        {"minimum_outputs": 2},
        "schema:verification:v1",
    )
    handoff = _handler(
        "evidence.handoff",
        "schema:acceptance:v1",
        {"minimum_outputs": 2},
        "schema:handoff:v1",
    )
    return RunSemanticInputBinding.create(
        request_scope="tenant-1",
        run_id="run-goal",
        blueprint_family="GoalDirected",
        effective_configuration_digest=DIGEST,
        blueprint_digest=BLUEPRINT_DIGEST,
        goal_operation_handlers=(
            GoalOperationHandlerBinding(operation_class="research", handler=iteration),
        ),
        goal_verifier=verifier,
        goal_handoff=handoff,
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
    )


def _stage_request(binding: RunSemanticInputBinding) -> StageOperationRequest:
    return StageOperationRequest(
        identity=StageExecutionIdentity(
            run_id=binding.run_id,
            stage_id="normalize",
            workflow_cycle=0,
            stage_cycle=0,
            operation_attempt=1,
            execution_epoch=1,
        ),
        idempotency_key="operation:normalize:1",
        objective="Normalize frozen claims.",
        input_refs=(),
        reservation_id="reservation:normalize:1",
        reservation={"operation.attempts": 1},
        workspace_namespace="run/run-stage/stage/normalize",
        request_scope=binding.request_scope,
        semantic_input_binding_ref=binding.binding_id,
        effective_configuration_digest=binding.effective_configuration_digest,
        blueprint_digest=binding.blueprint_digest,
    )


def _goal_blueprint() -> GoalDirectedBlueprint:
    return GoalDirectedBlueprint(
        logical_id="fixture.goal",
        title="Evidence collection",
        description="Collect and independently verify frozen evidence.",
        objective_contract="objective:evidence:v1",
        acceptance_contract="acceptance:evidence:v1",
        independent_verifier_ref="verifier:evidence:v1",
        allowed_operation_classes=frozenset({"research"}),
        max_iterations=2,
    )


def _goal_claim(
    binding: RunSemanticInputBinding,
    blueprint: GoalDirectedBlueprint,
) -> GoalExecutionClaim:
    assert binding.blueprint_digest == sha256_digest(blueprint)
    run_input = GoalDirectedRunInput(
        run_id=binding.run_id,
        request_scope=binding.request_scope,
        effective_configuration_digest=binding.effective_configuration_digest,
        blueprint_digest=binding.blueprint_digest,
        blueprint=blueprint.model_dump(mode="json"),
        protected_scope_digest=SCOPE_DIGEST,
        initial_revision=GoalRevision(
            revision_id="revision:1",
            revision=1,
            parent_revision_id=None,
            protected_scope_digest=SCOPE_DIGEST,
            objective="Find two independently verifiable facts.",
            evidence_refs=("input:goal",),
            unmet_obligations=("evidence",),
            author="test",
            deciding_authority="authority:test",
            applicability="remaining_run",
        ),
        semantic_input_binding_ref=binding.binding_id,
    )
    interpreter = GoalDirectedInterpreter(blueprint)
    state = interpreter.initial_state(run_input)
    _, claim = interpreter.claim_execution(state)
    return claim


def _goal_binding_for_blueprint(
    blueprint: GoalDirectedBlueprint,
) -> RunSemanticInputBinding:
    original = _goal_binding()
    return RunSemanticInputBinding.create(
        request_scope=original.request_scope,
        run_id=original.run_id,
        blueprint_family="GoalDirected",
        effective_configuration_digest=original.effective_configuration_digest,
        blueprint_digest=sha256_digest(blueprint),
        goal_operation_handlers=original.goal_operation_handlers,
        goal_verifier=original.goal_verifier,
        goal_handoff=original.goal_handoff,
        created_at=original.created_at,
    )


@pytest.mark.asyncio
async def test_binding_is_canonical_immutable_and_idempotently_persisted() -> None:
    binding = _stage_binding()
    repository = InMemoryRunSemanticInputBindingRepository()
    service = RunSemanticInputBindingService(repository)

    assert await service.freeze(binding) == binding.binding_id
    assert await service.freeze(binding) == binding.binding_id
    assert binding.stage_handlers[0].handler.input.payload_json == (
        '{"claims":["  NAD   therapy ","nad therapy","Cryotherapy"]}'
    )
    with pytest.raises(ValidationError):
        binding.run_id = "changed"  # type: ignore[misc]

    changed = _stage_binding(handler_id="claims.other")
    with pytest.raises(SemanticInputBindingConflict):
        await repository.create(changed)


@pytest.mark.asyncio
async def test_stage_router_executes_bound_semantics_and_exact_evaluation() -> None:
    binding = _stage_binding()
    repository = InMemoryRunSemanticInputBindingRepository()
    await repository.create(binding)
    registry = SemanticHandlerRegistry()
    registry.register_stage("claims.normalize", 1, CanonicalClaimStageHandler())
    registry.register_workflow_evaluator("claims.evaluate", 1, ExactOutputEvaluator())

    operation = await BoundStageOperationExecutor(repository, registry).execute(
        _stage_request(binding)
    )
    assert operation.disposition == "completed"
    assert operation.output_refs[0].startswith("evidence:normalized:")

    evaluation = await BoundWorkflowEvaluator(repository, registry).evaluate(
        WorkflowEvaluationRequest(
            run_id=binding.run_id,
            workflow_cycle=0,
            objective="Accept normalized claims.",
            current_output_refs={"normalize": operation.output_refs},
            execution_lineage=(operation,),
            request_scope=binding.request_scope,
            semantic_input_binding_ref=binding.binding_id,
            effective_configuration_digest=binding.effective_configuration_digest,
            blueprint_digest=binding.blueprint_digest,
            evaluation_contract_ref="evaluation:claims:v1",
        )
    )
    assert evaluation.action == "accept"
    assert evaluation.evaluation_contract_ref == "evaluation:claims:v1"


@pytest.mark.asyncio
async def test_routing_fails_closed_for_missing_unknown_and_mismatched_binding() -> None:
    binding = _stage_binding()
    repository = InMemoryRunSemanticInputBindingRepository()
    registry = SemanticHandlerRegistry()
    executor = BoundStageOperationExecutor(repository, registry)

    with pytest.raises(SemanticInputBindingNotFound):
        await executor.execute(_stage_request(binding))

    await repository.create(binding)
    with pytest.raises(SemanticRoutingError, match="unknown semantic handler"):
        await executor.execute(_stage_request(binding))

    registry.register_stage("claims.normalize", 1, CanonicalClaimStageHandler())
    mismatched = _stage_request(binding)
    mismatched = StageOperationRequest(
        **{
            **mismatched.__dict__,
            "effective_configuration_digest": "sha256:" + "f" * 64,
        }
    )
    with pytest.raises(SemanticRoutingError, match="admitted configuration"):
        await executor.execute(mismatched)

    invalid_route = _handler(
        "claims.normalize",
        "schema:claim-set:v1",
        {"unexpected": "shape"},
        "schema:normalized-evidence:v1",
    )
    invalid_binding = RunSemanticInputBinding.create(
        request_scope="tenant-1",
        run_id="run-invalid-schema",
        blueprint_family="StageGraph",
        effective_configuration_digest=DIGEST,
        blueprint_digest=BLUEPRINT_DIGEST,
        stage_handlers=(
            StageHandlerBinding(stage_id="normalize", handler=invalid_route),
        ),
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
    )
    invalid_repository = InMemoryRunSemanticInputBindingRepository()
    await invalid_repository.create(invalid_binding)
    with pytest.raises(SemanticRoutingError, match="does not satisfy handler schema"):
        await BoundStageOperationExecutor(
            invalid_repository,
            registry,
        ).execute(_stage_request(invalid_binding))


@pytest.mark.asyncio
async def test_goal_router_executes_iteration_verification_and_handoff_semantics() -> None:
    blueprint = _goal_blueprint()
    binding = _goal_binding_for_blueprint(blueprint)
    repository = InMemoryRunSemanticInputBindingRepository()
    await repository.create(binding)
    registry = SemanticHandlerRegistry()
    registry.register_goal_iteration("evidence.collect", 1, EvidenceGoalHandler())
    registry.register_goal_verifier("evidence.verify", 1, EvidenceAcceptanceVerifier())
    registry.register_goal_handoff("evidence.handoff", 1, EvidenceHandoffHandler())

    claim = _goal_claim(binding, blueprint)
    execution = await BoundGoalIterationExecutor(repository, registry).execute(claim)
    assert execution.completion_claim
    assert len(execution.output_refs) == 2

    verification_request = GoalVerificationRequest(
        claim=claim,
        execution_result=execution,
        verifier_ref=blueprint.independent_verifier_ref,
        acceptance_contract_ref=blueprint.acceptance_contract,
    )
    verification = await BoundGoalIndependentVerifier(
        repository,
        registry,
    ).verify(verification_request)
    assert verification.action == "verified_completion"
    assert verification.evidence_refs == execution.output_refs

    handoff = await BoundGoalHandoffPreparer(repository, registry).prepare(
        GoalHandoffRequest(
            claim=claim,
            execution_result=execution,
            protected_scope_digest=claim.protected_scope_digest,
            verification_ref=verification.verification_ref,
        )
    )
    assert "at least 2" in handoff.checkpoint.instructions
    assert handoff.checkpoint.artifact_refs == execution.output_refs


def test_interpreters_propagate_exact_semantic_binding_authority() -> None:
    stage_binding = _stage_binding()
    blueprint = StageGraphBlueprint(
        logical_id="fixture.stage",
        title="Normalize",
        description="Normalize one frozen claim set.",
        stages=(
            StageNode(
                stage_id="normalize",
                reservation={"operation.attempts": 1},
                output_slots=frozenset({"normalized"}),
            ),
        ),
        declared_output_slots=frozenset({"normalized"}),
    )
    interpreter = StageGraphInterpreter(blueprint, effective_max_concurrency=1)
    state = interpreter.initial_state(
        ExecutionIdentity(run_id=stage_binding.run_id),
        run_version=1,
        request_scope=stage_binding.request_scope,
        semantic_input_binding_ref=stage_binding.binding_id,
        effective_configuration_digest=stage_binding.effective_configuration_digest,
        blueprint_digest=stage_binding.blueprint_digest,
    )
    request = interpreter.operation_request(state, "normalize")

    assert request.request_scope == stage_binding.request_scope
    assert request.semantic_input_binding_ref == stage_binding.binding_id
    assert request.effective_configuration_digest == DIGEST
    assert request.blueprint_digest == BLUEPRINT_DIGEST
