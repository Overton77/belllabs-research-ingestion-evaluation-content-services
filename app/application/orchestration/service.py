from __future__ import annotations

from dataclasses import asdict, replace
from datetime import UTC, datetime
from typing import Literal, Protocol

from pydantic import TypeAdapter

from app.application.control_plane.service import ControlPlaneService
from app.application.operations.operation_execution import bind_operation_execution_request
from app.application.orchestration.orchestration_binding_repository import (
    RunSemanticInputBindingService,
)
from app.application.run_control.service import (
    ACTION_PERMISSIONS,
    FamilyAdmissionRegistry,
    RunControlService,
)
from app.application.run_control.run_control_repository import RunControlRepository
from app.application.operations.semantic_operation_bindings import SemanticOperationBindingRepository
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    GoalDirectedBlueprint,
    StageGraphBlueprint,
)
from app.domain.operation_execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationWorkflowRequest,
    PromptSegment,
)
from app.domain.orchestration.bindings import RunSemanticInputBinding
from app.domain.orchestration.contracts import (
    GoalDirectedRunInput,
    GoalRevision,
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
    StageGraphAcceptedProjection,
    StageGraphAdmissionActivityRequest,
    StageGraphAdmissionActivityResult,
    StageGraphCompletionActivityRequest,
    StageGraphCompletionActivityResult,
    StageGraphCycleActivityRequest,
    StageGraphCycleActivityResult,
    StageGraphDecisionMutation,
    StageGraphInitializeRequest,
    StageGraphInitializeResult,
    StageGraphResultActivityRequest,
    StageGraphResultActivityResult,
    StageGraphRunInput,
    StageInvalidationProposal,
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationRequest,
    WorkflowEvaluationResult,
    WorkflowInvalidationProposal,
)
from app.domain.orchestration.interpreter import StageGraphInterpreter
from app.domain.run_control.contracts import (
    AcceptedObligationEvidence,
    AcceptedOperationSettlementEvidence,
    AcceptedOutputEvidence,
    ActorContext,
    ApplyAuthorityBatchAction,
    CommandStatus,
    LifecycleAction,
    LifecycleCommand,
    RecordObligationEvidenceAction,
    RecordOperationSettlementEvidenceAction,
    RecordOutputEvidenceAction,
    RecordUsageAction,
    ReserveBudgetAction,
    StartAction,
    TerminalizationProposal,
    TerminalizeAction,
)
from app.domain.run_control.family_admission import AtomicFamilyMutation

LIFECYCLE_ACTION_ADAPTER: TypeAdapter[LifecycleAction] = TypeAdapter(LifecycleAction)
ORCHESTRATION_AUTHORITY_REF = "orchestration-authority"


def orchestration_lifecycle_actor() -> ActorContext:
    """Return the least-privilege service identity for workflow lifecycle commands."""

    return ActorContext(
        actor_id=ORCHESTRATION_AUTHORITY_REF,
        authority_refs=frozenset({ORCHESTRATION_AUTHORITY_REF}),
        permissions=frozenset(ACTION_PERMISSIONS.values()),
    )


class OrchestrationBindingVerifier(Protocol):
    async def verify(
        self,
        effective_configuration_digest: str,
        blueprint_digest: str,
    ) -> None: ...


class F1OrchestrationBindingVerifier:
    def __init__(self, control_plane: ControlPlaneService) -> None:
        self._control_plane = control_plane

    async def verify(
        self,
        effective_configuration_digest: str,
        blueprint_digest: str,
    ) -> None:
        configuration = await self._control_plane.retrieve_for_admission(
            effective_configuration_digest
        )
        blueprint_ref = next(
            (ref for ref in configuration.source_refs if ref.kind == DefinitionKind.BLUEPRINT),
            None,
        )
        if (
            configuration.digest != effective_configuration_digest
            or blueprint_ref is None
            or blueprint_ref.digest != blueprint_digest
            or sha256_digest(configuration.selected_blueprint) != blueprint_digest
        ):
            raise ValueError("orchestration blueprint does not match the admitted F1 binding")


class StageGraphLaunchService:
    """Resolves exact admitted F1/F2 bindings before Temporal receives immutable input."""

    def __init__(
        self,
        run_control: RunControlService,
        control_plane: ControlPlaneService,
    ) -> None:
        self._run_control = run_control
        self._control_plane = control_plane

    async def prepare(
        self,
        request_scope: str,
        run_id: str,
        *,
        execution_epoch: int = 1,
        task_timeout_seconds: int = 30,
        orchestration_authority_ref: str = ORCHESTRATION_AUTHORITY_REF,
        semantic_input_binding_ref: str = "",
    ) -> StageGraphRunInput:
        if execution_epoch != 1:
            raise ValueError(
                "execution epoch rollover requires the deferred orchestration continuity contract"
            )
        projection = await self._run_control.get_run(request_scope, run_id)
        configuration = await self._control_plane.retrieve_for_admission(
            projection.effective_configuration_digest
        )
        if configuration.digest != projection.effective_configuration_digest:
            raise ValueError("admitted effective configuration digest does not match F1 authority")
        blueprint = configuration.selected_blueprint
        if not isinstance(blueprint, StageGraphBlueprint):
            raise ValueError("admitted blueprint is not a StageGraph")
        blueprint_ref = next(
            (ref for ref in configuration.source_refs if ref.kind == DefinitionKind.BLUEPRINT),
            None,
        )
        if blueprint_ref is None or blueprint_ref.digest != sha256_digest(blueprint):
            raise ValueError("exact StageGraph reference does not match the frozen blueprint")
        budget = await self._run_control.get_budget(request_scope, run_id)
        return StageGraphRunInput(
            run_id=run_id,
            request_scope=request_scope,
            effective_configuration_digest=configuration.digest,
            workflow_type_digest=projection.workflow_type_ref.digest,
            blueprint_digest=blueprint_ref.digest,
            blueprint=blueprint.model_dump(mode="json"),
            initial_run_version=projection.version,
            execution_epoch=execution_epoch,
            max_concurrency=configuration.effective_authority.max_concurrency,
            task_timeout_seconds=task_timeout_seconds,
            orchestration_authority_ref=orchestration_authority_ref,
            correlation_id=f"orchestration:{run_id}:epoch:{execution_epoch}",
            baseline_reservation=dict(budget.reservations.get("baseline", {})),
            semantic_input_binding_ref=semantic_input_binding_ref,
        )


class StageGraphDecisionService:
    """Commit StageGraph proposals through the frozen atomic family seam."""

    def __init__(
        self,
        run_control: RunControlService,
        repository: RunControlRepository,
    ) -> None:
        self._run_control = run_control
        self._repository = repository

    async def initialize(
        self, request: StageGraphInitializeRequest
    ) -> StageGraphInitializeResult:
        result = await self._run_control.execute(
            LifecycleCommand(
                command_id=f"stagegraph:{request.run_id}:start",
                idempotency_issuer=request.idempotency_issuer,
                request_scope=request.request_scope,
                run_id=request.run_id,
                expected_run_version=request.expected_run_version,
                actor=orchestration_lifecycle_actor(),
                action=StartAction(),
                reason="Canonical StageGraph execution started",
                occurred_at=request.occurred_at,
                correlation_id=request.correlation_id,
            )
        )
        projection = replace(
            request.initial_projection,
            run_version=result.resulting_run_version,
        )
        return StageGraphInitializeResult(
            accepted=result.status == CommandStatus.ACCEPTED,
            projection=projection,
            reason_code=result.reason_code,
        )

    async def admit_operation(
        self, request: StageGraphAdmissionActivityRequest
    ) -> StageGraphAdmissionActivityResult:
        await self._verify_family_head(
            request.request_scope,
            request.run_id,
            request.projection,
        )
        proposal = request.proposal
        operation = request.operation
        if operation is None:
            raise ValueError("StageGraph admission requires a materialized operation request")
        interpreter = StageGraphInterpreter(
            StageGraphBlueprint.model_validate(request.blueprint),
            effective_max_concurrency=request.effective_max_concurrency,
        )
        available_concurrency = max(
            request.effective_max_concurrency
            - interpreter.running_concurrency(request.projection),
            0,
        )
        authoritative_frontier = interpreter.frontier(
            request.projection,
            available_concurrency=available_concurrency,
        )
        if not authoritative_frontier or authoritative_frontier[0] != proposal:
            raise ValueError("StageGraph admission proposal is stale or not authoritative")
        if (
            operation.semantic_attempt_id != proposal.identity.semantic_key
            or operation.operation.identity.run_id != request.run_id
            or operation.operation.budget_reservation_id != proposal.reservation_id
            or operation.operation.run_control_revision != request.projection.run_version + 1
        ):
            raise ValueError("exact operation request does not match StageGraph admission")
        next_projection = interpreter.apply_admission(
            request.projection,
            proposal,
            next_run_version=request.projection.run_version + 1,
            next_family_version=request.projection.family_version + 1,
        )
        mutation = self._mutation(
            request.projection,
            next_projection,
            decision_kind="operation_admitted",
            mutation_id=f"admit-{sha256_digest(proposal.identity.semantic_key)[7:]}",
            exact_operation_request_ref=proposal.exact_operation_request_ref,
            request_scope=request.request_scope,
            decided_at=request.occurred_at,
            decision_payload={"proposal": asdict(proposal)},
        )
        receipt = await self._run_control.execute_family_admission(
            LifecycleCommand(
                command_id=f"stagegraph:{mutation.mutation_id}",
                idempotency_issuer=request.idempotency_issuer,
                request_scope=request.request_scope,
                run_id=request.run_id,
                expected_run_version=request.projection.run_version,
                actor=orchestration_lifecycle_actor(),
                action=ReserveBudgetAction(
                    reservation_id=proposal.reservation_id,
                    amounts=proposal.reservation,
                ),
                reason="Atomically admit StageGraph operation and advance fairness cursors",
                evidence_refs=(proposal.exact_operation_request_ref,),
                occurred_at=request.occurred_at,
                correlation_id=request.correlation_id,
            ),
            mutation,
        )
        accepted = receipt.command_result.status == CommandStatus.ACCEPTED
        return StageGraphAdmissionActivityResult(
            accepted=accepted,
            projection=next_projection if accepted else request.projection,
            operation=operation if accepted else None,
            reason_code=receipt.command_result.reason_code,
        )

    async def decide_result(
        self, request: StageGraphResultActivityRequest
    ) -> StageGraphResultActivityResult:
        await self._verify_family_head(
            request.request_scope,
            request.run_id,
            request.projection,
        )
        interpreter = StageGraphInterpreter(
            StageGraphBlueprint.model_validate(request.blueprint),
            effective_max_concurrency=request.effective_max_concurrency,
        )
        proposal = interpreter.result_decision(
            request.observation.identity,
            request.late_facts,
            operation_disposition=request.observation.operation_disposition,
        )
        run = await self._run_control.get_run(request.request_scope, request.run_id)
        current_projection = replace(request.projection, run_version=run.version)
        effects = await self._run_control.get_effects(request.request_scope, request.run_id)
        budget = await self._run_control.get_budget(request.request_scope, request.run_id)
        observation = replace(
            request.observation,
            reservations_and_usage_settled=True,
            effects_settled=all(
                claim.settlement is not None for claim in effects.claims.values()
            ),
            cancellation_reconciled=(
                run.phase.value != "cancelling"
                or request.observation.cancellation_reconciled
            ),
        )
        liability = current_projection.producer_liabilities[
            observation.identity.semantic_key
        ]
        if liability.reservation_id in budget.reservations:
            raise ValueError("operation usage reservation is not authoritatively settled")
        reservation_id = next(
            (
                instance.admitted_operation_request_ref
                for instance in current_projection.stages.values()
                if instance.candidate == request.observation.identity.candidate
            ),
            None,
        )
        if reservation_id is None:
            raise ValueError("StageGraph result has no admitted operation reference")
        next_projection = interpreter.apply_result_decision(
            current_projection,
            observation,
            proposal,
            next_run_version=current_projection.run_version + 1,
            next_family_version=current_projection.family_version + 1,
        )
        mutation = self._mutation(
            current_projection,
            next_projection,
            decision_kind="result_decided",
            mutation_id=f"result-{sha256_digest(observation.identity.semantic_key)[7:]}",
            exact_operation_request_ref=reservation_id,
            request_scope=request.request_scope,
            decided_at=request.occurred_at,
            decision_payload={
                "proposal": asdict(proposal),
                "prior_liability": asdict(liability),
                "observation_digest": sha256_digest(observation.operation_result),
            },
        )
        output_evidence = observation.operation_result.get("output_refs", ())
        if not isinstance(output_evidence, list | tuple):
            raise ValueError("operation result output refs must be a sequence")
        obligation_evidence = observation.operation_result.get("obligation_refs", ())
        if not isinstance(obligation_evidence, list | tuple):
            raise ValueError("operation result obligation refs must be a sequence")
        batch_actions: list[
            RecordObligationEvidenceAction
            | RecordOutputEvidenceAction
            | RecordOperationSettlementEvidenceAction
        ] = []
        if proposal.decision.value == "admit":
            admitted_outputs = next(
                (
                    item.output_refs
                    for item in next_projection.stages.values()
                    if item.candidate == request.observation.identity.candidate
                ),
                (),
            )
            batch_actions.extend(
                RecordObligationEvidenceAction(
                    evidence=AcceptedObligationEvidence(
                        obligation_ref=str(obligation_ref),
                        evidence_digest=sha256_digest(str(obligation_ref)),
                        accepted_by_authority_ref=ORCHESTRATION_AUTHORITY_REF,
                    )
                )
                for obligation_ref in sorted(
                    {str(item) for item in obligation_evidence},
                    key=lambda item: item.encode("utf-8"),
                )
            )
            batch_actions.extend(
                RecordOutputEvidenceAction(
                    evidence=AcceptedOutputEvidence(
                        output_ref=output_ref,
                        evidence_digest=sha256_digest(output_ref),
                        accepted_by_authority_ref=ORCHESTRATION_AUTHORITY_REF,
                    )
                )
                for output_ref in sorted(
                    admitted_outputs,
                    key=lambda item: item.encode("utf-8"),
                )
            )
        batch_actions.append(
            RecordOperationSettlementEvidenceAction(
                evidence=AcceptedOperationSettlementEvidence(
                    settlement_id=(
                        f"stagegraph-result:{request.observation.identity.semantic_key}"
                    ),
                    settlement_payload_digest=sha256_digest(
                        observation.operation_result
                    ),
                    accepted_by_authority_ref=ORCHESTRATION_AUTHORITY_REF,
                )
            )
        )
        receipt = await self._run_control.execute_family_admission(
            LifecycleCommand(
                command_id=f"stagegraph:{mutation.mutation_id}",
                idempotency_issuer=request.idempotency_issuer,
                request_scope=request.request_scope,
                run_id=request.run_id,
                expected_run_version=current_projection.run_version,
                actor=orchestration_lifecycle_actor(),
                action=ApplyAuthorityBatchAction(actions=tuple(batch_actions)),
                reason="Settle StageGraph producer usage and decide its result",
                evidence_refs=tuple(str(item) for item in output_evidence),
                occurred_at=request.occurred_at,
                correlation_id=request.correlation_id,
            ),
            mutation,
        )
        accepted = receipt.command_result.status == CommandStatus.ACCEPTED
        if accepted:
            if receipt.family_receipt is None:
                raise RuntimeError("accepted StageGraph result is missing its family receipt")
            next_projection = replace(
                next_projection,
                run_version=receipt.command_result.resulting_run_version,
                family_version=receipt.family_receipt.family_version,
            )
        return StageGraphResultActivityResult(
            accepted=accepted,
            projection=next_projection if accepted else current_projection,
            proposal=proposal,
            reason_code=receipt.command_result.reason_code,
        )

    async def apply_cycle(
        self, request: StageGraphCycleActivityRequest
    ) -> StageGraphCycleActivityResult:
        await self._verify_family_head(
            request.request_scope,
            request.run_id,
            request.projection,
        )
        run = await self._run_control.get_run(request.request_scope, request.run_id)
        current_projection = replace(request.projection, run_version=run.version)
        blueprint = StageGraphBlueprint.model_validate(request.blueprint)
        stage = (
            next((item for item in blueprint.stages if item.stage_id == request.stage_id), None)
            if request.cycle_scope == "stage"
            else None
        )
        policy = (
            stage.stage_cycle_policy
            if stage is not None
            else blueprint.workflow_cycle_policy
        )
        if policy is None:
            raise ValueError(f"StageGraph {request.cycle_scope} cycle is not authored")
        if (
            request.evaluation_contract_ref != policy.evaluation_contract_ref
            or request.objective_contract_ref != policy.objective_contract_ref
            or not request.evaluation_ref
        ):
            raise ValueError(f"{request.cycle_scope} cycle evidence is outside frozen contracts")
        interpreter = StageGraphInterpreter(
            blueprint,
            effective_max_concurrency=request.effective_max_concurrency,
        )
        proposal: StageInvalidationProposal | WorkflowInvalidationProposal
        if request.cycle_scope == "stage":
            if request.stage_id is None:
                raise ValueError("stage cycle requires a stage identity")
            stage_proposal = interpreter.stage_invalidation(
                current_projection,
                stage_id=request.stage_id,
                next_objective=request.next_objective,
            )
            next_projection = interpreter.apply_stage_invalidation(
                current_projection,
                stage_proposal,
                next_run_version=current_projection.run_version + 1,
                next_family_version=current_projection.family_version + 1,
            )
            proposal = stage_proposal
            cycle_ordinal = stage_proposal.next_stage_cycle_ordinal
        else:
            workflow_proposal = interpreter.workflow_invalidation(
                current_projection,
                invalidation_frontier=request.invalidation_frontier,
                next_objective=request.next_objective,
            )
            next_projection = interpreter.apply_workflow_invalidation(
                current_projection,
                workflow_proposal,
                next_run_version=current_projection.run_version + 1,
                next_family_version=current_projection.family_version + 1,
            )
            proposal = workflow_proposal
            cycle_ordinal = workflow_proposal.next_workflow_cycle_ordinal
        exact_ref = next(
            (
                item.admitted_operation_request_ref
                for item in current_projection.stages.values()
                if item.admitted_operation_request_ref is not None
            ),
            None,
        )
        if exact_ref is None:
            raise ValueError("workflow cycle requires an admitted producer operation")
        mutation = self._mutation(
            current_projection,
            next_projection,
            decision_kind="cycle_decided",
            mutation_id=(
                f"cycle-{request.run_id}-{request.cycle_scope}-{request.stage_id or 'graph'}-"
                f"{cycle_ordinal}"
            ),
            exact_operation_request_ref=exact_ref,
            request_scope=request.request_scope,
            decided_at=request.occurred_at,
            decision_payload={
                "proposal": asdict(proposal),
                "evaluation_ref": request.evaluation_ref,
            },
        )
        receipt = await self._run_control.execute_family_admission(
            LifecycleCommand(
                command_id=f"stagegraph:{mutation.mutation_id}",
                idempotency_issuer=request.idempotency_issuer,
                request_scope=request.request_scope,
                run_id=request.run_id,
                expected_run_version=current_projection.run_version,
                actor=orchestration_lifecycle_actor(),
                action=RecordUsageAction(
                    usage_id=(
                        f"stagegraph-cycle:{request.run_id}:"
                        f"{request.cycle_scope}:{request.stage_id or 'graph'}:{cycle_ordinal}"
                    ),
                    authority_ref=ORCHESTRATION_AUTHORITY_REF,
                    actual_amounts=policy.reservation,
                ),
                reason="Accept typed StageGraph workflow cycle and invalidation frontier",
                evidence_refs=(request.evaluation_ref,),
                occurred_at=request.occurred_at,
                correlation_id=request.correlation_id,
            ),
            mutation,
        )
        accepted = receipt.command_result.status == CommandStatus.ACCEPTED
        if accepted:
            if receipt.family_receipt is None:
                raise RuntimeError("accepted StageGraph cycle is missing its family receipt")
            next_projection = replace(
                next_projection,
                run_version=receipt.command_result.resulting_run_version,
                family_version=receipt.family_receipt.family_version,
            )
        return StageGraphCycleActivityResult(
            accepted=accepted,
            projection=next_projection if accepted else current_projection,
            proposal=proposal,
            reason_code=receipt.command_result.reason_code,
        )

    async def complete(
        self, request: StageGraphCompletionActivityRequest
    ) -> StageGraphCompletionActivityResult:
        if not request.proposal.can_terminalize:
            raise ValueError("StageGraph completion proposal still has open obligations")
        await self._verify_family_head(
            request.request_scope,
            request.run_id,
            request.projection,
        )
        run = await self._run_control.get_run(request.request_scope, request.run_id)
        current_projection = replace(request.projection, run_version=run.version)
        budget = await self._run_control.get_budget(request.request_scope, request.run_id)
        effects = await self._run_control.get_effects(request.request_scope, request.run_id)
        next_projection = replace(
            current_projection,
            family_version=current_projection.family_version + 1,
            run_version=current_projection.run_version + 1,
        )
        exact_ref = next(
            (
                item.admitted_operation_request_ref
                for item in current_projection.stages.values()
                if item.admitted_operation_request_ref is not None
            ),
            None,
        )
        if exact_ref is None:
            raise ValueError("StageGraph terminalization requires an exact producer operation")
        mutation = self._mutation(
            current_projection,
            next_projection,
            decision_kind="completion_proposed",
            mutation_id=f"complete-{sha256_digest(request.projection.digest)[7:]}",
            exact_operation_request_ref=exact_ref,
            request_scope=request.request_scope,
            decided_at=request.occurred_at,
            decision_payload={"completion": asdict(request.proposal)},
        )
        obligation_payload = [
            item.model_dump(mode="json")
            for item in sorted(
                run.accepted_obligation_evidence,
                key=lambda item: item.obligation_ref,
            )
        ]
        terminal = TerminalizationProposal(
            proposal_id=mutation.mutation_id,
            expected_run_version=run.version,
            workflow_type_digest=request.workflow_type_digest,
            obligation_revision=run.obligation_revision,
            evidence_frontier_digest=run.evidence_frontier_digest,
            accepted_obligation_evidence_digest=sha256_digest(obligation_payload),
            proposing_execution_binding_ref=exact_ref,
            required_obligations_accepted=request.proposal.required_obligations_accepted,
            execution_failure_refs=tuple(
                item.candidate.semantic_prefix
                for item in current_projection.stages.values()
                if item.status == "failed"
            ),
            valid_output_refs=request.proposal.valid_output_refs,
            cancellation_settled=run.phase.value != "cancelling",
            budget_settled=(
                not any(budget.reserved.values())
                and not any(budget.pending_settlement.values())
            ),
            effects_settled=all(
                claim.settlement is not None for claim in effects.claims.values()
            ),
            pending_wait_or_link_ids=request.proposal.pending_dependency_ids,
            proposed_at=request.occurred_at,
            finalization_plan=run.finalization_plan,
            output_omission_reason=run.finalization_omission_reason,
        )
        receipt = await self._run_control.execute_family_admission(
            LifecycleCommand(
                command_id=f"stagegraph:{mutation.mutation_id}",
                idempotency_issuer=request.idempotency_issuer,
                request_scope=request.request_scope,
                run_id=request.run_id,
                expected_run_version=run.version,
                actor=orchestration_lifecycle_actor(),
                action=TerminalizeAction(proposal=terminal),
                reason="Reducer-authorized StageGraph obligation completion",
                evidence_refs=request.proposal.valid_output_refs,
                occurred_at=request.occurred_at,
                correlation_id=request.correlation_id,
            ),
            mutation,
        )
        result = receipt.command_result
        return StageGraphCompletionActivityResult(
            accepted=result.status == CommandStatus.ACCEPTED,
            terminal_outcome=result.terminal_outcome,
            resulting_run_version=result.resulting_run_version,
            reason_code=result.reason_code,
        )

    async def _verify_family_head(
        self,
        request_scope: str,
        run_id: str,
        projection: StageGraphAcceptedProjection,
    ) -> None:
        head = await self._repository.get_family_head(
            request_scope,
            run_id,
            "stagegraph",
            StageGraphDecisionMutation,
        )
        if projection.family_version == 0:
            if head is not None:
                raise ValueError("StageGraph projection is stale against the family head")
            return
        if (
            head is None
            or head.expected_family_version + 1 != projection.family_version
            or head.next_projection_digest != projection.digest
        ):
            raise ValueError("StageGraph projection does not match the authoritative family head")

    @staticmethod
    def _mutation(
        prior: StageGraphAcceptedProjection,
        next_projection: StageGraphAcceptedProjection,
        *,
        decision_kind: Literal[
            "operation_admitted",
            "result_decided",
            "wait_decided",
            "cycle_decided",
            "completion_proposed",
        ],
        mutation_id: str,
        exact_operation_request_ref: str,
        request_scope: str,
        decided_at: datetime,
        decision_payload: dict[str, object],
    ) -> StageGraphDecisionMutation:
        return StageGraphDecisionMutation(
            mutation_id=mutation_id,
            request_scope=request_scope,
            run_id=prior.identity.run_id,
            expected_family_version=prior.family_version,
            exact_operation_request_ref=exact_operation_request_ref,
            decided_at=decided_at,
            decision_kind=decision_kind,
            prior_projection_digest=prior.digest,
            next_projection_digest=next_projection.digest,
            decision_payload={
                **decision_payload,
                "projection": asdict(next_projection),
            },
        )


class StageGraphOperationMaterializer(Protocol):
    async def materialize(
        self,
        request: StageGraphAdmissionActivityRequest,
    ) -> OperationWorkflowRequest: ...


class StageGraphOperationTemplateProvider(Protocol):
    """Resolve an immutable operation template from the admitted semantic binding."""

    async def get_template(
        self,
        *,
        semantic_input_binding_ref: str,
        operation_request_key: str,
        request_scope: str,
        run_id: str,
    ) -> OperationExecutionRequest: ...


class StaticStageGraphOperationTemplateProvider:
    """Process-local adapter for an immutable, already-admitted template set."""

    def __init__(self, templates: dict[str, OperationExecutionRequest]) -> None:
        if not templates:
            raise ValueError("StageGraph operation templates cannot be empty")
        if any(not key for key in templates):
            raise ValueError("StageGraph operation template keys cannot be empty")
        self._templates = dict(templates)

    async def get_template(
        self,
        *,
        semantic_input_binding_ref: str,
        operation_request_key: str,
        request_scope: str,
        run_id: str,
    ) -> OperationExecutionRequest:
        del request_scope, run_id
        if not semantic_input_binding_ref:
            raise ValueError("StageGraph template lookup requires an immutable semantic binding")
        template = self._templates.get(operation_request_key)
        if template is None:
            raise ValueError(
                f"StageGraph operation template is unavailable: {operation_request_key}"
            )
        return template


class StageGraphOperationPreparationService:
    """Materialize and persist the exact OperationWorkflow child before admission."""

    def __init__(
        self,
        *,
        templates: StageGraphOperationTemplateProvider,
        operation_bindings: SemanticOperationBindingRepository,
    ) -> None:
        self._templates = templates
        self._operation_bindings = operation_bindings

    async def materialize(
        self,
        request: StageGraphAdmissionActivityRequest,
    ) -> OperationWorkflowRequest:
        proposal = request.proposal
        template = await self._templates.get_template(
            semantic_input_binding_ref=request.semantic_input_binding_ref,
            operation_request_key=proposal.operation_request_key,
            request_scope=request.request_scope,
            run_id=request.run_id,
        )
        workspace = template.workspace.model_copy(
            update={
                "namespace_id": template.workspace.namespace_id.replace(
                    "{run_id}", request.run_id
                ),
                "workspace_id": template.workspace.workspace_id.replace(
                    "{run_id}", request.run_id
                ),
            }
        )
        identity = OperationAttemptIdentity(
            run_id=request.run_id,
            operation_id=proposal.identity.operation_id,
            operation_attempt=proposal.identity.semantic_attempt,
        )
        control_revision = request.projection.run_version + 1
        deep_binding = template.deep_agent_binding
        if deep_binding is not None:
            deep_binding = deep_binding.__class__.create(
                **{
                    **deep_binding.model_dump(
                        mode="python", exclude={"binding_digest"}
                    ),
                    "binding_id": f"deep-agent:{identity.semantic_key}",
                    "run_id": request.run_id,
                    "operation_id": identity.operation_id,
                    "operation_attempt": identity.operation_attempt,
                    "execution_generation": proposal.identity.execution_generation,
                    "erc_digest": request.effective_configuration_digest,
                    "control_revision": control_revision,
                    "workspace": workspace,
                    "capability_grant": template.capability_grant,
                    "reservation_id": proposal.reservation_id,
                }
            )
        operation = OperationExecutionRequest.model_validate(
            {
                **template.model_dump(mode="python"),
                "identity": OperationAttemptIdentity(
                    run_id=identity.run_id,
                    operation_id=identity.operation_id,
                    operation_attempt=identity.operation_attempt,
                ),
                "request_scope": request.request_scope,
                "effective_configuration_digest": request.effective_configuration_digest,
                "run_control_revision": control_revision,
                "workspace": workspace,
                "deep_agent_binding": deep_binding,
                "budget_reservation_id": proposal.reservation_id,
                "budget_limits": proposal.reservation,
                "prompt_segments": (
                    template.prompt_segments
                    if proposal.objective_override is None
                    else (
                        *template.prompt_segments,
                        PromptSegment(
                            source_ref=f"stage-cycle-objective:{proposal.identity.semantic_key}",
                            source_revision=1,
                            # The cycle objective is a reducer-admitted run input,
                            # not a newly published privileged prompt definition.
                            trust_class="admitted_input",
                            content=proposal.objective_override,
                            rendered_digest=sha256_digest(proposal.objective_override),
                        ),
                    )
                ),
                "prior_binding_id": None,
                "requested_at": request.occurred_at,
                "idempotency_key": (
                    f"stagegraph:{proposal.identity.semantic_key}:"
                    f"generation:{proposal.identity.execution_generation}"
                ),
            }
        )
        if operation.identity.semantic_key != proposal.identity.semantic_key:
            raise ValueError("StageGraph and OperationWorkflow semantic identities diverged")
        binding = bind_operation_execution_request(operation)
        persisted = await self._operation_bindings.create_binding(
            binding,
            request_scope=request.request_scope,
        )
        if persisted != binding:
            raise ValueError("persisted StageGraph operation binding differs from exact intent")
        return OperationWorkflowRequest(
            semantic_attempt_id=operation.identity.semantic_key,
            execution_generation=proposal.identity.execution_generation,
            operation_kind="bound_operation",
            operation=operation,
        )


def validate_stagegraph_authority_batch(
    mutation: AtomicFamilyMutation,
    batch: ApplyAuthorityBatchAction,
) -> str | None:
    """Narrow StageGraph batch references to the exact producer decision under admission."""

    if not isinstance(mutation, StageGraphDecisionMutation):
        return "unexpected StageGraph mutation type"
    settlement_actions = tuple(
        action
        for action in batch.actions
        if isinstance(action, RecordOperationSettlementEvidenceAction)
    )
    output_actions = tuple(
        action for action in batch.actions if isinstance(action, RecordOutputEvidenceAction)
    )
    obligation_actions = tuple(
        action
        for action in batch.actions
        if isinstance(action, RecordObligationEvidenceAction)
    )
    if len(settlement_actions) != 1:
        return "StageGraph result batches require exactly one settlement observation"
    if mutation.decision_kind != "result_decided":
        if output_actions or obligation_actions:
            return "only StageGraph result decisions may batch evidence acceptance"
        return None
    payload = mutation.decision_payload.get("prior_liability")
    if not isinstance(payload, dict):
        return "StageGraph result batch lacks prior producer liability binding"
    semantic_key = payload.get("semantic_attempt_id")
    evidence = settlement_actions[0].evidence
    if evidence.settlement_id != f"stagegraph-result:{semantic_key}":
        return "StageGraph settlement observation does not bind the producer liability"
    if evidence.settlement_payload_digest != mutation.decision_payload.get(
        "observation_digest"
    ):
        return "StageGraph settlement observation digest does not bind the result"
    return None


def register_stagegraph_family_mutations(registry: FamilyAdmissionRegistry) -> None:
    """Publish the exact StageGraph policy for integrator-owned composition."""

    registry.register(
        StageGraphDecisionMutation,
        family_kind="stagegraph",
        mutation_kind="decision_committed",
        required_permission="workflow_run.reserve_budget",
        allowed_action_kinds=frozenset(
            {
                "reserve_budget",
                "record_usage",
                "terminalize",
                "apply_authority_batch",
            }
        ),
        allowed_batch_action_kinds=frozenset(
            {
                "record_obligation_evidence",
                "record_output_evidence",
                "record_operation_settlement_evidence",
            }
        ),
        required_batch_action_kinds=frozenset(
            {"record_operation_settlement_evidence"}
        ),
        batch_binding_validator=validate_stagegraph_authority_batch,
    )


class GoalDirectedLaunchService:
    """Resolve one admitted GoalDirected run into immutable Temporal input."""

    def __init__(
        self,
        run_control: RunControlService,
        control_plane: ControlPlaneService,
    ) -> None:
        self._run_control = run_control
        self._control_plane = control_plane

    async def prepare(
        self,
        request_scope: str,
        run_id: str,
        *,
        initial_goal: str,
        execution_epoch: int = 1,
        task_timeout_seconds: int = 300,
        orchestration_authority_ref: str = ORCHESTRATION_AUTHORITY_REF,
        semantic_input_binding_ref: str = "",
    ) -> GoalDirectedRunInput:
        if not initial_goal.strip():
            raise ValueError("GoalDirected launch requires a concrete initial goal")
        if execution_epoch != 1:
            raise ValueError(
                "execution epoch rollover requires the deferred orchestration continuity contract"
            )
        projection = await self._run_control.get_run(request_scope, run_id)
        configuration = await self._control_plane.retrieve_for_admission(
            projection.effective_configuration_digest
        )
        if configuration.digest != projection.effective_configuration_digest:
            raise ValueError("admitted effective configuration digest does not match F1 authority")
        blueprint = configuration.selected_blueprint
        if not isinstance(blueprint, GoalDirectedBlueprint):
            raise ValueError("admitted blueprint is not GoalDirected")
        blueprint_ref = next(
            (ref for ref in configuration.source_refs if ref.kind == DefinitionKind.BLUEPRINT),
            None,
        )
        if blueprint_ref is None or blueprint_ref.digest != sha256_digest(blueprint):
            raise ValueError("exact GoalDirected reference does not match the frozen blueprint")
        budget = await self._run_control.get_budget(request_scope, run_id)
        envelope_digest = sha256_digest(
            {
                "initial_goal": initial_goal,
                "objective_contract": blueprint.objective_contract,
                "acceptance_contract": blueprint.acceptance_contract,
                "workflow_invariants": configuration.workflow_type.invariants,
                "admitted_input_manifest": configuration.input_manifest,
                "admitted_input_classes": blueprint.admitted_input_classes,
                "effective_authority": configuration.effective_authority,
                "blueprint_authority_ceiling": blueprint.authority_ceiling,
                "budget_limits": budget.limits,
                "prohibited_work": blueprint.prohibited_work,
                "required_output_contracts": blueprint.required_output_contracts,
                "required_obligation_refs": blueprint.required_obligation_refs,
                "allowed_operation_classes": blueprint.allowed_operation_classes,
                "allowed_async_subgoal_classes": blueprint.allowed_async_subgoal_classes,
                "allowed_linked_run_slot_ids": blueprint.allowed_linked_run_slot_ids,
                "verifier_policy": blueprint.verifier_policy,
                "session_policy": blueprint.session_policy,
                "handoff_policy": blueprint.handoff_policy,
                "workspace_policy": blueprint.workspace_policy,
                "convergence_policy": blueprint.convergence_policy,
                "max_iterations": blueprint.max_iterations,
                "protected_fields": blueprint.protected_scope_policy.protected_fields,
            }
        )
        initial_revision_id = sha256_digest(
            {
                "run_id": run_id,
                "execution_epoch": execution_epoch,
                "revision": 1,
                "objective": initial_goal,
                "envelope_digest": envelope_digest,
            }
        )
        revision_digest = sha256_digest(
            {
            "schema_version": "belllabs.goal-revision.v1",
            "revision_id": initial_revision_id,
            "revision": 1,
            "parent_revision_id": None,
            "envelope_digest": envelope_digest,
            "objective": initial_goal,
            "tactical_changes": (),
            "evidence_refs": (configuration.input_manifest.digest,),
            "unmet_obligations": tuple(sorted(projection.required_obligation_refs)),
            "proposer": "application:goal-launch",
            "deciding_authority": orchestration_authority_ref,
            "applicability": "remaining_run",
            "tactics": (),
            "subgoals": (),
            "coverage_emphasis": (),
            }
        )
        initial_revision = GoalRevision(
            schema_version="belllabs.goal-revision.v1",
            revision_id=initial_revision_id,
            revision=1,
            parent_revision_id=None,
            canonical_digest=revision_digest,
            envelope_digest=envelope_digest,
            objective=initial_goal,
            tactical_changes=(),
            evidence_refs=(configuration.input_manifest.digest,),
            unmet_obligations=tuple(sorted(projection.required_obligation_refs)),
            proposer="application:goal-launch",
            deciding_authority=orchestration_authority_ref,
            applicability="remaining_run",
        )
        return GoalDirectedRunInput(
            run_id=run_id,
            request_scope=request_scope,
            effective_configuration_digest=configuration.digest,
            blueprint_digest=blueprint_ref.digest,
            blueprint=blueprint.model_dump(mode="json"),
            envelope_digest=envelope_digest,
            initial_revision=initial_revision,
            initial_run_version=projection.version,
            execution_epoch=execution_epoch,
            task_timeout_seconds=task_timeout_seconds,
            orchestration_authority_ref=orchestration_authority_ref,
            correlation_id=f"orchestration:{run_id}:epoch:{execution_epoch}",
            baseline_reservation=dict(budget.reservations.get("baseline", {})),
            required_obligation_refs=tuple(sorted(projection.required_obligation_refs)),
            required_output_contract_refs=tuple(
                sorted(configuration.workflow_type.output_contracts)
            ),
            semantic_input_binding_ref=semantic_input_binding_ref,
        )


PreparedWorkflowInput = StageGraphRunInput | GoalDirectedRunInput


class WorkflowLaunchDispatcher:
    """Select an available orchestrator only from the admitted blueprint family."""

    def __init__(
        self,
        *,
        stagegraph: StageGraphLaunchService | None,
        goal_directed: GoalDirectedLaunchService | None,
        run_control: RunControlService,
        control_plane: ControlPlaneService,
    ) -> None:
        self._stagegraph = stagegraph
        self._goal_directed = goal_directed
        self._run_control = run_control
        self._control_plane = control_plane

    async def prepare(
        self,
        request_scope: str,
        run_id: str,
        *,
        initial_goal: str | None = None,
        task_timeout_seconds: int = 300,
        orchestration_authority_ref: str = ORCHESTRATION_AUTHORITY_REF,
        semantic_input_binding_ref: str = "",
    ) -> PreparedWorkflowInput:
        projection = await self._run_control.get_run(request_scope, run_id)
        configuration = await self._control_plane.retrieve_for_admission(
            projection.effective_configuration_digest
        )
        blueprint = configuration.selected_blueprint
        if isinstance(blueprint, StageGraphBlueprint):
            if self._stagegraph is None:
                raise ValueError("StageGraph execution family is unavailable")
            if initial_goal is not None:
                raise ValueError("StageGraph launch does not accept a GoalDirected initial goal")
            return await self._stagegraph.prepare(
                request_scope,
                run_id,
                task_timeout_seconds=task_timeout_seconds,
                orchestration_authority_ref=orchestration_authority_ref,
                semantic_input_binding_ref=semantic_input_binding_ref,
            )
        if isinstance(blueprint, GoalDirectedBlueprint):
            if self._goal_directed is None:
                raise ValueError("GoalDirected execution family is unavailable")
            if initial_goal is None:
                raise ValueError("GoalDirected launch requires a concrete initial goal")
            return await self._goal_directed.prepare(
                request_scope,
                run_id,
                initial_goal=initial_goal,
                task_timeout_seconds=task_timeout_seconds,
                orchestration_authority_ref=orchestration_authority_ref,
                semantic_input_binding_ref=semantic_input_binding_ref,
            )
        raise ValueError(f"unsupported admitted blueprint family: {type(blueprint).__name__}")

    async def prepare_bound(
        self,
        request_scope: str,
        run_id: str,
        *,
        semantic_binding: RunSemanticInputBinding,
        binding_service: RunSemanticInputBindingService,
        initial_goal: str | None = None,
        task_timeout_seconds: int = 300,
        orchestration_authority_ref: str = ORCHESTRATION_AUTHORITY_REF,
    ) -> PreparedWorkflowInput:
        """Verify, freeze, and attach semantic inputs before Temporal submission."""

        prepared = await self.prepare(
            request_scope,
            run_id,
            initial_goal=initial_goal,
            task_timeout_seconds=task_timeout_seconds,
            orchestration_authority_ref=orchestration_authority_ref,
        )
        expected_family = (
            "StageGraph" if isinstance(prepared, StageGraphRunInput) else "GoalDirected"
        )
        if (
            semantic_binding.request_scope != request_scope
            or semantic_binding.run_id != run_id
            or semantic_binding.blueprint_family != expected_family
            or semantic_binding.effective_configuration_digest
            != prepared.effective_configuration_digest
            or semantic_binding.blueprint_digest != prepared.blueprint_digest
        ):
            raise ValueError("semantic input binding does not match the admitted workflow launch")
        binding_ref = await binding_service.freeze(semantic_binding)
        return replace(prepared, semantic_input_binding_ref=binding_ref)


class StageOperationExecutor(Protocol):
    """F4 seam for bounded runtime execution; issue 3 uses explicit fakes."""

    async def execute(self, request: StageOperationRequest) -> StageOperationResult: ...


class WorkflowEvaluator(Protocol):
    """Typed evaluator seam; free text never controls orchestration."""

    async def evaluate(self, request: WorkflowEvaluationRequest) -> WorkflowEvaluationResult: ...


class RunControlLifecycleGateway:
    """Issues orchestration facts only through the authoritative F2 command service."""

    def __init__(
        self,
        service: RunControlService,
        binding_verifier: OrchestrationBindingVerifier,
        actor: ActorContext,
    ) -> None:
        self._service = service
        self._binding_verifier = binding_verifier
        self._actor = actor

    async def execute(self, request: LifecycleCommandRequest) -> LifecycleCommandOutcome:
        if not all(
            (
                request.run_id,
                request.request_scope,
                request.effective_configuration_digest,
                request.idempotency_issuer,
                request.correlation_id,
                request.blueprint_digest,
            )
        ):
            raise ValueError("lifecycle activity request is missing its run-scoped binding")
        action = LIFECYCLE_ACTION_ADAPTER.validate_python(request.action)
        occurred_at = request.occurred_at or datetime.now(UTC)
        await self._binding_verifier.verify(
            request.effective_configuration_digest,
            request.blueprint_digest,
        )
        bound_projection = await self._service.get_run(
            request.request_scope,
            request.run_id,
        )
        if (
            bound_projection.effective_configuration_digest
            != request.effective_configuration_digest
        ):
            raise ValueError(
                "orchestration context does not match the admitted effective configuration"
            )
        result = await self._service.execute(
            LifecycleCommand(
                command_id=request.command_id,
                idempotency_issuer=request.idempotency_issuer,
                request_scope=request.request_scope,
                run_id=request.run_id,
                expected_run_version=request.expected_run_version,
                actor=self._actor,
                action=action,
                reason=request.reason,
                evidence_refs=request.evidence_refs,
                occurred_at=occurred_at,
                correlation_id=request.correlation_id,
                causation_id=request.command_id,
            )
        )
        projection = bound_projection
        if result.status == CommandStatus.ACCEPTED:
            transitions = await self._service.list_transitions(
                request.request_scope,
                request.run_id,
            )
            exact_projection = next(
                (
                    transition.resulting_projection
                    for transition in transitions
                    if transition.resulting_version == result.resulting_run_version
                ),
                None,
            )
            if exact_projection is None:
                raise ValueError("accepted lifecycle command has no exact resulting transition")
            projection = exact_projection
        evidence_payload = [
            item.model_dump(mode="json")
            for item in sorted(
                projection.accepted_obligation_evidence,
                key=lambda item: item.obligation_ref,
            )
        ]
        return LifecycleCommandOutcome(
            accepted=result.status == CommandStatus.ACCEPTED,
            resulting_run_version=result.resulting_run_version,
            phase=result.phase.value,
            reason_code=result.reason_code,
            evidence_frontier_digest=projection.evidence_frontier_digest,
            obligation_revision=projection.obligation_revision,
            accepted_obligation_evidence_digest=sha256_digest(evidence_payload),
            required_obligations_accepted=projection.required_obligation_refs
            <= {item.obligation_ref for item in projection.accepted_obligation_evidence},
            workflow_type_digest=projection.workflow_type_ref.digest,
            terminal_outcome=projection.terminal_outcome,
        )
