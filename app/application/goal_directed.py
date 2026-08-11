from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime
from typing import Literal, Protocol

from pydantic import TypeAdapter

from app.application.operation_execution import bind_operation_execution_request
from app.application.run_control import (
    FamilyAdmissionRegistry,
    RunControlService,
)
from app.application.semantic_operation_bindings import SemanticOperationBindingRepository
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    DeepAgentExecutionBinding,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationWorkflowRequest,
    PromptSegment,
    WorkspaceContract,
)
from app.domain.orchestration.contracts import (
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoff,
    GoalRevision,
    GoalVerificationResult,
)
from app.domain.orchestration.goal_directed_runtime import (
    GoalFamilyDecisionMutation,
    GoalOperationDispatch,
    GoalOperationPreparationRequest,
    GoalOperationReconciliationRequest,
    GoalOperationReconciliationResult,
)
from app.domain.run_control.contracts import (
    ActorContext,
    LifecycleCommand,
    ReserveBudgetAction,
)


class GoalDirectedDocumentRepository(Protocol):
    async def persist_revision(
        self, request_scope: str, run_id: str, revision: GoalRevision, recorded_at: datetime
    ) -> str: ...

    async def persist_iteration(
        self,
        request_scope: str,
        result: GoalExecutionResult,
        goal_revision_id: str,
        recorded_at: datetime,
    ) -> str: ...

    async def persist_handoff(
        self, request_scope: str, handoff: GoalHandoff, recorded_at: datetime
    ) -> str: ...

    async def persist_verification(
        self,
        request_scope: str,
        run_id: str,
        goal_revision_id: str,
        verification: GoalVerificationResult,
        recorded_at: datetime,
    ) -> str: ...


class GoalOperationTemplateProvider(Protocol):
    async def get_template(
        self,
        *,
        semantic_input_binding_ref: str,
        operation_role: str,
        request_scope: str,
        run_id: str,
    ) -> OperationExecutionRequest: ...


class GoalOperationTemplateRepository(GoalOperationTemplateProvider, Protocol):
    async def persist_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
        executor: OperationExecutionRequest,
        verifier: OperationExecutionRequest,
        recorded_at: datetime,
    ) -> None: ...


class InMemoryGoalOperationTemplateRepository:
    """Deterministic test adapter for immutable per-run operation templates."""

    def __init__(self) -> None:
        self._templates: dict[
            tuple[str, str, str],
            OperationExecutionRequest,
        ] = {}

    async def persist_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
        executor: OperationExecutionRequest,
        verifier: OperationExecutionRequest,
        recorded_at: datetime,
    ) -> None:
        del recorded_at
        for operation_role, template in (("executor", executor), ("verifier", verifier)):
            key = (request_scope, semantic_input_binding_ref, operation_role)
            prior = self._templates.get(key)
            if prior is not None and prior != template:
                raise ValueError("GoalDirected operation template identity conflict")
            self._templates[key] = template

    async def get_template(
        self,
        *,
        semantic_input_binding_ref: str,
        operation_role: str,
        request_scope: str,
        run_id: str,
    ) -> OperationExecutionRequest:
        del run_id
        template = self._templates.get(
            (request_scope, semantic_input_binding_ref, operation_role)
        )
        if template is None:
            raise ValueError("GoalDirected operation template is unavailable")
        return template


def configure_goal_directed_family_admissions(registry: FamilyAdmissionRegistry) -> None:
    """Register the one exact GoalDirected mutation shape on the frozen generic seam."""

    registry.register(
        GoalFamilyDecisionMutation,
        family_kind="goal_directed",
        mutation_kind="decision",
        required_permission="workflow_run.goal_directed",
        allowed_action_kinds=frozenset({"reserve_budget"}),
    )


class GoalDirectedOperationPreparationService:
    """Prepare and atomically admit one exact executor or verifier operation."""

    def __init__(
        self,
        *,
        templates: GoalOperationTemplateProvider,
        operation_bindings: SemanticOperationBindingRepository,
        run_control: RunControlService,
        documents: GoalDirectedDocumentRepository,
        actor: ActorContext,
    ) -> None:
        self._templates = templates
        self._operation_bindings = operation_bindings
        self._run_control = run_control
        self._documents = documents
        self._actor = actor

    async def prepare(self, request: GoalOperationPreparationRequest) -> GoalOperationDispatch:
        if request.operation_role == "executor":
            await self._documents.persist_revision(
                request.request_scope,
                request.run_id,
                request.goal_revision,
                request.decided_at,
            )
        template = await self._templates.get_template(
            semantic_input_binding_ref=request.semantic_input_binding_ref,
            operation_role=request.operation_role,
            request_scope=request.request_scope,
            run_id=request.run_id,
        )
        if template.output_schema is None or not template.output_schema.strict:
            raise ValueError(
                "GoalDirected operation template requires strict structured output"
            )
        operation = _instantiate_operation_request(template, request)
        binding = bind_operation_execution_request(operation)
        persisted = await self._operation_bindings.create_binding(
            binding,
            request_scope=request.request_scope,
        )
        if persisted != binding:
            raise ValueError("persisted GoalDirected operation binding differs from exact intent")

        operation_request_digest = sha256_digest(operation)
        operation_ref = f"goal-operation:{operation_request_digest.removeprefix('sha256:')}"
        mutation = GoalFamilyDecisionMutation(
            mutation_id=(
                f"{request.run_id}.goal.{request.goal_iteration}."
                f"{request.operation_role}.{request.operation_attempt}"
            ),
            request_scope=request.request_scope,
            run_id=request.run_id,
            expected_family_version=request.expected_family_version,
            exact_operation_request_ref=operation_ref,
            decided_at=request.decided_at,
            goal_revision_id=request.goal_revision_id,
            goal_revision_digest=request.goal_revision_digest,
            goal_iteration=request.goal_iteration,
            operation_role=request.operation_role,
            operation_request_digest=operation_request_digest,
            semantic_input_binding_ref=request.semantic_input_binding_ref,
            handoff_ref=request.handoff_ref,
            convergence_action="continue",
        )
        command = LifecycleCommand(
            command_id=f"goal-admission:{mutation.mutation_id}",
            idempotency_issuer="goal-directed-worker",
            request_scope=request.request_scope,
            run_id=request.run_id,
            expected_run_version=request.expected_run_version,
            actor=self._actor,
            action=ReserveBudgetAction(
                reservation_id=request.reservation_id,
                amounts=request.reservation,
            ),
            reason=f"Atomically admit GoalDirected {request.operation_role} operation",
            evidence_refs=(operation_ref, persisted.binding_id),
            occurred_at=request.decided_at,
            correlation_id=(
                f"goal:{request.run_id}:iteration:{request.goal_iteration}:"
                f"{request.operation_role}"
            ),
            causation_id=mutation.mutation_id,
        )
        receipt = await self._run_control.execute_family_admission(command, mutation)
        if receipt.family_receipt is None:
            raise ValueError("GoalDirected operation admission was not accepted")
        workflow_request = OperationWorkflowRequest(
            semantic_attempt_id=operation.identity.semantic_key,
            execution_generation=request.execution_generation,
            operation_kind="bound_operation",
            operation=operation,
        )
        return GoalOperationDispatch(
            workflow_request=workflow_request,
            operation_binding_ref=persisted.binding_id,
            operation_request_digest=operation_request_digest,
            resulting_run_version=receipt.command_result.resulting_run_version,
            resulting_family_version=receipt.family_receipt.family_version,
        )


class GoalDirectedOperationResultService:
    """Validate provider output against exact operation intent and persist immutable detail."""

    def __init__(self, documents: GoalDirectedDocumentRepository) -> None:
        self._documents = documents
        self._execution_adapter = TypeAdapter(GoalExecutionResult)
        self._verification_adapter = TypeAdapter(GoalVerificationResult)

    async def reconcile(
        self, request: GoalOperationReconciliationRequest
    ) -> GoalOperationReconciliationResult:
        operation = request.operation_request.operation
        if operation.output_schema is None or not operation.output_schema.strict:
            raise ValueError(
                "GoalDirected operation requires an exact strict structured-output binding"
            )
        observed = request.operation_result
        if (
            observed.semantic_attempt_id != request.operation_request.semantic_attempt_id
            or observed.execution_generation != request.operation_request.execution_generation
            or observed.disposition != "completed"
            or observed.result is None
        ):
            raise ValueError("GoalDirected operation result does not match its durable request")
        if request.operation_role == "executor":
            parsed = self._execution_adapter.validate_python(observed.result)
            if parsed.output_contract_ref not in request.required_output_contract_refs:
                raise ValueError(
                    "executor result is outside the frozen required output contracts"
                )
            handoff = (
                _bind_handoff(
                    parsed.handoff,
                    claim=request.claim,
                    operation_identity=operation.identity.semantic_key,
                )
                if parsed.handoff is not None
                else None
            )
            if (
                handoff is not None
                and handoff.compaction_status == "failed"
                and request.compaction_failure_action in {
                    "retry",
                    "fresh_from_handoff",
                }
            ):
                handoff = _recover_handoff_compaction(
                    handoff,
                    action=(
                        "retry"
                        if request.compaction_failure_action == "retry"
                        else "fresh_from_handoff"
                    ),
                )
            result = replace(
                parsed,
                identity=request.claim.identity,
                operation_identity=operation.identity.semantic_key,
                operation_binding_ref=request.operation_binding_ref,
                session_id=operation.session_id or "",
                workspace_id=operation.workspace.workspace_id,
                writable_paths=operation.workspace.exclusive_write_paths,
                effect_frontier_refs=tuple(
                    dict.fromkeys((*parsed.effect_frontier_refs, *observed.effect_frontier))
                ),
                pending_liability_refs=tuple(
                    dict.fromkeys(
                        (
                            *parsed.pending_liability_refs,
                            *(
                                f"async-child:{child_id}"
                                for child_id in observed.active_async_child_ids
                            ),
                        )
                    )
                ),
                handoff=handoff,
            )
            detail_ref = await self._documents.persist_iteration(
                request.request_scope,
                result,
                request.goal_revision_id,
                request.recorded_at,
            )
            if result.handoff is not None:
                await self._documents.persist_handoff(
                    request.request_scope,
                    result.handoff,
                    request.recorded_at,
                )
            return GoalOperationReconciliationResult(
                operation_role="executor",
                execution_result=result,
                detail_ref=detail_ref,
            )

        verification = self._verification_adapter.validate_python(observed.result)
        if verification.output_contract_ref not in request.required_output_contract_refs:
            raise ValueError(
                "verifier result is outside the frozen required output contracts"
            )
        executor_result = request.executor_result
        if executor_result is None:
            raise ValueError("verifier reconciliation omitted the admitted executor result")
        policy_binding_ref = request.verifier_policy_binding_ref
        rubric_ref = request.verifier_rubric_ref
        rubric_version = request.verifier_rubric_version
        acceptance_contract_ref = request.acceptance_contract_ref
        acceptance_version = request.acceptance_version
        if (
            policy_binding_ref is None
            or rubric_ref is None
            or rubric_version is None
            or acceptance_contract_ref is None
            or acceptance_version is None
        ):
            raise ValueError("verifier reconciliation omitted frozen policy authority")
        verification_identity = sha256_digest(
            {
                "executor_identity": request.claim.identity.semantic_key,
                "verifier_operation_identity": operation.identity.semantic_key,
                "operation_result": observed.model_dump(mode="json"),
            }
        )
        verification_id = (
            "goal-verification:" + verification_identity.removeprefix("sha256:")
        )
        verification = replace(
            verification,
            verification_id=verification_id,
            executor_identity=request.claim.identity,
            verifier_operation_identity=operation.identity.semantic_key,
            verifier_binding_ref=request.operation_binding_ref,
            verifier_policy_binding_ref=policy_binding_ref,
            verifier_session_id=operation.session_id or "",
            verifier_workspace_id=operation.workspace.workspace_id,
            verifier_writable_paths=operation.workspace.exclusive_write_paths,
            rubric_ref=rubric_ref,
            rubric_version=rubric_version,
            acceptance_contract_ref=acceptance_contract_ref,
            acceptance_version=acceptance_version,
            admitted_executor_output_refs=executor_result.output_refs,
            admitted_executor_evidence_refs=executor_result.evidence_refs,
            verification_ref=f"{verification_id}@{verification_identity}",
            stale_frontier_digest=sha256_digest(
                {
                    "output_refs": executor_result.output_refs,
                    "evidence_refs": executor_result.evidence_refs,
                    "effect_frontier_refs": executor_result.effect_frontier_refs,
                    "pending_liability_refs": executor_result.pending_liability_refs,
                }
            ),
            effect_refs=tuple(
                dict.fromkeys(
                    (
                        *verification.effect_refs,
                        *observed.effect_frontier,
                        *(
                            f"async-child:{child_id}"
                            for child_id in observed.active_async_child_ids
                        ),
                    )
                )
            ),
        )
        verification_payload = asdict(verification)
        verification_payload.pop("verification_digest")
        verification = replace(
            verification,
            verification_digest=sha256_digest(verification_payload),
        )
        detail_ref = await self._documents.persist_verification(
            request.request_scope,
            request.claim.identity.iteration.run_id,
            request.goal_revision_id,
            verification,
            request.recorded_at,
        )
        return GoalOperationReconciliationResult(
            operation_role="verifier",
            verification_result=verification,
            detail_ref=detail_ref,
        )


def _instantiate_operation_request(
    template: OperationExecutionRequest,
    request: GoalOperationPreparationRequest,
) -> OperationExecutionRequest:
    operation_id = (
        f"goal-iteration/{request.goal_iteration}/{request.operation_role}"
    )
    identity = OperationAttemptIdentity(
        run_id=request.run_id,
        operation_id=operation_id,
        operation_attempt=request.operation_attempt,
    )
    workspace = _workspace_for(template.workspace, request, operation_id)
    prompt_segments = _prompt_segments(template.prompt_segments, request)
    deep_binding = _deep_binding_for(
        template.deep_agent_binding,
        request=request,
        identity=identity,
        workspace=workspace,
    )
    payload = template.model_dump(mode="python")
    payload.update(
        {
            "identity": identity,
            "request_scope": request.request_scope,
            "effective_configuration_digest": request.effective_configuration_digest,
            "run_control_revision": request.expected_run_version,
            "prompt_segments": prompt_segments,
            "session_id": request.session_id,
            "workspace": workspace,
            "deep_agent_binding": deep_binding,
            "budget_reservation_id": request.reservation_id,
            "budget_limits": request.reservation,
            "prior_binding_id": None,
            "requested_at": request.decided_at,
            "idempotency_key": (
                f"goal:{identity.semantic_key}:generation:{request.execution_generation}"
            ),
        }
    )
    return OperationExecutionRequest.model_validate(payload)


def _bind_handoff(
    handoff: GoalHandoff,
    *,
    claim: GoalExecutionClaim,
    operation_identity: str,
) -> GoalHandoff:
    provider_content = asdict(handoff)
    for field_name in (
        "handoff_id",
        "handoff_digest",
        "run_id",
        "execution_epoch",
        "goal_revision_id",
        "source_iteration",
    ):
        provider_content.pop(field_name)
    identity_digest = sha256_digest(
        {
            "agent_run_identity": claim.identity.semantic_key,
            "operation_identity": operation_identity,
            "content": provider_content,
        }
    )
    bound = replace(
        handoff,
        handoff_id=f"goal-handoff:{identity_digest.removeprefix('sha256:')}",
        handoff_digest="pending",
        run_id=claim.identity.iteration.run_id,
        execution_epoch=claim.identity.iteration.execution_epoch,
        goal_revision_id=claim.identity.iteration.goal_revision_id,
        source_iteration=claim.identity.iteration,
    )
    digest_payload = asdict(bound)
    digest_payload.pop("handoff_digest")
    return replace(bound, handoff_digest=sha256_digest(digest_payload))


def _recover_handoff_compaction(
    handoff: GoalHandoff,
    *,
    action: Literal["retry", "fresh_from_handoff"],
) -> GoalHandoff:
    decision_ref = (
        "goal-compaction:"
        + sha256_digest(
            {
                "prior_handoff_digest": handoff.handoff_digest,
                "prior_decision_ref": handoff.compaction_decision_ref,
                "failure_ref": handoff.compaction_failure_ref,
                "action": action,
                "attempt": handoff.compaction_attempt + 1,
                "selected_context_refs": handoff.context_selection_refs,
                "protected_context_facts": handoff.protected_context_facts,
                "source_document_digests": handoff.source_document_digests,
                "source_binding_digests": handoff.source_binding_digests,
            }
        ).removeprefix("sha256:")
    )
    recovered = replace(
        handoff,
        handoff_digest="pending",
        compaction_decision_ref=decision_ref,
        compaction_status="accepted",
        compaction_attempt=handoff.compaction_attempt + 1,
        compaction_failure_ref="",
    )
    digest_payload = asdict(recovered)
    digest_payload.pop("handoff_digest")
    return replace(recovered, handoff_digest=sha256_digest(digest_payload))


def _workspace_for(
    template: WorkspaceContract,
    request: GoalOperationPreparationRequest,
    operation_id: str,
) -> WorkspaceContract:
    role_root = f"/goal/{request.goal_iteration}/{request.operation_role}"
    payload = template.model_dump(mode="python")
    payload.update(
        {
            "namespace_id": f"run/{request.run_id}",
            "workspace_id": request.workspace_id,
            "slot_bindings": (),
            "exclusive_write_paths": (f"{role_root}/work",),
            "restore_snapshot_id": None,
        }
    )
    return WorkspaceContract.model_validate(payload)


def _prompt_segments(
    template: tuple[PromptSegment, ...],
    request: GoalOperationPreparationRequest,
) -> tuple[PromptSegment, ...]:
    continuation = {
        "goal_revision_id": request.goal_revision_id,
        "goal_revision_digest": request.goal_revision_digest,
        "goal_iteration": request.goal_iteration,
        "operation_role": request.operation_role,
        "handoff_ref": request.handoff_ref,
        "handoff": (asdict(request.handoff) if request.handoff is not None else None),
        "verifier_input_refs": request.verifier_input_refs,
    }
    content = str(continuation)
    segment = PromptSegment(
        source_ref=(
            f"goal-context:{request.run_id}:{request.goal_iteration}:"
            f"{request.operation_role}"
        ),
        source_revision=request.goal_iteration,
        trust_class="authored_instruction",
        content=content,
        rendered_digest=sha256_digest(content),
    )
    return (*template, segment)


def _deep_binding_for(
    template: DeepAgentExecutionBinding | None,
    *,
    request: GoalOperationPreparationRequest,
    identity: OperationAttemptIdentity,
    workspace: WorkspaceContract,
) -> DeepAgentExecutionBinding | None:
    if template is None:
        return None
    values = template.model_dump(mode="python", exclude={"binding_digest"})
    values.update(
        {
            "binding_id": sha256_digest(
                {
                    "template": template.binding_id,
                    "semantic_attempt": identity.semantic_key,
                    "generation": request.execution_generation,
                }
            ),
            "run_id": request.run_id,
            "operation_id": identity.operation_id,
            "operation_attempt": identity.operation_attempt,
            "execution_generation": request.execution_generation,
            "erc_digest": request.effective_configuration_digest,
            "control_revision": request.expected_run_version,
            "workspace": workspace,
            "reservation_id": request.reservation_id,
        }
    )
    return DeepAgentExecutionBinding.create(**values)


def document_payload(
    value: GoalRevision | GoalExecutionResult | GoalHandoff | GoalVerificationResult,
) -> dict[str, object]:
    """Canonical dataclass payload helper used by family persistence adapters."""

    payload = asdict(value)
    return {str(key): item for key, item in payload.items()}


__all__ = [
    "GoalDirectedDocumentRepository",
    "GoalDirectedOperationPreparationService",
    "GoalDirectedOperationResultService",
    "GoalOperationTemplateProvider",
    "GoalOperationTemplateRepository",
    "InMemoryGoalOperationTemplateRepository",
    "configure_goal_directed_family_admissions",
    "document_payload",
]
