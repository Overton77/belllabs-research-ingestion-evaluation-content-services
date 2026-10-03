from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime
from typing import Literal, Protocol

from pydantic import TypeAdapter

from mission_control.application.execution.operations.journaled_operation_execution import (
    operation_effect_claim_id,
)
from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
    operation_settlement_id,
)
from mission_control.application.execution.operations.semantic_operation_bindings import (
    SemanticOperationBindingRepository,
)
from mission_control.application.execution.service import (
    FamilyAdmissionRegistry,
    RunControlService,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.checkpoint_lineage import cognitive_session_namespace
from mission_control.domain.execution.contracts import (
    DeepAgentExecutionBinding,
    OperationAttemptIdentity,
    OperationExecutionBinding,
    OperationExecutionRequest,
    OperationExecutionResult,
    OperationWorkflowRequest,
    PromptSegment,
    PromptTrustClass,
    WorkspaceContract,
    WorkspaceMount,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    workspace_durable_reference,
)
from mission_control.domain.execution.heartbeats import (
    DEFAULT_OPERATION_HEARTBEATS,
    OperationHeartbeatPolicy,
)
from mission_control.domain.graph_runtime.identities import RuntimeUnitIdentity
from mission_control.domain.policies.contracts import (
    ActorContext,
    BudgetState,
    CommandStatus,
    EffectLedgerState,
    LifecycleCommand,
    ReserveBudgetAction,
    RunProjection,
)
from mission_control.domain.programs.contracts import (
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoff,
    GoalRevision,
    GoalVerificationResult,
)
from mission_control.domain.programs.goal_directed_runtime import (
    GoalExecutorObservation,
    GoalFamilyDecisionMutation,
    GoalHandoffDraft,
    GoalOperationDispatch,
    GoalOperationPreparationRequest,
    GoalOperationReconciliationRequest,
    GoalOperationReconciliationResult,
    GoalOperationSettlement,
    GoalVerifierObservation,
)
from mission_control.domain.programs.runtime_units import (
    goal_operation_id,
    goal_runtime_unit,
    goal_unit_workspace_root,
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
        template = self._templates.get((request_scope, semantic_input_binding_ref, operation_role))
        if template is None:
            raise ValueError("GoalDirected operation template is unavailable")
        return template


class GoalOperationSettlementUnavailable(ValueError):
    """The operation has no accepted run-control settlement the family may consume."""


class GoalAdmissionStale(RuntimeError):
    """An operation admission was stale: the run moved past the family's expected version."""

    def __init__(self, *, current_run_version: int, phase: str) -> None:
        super().__init__(
            f"GoalDirected operation admission is stale: run is at version "
            f"{current_run_version} ({phase})"
        )
        self.current_run_version = current_run_version
        self.phase = phase


class GoalOperationSettlementPort(Protocol):
    async def observe(
        self,
        request: GoalOperationReconciliationRequest,
        provider_result: OperationExecutionResult,
    ) -> GoalOperationSettlement:
        """The operation's accepted run-control settlement, or raise."""
        ...

    async def observe_terminal(
        self,
        request: GoalOperationReconciliationRequest,
        provider_result: OperationExecutionResult,
    ) -> GoalOperationSettlement:
        """RRM-008: the settlement of a cancelled or failed operation, or raise."""
        ...


class GoalOperationBindingReader(Protocol):
    async def get_binding_by_id(
        self, binding_id: str, *, request_scope: str
    ) -> OperationExecutionBinding | None: ...


class GoalOperationSettlementReader(Protocol):
    async def get_run(self, request_scope: str, run_id: str) -> RunProjection: ...

    async def get_budget(self, request_scope: str, run_id: str) -> BudgetState: ...

    async def get_effects(self, request_scope: str, run_id: str) -> EffectLedgerState: ...


class RunControlGoalOperationSettlements:
    """Consume the journaled run-control settlement of one GoalDirected operation (RRM-016).

    The operation boundary (`OperationExecutionService` with the journaled coordinator)
    claims, observes and settles the operation exactly once in run control: one usage record
    under the operation's settlement identity against its own reservation, the effect
    settlement, and the accepted settlement evidence bound to the operation binding. The
    family consumes that settlement as StageGraph's `decide_result` does: it verifies the
    authoritative facts and continues from the current run version. It never records the
    operation's usage itself (REQ-CP-RUN-006/009: a budget settles once).
    """

    def __init__(
        self,
        run_control: GoalOperationSettlementReader,
        bindings: GoalOperationBindingReader,
    ) -> None:
        self._run_control = run_control
        self._bindings = bindings

    async def observe(
        self,
        request: GoalOperationReconciliationRequest,
        provider_result: OperationExecutionResult,
    ) -> GoalOperationSettlement:
        return await self._observe(request, provider_result)

    async def observe_terminal(
        self,
        request: GoalOperationReconciliationRequest,
        provider_result: OperationExecutionResult,
    ) -> GoalOperationSettlement:
        """RRM-008: consume the settlement of a `cancelled` or `failed` operation.

        The journaled operation boundary wrote it exactly as a completed one (usage, release,
        effect settlement, accepted evidence); the family verifies the same facts and never
        records usage for the unit itself. A unit whose effect is still pending (an async
        child's unattributed usage) is consumed too: the run's terminal settlement is what
        waits for the reconciliation, not the family's bookkeeping.
        """

        if provider_result.status not in {"cancelled", "failed"}:
            raise GoalOperationSettlementUnavailable(
                "terminal consumption requires a cancelled or failed operation result"
            )
        return await self._observe(request, provider_result)

    async def _observe(
        self,
        request: GoalOperationReconciliationRequest,
        provider_result: OperationExecutionResult,
    ) -> GoalOperationSettlement:
        operation = request.operation_request.operation
        scope = request.request_scope
        run_id = operation.identity.run_id
        binding_id = request.operation_binding_ref
        if provider_result.binding_id != binding_id or operation.request_scope != scope:
            raise GoalOperationSettlementUnavailable(
                "operation result does not belong to the admitted GoalDirected binding"
            )
        settlement_id = operation_settlement_id(binding_id)
        effect_claim_id = operation_effect_claim_id(scope, binding_id)
        reservation_id = operation.budget_reservation_id
        # Review fix 4 (required since the re-check): the binding and reservation the
        # settlement is read under are the stored binding's, the exact admitted intent.
        stored = await self._bindings.get_binding_by_id(binding_id, request_scope=scope)
        if (
            stored is None
            or stored != bind_operation_execution_request(operation)
            or stored.budget_reservation_id != reservation_id
        ):
            raise GoalOperationSettlementUnavailable(
                "operation binding is not the stored binding of the admitted operation"
            )
        # Settlement facts are monotonic: read them first and the run version last, so the
        # version the family continues from is at or after the settlement.
        budget = await self._run_control.get_budget(scope, run_id)
        effects = await self._run_control.get_effects(scope, run_id)
        run = await self._run_control.get_run(scope, run_id)
        usage = budget.usage_records.get(settlement_id)
        if (
            usage is None
            or usage.authority_ref != binding_id
            or usage.reservation_id != reservation_id
        ):
            raise GoalOperationSettlementUnavailable(
                "operation usage is not recorded once by its own run-control settlement"
            )
        if reservation_id in budget.reservations:
            raise GoalOperationSettlementUnavailable(
                "operation reservation is not authoritatively settled"
            )
        if (
            usage.actual_amounts != provider_result.usage.amounts
            or usage.pending_external_amounts != provider_result.usage.pending_external_amounts
        ):
            raise GoalOperationSettlementUnavailable(
                "settled operation usage differs from the operation result"
            )
        claim = effects.claims.get(effect_claim_id)
        if (
            claim is None
            or claim.operation_ref != binding_id
            or claim.reservation_id != reservation_id
        ):
            raise GoalOperationSettlementUnavailable(
                "operation has no journaled run-control effect claim"
            )
        pending_external = any(usage.pending_external_amounts.values())
        if not pending_external and (
            claim.settlement is None or claim.settlement.settlement_id != settlement_id
        ):
            raise GoalOperationSettlementUnavailable(
                "operation effect claim is not settled by its own settlement"
            )
        evidence = [
            item
            for item in run.accepted_operation_settlement_evidence
            if item.settlement_id == settlement_id
        ]
        if len(evidence) != 1 or evidence[0].accepted_by_authority_ref != binding_id:
            raise GoalOperationSettlementUnavailable(
                "operation settlement evidence is not accepted by run control"
            )
        if run.version < operation.run_control_revision:
            raise GoalOperationSettlementUnavailable(
                "run authority is older than the operation binding"
            )
        return GoalOperationSettlement(
            binding_id=binding_id,
            settlement_id=settlement_id,
            effect_claim_id=effect_claim_id,
            reservation_id=reservation_id,
            usage=dict(usage.actual_amounts),
            pending_external_usage=dict(usage.pending_external_amounts),
            settlement_payload_digest=evidence[0].settlement_payload_digest,
            settled_run_version=run.version,
        )


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
        heartbeats: OperationHeartbeatPolicy = DEFAULT_OPERATION_HEARTBEATS,
    ) -> None:
        self._templates = templates
        self._operation_bindings = operation_bindings
        self._run_control = run_control
        self._documents = documents
        self._actor = actor
        # RRM-009 (RRM-008 composition): heartbeat timeout per operation class.
        self._heartbeats = heartbeats

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
            raise ValueError("GoalDirected operation template requires strict structured output")
        # RRM-016: the operation binds the run version its own admission produces (an
        # accepted command advances the version by exactly one), as StageGraph does. The
        # journaled effect claim is made at exactly that revision (REQ-CP-EXEC-014).
        bound_revision = request.expected_run_version + 1
        operation = _instantiate_operation_request(template, request, bound_revision)
        binding = bind_operation_execution_request(operation)

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
            # RRM-016 review fix 2: a re-admission after a stale result is a new command (a
            # stale result is stored under its command identity).
            command_id=(
                f"goal-admission:{mutation.mutation_id}"
                if request.admission_attempt == 1
                else f"goal-admission:{mutation.mutation_id}:attempt:{request.admission_attempt}"
            ),
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
            evidence_refs=(operation_ref, binding.binding_id),
            occurred_at=request.decided_at,
            correlation_id=(
                f"goal:{request.run_id}:iteration:{request.goal_iteration}:{request.operation_role}"
            ),
            causation_id=mutation.mutation_id,
        )
        receipt = await self._run_control.execute_family_admission(command, mutation)
        if receipt.family_receipt is None:
            if receipt.command_result.status == CommandStatus.STALE:
                # A command outside the family moved the run between the family's read of
                # its version and this admission. Nothing was admitted or bound; the family
                # decides (re-admit once at the current version, or enter cancellation).
                raise GoalAdmissionStale(
                    current_run_version=receipt.command_result.resulting_run_version,
                    phase=receipt.command_result.phase.value,
                )
            raise ValueError("GoalDirected operation admission was not accepted")
        if receipt.command_result.resulting_run_version != bound_revision:
            raise ValueError(
                "GoalDirected operation admission did not produce the bound run revision"
            )
        # The binding is persisted only once its admission is accepted (review fix 2): a
        # stale admission leaves no binding behind, so the re-admission binds the same
        # semantic attempt at the new revision without an identity conflict.
        persisted = await self._operation_bindings.create_binding(
            binding,
            request_scope=request.request_scope,
        )
        if persisted != binding:
            raise ValueError("persisted GoalDirected operation binding differs from exact intent")
        workflow_request = OperationWorkflowRequest(
            semantic_attempt_id=operation.identity.semantic_key,
            execution_generation=request.execution_generation,
            operation_kind="bound_operation",
            operation=operation,
            heartbeat_timeout_seconds=self._heartbeats.timeout_for(operation),
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

    def __init__(
        self,
        documents: GoalDirectedDocumentRepository,
        settlements: GoalOperationSettlementPort | None = None,
    ) -> None:
        self._documents = documents
        self._settlements = settlements
        self._execution_adapter = TypeAdapter(GoalExecutorObservation)
        self._verification_adapter = TypeAdapter(GoalVerifierObservation)

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
            or observed.result is None
        ):
            raise ValueError("GoalDirected operation result does not match its durable request")
        if observed.disposition in {"cancelled", "failed"}:
            # RRM-008: a cancelled or failed unit is consumed through its own settlement path;
            # no family document is persisted and `_consume_settlement` is never reached.
            if self._settlements is None:
                raise ValueError(
                    "a cancelled or failed GoalDirected operation requires the settlement port"
                )
            terminal_result = OperationExecutionResult.model_validate(observed.result)
            if (
                terminal_result.status != observed.disposition
                or terminal_result.binding_id != request.operation_binding_ref
            ):
                raise ValueError(
                    "GoalDirected terminal operation result is not the exact bound settlement"
                )
            return GoalOperationReconciliationResult(
                operation_role=request.operation_role,
                detail_ref=f"goal-operation:{observed.disposition}:{request.operation_binding_ref}",
                settlement=await self._settlements.observe_terminal(request, terminal_result),
                operation_disposition=observed.disposition,
            )
        if observed.disposition != "completed":
            raise ValueError("GoalDirected operation result does not match its durable request")
        provider_result = OperationExecutionResult.model_validate(observed.result)
        if (
            provider_result.status != "completed"
            or provider_result.binding_id != request.operation_binding_ref
            or provider_result.semantic_attempt_key != request.operation_request.semantic_attempt_id
            or provider_result.structured_output is None
        ):
            mismatches = []
            if provider_result.status != "completed":
                mismatches.append(f"status={provider_result.status!r}")
            if provider_result.binding_id != request.operation_binding_ref:
                mismatches.append("binding_id")
            if (
                provider_result.semantic_attempt_key
                != request.operation_request.semantic_attempt_id
            ):
                mismatches.append("semantic_attempt_key")
            if provider_result.structured_output is None:
                mismatches.append("structured_output=None")
            raise ValueError(
                "GoalDirected provider result is not the exact completed structured operation: "
                + ", ".join(mismatches)
            )
        # RRM-016: the accepted run-control settlement is verified before any family document
        # is persisted. A composition without one returns no settlement, and the family
        # fails closed on it (no ungoverned usage recording).
        settlement = (
            await self._settlements.observe(request, provider_result)
            if self._settlements is not None
            else None
        )
        provider_payload = provider_result.structured_output
        observed_usage = dict(provider_result.usage.amounts)
        if request.operation_role == "executor":
            parsed = self._execution_adapter.validate_python(provider_payload)
            if parsed.output_contract_ref not in request.required_output_contract_refs:
                raise ValueError("executor result is outside the frozen required output contracts")
            handoff = (
                _bind_handoff(
                    parsed.handoff,
                    claim=request.claim,
                    operation_identity=operation.identity.semantic_key,
                    actual_usage=observed_usage,
                    remaining_iterations=request.remaining_iterations,
                    protected_fact_classes=request.protected_fact_classes,
                    context_selection_policy_ref=request.context_selection_policy_ref or "",
                    context_compaction_policy_ref=request.context_compaction_policy_ref or "",
                    workspace_ref_class=request.workspace_ref_class or "",
                )
                if parsed.handoff is not None
                else None
            )
            if (
                handoff is not None
                and handoff.compaction_status == "failed"
                and request.compaction_failure_action
                in {
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
            result = GoalExecutionResult(
                identity=request.claim.identity,
                disposition=parsed.disposition,
                operation_identity=operation.identity.semantic_key,
                operation_binding_ref=request.operation_binding_ref,
                session_id=operation.session_id or "",
                workspace_id=operation.workspace.workspace_id,
                writable_paths=operation.workspace.exclusive_write_paths,
                output_refs=parsed.output_refs,
                completion_claim=parsed.completion_claim,
                actual_usage=observed_usage,
                blocker_class=parsed.blocker_class,
                authority_breach_ref=parsed.authority_breach_ref,
                hard_budget_exhausted_dimensions=(parsed.hard_budget_exhausted_dimensions),
                irrecoverable_failure_ref=parsed.irrecoverable_failure_ref,
                accepted_fact_refs=parsed.accepted_fact_refs,
                evidence_refs=parsed.evidence_refs,
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
                output_contract_ref=parsed.output_contract_ref,
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
                settlement=settlement,
            )

        observation = self._verification_adapter.validate_python(provider_payload)
        if observation.output_contract_ref not in request.required_output_contract_refs:
            raise ValueError("verifier result is outside the frozen required output contracts")
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
        verification_id = "goal-verification:" + verification_identity.removeprefix("sha256:")
        verification_result = GoalVerificationResult(
            schema_version="belllabs.goal-verification.v1",
            verification_id=verification_id,
            verification_digest="pending",
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
            decision=observation.decision,
            progress_made=observation.progress_made,
            accepted_obligation_refs=observation.accepted_obligation_refs,
            findings=observation.findings,
            evidence_refs=observation.evidence_refs,
            admitted_executor_output_refs=executor_result.output_refs,
            admitted_executor_evidence_refs=executor_result.evidence_refs,
            unmet_obligations=observation.unmet_obligations,
            obligation_applicability=observation.obligation_applicability,
            verification_ref=f"{verification_id}@{verification_identity}",
            stale_frontier_digest=sha256_digest(
                {
                    "output_refs": executor_result.output_refs,
                    "evidence_refs": executor_result.evidence_refs,
                    "effect_frontier_refs": executor_result.effect_frontier_refs,
                    "pending_liability_refs": executor_result.pending_liability_refs,
                }
            ),
            blocker_class=observation.blocker_class,
            authority_breach_ref=observation.authority_breach_ref,
            hard_budget_exhausted_dimensions=(observation.hard_budget_exhausted_dimensions),
            soft_budget_dimensions=observation.soft_budget_dimensions,
            irrecoverable_failure_ref=observation.irrecoverable_failure_ref,
            proposed_revision=observation.proposed_revision,
            scope_expansion_route=observation.scope_expansion_route,
            route_ref=observation.route_ref,
            actual_usage=observed_usage,
            effect_refs=tuple(
                dict.fromkeys(
                    (
                        *observation.effect_refs,
                        *observed.effect_frontier,
                        *(
                            f"async-child:{child_id}"
                            for child_id in observed.active_async_child_ids
                        ),
                    )
                )
            ),
            output_contract_ref=observation.output_contract_ref,
        )
        verification_payload = asdict(verification_result)
        verification_payload.pop("verification_digest")
        verification_result = replace(
            verification_result,
            verification_digest=sha256_digest(verification_payload),
        )
        detail_ref = await self._documents.persist_verification(
            request.request_scope,
            request.claim.identity.iteration.run_id,
            request.goal_revision_id,
            verification_result,
            request.recorded_at,
        )
        return GoalOperationReconciliationResult(
            operation_role="verifier",
            verification_result=verification_result,
            detail_ref=detail_ref,
            settlement=settlement,
        )


def _instantiate_operation_request(
    template: OperationExecutionRequest,
    request: GoalOperationPreparationRequest,
    bound_revision: int,
) -> OperationExecutionRequest:
    operation_id = goal_operation_id(request.goal_iteration, request.operation_role)
    identity = OperationAttemptIdentity(
        run_id=request.run_id,
        operation_id=operation_id,
        operation_attempt=request.operation_attempt,
    )
    runtime_unit = _runtime_unit_for(request, identity)
    workspace = _workspace_for(template.workspace, request, runtime_unit)
    prompt_segments = _prompt_segments(template.prompt_segments, request)
    deep_binding = _deep_binding_for(
        template.deep_agent_binding,
        request=request,
        identity=identity,
        workspace=workspace,
        runtime_unit=runtime_unit,
        bound_revision=bound_revision,
    )
    payload = template.model_dump(mode="python")
    payload.update(
        {
            "identity": identity,
            "request_scope": request.request_scope,
            "effective_configuration_digest": request.effective_configuration_digest,
            "run_control_revision": bound_revision,
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
            "runtime_unit": runtime_unit,
        }
    )
    return OperationExecutionRequest.model_validate(payload)


def _runtime_unit_for(
    request: GoalOperationPreparationRequest,
    identity: OperationAttemptIdentity,
) -> RuntimeUnitIdentity:
    """REQ-CP-EXEC-013: iteration, revision, role, agent run, and session generation."""

    if request.agent_run is None or request.session_generation is None:
        raise ValueError(
            "GoalDirected operation preparation requires its agent run and session generation"
        )
    return goal_runtime_unit(
        request_scope=request.request_scope,
        run_id=request.run_id,
        execution_epoch=request.execution_epoch,
        operation_id=identity.operation_id,
        operation_attempt=identity.operation_attempt,
        goal_iteration=request.goal_iteration,
        goal_revision_id=request.goal_revision_id,
        operation_role=request.operation_role,
        agent_run=request.agent_run,
        session_generation=request.session_generation,
    )


def _bind_handoff(
    handoff: GoalHandoffDraft,
    *,
    claim: GoalExecutionClaim,
    operation_identity: str,
    actual_usage: dict[str, int],
    remaining_iterations: int,
    protected_fact_classes: tuple[str, ...],
    context_selection_policy_ref: str,
    context_compaction_policy_ref: str,
    workspace_ref_class: str,
) -> GoalHandoff:
    provider_content = handoff.model_dump(mode="python")
    identity_digest = sha256_digest(
        {
            "agent_run_identity": claim.identity.semantic_key,
            "operation_identity": operation_identity,
            "content": provider_content,
        }
    )
    provider_content.pop("schema_version")
    bound = GoalHandoff(
        schema_version="belllabs.goal-handoff.v1",
        handoff_id=f"goal-handoff:{identity_digest.removeprefix('sha256:')}",
        handoff_digest="pending",
        run_id=claim.identity.iteration.run_id,
        execution_epoch=claim.identity.iteration.execution_epoch,
        goal_revision_id=claim.identity.iteration.goal_revision_id,
        source_iteration=claim.identity.iteration,
        consumed_budget=actual_usage,
        reserved_budget=claim.reservation,
        remaining_budget={
            dimension: max(limit - actual_usage.get(dimension, 0), 0)
            for dimension, limit in claim.reservation.items()
        },
        remaining_iterations=remaining_iterations,
        protected_context_facts=tuple(
            (fact_class, _protected_context_value(fact_class, claim))
            for fact_class in protected_fact_classes
        ),
        context_selection_policy_ref=context_selection_policy_ref,
        context_compaction_policy_ref=context_compaction_policy_ref,
        workspace_refs=(f"{workspace_ref_class}:{claim.workspace_namespace}",),
        snapshot_refs=(),
        source_document_digests=(claim.goal_revision_digest,),
        source_binding_digests=(sha256_digest(operation_identity),),
        **provider_content,
    )
    digest_payload = asdict(bound)
    digest_payload.pop("handoff_digest")
    return replace(bound, handoff_digest=sha256_digest(digest_payload))


def _protected_context_value(fact_class: str, claim: GoalExecutionClaim) -> str:
    values = {
        "objective": claim.objective,
        "envelope_digest": claim.envelope_digest,
        "goal_revision_digest": claim.goal_revision_digest,
        "operation_class": claim.operation_class,
    }
    try:
        return values[fact_class]
    except KeyError as error:
        raise ValueError(
            f"unsupported protected GoalDirected context fact class: {fact_class}"
        ) from error


def _recover_handoff_compaction(
    handoff: GoalHandoff,
    *,
    action: Literal["retry", "fresh_from_handoff"],
) -> GoalHandoff:
    decision_ref = "goal-compaction:" + sha256_digest(
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
    runtime_unit: RuntimeUnitIdentity,
) -> WorkspaceContract:
    role_root = goal_unit_workspace_root(runtime_unit)
    if role_root is None:  # pragma: no cover - `_runtime_unit_for` always builds a goal unit
        raise ValueError("GoalDirected workspace requires a GoalDirected runtime unit")
    if not template.slot_bindings:
        # RRM-016 review fix 3: REQ-CP-DA-013 requires exact exclusive writable slots, and
        # the run-control authority admits a GoalDirected unit only with the compiled slots
        # rebased under its role root. A template without them fails closed here.
        raise ValueError("GoalDirected operation template requires compiled workspace slots")
    payload = template.model_dump(mode="python")
    namespace_id = f"run/{request.run_id}"
    read_mounts: tuple[WorkspaceMount, ...] = ()
    if request.read_workspace_id is not None:
        read_mounts = (
            WorkspaceMount(
                logical_path=f"{role_root}/input",
                durable_ref=workspace_durable_reference(namespace_id, request.read_workspace_id),
                content_digest=sha256_digest(
                    {
                        "workspace_id": request.read_workspace_id,
                        "input_refs": request.verifier_input_refs,
                    }
                ),
            ),
        )
    # RRM-016 / REQ-CP-DA-013: bind the exact compiled slots under the role-scoped root,
    # owned by this iteration's executor or verifier. The run-control authority recomputes
    # the root from the unit identity and verifies the exact slot set.
    owner = WorkspaceOwner(
        kind=(
            WorkspaceOwnerKind.ITERATION
            if request.operation_role == "executor"
            else WorkspaceOwnerKind.EVALUATOR
        ),
        owner_id=goal_operation_id(request.goal_iteration, request.operation_role),
    )
    slot_bindings = tuple(
        slot.model_copy(update={"logical_path": f"{role_root}{slot.logical_path}", "owner": owner})
        for slot in template.slot_bindings
    )
    writable = tuple(
        slot.logical_path for slot in slot_bindings if slot.access == "exclusive_write"
    )
    payload.update(
        {
            "namespace_id": namespace_id,
            "workspace_id": request.workspace_id,
            "slot_bindings": slot_bindings,
            "exclusive_write_paths": writable,
            "read_mounts": read_mounts,
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
            f"goal-context:{request.run_id}:{request.goal_iteration}:{request.operation_role}"
        ),
        source_revision=request.goal_iteration,
        # RRM-016: the goal context (revision, iteration, role, handoff and verifier input
        # refs) is run state derived partly from model output (the handoff and the
        # executor's output refs). It is never a configured prompt source and must not reach
        # the system prompt. SPEC-CP-DEFINITIONS (Security) treats prompt text and retrieved
        # content as untrusted inputs; SPEC-CP-DEEP-AGENT-RUNTIME invariant 5: model output
        # never grants authority; SPEC-BP-GOAL-DIRECTED invariant 1 and Security: goal text
        # and context cannot expand the envelope or grant capabilities. So the segment is
        # admitted under the most restrictive non-privileged trust class.
        trust_class=PromptTrustClass.UNTRUSTED_CONTENT,
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
    runtime_unit: RuntimeUnitIdentity,
    bound_revision: int,
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
            "control_revision": bound_revision,
            "workspace": workspace,
            "reservation_id": request.reservation_id,
            "runtime_unit": runtime_unit,
            "cognitive_session_namespace": cognitive_session_namespace(
                runtime_unit, request.execution_generation
            ),
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
    "GoalAdmissionStale",
    "GoalDirectedOperationResultService",
    "GoalOperationBindingReader",
    "GoalOperationSettlementPort",
    "GoalOperationSettlementReader",
    "GoalOperationSettlementUnavailable",
    "GoalOperationTemplateProvider",
    "GoalOperationTemplateRepository",
    "InMemoryGoalOperationTemplateRepository",
    "RunControlGoalOperationSettlements",
    "configure_goal_directed_family_admissions",
    "document_payload",
]
