from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from enum import StrEnum
from typing import Any, Protocol
from uuid import uuid4

from app.application.orchestration.orchestration_binding_repository import (
    RunSemanticInputBindingService,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    EffectiveRunConfiguration,
    GoalDirectedBlueprint,
    StageGraphBlueprint,
)
from app.domain.coordinator.launch import (
    AdmissionPreviewDecision,
    BlueprintFamily,
    LaunchIdempotencyConflict,
    LaunchRequestContext,
    LaunchTicketNotFound,
    LaunchTicketState,
    LaunchTicketUnavailable,
    PreparedLaunchTicket,
    PublicPreparedLaunchTicket,
    SemanticBindingPlan,
    WorkflowLaunchHandle,
    WorkflowLaunchProposal,
    WorkflowSubmission,
    is_expired,
    validate_launch_context,
)
from app.domain.graph_runtime.definitions import RunPlanV3, UnavailableStageSurface
from app.domain.orchestration.bindings import RunSemanticInputBinding
from app.domain.orchestration.contracts import GoalDirectedRunInput, StageGraphRunInput
from app.domain.run_control.contracts import (
    AdmissionDecision,
    DecisionStatus,
    RunRequest,
)


class CompilerPort(Protocol):
    async def compile(self, invocation: Any) -> EffectiveRunConfiguration: ...


class AdmissionPreviewPort(Protocol):
    async def preview(self, request: RunRequest) -> AdmissionPreviewDecision: ...


class AdmissionPort(Protocol):
    async def admit(self, request: RunRequest) -> AdmissionDecision: ...


class LaunchDispatcherPort(Protocol):
    async def prepare(
        self,
        request_scope: str,
        run_id: str,
        *,
        initial_goal: str | None = None,
        task_timeout_seconds: int = 300,
        orchestration_authority_ref: str = "orchestration-authority",
    ) -> Any: ...


class WorkflowSubmissionPort(Protocol):
    async def submit(
        self,
        workflow_input: Any,
        *,
        workflow_id: str,
        blueprint_family: BlueprintFamily,
    ) -> WorkflowSubmission: ...


class SemanticBindingProvider(Protocol):
    """Author family-neutral frozen plans and exact post-admission run bindings."""

    async def prepare(
        self,
        proposal: WorkflowLaunchProposal,
        configuration: EffectiveRunConfiguration,
    ) -> SemanticBindingPlan: ...

    async def author(
        self,
        plan: SemanticBindingPlan,
        ticket: PreparedLaunchTicket,
        *,
        run_id: str,
    ) -> RunSemanticInputBinding: ...


@dataclass(frozen=True)
class RuntimePlanPreparation:
    """Exact Stage-1 structural result to freeze with a coordinator launch ticket."""

    run_plan: RunPlanV3
    unavailable_surfaces: tuple[UnavailableStageSurface, ...] = ()


class RuntimePlanPreparer(Protocol):
    async def prepare_runtime_plan(
        self,
        proposal: WorkflowLaunchProposal,
        configuration: EffectiveRunConfiguration,
        semantic_plan: SemanticBindingPlan,
    ) -> RuntimePlanPreparation: ...


class RuntimePlanRequirement(StrEnum):
    """Whether a launch path accepts legacy tickets or requires RunPlanV3."""

    LEGACY_COMPATIBILITY = "legacy_compatibility"
    REQUIRE_RUN_PLAN_V3 = "require_run_plan_v3"


@dataclass(frozen=True)
class UnavailableRuntimePlanPreparer:
    """Explicit production placeholder until exact Stage-1 assets are published."""

    reason: str = "no exact RunPlanV3 authoring provider is configured"

    async def prepare_runtime_plan(
        self,
        proposal: WorkflowLaunchProposal,
        configuration: EffectiveRunConfiguration,
        semantic_plan: SemanticBindingPlan,
    ) -> RuntimePlanPreparation:
        del proposal, configuration, semantic_plan
        raise LaunchTicketUnavailable(self.reason)


class BoundLaunchDispatcherPort(LaunchDispatcherPort, Protocol):
    async def prepare_bound(
        self,
        request_scope: str,
        run_id: str,
        *,
        semantic_binding: RunSemanticInputBinding,
        binding_service: RunSemanticInputBindingService,
        initial_goal: str | None = None,
        task_timeout_seconds: int = 300,
        orchestration_authority_ref: str = "orchestration-authority",
    ) -> Any: ...


class LaunchTicketRepository(Protocol):
    async def create(self, ticket: PreparedLaunchTicket) -> PreparedLaunchTicket: ...

    async def get(
        self,
        ticket_id: str,
        *,
        request_scope: str,
    ) -> PreparedLaunchTicket | None: ...

    async def expire(
        self,
        ticket_id: str,
        *,
        request_scope: str,
        observed_at: Any,
    ) -> PreparedLaunchTicket: ...

    async def invalidate(
        self,
        ticket_id: str,
        *,
        request_scope: str,
        reason: str,
    ) -> PreparedLaunchTicket: ...

    async def consume(
        self,
        ticket_id: str,
        *,
        request_scope: str,
        run_id: str,
        consumed_at: Any,
    ) -> PreparedLaunchTicket: ...


class InMemoryLaunchTicketRepository:
    def __init__(self) -> None:
        self._by_id: dict[str, PreparedLaunchTicket] = {}
        self._by_identity: dict[tuple[str, str, str, str], str] = {}

    @staticmethod
    def _identity(ticket: PreparedLaunchTicket) -> tuple[str, str, str, str]:
        return (
            ticket.tenant_scope,
            ticket.caller_id,
            ticket.idempotency_issuer,
            ticket.idempotency_key,
        )

    async def create(self, ticket: PreparedLaunchTicket) -> PreparedLaunchTicket:
        identity = self._identity(ticket)
        prior_id = self._by_identity.get(identity)
        if prior_id is not None:
            prior = self._by_id[prior_id]
            if (
                prior.proposal_digest != ticket.proposal_digest
                or prior.semantic_binding_plan_digest != ticket.semantic_binding_plan_digest
                or _runtime_plan_digest(prior) != _runtime_plan_digest(ticket)
            ):
                raise LaunchIdempotencyConflict(
                    "launch idempotency identity was reused with a changed proposal "
                    "semantic binding plan, or runtime plan"
                )
            return prior
        self._by_id[ticket.ticket_id] = ticket
        self._by_identity[identity] = ticket.ticket_id
        return ticket

    async def get(
        self,
        ticket_id: str,
        *,
        request_scope: str,
    ) -> PreparedLaunchTicket | None:
        ticket = self._by_id.get(ticket_id)
        return ticket if ticket is not None and ticket.request_scope == request_scope else None

    async def expire(
        self,
        ticket_id: str,
        *,
        request_scope: str,
        observed_at: Any,
    ) -> PreparedLaunchTicket:
        ticket = self._required(ticket_id)
        self._require_scope(ticket, request_scope)
        if ticket.state == LaunchTicketState.EXPIRED:
            return ticket
        if ticket.state != LaunchTicketState.PREPARED:
            raise LaunchTicketUnavailable(f"cannot expire a {ticket.state.value} launch ticket")
        updated = ticket.model_copy(update={"state": LaunchTicketState.EXPIRED})
        self._by_id[ticket_id] = updated
        return updated

    async def invalidate(
        self,
        ticket_id: str,
        *,
        request_scope: str,
        reason: str,
    ) -> PreparedLaunchTicket:
        ticket = self._required(ticket_id)
        self._require_scope(ticket, request_scope)
        if ticket.state == LaunchTicketState.INVALIDATED:
            return ticket
        if ticket.state != LaunchTicketState.PREPARED:
            raise LaunchTicketUnavailable(f"cannot invalidate a {ticket.state.value} launch ticket")
        updated = ticket.model_copy(
            update={
                "state": LaunchTicketState.INVALIDATED,
                "invalidation_reason": reason,
            }
        )
        self._by_id[ticket_id] = updated
        return updated

    async def consume(
        self,
        ticket_id: str,
        *,
        request_scope: str,
        run_id: str,
        consumed_at: Any,
    ) -> PreparedLaunchTicket:
        ticket = self._required(ticket_id)
        self._require_scope(ticket, request_scope)
        if ticket.state == LaunchTicketState.CONSUMED:
            if ticket.consumed_run_id != run_id:
                raise LaunchIdempotencyConflict(
                    "launch ticket was consumed by a different Workflow Run"
                )
            return ticket
        if ticket.state != LaunchTicketState.PREPARED:
            raise LaunchTicketUnavailable(f"cannot consume a {ticket.state.value} launch ticket")
        updated = ticket.model_copy(
            update={
                "state": LaunchTicketState.CONSUMED,
                "consumed_run_id": run_id,
                "consumed_at": consumed_at,
            }
        )
        self._by_id[ticket_id] = updated
        return updated

    def _required(self, ticket_id: str) -> PreparedLaunchTicket:
        ticket = self._by_id.get(ticket_id)
        if ticket is None:
            raise LaunchTicketNotFound(f"launch ticket not found: {ticket_id}")
        return ticket

    @staticmethod
    def _require_scope(ticket: PreparedLaunchTicket, request_scope: str) -> None:
        if ticket.request_scope != request_scope:
            raise LaunchTicketNotFound(f"launch ticket not found: {ticket.ticket_id}")


class CoordinatorLaunchPreparationService:
    def __init__(
        self,
        *,
        compiler: CompilerPort,
        admission: AdmissionPreviewPort,
        tickets: LaunchTicketRepository,
        semantic_bindings: SemanticBindingProvider | None = None,
        runtime_plans: RuntimePlanPreparer | None = None,
        runtime_plan_requirement: RuntimePlanRequirement = (
            RuntimePlanRequirement.LEGACY_COMPATIBILITY
        ),
        ttl: timedelta = timedelta(minutes=15),
    ) -> None:
        if ttl.total_seconds() <= 0:
            raise ValueError("launch ticket TTL must be positive")
        self._compiler = compiler
        self._admission = admission
        self._tickets = tickets
        self._semantic_bindings = semantic_bindings
        self._runtime_plans = runtime_plans
        self._runtime_plan_requirement = runtime_plan_requirement
        self._ttl = ttl

    async def prepare(
        self,
        proposal: WorkflowLaunchProposal,
        context: LaunchRequestContext,
    ) -> PublicPreparedLaunchTicket:
        self._validate_proposal_context(proposal, context)
        erc = await self._compiler.compile(proposal.compilation)
        workflow_ref = _required_source_ref(erc, DefinitionKind.WORKFLOW_TYPE)
        blueprint_ref = _required_source_ref(erc, DefinitionKind.BLUEPRINT)
        family = _family_for(erc)
        initial_goal = _validated_initial_goal(family, proposal.initial_goal)
        semantic_plan = (
            await self._semantic_bindings.prepare(proposal, erc)
            if self._semantic_bindings is not None
            else None
        )
        if semantic_plan is not None and semantic_plan.blueprint_family != family:
            raise LaunchTicketUnavailable(
                "semantic binding plan differs from the compiled blueprint family"
            )
        if self._runtime_plan_requirement == RuntimePlanRequirement.REQUIRE_RUN_PLAN_V3 and (
            self._runtime_plans is None or semantic_plan is None
        ):
            raise LaunchTicketUnavailable(
                "production launch preparation requires a frozen RunPlanV3"
            )
        runtime_plan = (
            await self._runtime_plans.prepare_runtime_plan(proposal, erc, semantic_plan)
            if self._runtime_plans is not None and semantic_plan is not None
            else None
        )
        run_request = RunRequest(
            request_scope=proposal.request_scope,
            idempotency_issuer=proposal.idempotency_issuer,
            request_id=proposal.idempotency_key,
            actor=proposal.admission.actor,
            effective_configuration_digest=erc.digest,
            workflow_type_ref=workflow_ref,
            input_manifest=erc.input_manifest,
            budget_envelope=proposal.admission.budget_envelope,
            requested_at=proposal.admission.requested_at,
            correlation_id=proposal.admission.correlation_id,
            causation_id=proposal.admission.causation_id,
            parent_run_id=proposal.admission.parent_run_id,
            sponsorship_ref=proposal.admission.sponsorship_ref,
            approval_refs=proposal.admission.approval_refs,
            delegation_authority_refs=proposal.admission.delegation_authority_refs,
            admission_evidence_refs=proposal.admission.admission_evidence_refs,
        )
        preview = await self._admission.preview(run_request)
        resolved_refs = tuple(
            sorted(
                set(
                    (
                        *erc.source_refs,
                        *proposal.selected_asset_refs,
                        *(
                            (runtime_plan.run_plan.workflow_implementation_ref,)
                            if runtime_plan is not None
                            else ()
                        ),
                    )
                ),
                key=lambda ref: (ref.kind.value, ref.logical_id, ref.revision, ref.digest),
            )
        )
        run_request_digest = sha256_digest(run_request)
        warnings: tuple[str, ...] = (
            () if preview.accepted else (f"{preview.reason_code}: {preview.reason}",)
        )
        if semantic_plan is None:
            warnings += ("semantic_binding_provider_unavailable",)
        if runtime_plan is not None and runtime_plan.unavailable_surfaces:
            warnings += (
                "required_runtime_surfaces_unavailable:"
                + ",".join(
                    f"{surface.stage_id}:{surface.variant_name}:{surface.capability_id}"
                    for surface in runtime_plan.unavailable_surfaces
                ),
            )
        ticket = PreparedLaunchTicket(
            ticket_id=str(uuid4()),
            caller_id=context.caller_id,
            tenant_scope=context.tenant_scope,
            request_scope=context.request_scope,
            prepared_at=context.observed_at,
            expires_at=context.observed_at + self._ttl,
            proposal_digest=proposal.digest,
            workflow_type_ref=workflow_ref,
            blueprint_ref=blueprint_ref,
            blueprint_family=family,
            initial_goal=initial_goal,
            initial_goal_digest=sha256_digest(initial_goal) if initial_goal is not None else None,
            effective_configuration_digest=erc.digest,
            run_request_digest=run_request_digest,
            resolved_asset_refs=resolved_refs,
            authority_decisions=proposal.authority_decisions,
            availability_decisions=proposal.availability_decisions,
            approval_refs=proposal.admission.approval_refs,
            policy_snapshot_digest=proposal.policy_snapshot_digest,
            environment_snapshot_digest=proposal.environment_snapshot_digest,
            semantic_binding_plan_ref=(
                semantic_plan.plan_ref if semantic_plan is not None else None
            ),
            semantic_binding_plan_digest=(
                semantic_plan.plan_digest if semantic_plan is not None else None
            ),
            semantic_binding_plan=semantic_plan,
            runtime_run_plan=runtime_plan.run_plan if runtime_plan is not None else None,
            runtime_unavailable_surfaces=(
                runtime_plan.unavailable_surfaces if runtime_plan is not None else ()
            ),
            warnings=warnings,
            launchable=(
                preview.accepted
                and semantic_plan is not None
                and (runtime_plan is None or not runtime_plan.unavailable_surfaces)
            ),
            idempotency_issuer=proposal.idempotency_issuer,
            idempotency_key=proposal.idempotency_key,
            frozen_run_request=run_request,
        )
        return (await self._tickets.create(ticket)).public_view()

    @staticmethod
    def _validate_proposal_context(
        proposal: WorkflowLaunchProposal,
        context: LaunchRequestContext,
    ) -> None:
        validate_launch_context(
            caller_id=proposal.admission.actor.actor_id,
            tenant_scope=proposal.tenant_scope,
            request_scope=proposal.request_scope,
            approval_refs=proposal.admission.approval_refs,
            policy_snapshot_digest=proposal.policy_snapshot_digest,
            environment_snapshot_digest=proposal.environment_snapshot_digest,
            context=context,
        )


class CoordinatorWorkflowLaunchService:
    def __init__(
        self,
        *,
        tickets: LaunchTicketRepository,
        admission: AdmissionPort,
        dispatcher: BoundLaunchDispatcherPort,
        submissions: WorkflowSubmissionPort,
        semantic_bindings: SemanticBindingProvider | None = None,
        binding_service: RunSemanticInputBindingService | None = None,
        runtime_plan_requirement: RuntimePlanRequirement = (
            RuntimePlanRequirement.LEGACY_COMPATIBILITY
        ),
    ) -> None:
        self._tickets = tickets
        self._admission = admission
        self._dispatcher = dispatcher
        self._submissions = submissions
        self._semantic_bindings = semantic_bindings
        self._binding_service = binding_service
        self._runtime_plan_requirement = runtime_plan_requirement

    async def launch(
        self,
        ticket_id: str,
        context: LaunchRequestContext,
    ) -> WorkflowLaunchHandle:
        ticket = await self._tickets.get(ticket_id, request_scope=context.request_scope)
        if ticket is None:
            raise LaunchTicketNotFound(f"launch ticket not found: {ticket_id}")
        validate_launch_context(
            caller_id=ticket.caller_id,
            tenant_scope=ticket.tenant_scope,
            request_scope=ticket.request_scope,
            approval_refs=ticket.approval_refs,
            policy_snapshot_digest=ticket.policy_snapshot_digest,
            environment_snapshot_digest=ticket.environment_snapshot_digest,
            context=context,
        )
        if ticket.state in {LaunchTicketState.INVALIDATED, LaunchTicketState.EXPIRED}:
            raise LaunchTicketUnavailable(f"launch ticket is {ticket.state.value}")
        if ticket.state == LaunchTicketState.PREPARED and is_expired(
            ticket.expires_at, context.observed_at
        ):
            await self._tickets.expire(
                ticket_id,
                request_scope=context.request_scope,
                observed_at=context.observed_at,
            )
            raise LaunchTicketUnavailable("launch ticket expired")
        if ticket.semantic_binding_plan is None:
            raise LaunchTicketUnavailable("launch ticket has no frozen exact semantic binding plan")
        if ticket.runtime_unavailable_surfaces:
            raise LaunchTicketUnavailable("launch ticket has unavailable required runtime surfaces")
        if (
            self._runtime_plan_requirement == RuntimePlanRequirement.REQUIRE_RUN_PLAN_V3
            and ticket.runtime_run_plan is None
        ):
            raise LaunchTicketUnavailable("production launch ticket has no frozen RunPlanV3")
        if not ticket.launchable:
            raise LaunchTicketUnavailable("launch ticket did not pass admission preview")
        if self._semantic_bindings is None or self._binding_service is None:
            raise LaunchTicketUnavailable(
                "exact semantic binding provider and durable repository are required"
            )

        decision = await self._admission.admit(ticket.frozen_run_request)
        if decision.status != DecisionStatus.ACCEPTED or decision.run_id is None:
            raise LaunchTicketUnavailable(
                f"Run Request admission rejected: {decision.reason_code}: {decision.reason}"
            )
        semantic_binding = await self._semantic_bindings.author(
            ticket.semantic_binding_plan,
            ticket,
            run_id=decision.run_id,
        )
        prepared_input = await self._dispatcher.prepare_bound(
            ticket.request_scope,
            decision.run_id,
            semantic_binding=semantic_binding,
            binding_service=self._binding_service,
            initial_goal=ticket.initial_goal,
        )
        if isinstance(prepared_input, StageGraphRunInput | GoalDirectedRunInput):
            prepared_input = replace(
                prepared_input,
                tenant_scope=ticket.tenant_scope,
                materialize_typed_result=True,
            )
        workflow_id = f"belllabs:{decision.run_id}:epoch:1"
        submission = await self._submissions.submit(
            prepared_input,
            workflow_id=workflow_id,
            blueprint_family=ticket.blueprint_family,
        )
        await self._tickets.consume(
            ticket_id,
            request_scope=context.request_scope,
            run_id=decision.run_id,
            consumed_at=context.observed_at,
        )
        return WorkflowLaunchHandle(
            run_id=decision.run_id,
            request_scope=ticket.request_scope,
            workflow_type_ref=ticket.workflow_type_ref,
            effective_configuration_digest=ticket.effective_configuration_digest,
            blueprint_ref=ticket.blueprint_ref,
            blueprint_family=ticket.blueprint_family,
            phase="pending",
            result_resource_uri=f"belllabs://runs/{decision.run_id}/result",
            correlation_id=ticket.frozen_run_request.correlation_id,
            workflow_id=submission.workflow_id,
            temporal_run_id=submission.temporal_run_id,
        )


def _required_source_ref(
    erc: EffectiveRunConfiguration,
    kind: DefinitionKind,
):
    matches = tuple(ref for ref in erc.source_refs if ref.kind == kind)
    if len(matches) != 1:
        raise LaunchTicketUnavailable(
            f"compiled configuration must contain exactly one {kind.value} reference"
        )
    return matches[0]


def _family_for(erc: EffectiveRunConfiguration) -> BlueprintFamily:
    if isinstance(erc.selected_blueprint, StageGraphBlueprint):
        return BlueprintFamily.STAGE_GRAPH
    if isinstance(erc.selected_blueprint, GoalDirectedBlueprint):
        return BlueprintFamily.GOAL_DIRECTED
    raise LaunchTicketUnavailable("compiled configuration selected an unsupported blueprint family")


def _runtime_plan_digest(ticket: PreparedLaunchTicket) -> str | None:
    return ticket.runtime_run_plan.plan_digest if ticket.runtime_run_plan is not None else None


def _validated_initial_goal(
    family: BlueprintFamily,
    value: str | None,
) -> str | None:
    if family == BlueprintFamily.STAGE_GRAPH:
        if value is not None:
            raise LaunchTicketUnavailable("StageGraph launch does not accept initial_goal")
        return None
    if value is None or not value.strip():
        raise LaunchTicketUnavailable("GoalDirected launch requires a non-empty initial_goal")
    return value
