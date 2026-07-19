from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from pydantic import TypeAdapter

from app.application.control_plane import ControlPlaneService
from app.application.run_control import RunControlService
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, StageGraphBlueprint
from app.domain.orchestration.contracts import (
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
    StageGraphRunInput,
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationRequest,
    WorkflowEvaluationResult,
)
from app.domain.run_control.contracts import (
    ActorContext,
    CommandStatus,
    LifecycleAction,
    LifecycleCommand,
)

LIFECYCLE_ACTION_ADAPTER: TypeAdapter[LifecycleAction] = TypeAdapter(LifecycleAction)


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
        orchestration_authority_ref: str = "orchestration-authority",
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
            blueprint_digest=blueprint_ref.digest,
            blueprint=blueprint.model_dump(mode="json"),
            initial_run_version=projection.version,
            execution_epoch=execution_epoch,
            max_concurrency=configuration.effective_authority.max_concurrency,
            task_timeout_seconds=task_timeout_seconds,
            orchestration_authority_ref=orchestration_authority_ref,
            correlation_id=f"orchestration:{run_id}:epoch:{execution_epoch}",
            baseline_reservation=dict(budget.reservations.get("baseline", {})),
        )


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
        )
