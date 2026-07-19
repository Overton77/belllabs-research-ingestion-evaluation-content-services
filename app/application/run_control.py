from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from app.application.control_plane import ControlPlaneService
from app.application.run_control_repository import (
    AdmissionMutation,
    CommandMutation,
    RunControlRepository,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.errors import ControlPlaneError
from app.domain.run_control.contracts import (
    ActorContext,
    AdmissionDecision,
    BudgetLedgerEntry,
    BudgetLedgerKind,
    BudgetState,
    CommandResult,
    CommandStatus,
    ConsumerApplyResult,
    DecisionStatus,
    DomainEventEnvelope,
    LifecycleCommand,
    LifecycleTransitionRecord,
    OutboxCursor,
    OutboxRecord,
    RunPhase,
    RunProjection,
    RunRequest,
    VerifiedRunConfiguration,
)
from app.domain.run_control.errors import (
    AdmissionRejected,
    CommandRejected,
    ConfigurationVerificationFailed,
    IdempotencyConflict,
    RunVersionConflict,
)
from app.domain.run_control.reducer import (
    ACTION_PERMISSIONS,
    ReductionRejected,
    reduce_lifecycle,
)

AdmissionValidator = Callable[
    [RunRequest, VerifiedRunConfiguration], Awaitable[str | None] | str | None
]

REQUIRED_SHARED_BUDGET_DIMENSIONS = frozenset(
    {
        "currency.estimated_micros",
        "currency.actual_micros",
        "tokens.input",
        "tokens.output",
        "tokens.total",
        "time.elapsed_ms",
        "time.active_compute_ms",
        "model.turns",
        "tool.calls.total",
        "mcp.calls.total",
        "external.quotas.total",
        "stage.cycles",
        "workflow.cycles",
        "goal.iterations",
        "operation.attempts",
        "subagent.spawns",
        "concurrency.slots",
    }
)


class RunConfigurationVerifier(Protocol):
    async def verify(self, request: RunRequest) -> VerifiedRunConfiguration: ...


class F1RunConfigurationVerifier:
    """Digest-verifies immutable F1 configuration without copying its payload to PostgreSQL."""

    def __init__(self, control_plane: ControlPlaneService) -> None:
        self._control_plane = control_plane

    async def verify(self, request: RunRequest) -> VerifiedRunConfiguration:
        try:
            erc = await self._control_plane.retrieve_for_admission(
                request.effective_configuration_digest
            )
        except ControlPlaneError as error:
            raise ConfigurationVerificationFailed(error.message) from error
        workflow_ref = next(
            (ref for ref in erc.source_refs if ref.kind.value == "workflow_type"),
            None,
        )
        if workflow_ref is None:
            raise ConfigurationVerificationFailed(
                "effective configuration has no exact Workflow Type reference"
            )
        if (
            erc.context.authority_subject_id != request.actor.actor_id
            or erc.context.authority_scope != request.request_scope
        ):
            raise ConfigurationVerificationFailed(
                "effective configuration authority subject or tenant scope mismatch"
            )
        return VerifiedRunConfiguration(
            effective_configuration_digest=erc.digest,
            workflow_type_ref=workflow_ref,
            input_manifest=erc.input_manifest,
            effective_budget_ceilings=erc.effective_authority.budgets.dimensions,
            max_concurrency=erc.effective_authority.max_concurrency,
            input_admission_contract=erc.workflow_type.input_admission_contract,
            invariant_refs=erc.workflow_type.invariants,
            obligation_revision=sha256_digest(sorted(erc.workflow_type.obligations)),
            required_obligation_refs=erc.workflow_type.obligations,
        )


class AdmissionPolicyRegistry:
    """Deny-by-default executable boundary for admission contracts and invariants."""

    def __init__(self) -> None:
        self._validators: dict[str, AdmissionValidator] = {}

    def register(self, contract_ref: str, validator: AdmissionValidator) -> None:
        if contract_ref in self._validators:
            raise ValueError(f"admission validator already registered: {contract_ref}")
        self._validators[contract_ref] = validator

    async def validate(self, request: RunRequest, configuration: VerifiedRunConfiguration) -> None:
        refs = (configuration.input_admission_contract, *sorted(configuration.invariant_refs))
        for contract_ref in refs:
            validator = self._validators.get(contract_ref)
            if validator is None:
                raise AdmissionRejected(
                    f"no executable admission validator is registered for {contract_ref}"
                )
            result = validator(request, configuration)
            reason = await result if inspect.isawaitable(result) else result
            if reason is not None:
                raise AdmissionRejected(f"{contract_ref}: {reason}")


class RunControlService:
    def __init__(
        self,
        repository: RunControlRepository,
        configuration_verifier: RunConfigurationVerifier,
        policies: AdmissionPolicyRegistry,
    ) -> None:
        self._repository = repository
        self._configuration_verifier = configuration_verifier
        self._policies = policies

    async def admit(self, request: RunRequest) -> AdmissionDecision:
        if "workflow_run.admit" not in request.actor.permissions:
            raise AdmissionRejected("actor lacks workflow_run.admit permission")
        fingerprint = _fingerprint(request, exclude={"requested_at"})
        prior = await self._repository.get_admission_decision(
            request.request_scope, request.idempotency_issuer, request.request_id
        )
        if prior is not None:
            _require_same_fingerprint(prior.request_fingerprint, fingerprint, "run request")
            return prior

        try:
            if not request.delegation_authority_refs <= request.actor.authority_refs:
                raise AdmissionRejected("requested delegation exceeds the actor authority context")
            configuration = await self._configuration_verifier.verify(request)
            self._validate_configuration_binding(request, configuration)
            self._validate_budget_envelope(request, configuration)
            await self._validate_parent_binding(request)
            await self._policies.validate(request, configuration)
        except (AdmissionRejected, ConfigurationVerificationFailed) as error:
            decision = AdmissionDecision(
                request_scope=request.request_scope,
                idempotency_issuer=request.idempotency_issuer,
                request_id=request.request_id,
                request_fingerprint=fingerprint,
                status=DecisionStatus.REJECTED,
                reason_code=error.code,
                reason=error.message,
                recorded_at=request.requested_at,
            )
            return await self._repository.commit_admission(AdmissionMutation(decision=decision))

        run_id = _stable_id(
            "run",
            request.request_scope,
            request.idempotency_issuer,
            request.request_id,
        )
        account_id = _stable_id("budget-account", run_id)
        projection = RunProjection(
            run_id=run_id,
            request_scope=request.request_scope,
            idempotency_issuer=request.idempotency_issuer,
            request_id=request.request_id,
            version=1,
            phase=RunPhase.PENDING,
            effective_configuration_digest=request.effective_configuration_digest,
            workflow_type_ref=request.workflow_type_ref,
            input_manifest=request.input_manifest,
            obligation_revision=configuration.obligation_revision,
            required_obligation_refs=configuration.required_obligation_refs,
            evidence_frontier_digest=sha256_digest(
                {
                    "input_manifest_digest": request.input_manifest.digest,
                    "obligation_evidence": [],
                    "output_evidence": [],
                }
            ),
            updated_at=request.requested_at,
        )
        reservations = (
            {"baseline": dict(request.budget_envelope.baseline_reservations)}
            if request.budget_envelope.baseline_reservations
            else {}
        )
        budget = BudgetState(
            account_id=account_id,
            run_id=run_id,
            parent_account_id=request.budget_envelope.parent_account_id,
            limits=request.budget_envelope.dimensions,
            reserved=dict(request.budget_envelope.baseline_reservations),
            reservations=reservations,
        )
        ledger = (
            BudgetLedgerEntry(
                entry_id=_stable_id("ledger", account_id, "baseline"),
                account_id=account_id,
                run_id=run_id,
                kind=BudgetLedgerKind.RESERVATION,
                idempotency_id="baseline",
                amounts=dict(request.budget_envelope.baseline_reservations),
                occurred_at=request.requested_at,
                parent_account_id=request.budget_envelope.parent_account_id,
            ),
        )
        actor = request.actor
        transition = LifecycleTransitionRecord(
            transition_id=_stable_id("transition", run_id, "1"),
            run_id=run_id,
            command_id=f"admission:{request.request_id}",
            prior_version=0,
            resulting_version=1,
            prior_phase=None,
            resulting_phase=RunPhase.PENDING,
            prior_projection=None,
            resulting_projection=projection,
            actor=actor,
            reason="Run Request admitted",
            evidence_refs=request.admission_evidence_refs,
            occurred_at=request.requested_at,
            correlation_id=request.correlation_id,
            causation_id=request.causation_id,
        )
        events = (
            _event(
                run_id,
                1,
                1,
                "workflow_run.admitted",
                request.requested_at,
                actor,
                request.correlation_id,
                request.causation_id or request.request_id,
                {
                    "request_scope": request.request_scope,
                    "request_id": request.request_id,
                    "phase": RunPhase.PENDING.value,
                    "effective_configuration_digest": request.effective_configuration_digest,
                },
                is_version_final=False,
            ),
            _event(
                run_id,
                1,
                2,
                "workflow_run.start_requested",
                request.requested_at,
                actor,
                request.correlation_id,
                request.request_id,
                {"run_id": run_id, "expected_run_version": 1},
            ),
        )
        decision = AdmissionDecision(
            request_scope=request.request_scope,
            idempotency_issuer=request.idempotency_issuer,
            request_id=request.request_id,
            request_fingerprint=fingerprint,
            status=DecisionStatus.ACCEPTED,
            run_id=run_id,
            reason_code="accepted",
            reason="Run Request admitted",
            recorded_at=request.requested_at,
        )
        try:
            return await self._repository.commit_admission(
                AdmissionMutation(
                    decision=decision,
                    projection=projection,
                    budget=budget,
                    transition=transition,
                    ledger_entries=ledger,
                    events=events,
                )
            )
        except ReductionRejected as error:
            rejected = decision.model_copy(
                update={
                    "status": DecisionStatus.REJECTED,
                    "run_id": None,
                    "reason_code": error.code,
                    "reason": error.message,
                }
            )
            return await self._repository.commit_admission(AdmissionMutation(decision=rejected))

    async def execute(self, command: LifecycleCommand) -> CommandResult:
        required_permission = ACTION_PERMISSIONS[command.action.kind]
        if required_permission not in command.actor.permissions:
            raise CommandRejected(f"actor lacks {required_permission} permission")
        fingerprint = _fingerprint(command, exclude={"occurred_at"})
        prior = await self._repository.get_command_result(
            command.request_scope,
            command.run_id,
            command.idempotency_issuer,
            command.command_id,
        )
        if prior is not None:
            _require_same_fingerprint(prior.command_fingerprint, fingerprint, "lifecycle command")
            return prior

        projection = await self._repository.get_run(command.request_scope, command.run_id)
        if command.expected_run_version != projection.version:
            return await self._commit_non_transition_result(
                command,
                fingerprint,
                projection,
                CommandStatus.STALE,
                "stale_run_version",
                f"expected version {command.expected_run_version}, current version is "
                f"{projection.version}",
            )
        budget = await self._repository.get_budget(command.request_scope, command.run_id)
        try:
            reduction = reduce_lifecycle(projection, budget, command, fingerprint)
        except ReductionRejected as error:
            return await self._commit_non_transition_result(
                command,
                fingerprint,
                projection,
                (
                    CommandStatus.STALE
                    if error.code == "stale_run_version"
                    else CommandStatus.REJECTED
                ),
                error.code,
                error.message,
            )
        mutation = CommandMutation(
            result=reduction.result,
            request_scope=command.request_scope,
            expected_version=projection.version,
            projection=reduction.projection,
            budget=reduction.budget,
            transition=reduction.transition,
            ledger_entries=reduction.ledger_entries,
            events=reduction.events,
        )
        try:
            return await self._repository.commit_command(mutation)
        except ReductionRejected as error:
            return await self._commit_non_transition_result(
                command,
                fingerprint,
                projection,
                CommandStatus.REJECTED,
                error.code,
                error.message,
            )
        except RunVersionConflict:
            current = await self._repository.get_run(command.request_scope, command.run_id)
            return await self._commit_non_transition_result(
                command,
                fingerprint,
                current,
                CommandStatus.STALE,
                "stale_run_version",
                f"expected version {command.expected_run_version}, current version is "
                f"{current.version}",
            )

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection:
        return await self._repository.get_run(request_scope, run_id)

    async def get_budget(self, request_scope: str, run_id: str) -> BudgetState:
        return await self._repository.get_budget(request_scope, run_id)

    async def reconstruct_projection(self, request_scope: str, run_id: str) -> RunProjection:
        transitions = await self._repository.list_transitions(request_scope, run_id)
        if not transitions:
            raise ValueError("a run must have at least its admission transition")
        expected = 1
        projection: RunProjection | None = None
        for transition in transitions:
            if (
                transition.run_id != run_id
                or transition.prior_version != expected - 1
                or transition.resulting_version != expected
                or transition.resulting_projection.run_id != run_id
                or transition.resulting_projection.version != expected
                or transition.resulting_projection.phase != transition.resulting_phase
            ):
                raise ValueError(f"invalid transition metadata at expected version {expected}")
            if transition.prior_projection != projection:
                raise ValueError("transition prior projection does not match reconstructed state")
            if projection is None:
                if transition.prior_phase is not None:
                    raise ValueError("admission transition must not have a prior phase")
            elif (
                transition.prior_phase != projection.phase
                or transition.prior_version != projection.version
            ):
                raise ValueError("transition prior metadata does not match reconstructed state")
            projection = transition.resulting_projection
            expected += 1
        assert projection is not None
        return projection

    async def list_transitions(
        self, request_scope: str, run_id: str
    ) -> tuple[LifecycleTransitionRecord, ...]:
        return await self._repository.list_transitions(request_scope, run_id)

    async def pending_outbox(
        self,
        request_scope: str,
        *,
        after: OutboxCursor | None = None,
        limit: int = 100,
    ) -> tuple[OutboxRecord, ...]:
        records = await self._repository.list_outbox(request_scope, after=after, limit=limit)
        return tuple(record for record in records if record.delivered_at is None)

    async def mark_delivered(
        self, request_scope: str, event_id: str, delivered_at: datetime
    ) -> None:
        await self._repository.mark_outbox_delivered(request_scope, event_id, delivered_at)

    async def apply_consumer_event(
        self,
        request_scope: str,
        consumer_id: str,
        envelope: DomainEventEnvelope,
    ) -> ConsumerApplyResult:
        return await self._repository.apply_consumer_event(request_scope, consumer_id, envelope)

    async def _commit_non_transition_result(
        self,
        command: LifecycleCommand,
        fingerprint: str,
        projection: RunProjection,
        status: CommandStatus,
        reason_code: str,
        reason: str,
    ) -> CommandResult:
        result = CommandResult(
            command_id=command.command_id,
            idempotency_issuer=command.idempotency_issuer,
            run_id=command.run_id,
            command_fingerprint=fingerprint,
            status=status,
            resulting_run_version=projection.version,
            phase=projection.phase,
            terminal_outcome=projection.terminal_outcome,
            reason_code=reason_code,
            reason=reason,
            recorded_at=command.occurred_at,
        )
        return await self._repository.commit_command(
            CommandMutation(
                result=result,
                request_scope=command.request_scope,
                expected_version=projection.version,
            )
        )

    @staticmethod
    def _validate_configuration_binding(
        request: RunRequest, configuration: VerifiedRunConfiguration
    ) -> None:
        if configuration.effective_configuration_digest != request.effective_configuration_digest:
            raise ConfigurationVerificationFailed("configuration digest binding mismatch")
        if configuration.workflow_type_ref != request.workflow_type_ref:
            raise ConfigurationVerificationFailed("Workflow Type exact reference mismatch")
        if configuration.input_manifest != request.input_manifest:
            raise ConfigurationVerificationFailed("Run Input Manifest exact reference mismatch")

    @staticmethod
    def _validate_budget_envelope(
        request: RunRequest, configuration: VerifiedRunConfiguration
    ) -> None:
        dimensions = {item.dimension: item for item in request.budget_envelope.dimensions}
        undeclared_shared = REQUIRED_SHARED_BUDGET_DIMENSIONS - dimensions.keys()
        if undeclared_shared:
            raise AdmissionRejected(
                "budget envelope must explicitly declare shared dimensions: "
                + ", ".join(sorted(undeclared_shared))
            )
        concurrency = dimensions.get("concurrency.slots")
        if (
            concurrency is None
            or concurrency.hard_cap is None
            or concurrency.hard_cap > configuration.max_concurrency
        ):
            raise AdmissionRejected(
                "concurrency.slots must be bounded by effective concurrency authority"
            )
        missing = configuration.effective_budget_ceilings.keys() - dimensions.keys()
        if missing:
            raise AdmissionRejected(
                f"budget envelope omits configured dimensions: {', '.join(sorted(missing))}"
            )
        for dimension, ceiling in configuration.effective_budget_ceilings.items():
            requested = dimensions[dimension]
            if requested.hard_cap is None or requested.hard_cap > ceiling:
                raise AdmissionRejected(
                    f"budget hard cap for {dimension} exceeds effective authority"
                )

    async def _validate_parent_binding(self, request: RunRequest) -> None:
        if request.parent_run_id is None:
            return
        authority_ref = f"workflow_run.parent:{request.parent_run_id}:sponsor"
        if authority_ref not in request.actor.authority_refs:
            raise AdmissionRejected("actor lacks authority to sponsor the parent-linked run")
        parent = await self._repository.get_run(request.request_scope, request.parent_run_id)
        if parent.phase in {
            RunPhase.CANCELLING,
            RunPhase.TERMINAL,
        }:
            raise AdmissionRejected("parent run cannot sponsor new linked work")
        parent_budget = await self._repository.get_budget(
            request.request_scope, request.parent_run_id
        )
        if request.budget_envelope.parent_account_id != parent_budget.account_id:
            raise AdmissionRejected("parent budget account does not match parent run")


def _fingerprint(value: object, *, exclude: set[str]) -> str:
    if not hasattr(value, "model_dump"):
        raise TypeError("fingerprinted values must be Pydantic contracts")
    payload = value.model_dump(mode="json", exclude=exclude)
    return sha256_digest(payload)


def _require_same_fingerprint(actual: str, expected: str, subject: str) -> None:
    if actual != expected:
        raise IdempotencyConflict(f"{subject} identity was reused with a conflicting payload")


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))


def _event(
    run_id: str,
    version: int,
    sequence: int,
    event_type: str,
    occurred_at: datetime,
    actor: ActorContext,
    correlation_id: str,
    causation_id: str | None,
    payload: dict[str, object],
    *,
    is_version_final: bool = True,
) -> DomainEventEnvelope:
    return DomainEventEnvelope(
        event_id=_stable_id("event", run_id, str(version), str(sequence), event_type),
        event_type=event_type,
        aggregate_id=run_id,
        aggregate_version=version,
        sequence=sequence,
        is_version_final=is_version_final,
        occurred_at=occurred_at,
        recorded_at=occurred_at,
        actor=actor,
        correlation_id=correlation_id,
        causation_id=causation_id,
        payload=payload,
    )
