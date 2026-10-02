from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from app.application.run_control.run_control_repository import InMemoryRunControlRepository
from app.application.run_control.service import AdmissionPolicyRegistry, RunControlService
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    RunInputManifestRef,
)
from app.domain.graph_runtime.identities import QualifiedCheckpointKey
from app.domain.run_control.contracts import (
    AcceptedObligationEvidence,
    AcceptedOutputEvidence,
    AcceptFinalizationPlanAction,
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    CancelAction,
    CommandStatus,
    ConsumerApplyStatus,
    DecisionStatus,
    FinalizationPlan,
    LifecycleCommand,
    PauseAction,
    PauseDecision,
    ReconcileUnitAction,
    RecordFinalizationResultAction,
    RecordObligationEvidenceAction,
    RecordOutputEvidenceAction,
    RecordUsageAction,
    ReserveBudgetAction,
    ResumeAction,
    ResumeDecision,
    RunOutcome,
    RunPhase,
    RunRequest,
    SatisfyWaitAction,
    SetWaitAction,
    StartAction,
    TerminalizationProposal,
    TerminalizeAction,
    VerifiedRunConfiguration,
    WaitCondition,
    operator_reconciliation_condition_id,
)
from app.domain.run_control.errors import CommandRejected, IdempotencyConflict
from tests.fixtures.set_order import equal_sets_with_different_iteration_order

NOW = datetime(2026, 7, 19, 18, 0, tzinfo=UTC)
DIGEST = "sha256:" + "a" * 64
MANIFEST_DIGEST = "sha256:" + "b" * 64
WORKFLOW_DIGEST = "sha256:" + "c" * 64
ALL_PERMISSIONS = frozenset(
    {
        "workflow_run.admit",
        "workflow_run.start",
        "workflow_run.observe_wait",
        "workflow_run.pause",
        "workflow_run.resume",
        "workflow_run.cancel",
        "workflow_run.reserve_budget",
        "workflow_run.report_usage",
        "workflow_run.settle_usage",
        "workflow_run.claim_effect",
        "workflow_run.observe_effect",
        "workflow_run.settle_effect",
        "workflow_run.register_async_child",
        "workflow_run.observe_async_child",
        "workflow_run.decide_async_child",
        "workflow_run.propose_continuation",
        "workflow_run.decide_continuation",
        "workflow_run.accept_finalization",
        "workflow_run.record_finalization",
        "workflow_run.accept_obligation_evidence",
        "workflow_run.accept_output_evidence",
        "workflow_run.terminalize",
        "workflow_run.decide_readiness",
    }
)
EMPTY_EVIDENCE_DIGEST = sha256_digest([])
INITIAL_EVIDENCE_FRONTIER = sha256_digest(
    {
        "input_manifest_digest": MANIFEST_DIGEST,
        "obligation_evidence": [],
        "output_evidence": [],
    }
)


def actor() -> ActorContext:
    return ActorContext(
        actor_id="operator",
        authority_refs=frozenset({"authority:lifecycle"}),
        permissions=ALL_PERMISSIONS,
    )


def workflow_ref() -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=DefinitionKind.WORKFLOW_TYPE,
        logical_id="generic.workflow",
        revision=1,
        digest=WORKFLOW_DIGEST,
    )


def manifest_ref() -> RunInputManifestRef:
    return RunInputManifestRef(
        manifest_id="manifest-1",
        revision=1,
        digest=MANIFEST_DIGEST,
    )


def request(
    *,
    request_scope: str = "tenant-1",
    request_id: str = "request-1",
    hard_cap: int = 100,
) -> RunRequest:
    return RunRequest(
        request_scope=request_scope,
        idempotency_issuer="operator",
        request_id=request_id,
        actor=actor(),
        effective_configuration_digest=DIGEST,
        workflow_type_ref=workflow_ref(),
        input_manifest=manifest_ref(),
        budget_envelope=BudgetEnvelope(
            dimensions=(
                BudgetDimensionLimit(
                    dimension="tokens.total",
                    applicability=BudgetApplicability.BOUNDED,
                    soft_limit=60,
                    hard_cap=hard_cap,
                ),
                BudgetDimensionLimit(
                    dimension="external.openai",
                    applicability=BudgetApplicability.BOUNDED,
                    hard_cap=20,
                ),
                BudgetDimensionLimit(
                    dimension="currency.usd_micros",
                    applicability=BudgetApplicability.UNBOUNDED,
                ),
                BudgetDimensionLimit(
                    dimension="concurrency.slots",
                    applicability=BudgetApplicability.BOUNDED,
                    hard_cap=2,
                ),
                *(
                    BudgetDimensionLimit(
                        dimension=dimension,
                        applicability=BudgetApplicability.NOT_APPLICABLE,
                    )
                    for dimension in sorted(
                        {
                            "currency.estimated_micros",
                            "currency.actual_micros",
                            "tokens.input",
                            "tokens.output",
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
                        }
                    )
                ),
            ),
            baseline_reservations={"tokens.total": 20},
        ),
        requested_at=NOW,
        correlation_id="correlation-1",
        sponsorship_ref="sponsorship:test",
        approval_refs=("approval:test",),
        delegation_authority_refs=frozenset({"authority:lifecycle"}),
    )


class ConfigurationVerifier:
    def __init__(self, required_obligations: frozenset[str] = frozenset()) -> None:
        self._required_obligations = required_obligations

    async def verify(self, run_request: RunRequest) -> VerifiedRunConfiguration:
        return VerifiedRunConfiguration(
            effective_configuration_digest=run_request.effective_configuration_digest,
            workflow_type_ref=run_request.workflow_type_ref,
            input_manifest=run_request.input_manifest,
            effective_budget_ceilings={"tokens.total": 100, "external.openai": 20},
            max_concurrency=2,
            input_admission_contract="contract:input@1",
            invariant_refs=frozenset({"contract:invariant@1"}),
            obligation_revision="obligations:1",
            required_obligation_refs=self._required_obligations,
        )


def service(
    repository: InMemoryRunControlRepository | None = None,
    *,
    reject_input: bool = False,
    required_obligations: frozenset[str] = frozenset(),
) -> tuple[RunControlService, InMemoryRunControlRepository]:
    repository = repository or InMemoryRunControlRepository()
    policies = AdmissionPolicyRegistry()
    policies.register(
        "contract:input@1",
        lambda _request, _configuration: "input rejected" if reject_input else None,
    )
    policies.register("contract:invariant@1", lambda _request, _configuration: None)
    return (
        RunControlService(
            repository,
            ConfigurationVerifier(required_obligations),
            policies,
        ),
        repository,
    )


def command(run_id: str, version: int, command_id: str, action: object) -> LifecycleCommand:
    return LifecycleCommand(
        command_id=command_id,
        idempotency_issuer="operator",
        request_scope="tenant-1",
        run_id=run_id,
        expected_run_version=version,
        actor=actor(),
        action=action,  # type: ignore[arg-type]
        reason=f"test {command_id}",
        occurred_at=NOW + timedelta(minutes=version),
        correlation_id="correlation-1",
    )


@pytest.mark.asyncio
async def test_admission_is_idempotent_and_rejection_creates_no_run() -> None:
    run_service, repository = service()
    accepted = await run_service.admit(request())
    duplicate = await run_service.admit(request())

    assert accepted.status == DecisionStatus.ACCEPTED
    assert duplicate == accepted
    assert accepted.run_id is not None
    projection = await run_service.get_run("tenant-1", accepted.run_id)
    budget = await run_service.get_budget("tenant-1", accepted.run_id)
    assert projection.phase == RunPhase.PENDING
    assert projection.version == 1
    assert budget.reserved == {"tokens.total": 20}
    assert len(await repository.list_transitions("tenant-1", accepted.run_id)) == 1
    assert len(await repository.list_budget_ledger("tenant-1", accepted.run_id)) == 1
    assert [item.envelope.event_type for item in await run_service.pending_outbox("tenant-1")] == [
        "workflow_run.admitted",
        "workflow_run.start_requested",
    ]

    with pytest.raises(IdempotencyConflict):
        await run_service.admit(request(hard_cap=90))

    rejecting_service, rejecting_repository = service(reject_input=True)
    rejected = await rejecting_service.admit(request(request_id="rejected"))
    assert rejected.status == DecisionStatus.REJECTED
    assert rejected.run_id is None
    assert await rejecting_service.pending_outbox("tenant-1") == ()
    assert (
        await rejecting_repository.get_admission_decision("tenant-1", "operator", "rejected")
        == rejected
    )


@pytest.mark.asyncio
async def test_wait_and_pause_are_separate_and_commands_use_optimistic_concurrency() -> None:
    run_service, repository = service()
    admitted = await run_service.admit(request())
    assert admitted.run_id
    run_id = admitted.run_id

    started = await run_service.execute(command(run_id, 1, "start", StartAction()))
    assert started.phase == RunPhase.ACTIVE

    wait = WaitCondition(
        condition_id="dependency-1",
        kind="dependency",
        scope=frozenset({"stage:a"}),
        verification_ref="verification:dependency",
        timeout_policy_ref="timeout:default",
    )
    waiting = await run_service.execute(
        command(
            run_id,
            2,
            "wait",
            SetWaitAction(condition=wait, runnable_work_remains=False),
        )
    )
    assert waiting.phase == RunPhase.WAITING

    pause = PauseDecision(
        decision_id="pause-1",
        scope=frozenset({"stage:a"}),
        reason="operator hold",
        authority_ref="authority:lifecycle",
    )
    paused = await run_service.execute(
        command(
            run_id,
            3,
            "pause",
            PauseAction(decision=pause, runnable_work_remains=False),
        )
    )
    assert paused.phase == RunPhase.PAUSED

    wait_satisfied = await run_service.execute(
        command(
            run_id,
            4,
            "satisfy",
            SatisfyWaitAction(
                condition_id="dependency-1",
                verification_evidence_ref="evidence:dependency",
                runnable_work_remains=False,
            ),
        )
    )
    assert wait_satisfied.phase == RunPhase.PAUSED

    resumed = await run_service.execute(
        command(
            run_id,
            5,
            "resume",
            ResumeAction(
                decision=ResumeDecision(
                    decision_id="resume-1",
                    pause_decision_id="pause-1",
                    reason="operator released hold",
                    authority_ref="authority:lifecycle",
                )
            ),
        )
    )
    assert resumed.phase == RunPhase.ACTIVE

    stale_a, stale_b = await asyncio.gather(
        run_service.execute(command(run_id, 6, "cancel-a", CancelAction())),
        run_service.execute(command(run_id, 6, "cancel-b", CancelAction())),
    )
    assert {stale_a.status, stale_b.status} == {
        CommandStatus.ACCEPTED,
        CommandStatus.STALE,
    }
    accepted = stale_a if stale_a.status == CommandStatus.ACCEPTED else stale_b
    duplicate = await run_service.execute(command(run_id, 6, accepted.command_id, CancelAction()))
    assert duplicate == accepted
    assert len(await repository.list_transitions("tenant-1", run_id)) == 7
    assert await run_service.reconstruct_projection(
        "tenant-1", run_id
    ) == await run_service.get_run("tenant-1", run_id)


@pytest.mark.asyncio
async def test_budget_dimensions_enforce_hard_caps_and_cancellation_waits_for_settlement() -> None:
    run_service, _repository = service()
    admitted = await run_service.admit(request())
    assert admitted.run_id
    run_id = admitted.run_id
    await run_service.execute(command(run_id, 1, "start", StartAction()))

    reserved = await run_service.execute(
        command(
            run_id,
            2,
            "reserve-more",
            ReserveBudgetAction(
                reservation_id="operation-1",
                amounts={"tokens.total": 40, "external.openai": 10},
            ),
        )
    )
    assert reserved.status == CommandStatus.ACCEPTED
    blocked = await run_service.execute(
        command(
            run_id,
            3,
            "oversubscribe",
            ReserveBudgetAction(
                reservation_id="operation-2",
                amounts={"tokens.total": 50},
            ),
        )
    )
    assert blocked.status == CommandStatus.REJECTED
    assert blocked.reason_code == "budget_hard_cap_exceeded"

    usage = await run_service.execute(
        command(
            run_id,
            3,
            "usage",
            RecordUsageAction(
                usage_id="usage-1",
                reservation_id="operation-1",
                actual_amounts={"tokens.total": 45, "external.openai": 2},
                release_amounts={"external.openai": 8},
            ),
        )
    )
    assert usage.status == CommandStatus.ACCEPTED
    budget = await run_service.get_budget("tenant-1", run_id)
    assert budget.consumed == {"tokens.total": 45, "external.openai": 2}
    assert budget.reserved == {"tokens.total": 20, "external.openai": 0}
    assert (await run_service.get_run("tenant-1", run_id)).pending_continuation_proposals

    cancelled = await run_service.execute(command(run_id, 4, "cancel", CancelAction()))
    assert cancelled.phase == RunPhase.CANCELLING
    premature = await run_service.execute(
        command(
            run_id,
            5,
            "terminalize-early",
            TerminalizeAction(
                proposal=TerminalizationProposal(
                    proposal_id="terminal-1",
                    expected_run_version=5,
                    workflow_type_digest=WORKFLOW_DIGEST,
                    obligation_revision="obligations:1",
                    evidence_frontier_digest=INITIAL_EVIDENCE_FRONTIER,
                    accepted_obligation_evidence_digest=EMPTY_EVIDENCE_DIGEST,
                    proposing_execution_binding_ref="execution:test",
                    required_obligations_accepted=True,
                    cancellation_settled=True,
                    budget_settled=True,
                    effects_settled=True,
                    proposed_at=NOW,
                )
            ),
        )
    )
    assert premature.status == CommandStatus.REJECTED
    assert premature.reason_code == "budget_not_settled"

    released = await run_service.execute(
        command(
            run_id,
            5,
            "release-baseline",
            RecordUsageAction(
                usage_id="baseline-release",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        )
    )
    assert released.status == CommandStatus.ACCEPTED
    await run_service.execute(
        command(
            run_id,
            6,
            "accept-partial-output",
            RecordOutputEvidenceAction(
                evidence=AcceptedOutputEvidence(
                    output_ref="artifact:partial",
                    evidence_digest="sha256:" + "f" * 64,
                    accepted_by_authority_ref="authority:lifecycle",
                )
            ),
        )
    )
    output_frontier = (await run_service.get_run("tenant-1", run_id)).evidence_frontier_digest
    terminal = await run_service.execute(
        command(
            run_id,
            7,
            "terminalize",
            TerminalizeAction(
                proposal=TerminalizationProposal(
                    proposal_id="terminal-2",
                    expected_run_version=7,
                    workflow_type_digest=WORKFLOW_DIGEST,
                    obligation_revision="obligations:1",
                    evidence_frontier_digest=output_frontier,
                    accepted_obligation_evidence_digest=EMPTY_EVIDENCE_DIGEST,
                    proposing_execution_binding_ref="execution:test",
                    required_obligations_accepted=True,
                    valid_output_refs=("artifact:partial",),
                    cancellation_settled=True,
                    budget_settled=True,
                    effects_settled=True,
                    proposed_at=NOW,
                )
            ),
        )
    )
    assert terminal.status == CommandStatus.ACCEPTED
    assert terminal.terminal_outcome == RunOutcome.CANCELLED


@pytest.mark.asyncio
async def test_typed_execution_failure_terminalizes_as_failed() -> None:
    run_service, _repository = service()
    admitted = await run_service.admit(request(request_id="execution-failure"))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    await run_service.execute(command(run_id, 1, "start-failure-run", StartAction()))
    await run_service.execute(
        command(
            run_id,
            2,
            "release-failure-baseline",
            RecordUsageAction(
                usage_id="failure-baseline-release",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        )
    )
    terminal = await run_service.execute(
        command(
            run_id,
            3,
            "terminalize-execution-failure",
            TerminalizeAction(
                proposal=TerminalizationProposal(
                    proposal_id="terminal-execution-failure",
                    expected_run_version=3,
                    workflow_type_digest=WORKFLOW_DIGEST,
                    obligation_revision="obligations:1",
                    evidence_frontier_digest=INITIAL_EVIDENCE_FRONTIER,
                    accepted_obligation_evidence_digest=EMPTY_EVIDENCE_DIGEST,
                    proposing_execution_binding_ref="execution:stagegraph",
                    required_obligations_accepted=True,
                    execution_failure_refs=("evaluation:workflow:failed",),
                    budget_settled=True,
                    effects_settled=True,
                    proposed_at=NOW,
                )
            ),
        )
    )
    assert terminal.status == CommandStatus.ACCEPTED
    assert terminal.terminal_outcome == RunOutcome.FAILED


@pytest.mark.asyncio
async def test_outbox_consumers_deduplicate_detect_gaps_and_recover_in_order() -> None:
    run_service, _repository = service()
    admitted = await run_service.admit(request())
    assert admitted.run_id
    await run_service.execute(command(admitted.run_id, 1, "start", StartAction()))
    records = await run_service.pending_outbox("tenant-1")
    first, second, third = [item.envelope for item in records]

    gap = await run_service.apply_consumer_event("tenant-1", "projection", third)
    assert gap.status == ConsumerApplyStatus.GAP
    assert gap.expected_version == 1

    applied = await run_service.apply_consumer_event("tenant-1", "projection", first)
    still_gap = await run_service.apply_consumer_event("tenant-1", "projection", third)
    duplicate = await run_service.apply_consumer_event("tenant-1", "projection", first)
    second_applied = await run_service.apply_consumer_event("tenant-1", "projection", second)
    recovered = await run_service.apply_consumer_event("tenant-1", "projection", third)

    assert applied.status == ConsumerApplyStatus.APPLIED
    assert still_gap.status == ConsumerApplyStatus.GAP
    assert duplicate.status == ConsumerApplyStatus.DUPLICATE
    assert second_applied.status == ConsumerApplyStatus.APPLIED
    assert recovered.status == ConsumerApplyStatus.APPLIED
    assert recovered.cursor.last_aggregate_version == 2


@pytest.mark.asyncio
async def test_terminal_finalization_freezes_evidence_and_uses_dedicated_budget() -> None:
    run_service, _repository = service()
    admitted = await run_service.admit(request())
    assert admitted.run_id
    run_id = admitted.run_id
    await run_service.execute(command(run_id, 1, "start", StartAction()))
    await run_service.execute(
        command(
            run_id,
            2,
            "finalization-budget",
            ReserveBudgetAction(
                reservation_id="finalization-budget",
                amounts={"tokens.total": 5},
            ),
        )
    )
    plan = FinalizationPlan(
        plan_id="finalization-1",
        eligible_evidence_frontier_digest=INITIAL_EVIDENCE_FRONTIER,
        permitted_operations=frozenset({"assemble_existing_output"}),
        budget_reservation_id="finalization-budget",
        deadline=NOW + timedelta(hours=1),
        omission_reason_contract="contract:output-omission@1",
    )
    accepted_plan = await run_service.execute(
        command(
            run_id,
            3,
            "accept-finalization",
            AcceptFinalizationPlanAction(plan=plan),
        )
    )
    assert accepted_plan.status == CommandStatus.ACCEPTED
    stale_evidence = await run_service.execute(
        command(
            run_id,
            4,
            "bad-finalization",
            RecordFinalizationResultAction(
                plan_id=plan.plan_id,
                operation="assemble_existing_output",
                evidence_frontier_digest="sha256:" + "d" * 64,
                output_refs=("artifact:partial",),
            ),
        )
    )
    assert stale_evidence.status == CommandStatus.REJECTED
    assert stale_evidence.reason_code == "finalization_evidence_mismatch"

    finalized = await run_service.execute(
        command(
            run_id,
            4,
            "record-finalization",
            RecordFinalizationResultAction(
                plan_id=plan.plan_id,
                operation="assemble_existing_output",
                evidence_frontier_digest=INITIAL_EVIDENCE_FRONTIER,
                output_refs=("artifact:partial",),
            ),
        )
    )
    assert finalized.status == CommandStatus.ACCEPTED
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.finalization_output_refs == ("artifact:partial",)


@pytest.mark.asyncio
async def test_child_budget_reservations_roll_up_and_cannot_oversubscribe_parent() -> None:
    run_service, _repository = service()
    parent = await run_service.admit(request(request_id="parent"))
    assert parent.run_id
    parent_budget = await run_service.get_budget("tenant-1", parent.run_id)

    first_child_run_id: str | None = None
    for index in range(4):
        child_request = request(request_id=f"child-{index}")
        child_request = child_request.model_copy(
            update={
                "parent_run_id": parent.run_id,
                "actor": child_request.actor.model_copy(
                    update={
                        "authority_refs": child_request.actor.authority_refs
                        | {f"workflow_run.parent:{parent.run_id}:sponsor"}
                    }
                ),
                "budget_envelope": child_request.budget_envelope.model_copy(
                    update={"parent_account_id": parent_budget.account_id}
                ),
            }
        )
        child = await run_service.admit(child_request)
        assert child.status == DecisionStatus.ACCEPTED
        if index == 0:
            first_child_run_id = child.run_id

    assert first_child_run_id
    over_estimate = await run_service.execute(
        command(
            first_child_run_id,
            1,
            "child-over-estimate-usage",
            RecordUsageAction(
                usage_id="child-over-estimate",
                reservation_id="baseline",
                actual_amounts={"tokens.total": 30},
            ),
        )
    )
    assert over_estimate.status == CommandStatus.ACCEPTED

    over_cap_request = request(request_id="child-over-cap")
    over_cap_request = over_cap_request.model_copy(
        update={
            "parent_run_id": parent.run_id,
            "actor": over_cap_request.actor.model_copy(
                update={
                    "authority_refs": over_cap_request.actor.authority_refs
                    | {f"workflow_run.parent:{parent.run_id}:sponsor"}
                }
            ),
            "budget_envelope": over_cap_request.budget_envelope.model_copy(
                update={"parent_account_id": parent_budget.account_id}
            ),
        }
    )
    rejected = await run_service.admit(over_cap_request)
    assert rejected.status == DecisionStatus.REJECTED
    assert rejected.reason_code == "parent_budget_hard_cap_exceeded"
    rolled_up = await run_service.get_budget("tenant-1", parent.run_id)
    assert rolled_up.reserved["tokens.total"] == 80
    assert rolled_up.consumed["tokens.total"] == 30


@pytest.mark.asyncio
async def test_terminalization_binds_authoritatively_accepted_obligation_evidence() -> None:
    run_service, _repository = service(required_obligations=frozenset({"obligation:required"}))
    admitted = await run_service.admit(request(request_id="evidence-run"))
    assert admitted.run_id
    run_id = admitted.run_id
    await run_service.execute(command(run_id, 1, "start", StartAction()))
    evidence = AcceptedObligationEvidence(
        obligation_ref="obligation:required",
        evidence_digest="sha256:" + "e" * 64,
        accepted_by_authority_ref="authority:lifecycle",
    )
    recorded = await run_service.execute(
        command(
            run_id,
            2,
            "accept-evidence",
            RecordObligationEvidenceAction(evidence=evidence),
        )
    )
    assert recorded.status == CommandStatus.ACCEPTED
    await run_service.execute(
        command(
            run_id,
            3,
            "release-budget",
            RecordUsageAction(
                usage_id="release-evidence-run",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        )
    )
    evidence_digest = sha256_digest([evidence.model_dump(mode="json")])
    evidence_frontier = (await run_service.get_run("tenant-1", run_id)).evidence_frontier_digest
    terminal = await run_service.execute(
        command(
            run_id,
            4,
            "terminalize-evidence-run",
            TerminalizeAction(
                proposal=TerminalizationProposal(
                    proposal_id="terminal-evidence",
                    expected_run_version=4,
                    workflow_type_digest=WORKFLOW_DIGEST,
                    obligation_revision="obligations:1",
                    evidence_frontier_digest=evidence_frontier,
                    accepted_obligation_evidence_digest=evidence_digest,
                    proposing_execution_binding_ref="execution:test",
                    required_obligations_accepted=True,
                    budget_settled=True,
                    effects_settled=True,
                    proposed_at=NOW,
                )
            ),
        )
    )
    assert terminal.status == CommandStatus.ACCEPTED
    assert terminal.terminal_outcome == RunOutcome.COMPLETED


# --- RRM-004: `reconcile_unit` and the `operator_reconciliation` wait (CON-CP-LIFECYCLE-V1) ---

UNIT_KEY = "bl-unit-v1:" + "e" * 64
# `reconcile_unit` is privileged: the ordinary operator fixture does not hold it.
RECONCILE_PERMISSIONS = ALL_PERMISSIONS | {"workflow_run.reconcile_unit"}


def reconciler_command(
    run_id: str, version: int, command_id: str, action: object
) -> LifecycleCommand:
    return command(run_id, version, command_id, action).model_copy(
        update={"actor": actor().model_copy(update={"permissions": RECONCILE_PERMISSIONS})}
    )
INCIDENT_ID = "unit-in-doubt:fixture"


def operator_wait(unit_key: str = UNIT_KEY, generation: int = 1) -> WaitCondition:
    return WaitCondition(
        condition_id=operator_reconciliation_condition_id(unit_key, generation),
        kind="operator_reconciliation",
        scope=frozenset({unit_key}),
        verification_ref=INCIDENT_ID,
        timeout_policy_ref="operator-reconciliation:no-timeout",
    )


def reconcile(
    decision: str = "abandon_unit",
    *,
    generation: int = 1,
    incident_id: str = INCIDENT_ID,
    accepted: QualifiedCheckpointKey | None = None,
) -> ReconcileUnitAction:
    return ReconcileUnitAction(
        unit_key=UNIT_KEY,
        execution_generation=generation,
        incident_id=incident_id,
        decision=decision,  # type: ignore[arg-type]
        accepted_checkpoint=accepted,
    )


def set_wait(
    run_id: str, version: int, command_id: str, condition: WaitCondition, *, runnable: bool
) -> LifecycleCommand:
    return command(
        run_id,
        version,
        command_id,
        SetWaitAction(condition=condition, runnable_work_remains=runnable),
    )


async def _started_run(run_service: RunControlService, request_id: str) -> str:
    admitted = await run_service.admit(request(request_id=request_id))
    assert admitted.run_id is not None
    started = await run_service.execute(command(admitted.run_id, 1, "start", StartAction()))
    assert started.status == CommandStatus.ACCEPTED
    return admitted.run_id


@pytest.mark.asyncio
async def test_in_doubt_unit_keeps_the_run_phase_until_reconcile_unit_decides() -> None:
    """REQ-CP-RUN-007: the run stays in its phase with an `operator_reconciliation` wait,
    which only the typed, privileged `reconcile_unit` command releases."""

    run_service, _repository = service()
    run_id = await _started_run(run_service, "reconcile-unit")
    parked = await run_service.execute(
        set_wait(run_id, 2, "park", operator_wait(), runnable=False)
    )
    assert parked.status == CommandStatus.ACCEPTED
    assert parked.phase == RunPhase.ACTIVE, "an in_doubt unit does not change the run phase"

    released = await run_service.execute(
        command(
            run_id,
            3,
            "satisfy-operator-wait",
            SatisfyWaitAction(
                condition_id=operator_wait().condition_id, verification_evidence_ref="evidence"
            ),
        )
    )
    assert released.status == CommandStatus.REJECTED
    assert released.reason_code == "operator_decision_required"

    for command_id, action in (
        ("reconcile-other-incident", reconcile(incident_id="unit-in-doubt:other")),
        ("reconcile-other-generation", reconcile(generation=2)),
    ):
        rejected = await run_service.execute(reconciler_command(run_id, 3, command_id, action))
        assert rejected.status == CommandStatus.REJECTED
        assert rejected.reason_code == "reconciliation_not_pending"

    stale = await run_service.execute(
        reconciler_command(run_id, 2, "reconcile-stale", reconcile())
    )
    assert stale.status == CommandStatus.STALE

    # The ordinary operator fixture lacks the privileged permission.
    unprivileged = command(run_id, 3, "reconcile-unprivileged", reconcile())
    with pytest.raises(CommandRejected, match="reconcile_unit"):
        await run_service.execute(unprivileged)

    decided = await run_service.execute(
        reconciler_command(run_id, 3, "reconcile-abandon", reconcile())
    )
    assert decided.status == CommandStatus.ACCEPTED
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.active_waits == ()
    assert projection.phase == RunPhase.ACTIVE
    assert [
        (item.decision_id, item.unit_key, item.execution_generation, item.decision)
        for item in projection.unit_reconciliations
    ] == [("reconcile-abandon", UNIT_KEY, 1, "abandon_unit")]

    again = await run_service.execute(
        reconciler_command(run_id, 4, "reconcile-again", reconcile())
    )
    assert again.status == CommandStatus.REJECTED
    assert again.reason_code == "reconciliation_not_pending"
    replayed = await run_service.execute(
        reconciler_command(run_id, 3, "reconcile-abandon", reconcile())
    )
    assert replayed == decided, "the accepted decision is idempotent by command identity"


@pytest.mark.asyncio
async def test_operator_wait_preserves_cancelling_and_waiting_phases() -> None:
    """A cancelling run stays cancelling (`operator_required`); a run waiting on other
    conditions stays waiting after the unit's decision."""

    run_service, _repository = service()
    cancelling_run = await _started_run(run_service, "reconcile-cancelling")
    cancelled = await run_service.execute(command(cancelling_run, 2, "cancel", CancelAction()))
    assert cancelled.phase == RunPhase.CANCELLING
    parked = await run_service.execute(
        set_wait(cancelling_run, 3, "park", operator_wait(), runnable=True)
    )
    assert parked.status == CommandStatus.ACCEPTED and parked.phase == RunPhase.CANCELLING
    decided = await run_service.execute(
        reconciler_command(cancelling_run, 4, "decide", reconcile())
    )
    assert decided.status == CommandStatus.ACCEPTED and decided.phase == RunPhase.CANCELLING

    waiting_run = await _started_run(run_service, "reconcile-waiting")
    other = WaitCondition(
        condition_id="dependency:other",
        kind="dependency",
        scope=frozenset({"stage:other"}),
        verification_ref="dependency:other",
        timeout_policy_ref="timeout:none",
    )
    waiting = await run_service.execute(
        set_wait(waiting_run, 2, "wait-other", other, runnable=False)
    )
    assert waiting.phase == RunPhase.WAITING
    parked = await run_service.execute(
        set_wait(waiting_run, 3, "park", operator_wait(), runnable=True)
    )
    assert parked.phase == RunPhase.WAITING
    decided = await run_service.execute(
        reconciler_command(waiting_run, 4, "decide", reconcile())
    )
    assert decided.phase == RunPhase.WAITING
    projection = await run_service.get_run("tenant-1", waiting_run)
    assert [item.condition_id for item in projection.active_waits] == ["dependency:other"]


def test_reconcile_unit_decisions_are_typed() -> None:
    accepted = QualifiedCheckpointKey(
        checkpointer_ref_digest=DIGEST,
        thread_id="belllabs/stage/unit/gen/1",
        checkpoint_id="checkpoint-1",
        parent_checkpoint_id="checkpoint-0",
    )
    assert reconcile("accept_descendant", accepted=accepted).accepted_checkpoint == accepted
    with pytest.raises(ValueError, match="accepted checkpoint"):
        reconcile("accept_descendant")
    with pytest.raises(ValueError, match="accepted checkpoint"):
        reconcile("start_new_generation", accepted=accepted)
    with pytest.raises(ValueError, match="root-namespace"):
        reconcile(
            "accept_descendant", accepted=accepted.model_copy(update={"checkpoint_ns": "sub"})
        )


@pytest.mark.asyncio
async def test_command_fingerprint_is_independent_of_set_iteration_order() -> None:
    """RRM-004 root cause of the intermittent RRM-003 merge-gate failure.

    `model_dump(mode="json")` lists a `frozenset` in iteration order, which depends on the
    per-process string-hash seed and on how the set was built. Run control fingerprinted a
    revalidated copy of each command while the journal coordinator re-fingerprinted its own
    instance, so an exact replay failed for some `PYTHONHASHSEED` values ("stored operation
    authority result conflicts with exact replay command"). Fingerprints now sort sets.
    """

    from app.domain.control_plane.canonical import contract_fingerprint

    first_order, second_order = equal_sets_with_different_iteration_order(
        ALL_PERMISSIONS, probe_prefix="workflow_run.fingerprint_probe"
    )
    first = command("run-x", 1, "fingerprint", StartAction()).model_copy(
        update={
            "actor": ActorContext.model_construct(
                actor_id="operator",
                authority_refs=frozenset({"authority:lifecycle"}),
                permissions=first_order,
            )
        }
    )
    second = first.model_copy(
        update={"actor": first.actor.model_copy(update={"permissions": second_order})}
    )
    assert first == second
    # The defect: JSON-mode dumps of equal commands differ, so their digests differ.
    assert sha256_digest(first.model_dump(mode="json")) != sha256_digest(
        second.model_dump(mode="json")
    )
    assert contract_fingerprint(first, exclude={"occurred_at"}) == contract_fingerprint(
        second, exclude={"occurred_at"}
    )

    # End to end: an exact replay rebuilt with a differently ordered actor is accepted.
    run_service, _repository = service()
    admitted = await run_service.admit(request(request_id="fingerprint-order"))
    assert admitted.run_id is not None
    ordered_actor = ActorContext.model_construct(
        actor_id="operator",
        authority_refs=frozenset({"authority:lifecycle"}),
        permissions=first_order,
    )
    started = await run_service.execute(
        command(admitted.run_id, 1, "start", StartAction()).model_copy(
            update={"actor": ordered_actor}
        )
    )
    replay = await run_service.execute(
        command(admitted.run_id, 1, "start", StartAction()).model_copy(
            update={"actor": ordered_actor.model_copy(update={"permissions": second_order})}
        )
    )
    assert replay == started
