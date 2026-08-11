from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.application.run_control import RunControlOutboxRelay
from app.application.run_control_repository import InMemoryRunControlRepository
from app.domain.control_plane.canonical import canonical_json, sha256_digest
from app.domain.run_control.contracts import (
    AsyncChildDecisionOutcome,
    AsyncChildDependencyClass,
    ClaimEffectAction,
    CommandStatus,
    DecideAsyncChildFactAction,
    EffectDisposition,
    EffectLedgerState,
    EffectSettlementOutcome,
    ObserveEffectAction,
    RecordAsyncChildFactAction,
    RecordUsageAction,
    RegisterAsyncChildAction,
    RunOutcome,
    SettleEffectAction,
    StartAction,
    TerminalizationProposal,
    TerminalizeAction,
)
from tests.test_run_control import (
    EMPTY_EVIDENCE_DIGEST,
    INITIAL_EVIDENCE_FRONTIER,
    WORKFLOW_DIGEST,
    command,
    request,
    service,
)

NOW = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)


def terminal_action(version: int, proposal_id: str = "terminal") -> TerminalizeAction:
    return TerminalizeAction(
        proposal=TerminalizationProposal(
            proposal_id=proposal_id,
            expected_run_version=version,
            workflow_type_digest=WORKFLOW_DIGEST,
            obligation_revision="obligations:1",
            evidence_frontier_digest=INITIAL_EVIDENCE_FRONTIER,
            accepted_obligation_evidence_digest=EMPTY_EVIDENCE_DIGEST,
            proposing_execution_binding_ref="binding:qualification",
            required_obligations_accepted=True,
            budget_settled=True,
            effects_settled=True,
            proposed_at=NOW,
        )
    )


@pytest.mark.asyncio
async def test_effect_ambiguity_requires_one_authoritative_settlement() -> None:
    run_service, _repository = service()
    admission = await run_service.admit(request(request_id="effect-ambiguity"))
    assert admission.run_id is not None
    run_id = admission.run_id

    await run_service.execute(command(run_id, 1, "start", StartAction()))
    claimed = await run_service.execute(
        command(
            run_id,
            2,
            "claim-effect",
            ClaimEffectAction(
                effect_id="effect:email:1",
                effect_kind="external.email.send",
                operation_ref="operation:1",
                provider_idempotency_key="provider-key-1",
                reservation_id="baseline",
            ),
        )
    )
    assert claimed.status == CommandStatus.ACCEPTED
    ambiguous = await run_service.execute(
        command(
            run_id,
            3,
            "observe-timeout",
            ObserveEffectAction(
                effect_id="effect:email:1",
                observation_id="observation:timeout",
                disposition=EffectDisposition.AMBIGUOUS,
                provider_effect_ref="provider:email:1",
            ),
        )
    )
    assert ambiguous.status == CommandStatus.ACCEPTED
    await run_service.execute(
        command(
            run_id,
            4,
            "release-baseline",
            RecordUsageAction(
                usage_id="usage:effect:1",
                authority_ref="operation:1",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        )
    )

    blocked = await run_service.execute(
        command(run_id, 5, "terminal-before-effect-settlement", terminal_action(5))
    )
    assert blocked.status == CommandStatus.REJECTED
    assert blocked.reason_code == "effects_not_settled"

    await run_service.execute(
        command(
            run_id,
            5,
            "observe-success",
            ObserveEffectAction(
                effect_id="effect:email:1",
                observation_id="observation:success",
                disposition=EffectDisposition.SUCCEEDED,
                provider_effect_ref="provider:email:1",
                evidence_refs=("evidence:provider-receipt",),
            ),
        )
    )
    settled = await run_service.execute(
        command(
            run_id,
            6,
            "settle-effect",
            SettleEffectAction(
                effect_id="effect:email:1",
                settlement_id="settlement:email:1",
                observation_id="observation:success",
                outcome=EffectSettlementOutcome.SUCCEEDED,
                usage_settlement_ref="usage:effect:1",
            ),
        )
    )
    assert settled.status == CommandStatus.ACCEPTED
    duplicate = await run_service.execute(
        command(
            run_id,
            7,
            "settle-effect-again",
            SettleEffectAction(
                effect_id="effect:email:1",
                settlement_id="settlement:email:2",
                observation_id="observation:success",
                outcome=EffectSettlementOutcome.SUCCEEDED,
                usage_settlement_ref="usage:effect:1",
            ),
        )
    )
    assert duplicate.status == CommandStatus.REJECTED
    assert duplicate.reason_code == "effect_already_settled"
    terminal = await run_service.execute(
        command(run_id, 7, "terminal-after-effect-settlement", terminal_action(7))
    )
    assert terminal.terminal_outcome == RunOutcome.COMPLETED
    assert [entry.kind for entry in await run_service.list_effect_ledger("tenant-1", run_id)] == [
        "claim",
        "observation",
        "observation",
        "settlement",
    ]


@pytest.mark.asyncio
async def test_async_child_fact_cannot_mutate_parent_without_parent_decision() -> None:
    run_service, _repository = service()
    admission = await run_service.admit(request(request_id="async-child-authority"))
    assert admission.run_id is not None
    run_id = admission.run_id
    await run_service.execute(command(run_id, 1, "start", StartAction()))
    await run_service.execute(
        command(
            run_id,
            2,
            "register-child",
            RegisterAsyncChildAction(
                child_execution_id="async-child:1",
                parent_operation_ref="operation:parent:1",
                dependency_class=AsyncChildDependencyClass.REQUIRED,
                reservation_id="baseline",
            ),
        )
    )
    observed = await run_service.execute(
        command(
            run_id,
            3,
            "observe-child-result",
            RecordAsyncChildFactAction(
                fact_id="child-fact:result:1",
                child_execution_id="async-child:1",
                fact_kind="result",
                lifecycle_status="completed",
                result_manifest_ref="manifest:child:1",
            ),
        )
    )
    projection = await run_service.get_run("tenant-1", run_id)
    assert observed.status == CommandStatus.ACCEPTED
    assert projection.phase.value == "active"
    assert projection.async_children[0].decisions == ()

    await run_service.execute(
        command(
            run_id,
            4,
            "release-child-budget",
            RecordUsageAction(
                usage_id="usage:child:1",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        )
    )
    blocked = await run_service.execute(
        command(run_id, 5, "terminal-before-child-decision", terminal_action(5))
    )
    assert blocked.reason_code == "unresolved_async_children"
    decided = await run_service.execute(
        command(
            run_id,
            5,
            "admit-child-result",
            DecideAsyncChildFactAction(
                decision_id="child-decision:1",
                fact_id="child-fact:result:1",
                outcome=AsyncChildDecisionOutcome.ACCEPTED,
                authority_ref="authority:lifecycle",
                reason="result manifest satisfies the parent dependency",
            ),
        )
    )
    assert decided.status == CommandStatus.ACCEPTED
    terminal = await run_service.execute(
        command(run_id, 6, "terminal-after-child-decision", terminal_action(6))
    )
    assert terminal.terminal_outcome == RunOutcome.COMPLETED


class AckFailureRepository(InMemoryRunControlRepository):
    def __init__(self) -> None:
        super().__init__()
        self.fail_ack_once = True

    async def mark_outbox_delivered(self, request_scope, event_id, delivered_at):  # type: ignore[no-untyped-def]
        if self.fail_ack_once:
            self.fail_ack_once = False
            raise RuntimeError("injected acknowledgement failure")
        await super().mark_outbox_delivered(request_scope, event_id, delivered_at)


class RecordingPublisher:
    def __init__(self) -> None:
        self.event_ids: list[str] = []

    async def publish(self, envelope) -> None:  # type: ignore[no-untyped-def]
        self.event_ids.append(envelope.event_id)


@pytest.mark.asyncio
async def test_outbox_publish_ack_ambiguity_redelivers_stable_event_identity() -> None:
    repository = AckFailureRepository()
    run_service, _repository = service(repository)
    await run_service.admit(request(request_id="relay-ambiguity"))
    publisher = RecordingPublisher()
    relay = RunControlOutboxRelay(run_service, publisher)

    with pytest.raises(RuntimeError, match="acknowledgement failure"):
        await relay.relay_pending("tenant-1", delivered_at=NOW)
    delivered = await relay.relay_pending("tenant-1", delivered_at=NOW)

    assert publisher.event_ids[0] == publisher.event_ids[1]
    assert len(delivered) == 2
    assert await run_service.pending_outbox("tenant-1") == ()


def test_domain_owner_has_no_runtime_or_persistence_authority_imports() -> None:
    root = Path(__file__).resolve().parents[3]
    domain_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((root / "app" / "domain" / "run_control").glob("*.py"))
    )
    forbidden = ("temporalio", "asyncpg", "beanie", "langgraph", "deepagents")
    assert not any(name in domain_source for name in forbidden)
    journaled_operation = (
        root / "app" / "application" / "journaled_operation_execution.py"
    ).read_text(encoding="utf-8")
    assert "reduce_lifecycle" not in journaled_operation
    assert all(
        command in journaled_operation
        for command in ("ClaimEffectAction", "ObserveEffectAction", "SettleEffectAction")
    )


def test_run_control_contracts_are_strict_canonical_and_secret_free() -> None:
    state = EffectLedgerState(run_id="run:canonical", claims={})
    assert canonical_json(state) == canonical_json(
        EffectLedgerState.model_validate({"claims": {}, "run_id": "run:canonical"})
    )
    assert sha256_digest(state) == sha256_digest(state.model_dump(mode="python"))
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        EffectLedgerState.model_validate(
            {"run_id": "run:canonical", "claims": {}, "unknown": True}
        )
    with pytest.raises(ValidationError, match="raw secrets, PHI, or content"):
        EffectLedgerState.model_validate(
            {
                "run_id": "run:canonical",
                "claims": {},
                "metadata": {"api_key": "must-not-persist"},
            }
        )
