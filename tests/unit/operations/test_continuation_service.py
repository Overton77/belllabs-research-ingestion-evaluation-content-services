"""FT-B4: the continuation saga (trigger, seal, failure policy, governors, transfer)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from mission_control.application.context.continuation import (
    ContinuationRejected,
    HydrationReceipt,
    HydrationRequest,
    TransferStatus,
)
from mission_control.domain.context.checkpoint import (
    CHECKPOINT_INVALID,
    CompactionFailurePolicy,
    CompactorSynthesis,
    ContinuationGovernorPolicy,
    ContinuationTriggerKind,
)
from mission_control.domain.policies.contracts import RecordContinuationAction
from tests.fixtures.continuation import (
    CONT_SCOPE,
    RUN_KEY,
    build_service,
    facts,
    seal_target,
    trigger,
)

SESSION_FILES = {
    "thread-collect-1": {
        "/inputs/sources/source_manifest.json": '{"records": 180}',
        "/outputs/evidence_map.md": "# draft",
    }
}


async def _request(service: Any, **overrides: Any) -> Any:
    values: dict[str, Any] = {
        "request_scope": CONT_SCOPE,
        "run_key": RUN_KEY,
        "activation_key": "unit-collect-1",
        "logical_execution_id": "logical-collect-1",
        "lane_profile": "deep_agents",
        "source_session_ref": "thread-collect-1",
    }
    values.update(overrides)
    return await service.request(trigger(), **values)


@dataclass
class FakeHydrator:
    corrupt: bool = False
    requests: list[HydrationRequest] = field(default_factory=list)

    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt:
        self.requests.append(request)
        restored = dict(request.snapshot.manifest)
        if self.corrupt:
            restored["/outputs/evidence_map.md"] = "sha256:" + "f" * 64
        return HydrationReceipt(
            target_session_ref="thread-collect-1~continuation~x", restored=restored
        )


@dataclass
class Confirmation:
    initialized: bool = False

    async def session_initialized(self, request_scope: str, run_key: str, target: str) -> bool:
        return self.initialized


class FailingCompactor:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def synthesize(self, facts: Any, *, compactor: Any, attempt: int) -> CompactorSynthesis:
        self.calls.append(compactor.ref)
        raise RuntimeError("compactor crashed")


class GoodCompactor:
    async def synthesize(self, facts: Any, *, compactor: Any, attempt: int) -> CompactorSynthesis:
        return CompactorSynthesis(
            compactor_ref=compactor.ref,
            rationale_by_decision={"decision://scope-muscle-aging": "chosen for coverage"},
            recommended_next_actions=("cite the 12 strongest trials",),
        )


@pytest.mark.asyncio
async def test_trigger_is_idempotent_and_carries_its_lane_delivery() -> None:
    wired = build_service(session_files=SESSION_FILES)
    first = await _request(wired["service"])
    again = await _request(wired["service"])
    assert first == again
    assert first.delivery == "turn_boundary_guaranteed"
    assert first.status == TransferStatus.REQUESTED
    cursor = await _request(wired["service"], lane_profile="cursor_local")
    assert cursor.transfer_id == first.transfer_id  # same trigger, same logical execution
    pending = await wired["service"].pending(CONT_SCOPE, RUN_KEY)
    assert [item.transfer_id for item in pending] == [first.transfer_id]


@pytest.mark.asyncio
async def test_admitted_compactor_output_is_merged_and_validated() -> None:
    wired = build_service(session_files=SESSION_FILES, compactor=GoodCompactor())
    transfer = await _request(wired["service"])
    outcome = await wired["service"].seal(
        transfer.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE
    )
    checkpoint = outcome.checkpoint
    assert checkpoint.valid
    assert checkpoint.decisions[0].rationale_summary == "chosen for coverage"
    assert checkpoint.recommended_next_actions == (
        "draft evidence map",
        "cite the 12 strongest trials",
    )
    assert checkpoint.compactor.ref == "mc.admitted_compactor"


@pytest.mark.asyncio
async def test_failed_compaction_parks_retries_falls_back_then_fails_explicitly() -> None:
    compactor = FailingCompactor()
    wired = build_service(
        session_files=SESSION_FILES,
        compactor=compactor,
        failure_policy=CompactionFailurePolicy(
            compactor_retries=1, fallback_compactor_ref="mc.fallback_compactor"
        ),
        governors=ContinuationGovernorPolicy(max_failed_compactions=10),
    )
    transfer = await _request(wired["service"])
    outcome = await wired["service"].seal(
        transfer.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE
    )
    assert outcome.checkpoint is None
    assert outcome.transfer.status == TransferStatus.FAILED
    assert compactor.calls == [
        "mc.admitted_compactor",
        "mc.admitted_compactor",
        "mc.fallback_compactor",
    ]
    assert [item.result for item in outcome.transfer.attempts] == ["error", "error", "error"]
    actions: list[RecordContinuationAction] = wired["events"].actions
    assert [item.event for item in actions] == ["continuation_failed"]
    # A fresh session never starts without a sealed valid checkpoint.
    with pytest.raises(ContinuationRejected) as raised:
        await wired["service"].transfer(
            transfer.transfer_id, FakeHydrator(), request_scope=CONT_SCOPE
        )
    assert raised.value.code == CHECKPOINT_INVALID


@pytest.mark.asyncio
async def test_human_review_when_policy_requires_it() -> None:
    wired = build_service(
        session_files=SESSION_FILES,
        compactor=FailingCompactor(),
        failure_policy=CompactionFailurePolicy(compactor_retries=0, human_review_required=True),
    )
    transfer = await _request(wired["service"])
    outcome = await wired["service"].seal(
        transfer.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE
    )
    assert outcome.transfer.status == TransferStatus.HUMAN_REVIEW


@pytest.mark.asyncio
async def test_invalid_checkpoints_are_sealed_as_evidence_but_never_transferred() -> None:
    # The ledger names a workspace path the continuation packet cannot restore.
    wired = build_service(
        session_files={"thread-collect-1": {"/scratch/notes.md": "x"}},
        failure_policy=CompactionFailurePolicy(compactor_retries=0),
    )
    transfer = await _request(wired["service"])
    outcome = await wired["service"].seal(
        transfer.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE
    )
    assert outcome.transfer.status == TransferStatus.FAILED
    stored = await wired["service"].checkpoints(CONT_SCOPE, RUN_KEY)
    assert len(stored) == 1 and stored[0].checkpoint.validator.result == "invalid"
    events = [(item.event, item.validator_result) for item in wired["events"].actions]
    assert events == [("checkpoint_sealed", "invalid"), ("continuation_failed", None)]


@pytest.mark.asyncio
async def test_governor_exhaustion_is_a_governed_terminal_outcome() -> None:
    wired = build_service(
        session_files=SESSION_FILES, governors=ContinuationGovernorPolicy(max_transfers=0)
    )
    transfer = await _request(wired["service"])
    outcome = await wired["service"].seal(
        transfer.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE
    )
    assert outcome.transfer.status == TransferStatus.GOVERNOR_EXHAUSTED
    assert outcome.transfer.failure_reason == "continuation_governor_exhausted: max_transfers"
    assert wired["events"].actions[-1].failure_reason.startswith("continuation_governor_exhausted")


@pytest.mark.asyncio
async def test_transfer_hydrates_writes_transferred_and_releases_after_session_init() -> None:
    confirmation = Confirmation()
    wired = build_service(
        session_files=SESSION_FILES,
        pending_commands=("cmd-held-1",),
        confirmation=confirmation,
    )
    service = wired["service"]
    transfer = await _request(service)
    sealed = await service.seal(
        transfer.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE
    )
    hydrator = FakeHydrator()
    outcome = await service.transfer(transfer.transfer_id, hydrator, request_scope=CONT_SCOPE)
    request = hydrator.requests[0]
    assert request.checkpoint.checkpoint_id == sealed.checkpoint.checkpoint_id
    assert "pending_commitments" in request.prompt_text or "Pending" in request.prompt_text
    assert set(request.mission_files) == {".mission/context.md", ".mission/inputs.json"}
    assert outcome.transfer.status == TransferStatus.TRANSFERRED
    assert outcome.transfer.target_session_ref == "thread-collect-1~continuation~x"
    assert outcome.transfer.ledger.transfers == 1
    transferred = wired["events"].actions[-1]
    assert transferred.event == "transferred"
    assert transferred.source_session_ref == "thread-collect-1"
    assert transferred.target_session_ref == "thread-collect-1~continuation~x"
    # Held until the target's first session_init frame exists.
    assert not outcome.transfer.released
    assert wired["mailbox"].released == {}
    confirmation.initialized = True
    released = await service.release_if_hydrated(transfer.transfer_id, request_scope=CONT_SCOPE)
    assert released.released
    assert wired["mailbox"].released[transfer.transfer_id] == ("cmd-held-1",)
    # Replays are no-ops.
    again = await service.transfer(transfer.transfer_id, hydrator, request_scope=CONT_SCOPE)
    assert again.transfer.status == TransferStatus.TRANSFERRED and len(hydrator.requests) == 1


@pytest.mark.asyncio
async def test_restored_digest_mismatch_fails_the_transfer_with_checkpoint_invalid() -> None:
    wired = build_service(session_files=SESSION_FILES)
    service = wired["service"]
    transfer = await _request(service)
    await service.seal(transfer.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE)
    with pytest.raises(ContinuationRejected) as raised:
        await service.transfer(
            transfer.transfer_id, FakeHydrator(corrupt=True), request_scope=CONT_SCOPE
        )
    assert raised.value.code == CHECKPOINT_INVALID
    stored = await wired["store"].get(CONT_SCOPE, transfer.transfer_id)
    assert stored.status == TransferStatus.FAILED
    assert stored.failure_reason.startswith(CHECKPOINT_INVALID)


@pytest.mark.asyncio
async def test_second_transfer_counts_against_governors() -> None:
    wired = build_service(
        session_files=SESSION_FILES,
        governors=ContinuationGovernorPolicy(max_transfers=1),
    )
    service = wired["service"]
    first = await _request(service)
    await service.seal(first.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE)
    await service.transfer(first.transfer_id, FakeHydrator(), request_scope=CONT_SCOPE)
    second = await service.request(
        trigger(ContinuationTriggerKind.TURN_COUNT, "policy://turns/40"),
        request_scope=CONT_SCOPE,
        run_key=RUN_KEY,
        activation_key="unit-collect-1",
        logical_execution_id="logical-collect-1",
        lane_profile="deep_agents",
        source_session_ref="thread-collect-1~continuation~x",
    )
    assert second.ledger.transfers == 1
    outcome = await service.seal(
        second.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE
    )
    assert outcome.transfer.status == TransferStatus.GOVERNOR_EXHAUSTED
