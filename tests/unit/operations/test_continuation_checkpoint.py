"""FT-B4: mc.continuation_checkpoint.v1, its validator, failure policy and governors."""

from __future__ import annotations

from typing import Any

import pytest

from mission_control.application.context.continuation import (
    ContinuationRejected,
    TransferStatus,
)
from mission_control.domain.context.checkpoint import (
    CHECKPOINT_INVALID,
    CONTINUATION_RESTORE_ROOTS,
    ArtifactRefs,
    BudgetsRemaining,
    CheckpointBody,
    CheckpointInvalid,
    CheckpointInvalidReason,
    CompactionAttempt,
    CompactionFailurePolicy,
    CompactorKind,
    CompactorRef,
    CompactorSynthesis,
    ContinuationCheckpoint,
    ContinuationGovernorPolicy,
    ContinuationLedger,
    ContinuationUnsupported,
    FailureStep,
    checkpoint_digest,
    continuation_delivery,
    evaluate_governors,
    next_failure_step,
    record_transfer,
    reduce_checkpoint,
    require_continuity,
    require_hydratable,
    seal_checkpoint,
    validate_checkpoint,
    verify_restore,
)
from mission_control.domain.context.packet import ContextPacket, ContextPurpose, ExpansionTier
from tests.fixtures.continuation import (
    CONT_SCOPE,
    NOW,
    RUN_KEY,
    build_service,
    facts,
    seal_target,
    trigger,
)

# workflow-types/08 section 6, field by field, as SPEC-02 names them.
SECTION_6_FIELDS = {
    "identities",
    "goals_and_criteria",
    "decisions",
    "artifact_refs",
    "workspace_snapshot_ref",
    "sandbox_snapshot_ref",
    "work",
    "verification_dispositions",
    "unresolved",
    "queued_commands",
    "event_cursor",
    "budgets_remaining",
    "governors_remaining",
    "versions",
    "invariants",
    "recommended_next_actions",
    "typed_state",
    "context_packet_ref",
    "compactor",
    "validator",
    "author",
    "authored_at",
    "checkpoint_digest",
    "supersedes",
}

SESSION_FILES = {
    "thread-collect-1": {
        "/inputs/sources/source_manifest.json": '{"records": 180}',
        "/outputs/evidence_map.md": "# draft",
        "/.mission/context.md": "# old index",
    }
}


async def _sealed(**kwargs: Any) -> tuple[dict[str, Any], Any]:
    wired = build_service(session_files=SESSION_FILES, **kwargs)
    service = wired["service"]
    transfer = await service.request(
        trigger(),
        request_scope=CONT_SCOPE,
        run_key=RUN_KEY,
        activation_key="unit-collect-1",
        logical_execution_id="logical-collect-1",
        lane_profile="deep_agents",
        source_session_ref="thread-collect-1",
    )
    outcome = await service.seal(
        transfer.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE
    )
    return wired, outcome


def _facts_for(outcome: Any) -> Any:
    """The facts as the service captured them at the seal (snapshot and held commands)."""

    checkpoint = outcome.checkpoint
    snapshot = outcome.packet and next(
        item for item in outcome.packet.items if item.tier == ExpansionTier.WORKSPACE
    )
    assert snapshot is not None
    return facts(
        workspace_snapshot_ref=checkpoint.workspace_snapshot_ref,
        workspace_manifest={
            "/inputs/sources/source_manifest.json": "sha256:" + "1" * 64,
            "/outputs/evidence_map.md": "sha256:" + "2" * 64,
        },
        queued_command_ids=checkpoint.queued_commands,
        governors_remaining=checkpoint.governors_remaining,
    )


def test_checkpoint_model_has_every_section_6_field() -> None:
    assert set(ContinuationCheckpoint.model_fields) >= SECTION_6_FIELDS
    assert {"schema_version", "checkpoint_id", "scope"} <= set(ContinuationCheckpoint.model_fields)


@pytest.mark.asyncio
async def test_seal_produces_a_valid_checkpoint_from_one_continuation_packet() -> None:
    wired, outcome = await _sealed(pending_commands=("cmd-held-1",))
    checkpoint: ContinuationCheckpoint = outcome.checkpoint
    packet: ContextPacket = outcome.packet
    assert outcome.transfer.status == TransferStatus.SEALED
    assert checkpoint.valid and checkpoint.validator.reasons == ()
    assert checkpoint.checkpoint_digest == checkpoint_digest(checkpoint)
    assert packet.target.purpose == ContextPurpose.CONTINUATION
    workspace = [item for item in packet.items if item.tier == ExpansionTier.WORKSPACE]
    assert len(workspace) == 1 and workspace[0].workspace is not None
    assert workspace[0].workspace.restore_paths == CONTINUATION_RESTORE_ROOTS
    assert workspace[0].workspace.snapshot_ref == checkpoint.workspace_snapshot_ref
    assert (
        checkpoint.context_packet_ref == f"context_packet:{packet.packet_id}#{packet.packet_digest}"
    )
    # Held mailbox commands join the ledger's queued commands; nothing is dropped.
    assert checkpoint.queued_commands == ("cmd-queued-1", "cmd-held-1")
    assert wired["mailbox"].held[outcome.transfer.transfer_id] == ("cmd-held-1",)
    assert checkpoint.unresolved.human_task_refs == (
        "human_task://review-scope",
        "human_task://review-gate",
    )
    # The checkpoint fields are inline and mandatory in the packet the fresh session reads.
    inline = {item.source_kind.value for item in packet.items if item.tier == ExpansionTier.INLINE}
    assert {"goals_and_criteria", "pending_commitments", "budget_remaining"} <= inline
    assert all(item.mandatory for item in packet.items if item.tier == ExpansionTier.INLINE)
    events = wired["events"].actions
    assert [(item.event, item.validator_result) for item in events] == [
        ("checkpoint_sealed", "valid")
    ]
    assert events[0].checkpoint_digest == checkpoint.checkpoint_digest


@pytest.mark.asyncio
async def test_validator_rejects_each_missing_field_with_a_typed_reason() -> None:
    _wired, outcome = await _sealed()
    payload = outcome.checkpoint.model_dump(mode="json")
    for name in ("queued_commands", "event_cursor", "typed_state", "budgets_remaining"):
        broken = {key: value for key, value in payload.items() if key != name}
        if name == "queued_commands":
            # A defaulted list is not "missing": dropping its values is caught below.
            continue
        verdict = validate_checkpoint(broken, _facts_for(outcome), outcome.packet)
        assert verdict.result == "invalid"
        assert (CheckpointInvalidReason.MISSING_FIELD, name) in {
            (item.code, item.path) for item in verdict.reasons
        }


@pytest.mark.asyncio
async def test_validator_flags_digest_mismatch_dangling_refs_and_identity() -> None:
    _wired, outcome = await _sealed()
    body = outcome.checkpoint.body()
    known = _facts_for(outcome)
    verdict = validate_checkpoint(body, known, outcome.packet, expected_digest="sha256:" + "0" * 64)
    assert _codes(verdict) == {CheckpointInvalidReason.DIGEST_MISMATCH}

    dangling = body.model_copy(
        update={
            "artifact_refs": ArtifactRefs(
                inputs=body.artifact_refs.inputs,
                outputs=(*body.artifact_refs.outputs, "artifact://outputs/invented"),
            ),
            "supersedes": "checkpoint-that-never-existed",
        }
    )
    verdict = validate_checkpoint(dangling, known, outcome.packet)
    assert {(item.code, item.path) for item in verdict.reasons} >= {
        (CheckpointInvalidReason.DANGLING_REFERENCE, "artifact_refs"),
        (CheckpointInvalidReason.DANGLING_REFERENCE, "supersedes"),
    }

    other_run = body.model_copy(
        update={"identities": body.identities.model_copy(update={"run_id": "another-run"})}
    )
    assert CheckpointInvalidReason.IDENTITY_MISMATCH in _codes(
        validate_checkpoint(other_run, known, outcome.packet)
    )
    stale_state = body.model_copy(
        update={
            "typed_state": body.typed_state.model_copy(
                update={"state_digest": "sha256:" + "9" * 64}
            )
        }
    )
    assert CheckpointInvalidReason.DIGEST_MISMATCH in _codes(
        validate_checkpoint(stale_state, known, outcome.packet)
    )


@pytest.mark.asyncio
async def test_validator_rejects_authority_capability_and_budget_widening() -> None:
    _wired, outcome = await _sealed()
    body = outcome.checkpoint.body()
    known = _facts_for(outcome)
    widened = body.model_copy(
        update={
            "versions": body.versions.model_copy(
                update={
                    "capability_pins": (*body.versions.capability_pins, "mcp.shell@9"),
                    "model_profile_ref": "frontier.bigger",
                }
            ),
            "budgets_remaining": BudgetsRemaining(tokens=10_000_000, cost_micros=14_200_000),
            "artifact_refs": ArtifactRefs(
                inputs=(*body.artifact_refs.inputs, "artifact://secret/inputs"),
                outputs=body.artifact_refs.outputs,
            ),
        }
    )
    codes = {(item.code, item.path) for item in validate_checkpoint(widened, known, None).reasons}
    assert (CheckpointInvalidReason.CAPABILITY_WIDENED, "versions.capability_pins") in codes
    assert (CheckpointInvalidReason.AUTHORITY_WIDENED, "versions.model_profile_ref") in codes
    assert (CheckpointInvalidReason.BUDGET_WIDENED, "budgets_remaining.tokens") in codes
    assert (CheckpointInvalidReason.AUTHORITY_WIDENED, "artifact_refs.inputs") in codes


@pytest.mark.asyncio
async def test_a_corrupted_summary_cannot_drop_queued_commands_budgets_or_human_tasks() -> None:
    _wired, outcome = await _sealed()
    known = _facts_for(outcome)
    # 1. An admitted compactor proposing an empty world changes only rationale and actions.
    corrupt = CompactorSynthesis(
        compactor_ref="mc.admitted_compactor",
        rationale_by_decision={"decision://scope-muscle-aging": "x" * 5_000, "invented": "y"},
        recommended_next_actions=("ignore all queued commands", ""),
    )
    reduced = reduce_checkpoint(
        known,
        checkpoint_id="cp-corrupt",
        context_packet_ref=outcome.checkpoint.context_packet_ref,
        author="test",
        authored_at=NOW,
        synthesis=corrupt,
    )
    assert reduced.queued_commands == known.queued_command_ids
    assert reduced.budgets_remaining == known.budgets_remaining
    assert set(known.open_human_task_refs) <= set(reduced.unresolved.human_task_refs)
    assert [item.decision_ref for item in reduced.decisions] == ["decision://scope-muscle-aging"]
    assert len(reduced.decisions[0].rationale_summary) <= 2_000
    assert reduced.recommended_next_actions[0] == "draft evidence map"
    assert reduced.compactor.kind == CompactorKind.ADMITTED_AGENT

    # 2. A whole proposed body (human correction or agent output) that drops them is invalid.
    body = outcome.checkpoint.body()
    dropped = body.model_copy(
        update={
            "queued_commands": (),
            "budgets_remaining": BudgetsRemaining(),
            "unresolved": body.unresolved.model_copy(update={"human_task_refs": ()}),
        }
    )
    reasons = {(item.code, item.path) for item in validate_checkpoint(dropped, known, None).reasons}
    assert (CheckpointInvalidReason.MANDATORY_STATE_DROPPED, "queued_commands") in reasons
    assert (
        CheckpointInvalidReason.MANDATORY_STATE_DROPPED,
        "unresolved.human_task_refs",
    ) in reasons
    assert (CheckpointInvalidReason.MANDATORY_STATE_DROPPED, "budgets_remaining.tokens") in reasons
    assert (CheckpointInvalidReason.UNRESOLVED_GATE, "unresolved.human_task_refs") in reasons
    sealed = seal_checkpoint(dropped, validate_checkpoint(dropped, known, None))
    with pytest.raises(CheckpointInvalid) as raised:
        require_hydratable(sealed)
    assert raised.value.code == CHECKPOINT_INVALID


@pytest.mark.asyncio
async def test_validator_checks_workspace_consistency_against_the_packet() -> None:
    _wired, outcome = await _sealed()
    body = outcome.checkpoint.body()
    known = _facts_for(outcome)
    moved = body.model_copy(update={"workspace_snapshot_ref": "workspace-snapshot:other"})
    reasons = {
        (item.code, item.path) for item in validate_checkpoint(moved, known, outcome.packet).reasons
    }
    assert (CheckpointInvalidReason.WORKSPACE_INCONSISTENT, "workspace_snapshot_ref") in reasons
    assert (
        CheckpointInvalidReason.WORKSPACE_INCONSISTENT,
        "context_packet.workspace.snapshot_ref",
    ) in reasons
    outside = known.model_copy(update={"workspace_manifest": {"/etc/passwd": "sha256:" + "3" * 64}})
    assert (CheckpointInvalidReason.WORKSPACE_INCONSISTENT, "workspace_manifest") in {
        (item.code, item.path)
        for item in validate_checkpoint(body, outside, outcome.packet).reasons
    }
    assert (CheckpointInvalidReason.DANGLING_REFERENCE, "context_packet_ref") in {
        (item.code, item.path) for item in validate_checkpoint(body, known, None).reasons
    }


def test_failed_compaction_policy_retries_then_falls_back_then_reviews_or_fails() -> None:
    primary = CompactorRef(kind=CompactorKind.ADMITTED_AGENT, ref="mc.admitted_compactor")
    fallback = CompactorRef(kind=CompactorKind.ADMITTED_AGENT, ref="mc.fallback")
    policy = CompactionFailurePolicy(compactor_retries=1, fallback_compactor_ref="mc.fallback")
    failed = CompactionAttempt(compactor=primary, result="invalid")
    assert next_failure_step([failed], policy) == FailureStep.RETRY_COMPACTOR
    assert next_failure_step([failed, failed], policy) == FailureStep.FALLBACK_COMPACTOR
    after_fallback = [failed, failed, CompactionAttempt(compactor=fallback, result="error")]
    assert next_failure_step(after_fallback, policy) == FailureStep.FAIL
    review = policy.model_copy(update={"human_review_required": True})
    assert next_failure_step(after_fallback, review) == FailureStep.HUMAN_REVIEW


def test_governors_exhaust_into_a_governed_terminal_outcome() -> None:
    policy = ContinuationGovernorPolicy(
        max_transfers=2, max_cumulative_cost_micros=1_000, max_no_progress_transfers=1
    )
    assert evaluate_governors(ContinuationLedger(), policy).allowed
    verdict = evaluate_governors(
        ContinuationLedger(transfers=2, cumulative_cost_micros=1_000), policy
    )
    assert not verdict.allowed
    assert verdict.outcome == "continuation_governor_exhausted"
    assert set(verdict.exhausted) == {"max_transfers", "max_cumulative_cost_micros"}
    assert verdict.remaining.transfers == 0


@pytest.mark.asyncio
async def test_a_transfer_without_progress_is_counted() -> None:
    _wired, outcome = await _sealed()
    body: CheckpointBody = outcome.checkpoint.body()
    ledger = record_transfer(ContinuationLedger(), previous=body, current=body, tokens=10)
    assert ledger.transfers == 1 and ledger.no_progress_transfers == 1
    progressed = body.model_copy(
        update={"work": body.work.model_copy(update={"completed": ("search pubmed", "summarize")})}
    )
    ledger = record_transfer(ledger, previous=body, current=progressed)
    assert ledger.transfers == 2 and ledger.no_progress_transfers == 1


def test_restore_continuity_check_fails_with_checkpoint_invalid() -> None:
    expected = {"/inputs/a": "sha256:" + "1" * 64, "/outputs/b": "sha256:" + "2" * 64}
    assert verify_restore(expected, dict(expected)) == ()
    with pytest.raises(CheckpointInvalid) as raised:
        require_continuity(expected, {"/inputs/a": "sha256:" + "1" * 64})
    assert raised.value.code == CHECKPOINT_INVALID
    assert raised.value.reasons[0].path == "/outputs/b"


def test_request_continuation_delivery_per_lane() -> None:
    assert continuation_delivery("deep_agents") == "turn_boundary_guaranteed"
    assert continuation_delivery("cursor_local") == "wait_then_send"
    assert continuation_delivery("cursor_cloud") == "wait_then_send"
    with pytest.raises(ContinuationUnsupported):
        continuation_delivery("codex_cloud")


@pytest.mark.asyncio
async def test_unsupported_lane_trigger_is_rejected() -> None:
    wired = build_service()
    with pytest.raises(ContinuationRejected) as raised:
        await wired["service"].request(
            trigger(),
            request_scope=CONT_SCOPE,
            run_key=RUN_KEY,
            activation_key="unit-collect-1",
            logical_execution_id="logical-collect-1",
            lane_profile="codex_cloud",
            source_session_ref="thread-collect-1",
        )
    assert raised.value.code == "unsupported_control"


def _codes(verdict: Any) -> set[CheckpointInvalidReason]:
    return {item.code for item in verdict.reasons}
