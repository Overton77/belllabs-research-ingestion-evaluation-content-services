"""MP-10 Human Gate rules: identity, admitted answers, timeouts and outcome binding."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mission_control.domain.authoring.manifest import HumanGateNode
from mission_control.domain.execution.approvals import ADMITTED_DECISIONS
from mission_control.domain.programs.human_gate import (
    HumanGateBindingError,
    HumanGateSpec,
    bind_outcome,
    decide_resolution,
    default_resolution,
    feedback_instruction,
    gate_spec_from_manifest,
    outcome_for,
    stagegraph_gate_result,
    timeout_disposition,
)
from tests.unit.human_tasks.fixtures import (
    activation,
    later,
    open_task,
    request,
    spec,
)


def _decide(task, body, actor="owner", permissions=frozenset(), now=None):  # type: ignore[no-untyped-def]
    return decide_resolution(
        task, body, actor_id=actor, permissions=permissions, now=now or later(10)
    )


def test_one_deterministic_task_identity_per_activation_round_and_scope() -> None:
    first = activation()
    assert first.human_task_id == activation().human_task_id
    assert first.workflow_id == f"mc-human-gate:{first.human_task_id}"
    assert (
        first.human_task_id
        != activation(
            review_round=2, gate=spec(remediation_target="draft", max_review_rounds=2)
        ).human_task_id
    )
    assert first.human_task_id != activation(scope="tenant-2").human_task_id
    assert first.kind == "human_gate:REVIEW"
    assert first.target_ref == "run:run-1/gate:review"


def test_a_changed_artifact_is_a_different_review_packet() -> None:
    original = activation()
    changed = activation(digest="sha256:" + "e" * 64)
    assert original.packet_digest != changed.packet_digest
    # The embedded content digest is what the approval binds, not the ref text alone.
    assert original.packet[0].digest == "sha256:" + "d" * 64


def test_activation_rejects_tampered_digests_and_rounds() -> None:
    item = activation()
    payload = item.model_dump(mode="python")
    with pytest.raises(ValidationError, match="packet_digest"):
        type(item).model_validate({**payload, "packet_digest": "sha256:" + "0" * 64})
    with pytest.raises(ValidationError, match="permitted decisions"):
        type(item).model_validate({**payload, "permitted_decisions": ("approve",)})
    with pytest.raises(ValidationError, match="round governor"):
        activation(review_round=2)


def test_request_changes_only_with_a_declared_remediation_route_and_a_round_left() -> None:
    assert spec().permitted_decisions(1) == ("approve", "deny")
    bounded = spec(remediation_target="draft", max_review_rounds=2)
    assert bounded.permitted_decisions(1) == ("approve", "deny", "request_changes")
    assert bounded.permitted_decisions(2) == ("approve", "deny")
    assert set(bounded.permitted_decisions(1)) <= ADMITTED_DECISIONS["workflow_gate"]


def test_defaults_and_non_waiting_timeouts_must_be_explicit() -> None:
    with pytest.raises(ValidationError, match="explicitly declared default"):
        spec(on_timeout="default_answer", timeout_seconds=60)
    with pytest.raises(ValidationError, match="requires a timeout"):
        spec(on_timeout="stop")
    with pytest.raises(ValidationError, match="only under on_timeout default_answer"):
        spec(default_decision="approve")


def test_one_attributed_resolution_and_idempotent_retry() -> None:
    task = open_task(activation())
    accepted = _decide(task, request(task.activation))
    assert accepted.status == "accept" and accepted.resolution is not None
    assert accepted.resolution.actor_ref == "owner"
    assert accepted.resolution.resolution_action == "review_accept"
    assert accepted.resolution.review_decision == "approve"
    resolved = task.model_copy(
        update={"lifecycle": "resolved", "version": 2, "resolution": accepted.resolution}
    )
    retry = _decide(resolved, request(task.activation))
    assert retry.status == "duplicate" and retry.resolution == accepted.resolution
    second = _decide(resolved, request(task.activation, request_id="req-2", decision="deny"))
    assert (second.status, second.code) == ("reject", "already_resolved")
    other_reviewer = _decide(resolved, request(task.activation), actor="someone-else")
    assert (other_reviewer.status, other_reviewer.code) == ("reject", "already_resolved")


def test_reviewer_authority_is_the_principal_or_a_verified_reviewer_grant() -> None:
    task = open_task(activation())
    assert _decide(task, request(task.activation), actor="intruder").code == "not_reviewer"
    granted = _decide(
        task, request(task.activation), actor="alice", permissions=frozenset({"reviewer:owner"})
    )
    assert granted.status == "accept"


def test_stale_version_and_changed_packet_digest_cannot_reuse_an_approval() -> None:
    task = open_task(activation())
    stale = _decide(task, request(task.activation, expected_task_version=2))
    assert stale.code == "stale_version"
    old_packet = activation(digest="sha256:" + "a" * 64).packet_digest
    mismatch = _decide(task, request(task.activation, reviewed_packet_digest=old_packet))
    assert mismatch.code == "packet_digest_mismatch"


def test_decisions_outside_the_gate_and_feedback_free_changes_are_refused() -> None:
    task = open_task(activation())
    assert _decide(task, request(task.activation, decision="request_changes")).code == (
        "decision_not_admitted"
    )
    bounded = open_task(activation(gate=spec(remediation_target="draft", max_review_rounds=2)))
    assert (
        _decide(bounded, request(bounded.activation, decision="request_changes")).code
        == "feedback_required"
    )
    ok = _decide(
        bounded, request(bounded.activation, decision="request_changes", comment="cite sources")
    )
    assert ok.status == "accept" and ok.resolution is not None
    assert ok.resolution.resolution_action == "review_reject"
    assert ok.resolution.review_decision == "request_changes"


def test_keep_waiting_stays_answerable_after_the_deadline_and_is_never_approval() -> None:
    task = open_task(activation(gate=spec(timeout_seconds=60)))
    assert timeout_disposition(task, later(30)) == "none"
    assert timeout_disposition(task, later(61)) == "keep_waiting"
    assert outcome_for(task) is None
    assert _decide(task, request(task.activation), now=later(3600)).status == "accept"


def test_stop_expires_and_refuses_late_answers_and_default_needs_declaration() -> None:
    stopping = open_task(activation(gate=spec(timeout_seconds=60, on_timeout="stop")))
    assert timeout_disposition(stopping, later(61)) == "stop"
    late = _decide(stopping, request(stopping.activation), now=later(61))
    assert late.code == "deadline_passed"
    expired = stopping.model_copy(update={"lifecycle": "expired", "version": 2})
    outcome = outcome_for(expired)
    assert outcome is not None and outcome.status == "stopped_by_policy"
    with pytest.raises(HumanGateBindingError, match="no default"):
        default_resolution(stopping, later(61))
    defaulting = open_task(
        activation(
            gate=spec(timeout_seconds=60, on_timeout="default_answer", default_decision="deny")
        )
    )
    resolution = default_resolution(defaulting, later(61))
    assert resolution.default_applied and resolution.actor_ref == "policy:on_timeout"
    assert resolution.decision == "deny"


def test_escalation_keeps_the_task_open_and_answerable() -> None:
    task = open_task(activation(gate=spec(timeout_seconds=60, on_timeout="escalate")))
    assert timeout_disposition(task, later(61)) == "escalate"
    escalated = task.model_copy(update={"version": 2, "escalated": True})
    assert timeout_disposition(escalated, later(61)) == "keep_waiting"
    answer = _decide(escalated, request(task.activation, expected_task_version=2), now=later(90))
    assert answer.status == "accept"


def test_outcome_binds_only_its_own_task_round_and_packet() -> None:
    item = activation()
    task = open_task(item)
    check = _decide(task, request(item))
    resolved = task.model_copy(
        update={"lifecycle": "resolved", "version": 2, "resolution": check.resolution}
    )
    outcome = outcome_for(resolved)
    assert outcome is not None and outcome.status == "accepted"
    assert bind_outcome(item, outcome) == outcome
    with pytest.raises(HumanGateBindingError, match="review packet digest"):
        bind_outcome(activation(digest="sha256:" + "f" * 64), outcome)
    with pytest.raises(HumanGateBindingError):
        bind_outcome(activation(run_id="run-2"), outcome)


def test_manifest_gate_lowers_without_inventing_remediation_or_defaults() -> None:
    node = HumanGateNode.model_validate(
        {
            "key": "review",
            "behavior": "human_gate",
            "depends_on": ["synthesize"],
            "task": {
                "kind": "REVIEW",
                "prompt": "Accept the claim table?",
                "reviewers": ["owner"],
                "packet": ["synthesize.claims"],
                "timeout": "4h",
                "on_timeout": "keep_waiting",
            },
        }
    )
    lowered = gate_spec_from_manifest(node)
    assert lowered == HumanGateSpec(
        gate_key="review",
        prompt="Accept the claim table?",
        reviewers=("owner",),
        packet_sources=("synthesize.claims",),
        timeout_seconds=4 * 3600,
    )
    assert lowered.permitted_decisions(1) == ("approve", "deny")
    routed = node.model_copy(update={"task": node.task.model_copy(update={"on_timeout": "x"})})
    with pytest.raises(ValueError, match="on_timeout"):
        gate_spec_from_manifest(routed)


def test_stagegraph_result_mapping_routes_changes_to_the_declared_remediation_cycle() -> None:
    item = activation(gate=spec(remediation_target="draft", max_review_rounds=2))
    task = open_task(item)
    check = _decide(
        task,
        request(
            item,
            decision="request_changes",
            feedback_artifact_refs=("artifact://review-notes.md",),
            comment="Cite primary sources.",
        ),
    )
    outcome = outcome_for(
        task.model_copy(
            update={"lifecycle": "resolved", "version": 2, "resolution": check.resolution}
        )
    )
    assert outcome is not None and outcome.status == "changes_requested"
    disposition, payload = stagegraph_gate_result(
        outcome,
        evaluation_contract_ref="evaluation:review@1",
        objective_contract_ref="objective:review@1",
    )
    assert disposition == "completed"
    assert payload["evaluation"] == "cycle"
    assert payload["invalidation_frontier"] == ["draft"]
    assert "Cite primary sources." in str(payload["next_objective"])
    assert "criteria, budget and authority are unchanged" in feedback_instruction(outcome)
    with pytest.raises(HumanGateBindingError, match="remediation cycle"):
        stagegraph_gate_result(outcome, evaluation_contract_ref=None, objective_contract_ref=None)
    denied = outcome.model_copy(update={"status": "not_accepted", "decision": "deny"})
    assert (
        stagegraph_gate_result(denied, evaluation_contract_ref=None, objective_contract_ref=None)[0]
        == "failed"
    )
