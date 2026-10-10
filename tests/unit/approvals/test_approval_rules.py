"""MP-11 pure rules: binding identity, admitted resolutions per origin and translation."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from mission_control.application.execution.approvals import (
    REDACTED,
    ApprovalResolutionRequest,
    ApprovalTaskView,
    ElicitationPrompt,
    decide_approval_resolution,
    native_reply,
    open_approval_packet,
    redacted_preview,
    system_reply,
    tool_input_digest,
)
from mission_control.domain.execution.approvals import (
    ADMITTED_DECISIONS,
    NativeApprovalCorrelation,
)
from mission_control.domain.programs.human_gate import HumanResolutionRequest
from tests.fixtures.provider_frames import SCOPE
from tests.unit.approvals.fixtures import (
    EXECUTION,
    OPENED_AT,
    POLICY,
    REVIEWER,
    RUN,
    answer,
    connection_scoped_request,
    elicitation_request,
    permission_request,
    question_request,
)


def _packet(request: Any, connection_ref: str = "worker-a") -> Any:
    return open_approval_packet(
        request_scope=request.request_scope,
        run_id=request.run_id,
        origin=request.origin,
        lane_profile=request.lane_profile,
        harness_execution_id=request.harness_execution_id,
        generation=request.generation,
        native=request.native,
        tool_name=request.tool_name,
        arguments=request.arguments,
        policy_digest=request.policy_digest,
        reviewers=request.reviewers,
        prompt=request.prompt,
        opened_at=OPENED_AT,
        replay_strategy=request.replay_strategy,
        effect_kind=request.effect_kind,
        connection_ref=connection_ref,
        question=request.question,
        elicitation=request.elicitation,
        timeout_seconds=request.timeout_seconds,
        on_timeout=request.on_timeout,
    )


def _open(request: Any, connection_ref: str = "worker-a") -> ApprovalTaskView:
    packet = _packet(request, connection_ref)
    return ApprovalTaskView(
        human_task_id=str(packet.human_task_id),
        task_key=packet.task_key,
        kind=packet.kind,
        target_ref=packet.target_ref,
        lifecycle="open",
        version=1,
        deadline_at=packet.binding.deadline,
        on_timeout=packet.on_timeout,
        packet=packet,
        created_at=OPENED_AT,
        updated_at=OPENED_AT,
    )


def _decide(task: ApprovalTaskView, request: ApprovalResolutionRequest, actor: str = REVIEWER):
    return decide_approval_resolution(
        task, request, actor_id=actor, permissions=frozenset(), now=OPENED_AT
    )


def test_input_digest_is_canonical_and_bound_to_the_tool() -> None:
    first = tool_input_digest("Bash", {"b": 1, "a": [1, 2]})
    assert first == tool_input_digest("Bash", {"a": [1, 2], "b": 1})
    assert first != tool_input_digest("Bash", {"a": [1, 2], "b": 2})
    assert first != tool_input_digest("Write", {"a": [1, 2], "b": 1})
    with pytest.raises(ValueError, match="canonical JSON"):
        tool_input_digest("Bash", {"x": float("nan")})


def test_preview_redacts_credentials_and_bounds_size() -> None:
    preview, truncated = redacted_preview(
        {"command": "x" * 2_000, "api_key": "sk-fixture", "nested": {"Authorization": "Bearer"}}
    )
    assert preview["api_key"] == REDACTED
    assert preview["nested"]["Authorization"] == REDACTED
    assert preview["command"].endswith("[truncated]") and truncated
    task = _open(permission_request(arguments={"token": "fixture-secret", "path": "a.txt"}))
    body = task.public()
    assert body["pending_tool"]["arguments"] == {"path": "a.txt", "token": REDACTED}
    # Native transport handles are never public.
    text = str(body)
    for handle in ("fixture-session", "fixture-turn", "toolu_fixture_1", "worker-a"):
        assert handle not in text


def test_task_identity_is_the_binding_not_a_transport_id() -> None:
    base = _open(permission_request())
    assert base.packet.binding.human_task_id == base.human_task_id
    assert base.kind == "approval:provider_permission"
    assert base.packet.permitted_decisions == ("approve", "approve_edited", "deny", "cancel")
    # Same request delivered again: same task.
    assert _open(permission_request()).human_task_id == base.human_task_id
    variants = [
        permission_request(arguments={"command": "git push origin dev", "timeout": 30}),
        permission_request(generation=2),
        permission_request(policy_digest="sha256:" + "c" * 64),
        permission_request(native=NativeApprovalCorrelation(tool_call_ref="toolu_fixture_2")),
    ]
    assert len({_open(item).human_task_id for item in variants} | {base.human_task_id}) == 5


def test_connection_scoped_ids_are_bound_to_their_connection() -> None:
    before = _open(connection_scoped_request("7"), connection_ref="worker-a#1")
    after_restart = _open(connection_scoped_request("7"), connection_ref="worker-a#2")
    assert before.human_task_id != after_restart.human_task_id
    assert before.packet.connection_ref == "worker-a#1"
    with pytest.raises(ValidationError, match="cannot be reissued"):
        _packet(connection_scoped_request("7", replay_strategy="reissue_native_request"))
    # A stable tool-call identity is not bound to a connection.
    assert _open(permission_request(), connection_ref="worker-a#2").packet.connection_ref is None


def test_resolution_checks_reviewer_version_digest_and_admitted_decisions() -> None:
    task = _open(permission_request())
    assert _decide(task, answer(task), actor="intruder").code == "not_reviewer"
    assert _decide(task, answer(task, expected_task_version=2)).code == "stale_version"
    other = "sha256:" + "f" * 64
    assert _decide(task, answer(task, reviewed_digest=other)).code == "packet_digest_mismatch"
    assert _decide(task, answer(task, decision="request_changes")).code == "decision_not_admitted"
    assert (
        _decide(task, answer(task, decision="approve_edited")).code == "edited_arguments_required"
    )
    same = _decide(
        task, answer(task, decision="approve_edited", edited_arguments=dict(task_arguments(task)))
    )
    assert same.code == "decision_not_admitted"  # the frozen validator: the digest must change
    edited = _decide(
        task,
        answer(task, decision="approve_edited", edited_arguments={"command": "git push --dry-run"}),
    )
    assert edited.status == "accept" and edited.resolution is not None
    assert edited.resolution.edited_input_digest == tool_input_digest(
        "Bash", {"command": "git push --dry-run"}
    )
    granted = decide_approval_resolution(
        task,
        answer(task),
        actor_id="alice",
        permissions=frozenset({f"reviewer:{REVIEWER}"}),
        now=OPENED_AT,
    )
    assert granted.status == "accept"


def task_arguments(task: ApprovalTaskView) -> dict[str, Any]:
    return {"command": "git push origin main", "timeout": 30}


def test_retry_is_a_duplicate_and_a_different_answer_is_refused() -> None:
    task = _open(permission_request())
    accepted = _decide(task, answer(task, decision="deny", comment="use a PR"))
    assert accepted.resolution is not None
    resolved = task.model_copy(
        update={"lifecycle": "resolved", "version": 2, "resolution": accepted.resolution}
    )
    assert _decide(resolved, answer(task, decision="deny", comment="use a PR")).status == (
        "duplicate"
    )
    assert _decide(resolved, answer(task, request_id="req-2")).code == "already_resolved"


def test_deny_and_cancel_translate_to_distinct_native_replies() -> None:
    task = _open(permission_request())
    deny = _decide(task, answer(task, decision="deny", comment="push to a branch instead"))
    cancel = _decide(task, answer(task, decision="cancel", request_id="req-2"))
    assert deny.resolution is not None and cancel.resolution is not None
    denied = native_reply(task.packet, deny.resolution)
    cancelled = native_reply(task.packet, cancel.resolution)
    assert (denied.action, denied.interrupt) == ("deny", False)
    assert (cancelled.action, cancelled.interrupt) == ("cancel", True)
    assert deny.resolution.resolution_action == "denied"
    assert cancel.resolution.resolution_action == "cancelled"
    assert denied.message is not None and "push to a branch instead" in denied.message
    approve = _decide(task, answer(task))
    assert approve.resolution is not None
    assert native_reply(task.packet, approve.resolution).action == "allow"


def test_system_replies_never_allow() -> None:
    task = _open(permission_request())
    for reason in ("wait_expired", "stale_generation", "stop_fenced", "not_replayable"):
        reply = system_reply(task.packet, reason)  # type: ignore[arg-type]
        assert reply.action == "deny" and reply.interrupt and reply.reason == reason
    with pytest.raises(ValidationError, match="only an attributed human decision"):
        type(system_reply(task.packet, "wait_expired"))(
            action="allow", reason="wait_expired", human_task_id=task.human_task_id
        )


def test_question_answers_and_elicitation_actions_per_origin() -> None:
    question = _open(question_request())
    assert question.packet.permitted_decisions == ("approve", "cancel")
    assert _decide(question, answer(question)).code == "answer_required"
    picked = _decide(question, answer(question, selected=("dev",)))
    assert picked.resolution is not None and picked.resolution.resolution_action == "answered"
    reply = native_reply(question.packet, picked.resolution)
    assert reply.action == "answer" and reply.selected == ("dev",)
    assert _decide(question, answer(question, selected=("prod",))).code == "answer_required"
    assert _decide(question, answer(question, decision="deny")).code == "decision_not_admitted"

    elicit = _open(elicitation_request())
    assert set(elicit.packet.permitted_decisions) == ADMITTED_DECISIONS["mcp_elicitation"]
    assert _decide(elicit, answer(elicit)).code == "elicitation_content_invalid"
    leaked = answer(elicit, elicitation_content={"environment": "prod", "password": "x"})
    assert _decide(elicit, leaked).code == "elicitation_content_invalid"
    accepted = _decide(elicit, answer(elicit, elicitation_content={"environment": "prod"}))
    declined = _decide(elicit, answer(elicit, decision="deny", request_id="r2"))
    dismissed = _decide(elicit, answer(elicit, decision="cancel", request_id="r3"))
    actions = [
        native_reply(elicit.packet, check.resolution).elicitation_action  # type: ignore[arg-type]
        for check in (accepted, declined, dismissed)
    ]
    assert actions == ["accept", "decline", "cancel"]


def test_forms_never_collect_credentials_and_urls_are_https() -> None:
    with pytest.raises(ValidationError, match="may not collect credentials"):
        ElicitationPrompt(
            message="Sign in",
            requested_schema={"type": "object", "properties": {"api_key": {"type": "string"}}},
        )
    with pytest.raises(ValidationError, match="https"):
        ElicitationPrompt(mode="url", message="Authorize", url="http://example.invalid/auth")
    ElicitationPrompt(mode="url", message="Authorize", url="https://example.invalid/auth")


def test_the_frozen_gate_body_maps_onto_approval_tasks() -> None:
    task = _open(question_request())
    body = HumanResolutionRequest(
        request_id="http-1",
        expected_task_version=1,
        decision="approve",
        reviewed_packet_digest=task.packet.review_digest,
        comment="dev",
    )
    mapped = ApprovalResolutionRequest.from_gate_body(body, origin="provider_question")
    assert mapped.answer == "dev" and mapped.comment is None
    assert _decide(task, mapped).status == "accept"
    permission = _open(permission_request())
    plain = ApprovalResolutionRequest.from_gate_body(
        body.model_copy(update={"reviewed_packet_digest": permission.packet.review_digest}),
        origin="provider_permission",
    )
    assert plain.answer is None and plain.comment == "dev"


def test_packet_rejects_inconsistent_shapes() -> None:
    with pytest.raises(ValidationError, match="carries a question"):
        _packet(permission_request(origin="provider_question"))
    assert EXECUTION and RUN and POLICY and SCOPE
