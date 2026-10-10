"""MP-11 over the socket: approval-origin bodies go to `HumanTaskService.resolve_approval`,
with the same receipts and rejection codes as `POST /human-tasks/{id}/approval-resolutions`.

In-memory stores (FIXTURE semantics), the real HTTP router and the real socket gateway.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from mission_control.application.execution.approvals import ApprovalResolutionRequest
from mission_control.application.streams.service import StreamFailure
from mission_control.domain.programs.human_gate import HumanResolutionRequest
from mission_control.interfaces.http.human_tasks import REJECTION_STATUS
from mission_control.interfaces.socketio.commands import (
    forward_human_task_resolution,
    parse_human_task_resolution,
    resolution_failure,
)
from tests.unit.approvals.fixtures import answer, permission_request
from tests.unit.approvals.test_approval_task_interfaces import BASE, _app, _compose, _principal


def socket_body(task_id: str, request: Any) -> dict[str, Any]:
    return {
        "application_id": "biotech",
        "human_task_id": task_id,
        **request.model_dump(mode="json", exclude_none=True),
    }


async def test_an_approval_body_resolves_through_resolve_approval_with_http_parity() -> None:
    service, broker, _store, _gate = await _compose()
    bound = await broker.bind(permission_request())
    task_id = bound.task.human_task_id
    app = _app(service)

    missing = answer(bound.task, request_id="edit-1", decision="approve_edited")
    parsed = parse_human_task_resolution(socket_body(task_id, missing))
    assert isinstance(parsed.resolution, ApprovalResolutionRequest)
    with pytest.raises(StreamFailure) as refused:
        await forward_human_task_resolution(app, _principal(), parsed)
    assert (refused.value.code, refused.value.detail) == (
        "COMMAND_CONFLICT",
        "edited_arguments_required",
    )
    assert refused.value.retryable is False
    http = TestClient(app).post(
        f"{BASE}/human-tasks/{task_id}/approval-resolutions",
        json=missing.model_dump(mode="json", exclude_none=True),
    )
    assert http.status_code == 422
    assert http.json()["detail"]["code"] == "edited_arguments_required"

    edited = answer(
        bound.task,
        request_id="edit-2",
        decision="approve_edited",
        edited_arguments={"command": "git push --dry-run"},
    )
    receipt = await forward_human_task_resolution(
        app, _principal(), parse_human_task_resolution(socket_body(task_id, edited))
    )
    assert receipt["status"] == "accepted" and receipt["human_task_id"] == task_id
    replay = TestClient(app).post(
        f"{BASE}/human-tasks/{task_id}/approval-resolutions",
        json=edited.model_dump(mode="json", exclude_none=True),
    )
    assert replay.status_code == 200 and replay.json()["status"] == "duplicate"
    assert replay.json()["task"]["resolution"] == receipt["task"]["resolution"]


async def test_a_human_gate_body_still_resolves_through_resolve() -> None:
    service, _broker, _store, gate = await _compose()
    app = _app(service)
    body = {
        "application_id": "biotech",
        "human_task_id": gate.human_task_id,
        "request_id": "gate-1",
        "expected_task_version": gate.version,
        "decision": "approve",
        "reviewed_packet_digest": gate.activation.packet_digest,
    }
    parsed = parse_human_task_resolution(body)
    assert isinstance(parsed.resolution, HumanResolutionRequest)
    receipt = await forward_human_task_resolution(app, _principal(), parsed)
    assert receipt["status"] == "accepted"
    with pytest.raises(StreamFailure) as invalid:
        parse_human_task_resolution({**body, "reviewed_digest": gate.activation.packet_digest})
    assert invalid.value.code == "COMMAND_CONFLICT"


@pytest.mark.parametrize(
    "code",
    [
        "edited_arguments_required",
        "answer_required",
        "elicitation_content_invalid",
        "invalid_request",
        "decision_not_admitted",
        "stale_version",
    ],
)
def test_rejections_follow_the_http_status_vocabulary(code: str) -> None:
    assert REJECTION_STATUS[code] in {409, 422}
    failure = resolution_failure(code)
    assert (failure.code, failure.detail, failure.retryable) == ("COMMAND_CONFLICT", code, False)


def test_absent_and_unauthorized_map_like_http() -> None:
    assert resolution_failure("not_found").code == "TARGET_NOT_FOUND"
    assert resolution_failure("not_reviewer").code == "UNAUTHORIZED"
