"""`missionctl command cancel RUN --urgency immediate` (SPEC-06 scenario 3, 00-ARCHITECTURE 8)."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from mission_control.interfaces.cli.main import main

RUN = "run-cancel-1"


def _transport(sent: list[dict[str, Any]]) -> httpx.MockTransport:
    def receive(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith(f"/runs/{RUN}/inspection"):
            return httpx.Response(200, json={"version": 7, "execution_generation": 2})
        if request.method == "POST" and request.url.path.endswith(f"/runs/{RUN}/commands"):
            body = json.loads(request.content)
            sent.append(body)
            assert request.headers["Idempotency-Key"] == body["request_id"]
            return httpx.Response(202, json={"admission": {"status": "accepted"}})
        return httpx.Response(404, json={"detail": {"code": "not_found"}})

    return httpx.MockTransport(receive)


@pytest.mark.parametrize("urgency", ["immediate", "normal"])
def test_command_cancel_binds_the_run_version_and_urgency(
    monkeypatch: pytest.MonkeyPatch, urgency: str
) -> None:
    sent: list[dict[str, Any]] = []
    original = httpx.Client
    monkeypatch.setattr(
        httpx, "Client", lambda **kwargs: original(**kwargs, transport=_transport(sent))
    )
    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "token")
    code = main(
        [
            "command",
            "cancel",
            RUN,
            "--urgency",
            urgency,
            "--reason",
            "wrong repo",
            "--application",
            "ai-engineer",
            "--url",
            "http://127.0.0.1:8000",
        ]
    )
    assert code == 0
    (body,) = sent
    assert body["kind"] == "cancel"
    assert body["payload"] == {"urgency": urgency}
    assert (body["expected_version"], body["expected_generation"]) == (7, 2)
    assert body["target"] == {"kind": "run", "id": RUN}
    assert body["reason"] == "wrong repo"
