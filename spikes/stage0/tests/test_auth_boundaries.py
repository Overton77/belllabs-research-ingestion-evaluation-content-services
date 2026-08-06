from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from langgraph_sdk import Auth

from auth import (
    authenticate,
    authorize_assistants,
    authorize_store,
    authorize_threads,
    default_deny,
    deny_crons,
)
from http_app import app
from local_identity import authenticate_local, scope_from_identity


@pytest.fixture
def local_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STAGE0_LOCAL_AUTH_TOKEN", "test-only-random-token")
    monkeypatch.setenv("STAGE0_LOCAL_REQUEST_SCOPE", "tenant-a")
    monkeypatch.setenv("STAGE0_LOCAL_ENVIRONMENT", "test")


def test_local_identity_is_environment_bound(
    local_identity: None,
) -> None:
    del local_identity
    assert (
        authenticate_local(
            token="test-only-random-token",
            claimed_request_scope="tenant-a",
        ).request_scope
        == "tenant-a"
    )
    assert (
        authenticate_local(
            token="test-only-random-token",
            claimed_request_scope="tenant-b",
        )
        is None
    )
    assert authenticate_local(token="wrong", claimed_request_scope="tenant-a") is None
    assert scope_from_identity("stage0:tenant-a") == "tenant-a"
    with pytest.raises(ValueError, match="invalid Stage 0 identity"):
        scope_from_identity("tenant-a")


@pytest.mark.asyncio
async def test_agent_server_auth_rejects_invalid_and_non_utf8_headers(
    local_identity: None,
) -> None:
    del local_identity
    user = await authenticate(
        {
            b"x-stage0-token": b"test-only-random-token",
            b"x-request-scope": b"tenant-a",
        }
    )
    assert user == {"identity": "stage0:tenant-a"}

    for headers in (
        {},
        {b"x-stage0-token": b"wrong", b"x-request-scope": b"tenant-a"},
        {
            b"x-stage0-token": b"test-only-random-token",
            b"x-request-scope": b"tenant-b",
        },
        {b"x-stage0-token": b"\xff", b"x-request-scope": b"tenant-a"},
    ):
        with pytest.raises(Auth.exceptions.HTTPException) as captured:
            await authenticate(headers)
        assert captured.value.status_code == 401


def test_custom_route_rejects_missing_wrong_and_cross_tenant_identity(
    local_identity: None,
) -> None:
    del local_identity
    client = TestClient(app)
    assert client.get("/stage0/qualification").status_code == 401
    assert (
        client.get(
            "/stage0/qualification",
            headers={"X-Stage0-Token": "wrong", "X-Request-Scope": "tenant-a"},
        ).status_code
        == 401
    )
    assert (
        client.get(
            "/stage0/qualification",
            headers={
                "X-Stage0-Token": "test-only-random-token",
                "X-Request-Scope": "tenant-b",
            },
        ).status_code
        == 401
    )
    response = client.get(
        "/stage0/qualification",
        headers={
            "X-Stage0-Token": "test-only-random-token",
            "X-Request-Scope": "tenant-a",
        },
    )
    assert response.status_code == 200
    assert response.json()["request_scope"] == "tenant-a"


@pytest.mark.asyncio
async def test_native_resource_handlers_override_caller_scope_and_deny_store_cross_tenant(
    local_identity: None,
) -> None:
    del local_identity
    context = SimpleNamespace(user=SimpleNamespace(identity="stage0:tenant-a"))
    thread_value = {"metadata": {"request_scope": "tenant-b"}}
    assistant_value = {"metadata": {"request_scope": "tenant-b"}}
    assert await authorize_threads(context, thread_value) == {"request_scope": "tenant-a"}
    assert thread_value["metadata"]["request_scope"] == "tenant-a"
    assert await authorize_assistants(context, assistant_value) == {
        "request_scope": "tenant-a"
    }
    assert assistant_value["metadata"]["request_scope"] == "tenant-a"
    assert (
        await authorize_store(
            context,
            {
                "namespace": ("tenant-a", "test", "procedural_preference"),
                "value": {"value": "concise"},
            },
        )
        is True
    )
    for namespace in (
        (),
        ("tenant-b", "test", "procedural_preference"),
        ("tenant-a-prefix", "test", "procedural_preference"),
        ("tenant-a", "production", "procedural_preference"),
        ("tenant-a", "test", "scientific_claim_authority"),
        ("tenant-a", "test", "approval"),
        ("tenant-a", "test", "budget"),
        ("tenant-a", "test", "terminality"),
        ("tenant-a", "test", "workflow_hint", "approval"),
    ):
        with pytest.raises(Auth.exceptions.HTTPException) as captured:
            await authorize_store(context, {"namespace": namespace})
        assert captured.value.status_code == 403
    for authority_value in (
        {"accepted": True},
        {"approval": "approved"},
        {"budget": 10},
        {"terminality": "completed"},
    ):
        with pytest.raises(Auth.exceptions.HTTPException) as captured:
            await authorize_store(
                context,
                {
                    "namespace": ("tenant-a", "test", "procedural_preference"),
                    "value": authority_value,
                },
            )
        assert captured.value.status_code == 403


@pytest.mark.asyncio
async def test_unhandled_resources_and_crons_fail_closed() -> None:
    context = SimpleNamespace(user=SimpleNamespace(identity="stage0:tenant-a"))
    with pytest.raises(Auth.exceptions.HTTPException) as unhandled:
        await default_deny(context, {})
    assert unhandled.value.status_code == 403
    with pytest.raises(Auth.exceptions.HTTPException) as cron:
        await deny_crons(context, {})
    assert cron.value.status_code == 403
