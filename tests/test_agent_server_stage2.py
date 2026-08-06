from __future__ import annotations

import importlib
import json
import os
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace

import pytest
from deepagents import create_deep_agent
from fastapi.testclient import TestClient
from joserfc import jwt
from joserfc.jwk import RSAKey
from langchain.agents import create_agent
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_quickjs import CodeInterpreterMiddleware
from langgraph_sdk import Auth
from langgraph_sdk.runtime import ServerRuntime

from app.agent_server.auth import (
    _allowed_store_value,
    _authorize_scoped_resource,
    _verifier,
    authenticate_bearer_token,
    authentication_is_configured,
    authorize_assistants,
    authorize_store,
)
from app.agent_server.context import principal_from_claims
from app.agent_server.goal_directed.nodes import (
    admit_goal_binding,
    bounded_agent_placeholder,
    independent_verifier_placeholder,
)
from app.agent_server.graphs import GRAPH_REGISTRY
from app.agent_server.health import readiness_report
from app.agent_server.http_app import app
from app.agent_server.reducers import merge_unique_events
from app.agent_server.stagegraph.graph import graph as stagegraph_graph
from app.agent_server.stagegraph.nodes import (
    admit_runtime_binding,
    interpret_next_stage,
)
from app.agent_server.tracing import (
    configure_agent_server_tracing,
    correlation_metadata,
    mask_trace_payload,
    require_masked_tracing_posture,
)
from app.domain.graph_runtime.contracts import RuntimeExecutionStatus

DIGEST = "sha256:" + "a" * 64
PROJECT_ROOT = Path(__file__).parents[1]


def test_graph_registry_imports_without_external_resource_construction() -> None:
    imported = importlib.import_module("app.agent_server.graphs")

    assert imported.GRAPH_REGISTRY == GRAPH_REGISTRY
    assert set(GRAPH_REGISTRY) == {
        "belllabs_stagegraph",
        "belllabs_goal_directed",
    }


def test_event_reducer_accepts_json_round_tripped_sequences() -> None:
    assert merge_unique_events(
        ["event:a", "event:b"],
        ("event:b", "event:c"),
    ) == ("event:a", "event:b", "event:c")


def test_accepted_dependency_and_runtime_surfaces_are_locked() -> None:
    assert version("langgraph") == "1.2.10"
    assert version("langgraph-sdk") == "0.4.2"
    assert version("langgraph-api") == "0.12.0"
    assert version("langgraph-runtime-inmem") == "0.32.0"
    assert version("deepagents") == "0.7.4"
    assert version("mcp") == "1.29.0"
    assert callable(create_agent)
    assert callable(create_deep_agent)
    assert MultiServerMCPClient is not None
    assert CodeInterpreterMiddleware is not None
    assert ServerRuntime is not None


def test_langgraph_config_registers_exact_graphs_without_route_collision() -> None:
    config = json.loads((PROJECT_ROOT / "langgraph.json").read_text(encoding="utf-8"))
    assert config["graphs"] == {
        "belllabs_stagegraph": "./app/agent_server/stagegraph/graph.py:graph",
        "belllabs_goal_directed": "./app/agent_server/goal_directed/graph.py:graph",
    }
    assert config["auth"]["disable_studio_auth"] is False
    custom_paths = {
        path
        for route in app.routes
        if (path := getattr(route, "path", None)) is not None
    }
    assert "/ok" not in custom_paths


@pytest.mark.asyncio
async def test_stagegraph_nodes_require_qualified_runtime_identity() -> None:
    state = {
        "request_scope": "tenant-a",
        "belllabs_run_id": "run-1",
        "execution_epoch": 1,
        "graph_assembly_digest": DIGEST,
        "run_plan_digest": DIGEST,
        "next_stage_ref": "stage:collect:v1",
        "event_refs": (),
    }
    runtime = SimpleNamespace(
        server_info=SimpleNamespace(
            user=SimpleNamespace(permissions=("request_scope:tenant-a",))
        )
    )
    admitted = await admit_runtime_binding(state, runtime)  # type: ignore[arg-type]
    interpreted = await interpret_next_stage(state)  # type: ignore[arg-type]

    assert admitted["event_refs"] == (f"runtime-binding-admitted:{DIGEST}",)
    assert interpreted["event_refs"] == ("stage-placeholder:stage:collect:v1",)


@pytest.mark.asyncio
async def test_goal_directed_nodes_keep_verifier_separate() -> None:
    state = {
        "request_scope": "tenant-a",
        "belllabs_run_id": "run-2",
        "execution_epoch": 2,
        "graph_assembly_digest": DIGEST,
        "run_plan_digest": DIGEST,
        "goal_ref": "goal:1",
        "verifier_ref": "verifier:independent:v1",
        "event_refs": (),
    }
    runtime = SimpleNamespace(
        server_info=SimpleNamespace(
            user=SimpleNamespace(permissions=("request_scope:tenant-a",))
        )
    )
    admitted = await admit_goal_binding(state, runtime)  # type: ignore[arg-type]
    bounded = await bounded_agent_placeholder(state)  # type: ignore[arg-type]
    verified = await independent_verifier_placeholder(state)  # type: ignore[arg-type]

    assert admitted["event_refs"] == ("goal-binding-admitted:goal:1",)
    assert bounded["event_refs"] == (f"bounded-agent-placeholder:{DIGEST}",)
    assert verified["event_refs"] == (
        "verifier-placeholder:verifier:independent:v1",
    )


@pytest.mark.asyncio
async def test_direct_graph_execution_fails_closed_without_test_posture() -> None:
    with pytest.raises(PermissionError, match="direct graph execution"):
        await stagegraph_graph.ainvoke(
            {
                "request_scope": "tenant-a",
                "belllabs_run_id": "run-1",
                "execution_epoch": 1,
                "graph_assembly_digest": DIGEST,
                "run_plan_digest": DIGEST,
                "next_stage_ref": "stage:collect:v1",
            }
        )


def test_principal_mapping_and_resource_filter_deny_cross_scope() -> None:
    principal = principal_from_claims(
        {
            "sub": "user-1",
            "request_scopes": ["tenant-a"],
            "roles": ["operator"],
        }
    )
    user = SimpleNamespace(
        identity=principal.subject,
        permissions=("request_scope:tenant-a", "role:operator"),
    )
    ctx = SimpleNamespace(user=user, action="threads.create")
    value: dict[str, object] = {"metadata": {"request_scope": "tenant-a"}}

    assert _authorize_scoped_resource(ctx, value) == {"request_scope": "tenant-a"}
    with pytest.raises(Exception, match="cross-scope"):
        _authorize_scoped_resource(
            ctx,
            {"metadata": {"request_scope": "tenant-b"}},
        )


@pytest.mark.asyncio
async def test_only_immutable_deployment_assistants_are_available() -> None:
    user = SimpleNamespace(
        identity="user-1",
        permissions=("request_scope:tenant-a", "role:operator"),
    )

    assert await authorize_assistants(
        SimpleNamespace(user=user, action="assistants.search"),
        {},
    ) == {}
    assert await authorize_assistants(
        SimpleNamespace(user=user, action="assistants.read"),
        {"assistant_id": "deployment-assistant"},
    ) == {}
    with pytest.raises(Exception, match="mutable assistants are disabled"):
        await authorize_assistants(
            SimpleNamespace(user=user, action="assistants.create"),
            {
                "graph_id": "belllabs_stagegraph",
                "metadata": {"request_scope": "tenant-a"},
            },
        )


@pytest.mark.asyncio
async def test_unscoped_store_namespace_listing_fails_closed() -> None:
    user = SimpleNamespace(
        identity="user-1",
        permissions=("request_scope:tenant-a", "role:auditor"),
    )
    ctx = SimpleNamespace(user=user, action="store.list_namespaces")

    with pytest.raises(
        Auth.exceptions.HTTPException,
        match="unscoped Store namespace operations are disabled",
    ) as error:
        await authorize_store(ctx, {"namespace": None})

    assert error.value.status_code == 403


@pytest.mark.asyncio
async def test_store_actions_enforce_scope_environment_and_value_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BELL_LABS_ENVIRONMENT", "test")
    user = SimpleNamespace(
        identity="user-1",
        permissions=("request_scope:tenant-a", "role:operator"),
    )
    ctx = SimpleNamespace(user=user, action="store.put")
    value = {
        "namespace": ("tenant-a", "test", "runtime_projection"),
        "value": {
            "graph_id": "belllabs_stagegraph",
            "binding_id": "binding-1",
            "status": RuntimeExecutionStatus.RUNNING.value,
            "digest": DIGEST,
            "updated_at": "2026-08-05T00:00:00Z",
        },
    }

    assert await authorize_store(ctx, value)
    with pytest.raises(Auth.exceptions.HTTPException, match="Store namespace denied"):
        await authorize_store(
            ctx,
            {**value, "namespace": ("tenant-b", "test", "runtime_projection")},
        )


def test_store_values_reject_authority_shaped_data() -> None:
    assert _allowed_store_value(
        "procedural_memory",
        {
            "summary_ref": "artifact:summary:1",
            "content_digest": DIGEST,
            "source_refs": ["artifact:source:1"],
            "created_at": "2026-08-05T00:00:00Z",
        },
    )
    assert not _allowed_store_value(
        "procedural_memory",
        {
            "summary_ref": "artifact:summary:1",
            "approval_decision": "approved",
        },
    )
    for status in RuntimeExecutionStatus:
        assert _allowed_store_value(
            "runtime_projection",
            {
                "graph_id": "belllabs_stagegraph",
                "binding_id": "binding-1",
                "status": status.value,
                "digest": DIGEST,
                "updated_at": "2026-08-05T00:00:00Z",
            },
        )
    assert not _allowed_store_value(
        "runtime_projection",
        {"budget": {"tokens": 1}},
    )
    assert not _allowed_store_value(
        "runtime_projection",
        {
            "graph_id": "belllabs_stagegraph",
            "binding_id": "binding-1",
            "status": {"approval": "approved"},
            "digest": DIGEST,
            "updated_at": "2026-08-05T00:00:00Z",
        },
    )


@pytest.mark.asyncio
async def test_signed_jwt_authenticates_without_exposing_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = RSAKey.generate_key()
    issuer = "https://identity.example.test/auth/v1"
    token = jwt.encode(
        {"alg": "RS256"},
        {
            "sub": "user-1",
            "iss": issuer,
            "aud": "authenticated",
            "exp": 4_102_444_800,
            "request_scopes": ["tenant-a"],
            "roles": ["operator"],
        },
        key,
        algorithms=["RS256"],
    )
    monkeypatch.setenv("BELL_LABS_AGENT_AUTH_ISSUER", issuer)
    monkeypatch.setenv(
        "BELL_LABS_AGENT_AUTH_PUBLIC_KEY",
        key.as_pem(private=False).decode(),
    )
    _verifier.cache_clear()
    try:
        principal = await authenticate_bearer_token(token)
    finally:
        _verifier.cache_clear()

    assert principal.subject == "user-1"
    assert principal.request_scopes == {"tenant-a"}
    assert token not in repr(principal)


def test_blank_jwks_uri_uses_supabase_issuer_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BELL_LABS_AGENT_AUTH_ISSUER", raising=False)
    monkeypatch.delenv("BELL_LABS_AGENT_AUTH_PUBLIC_KEY", raising=False)
    monkeypatch.setenv("BELL_LABS_AGENT_AUTH_JWKS_URI", "")
    monkeypatch.setenv("SUPABASE_URL", "https://project.supabase.co/")
    _verifier.cache_clear()
    try:
        assert authentication_is_configured()
        assert _verifier() is not None
    finally:
        _verifier.cache_clear()


def test_trace_masking_redacts_secrets_signed_urls_and_large_payloads() -> None:
    masked = mask_trace_payload(
        {
            "Authorization": "Bearer sentinel-secret",
            "secret_ref": "vault:item",
            "url": "https://example.test/x?X-Amz-Signature=secret",
            "raw_payload": "x" * 600,
        }
    )
    metadata = correlation_metadata(
        request_scope="tenant-a",
        belllabs_run_id="run-1",
        graph_id="belllabs_stagegraph",
        graph_assembly_digest=DIGEST,
        deployment_endpoint_id="deployment:local",
        pseudonym_key=b"synthetic-test-key",
    )

    assert masked["Authorization"] == "[redacted]"
    assert masked["secret_ref"] == "[redacted]"
    assert masked["url"] == "[signed-url-redacted]"
    assert masked["raw_payload"]["character_count"] == 600
    assert "tenant-a" not in metadata.values()
    assert "run-1" not in metadata.values()


def test_native_tracing_cannot_start_before_masked_export_is_wired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LANGSMITH_TRACING", "true")
    monkeypatch.delenv("LANGSMITH_HIDE_INPUTS", raising=False)
    monkeypatch.delenv("LANGSMITH_HIDE_OUTPUTS", raising=False)
    with pytest.raises(RuntimeError, match="LANGSMITH_HIDE_INPUTS"):
        require_masked_tracing_posture()


@pytest.mark.asyncio
async def test_native_tracing_requires_hidden_inputs_and_outputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LANGSMITH_TRACING", "true")
    monkeypatch.setenv("LANGSMITH_HIDE_INPUTS", "true")
    monkeypatch.setenv("LANGSMITH_HIDE_OUTPUTS", "true")
    monkeypatch.setenv("LANGSMITH_API_KEY", "synthetic-test-key")
    monkeypatch.setenv(
        "AGENT_SERVER_LANGSMITH_PROJECT",
        "BellLabsBiotech-AgentServer-Test",
    )
    monkeypatch.setenv("LANGSMITH_PROJECT", "BellLabsBiotech")

    configure_agent_server_tracing()
    report = await readiness_report()

    assert report.capabilities["tracing"] == "ready"
    assert os.environ["LANGSMITH_PROJECT"] == "BellLabsBiotech-AgentServer-Test"


def test_custom_http_app_does_not_collide_with_native_liveness() -> None:
    client = TestClient(app)

    assert client.get("/ok").status_code == 404
    assert client.get("/v2/agent-runtime/readiness").status_code == 401
