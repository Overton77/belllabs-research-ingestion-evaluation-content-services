"""Offline guards for the dedicated async subagent Agent Server (REQ-CP-DA-019, ADR-0003).

The dedicated config lists only bound async graphs; the root config stays graph-free; the
hosted graph is compiled from the exact binding through the canonical adapter and
materializer and stamps its served identity; the identity route and auth guard match.
"""

from __future__ import annotations

import json
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool
from langgraph_cli.cli import prepare_args_and_stdin
from langgraph_cli.config import validate_config_file
from langgraph_cli.docker import DockerCapabilities

from app.agent_server.async_subagents import auth as auth_module
from app.agent_server.async_subagents.bindings import (
    TECHNICAL_CHILD_GRAPH_ID,
    hosted_definitions,
    served_graph_identities,
    technical_child_definition,
)
from app.agent_server.async_subagents.graph import build_hosted_graph, hosting_registry
from app.agent_server.async_subagents.http_app import app as identity_app
from app.integrations.agents.deep_agents.materializer import ExactComponentRegistry

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ROOT_LANGGRAPH = PROJECT_ROOT / "langgraph.json"
ASYNC_CONFIG = PROJECT_ROOT / "langgraph.async_subagents.json"
ASYNC_ENV_FILE = PROJECT_ROOT / "langgraph.async_subagents.env"


class ScriptedChildModel(BaseChatModel):
    calls: int = 0

    @property
    def _llm_type(self) -> str:
        return "rrm-013-scripted-child"

    def bind_tools(self, tools: Any, *, tool_choice: Any = None, **kwargs: Any) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _generate(
        self, messages: list[BaseMessage], stop: Any = None, run_manager: Any = None, **kwargs: Any
    ) -> ChatResult:  # noqa: E501
        del messages, stop, run_manager, kwargs
        self.calls += 1
        return ChatResult(
            generations=[
                ChatGeneration(
                    message=AIMessage(
                        content="PONG",
                        usage_metadata={"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
                    )
                )
            ]
        )


def test_dedicated_config_lists_only_bound_async_graphs_and_root_stays_graph_free() -> None:
    config = json.loads(ASYNC_CONFIG.read_text(encoding="utf-8"))
    assert set(config["graphs"]) == {definition.graph_id for definition in hosted_definitions()}
    assert config["graphs"][TECHNICAL_CHILD_GRAPH_ID] == (
        "app.agent_server.async_subagents.graph:graph"
    )
    assert config["dependencies"] == ["."]
    assert config["env"] == "langgraph.async_subagents.env"
    assert config["auth"]["path"] == "app.agent_server.async_subagents.auth:auth"
    assert config["http"]["app"] == "app.agent_server.async_subagents.http_app:app"
    assert config["api_version"] == "0.12.0"
    assert "block_c" not in json.dumps(config)
    for graph_path in config["graphs"].values():
        assert graph_path.startswith("app.agent_server.async_subagents.")
    root = json.loads(ROOT_LANGGRAPH.read_text(encoding="utf-8"))
    assert root["graphs"] == {}
    assert "async_subagents" not in json.dumps(root)
    # Variable references only: no secret value lives in the tracked env file.
    env_text = ASYNC_ENV_FILE.read_text(encoding="utf-8")
    for line in env_text.strip().splitlines():
        name, _, value = line.partition("=")
        assert value.startswith("${") and value.endswith("}"), name


def test_compose_generation_uses_external_postgres_and_env_file_references() -> None:
    config = validate_config_file(ASYNC_CONFIG)
    caps = DockerCapabilities(
        version_docker=(24, 0, 0),
        version_compose=(2, 20, 0),
        healthcheck_start_interval=True,
        compose_type="plugin",
    )
    _args, stdin = prepare_args_and_stdin(
        capabilities=caps,
        config_path=ASYNC_CONFIG.resolve(),
        config=config,
        docker_compose=None,
        port=8143,
        watch=False,
        postgres_uri="postgresql://langgraph:local@host.docker.internal:55433/langgraph",
        api_version="0.12.0",
    )
    parsed = yaml.safe_load(stdin)
    assert "langgraph-api" in parsed["services"]
    assert "langgraph-redis" in parsed["services"]
    assert "langgraph-postgres" not in parsed["services"]
    assert "env_file: langgraph.async_subagents.env" in stdin
    assert "FROM langchain/langgraph-api:0.12.0-py3.12" in stdin
    assert "LANGSMITH_API_KEY:" not in stdin


def test_served_identities_are_the_exact_binding_digests() -> None:
    definition = technical_child_definition()
    served = served_graph_identities()
    assert [item.graph_id for item in served] == [TECHNICAL_CHILD_GRAPH_ID]
    assert served[0].graph_binding_digest == definition.binding.binding_digest
    assert served[0].graph_revision == "agent.async-technical-child@1"
    assert definition.binding.placement == "remote_langsmith_deployment"
    assert definition.binding.checkpoint_behavior == "remote_managed"
    contract = definition.contract(agent_protocol_url="http://127.0.0.1:8143")
    assert served[0].matches(contract)
    assert contract.deployment_credential_ref == "environment:BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN"
    assert technical_child_definition().binding.binding_digest == definition.binding.binding_digest


@pytest.mark.asyncio
async def test_hosted_graph_is_built_through_the_canonical_adapter_and_stamps_identity() -> None:
    definition = technical_child_definition()
    base = hosting_registry(definition)
    model = ScriptedChildModel()
    registry = ExactComponentRegistry(
        model_factories={definition.binding.model.ref.digest: lambda _b, _s: model},
        prompts=base.prompts,
        tools=base.tools,
        sandbox_factories=base.sandbox_factories,
    )
    async with AsyncExitStack() as stack:
        graph = await build_hosted_graph(definition, registry=registry, secrets={}, stack=stack)
        assert graph.checkpointer is None  # the Agent Server owns the checkpointer
        tool_names = {
            name
            for node in graph.get_graph().nodes.values()
            for name in (getattr(getattr(node, "data", None), "tools_by_name", {}) or {})
        }
        # The Agent Server invokes the graph without a runtime context: the frozen context
        # values of the exact binding are the hosted context's defaults.
        result = await graph.ainvoke({"messages": [{"role": "user", "content": "say PONG"}]})
        context_type = graph.context_schema
        assert context_type is not None
        assert context_type().hosted_graph_id == TECHNICAL_CHILD_GRAPH_ID
    assert result["belllabs_served_graph"] == definition.served.model_dump(mode="json")
    assert result["messages"][-1].content == "PONG"
    # REQ-CP-DA-011: the hosted graph stamps provider-reported usage per model call.
    assert result["belllabs_provider_usage"] == [
        {
            "message_id": str(result["messages"][-1].id),
            "input_tokens": 1,
            "output_tokens": 1,
            "total_tokens": 2,
        }
    ]
    assert model.calls == 1
    assert "wait_seconds" in tool_names or any(
        isinstance(item, BaseTool) and item.name == "wait_seconds" for item in definition.tools
    )


def test_identity_route_requires_the_deployment_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RRM-009 (RRM-013 N8): the credential reference names a signing secret; the bearer
    is a scope claim signed with it, valid for one scope until it expires. The raw secret
    is never an accepted bearer."""

    from datetime import UTC, datetime, timedelta

    monkeypatch.setenv(auth_module.TOKEN_ENV, "offline-secret")
    claim = auth_module.mint_scope_claim("offline-secret", "tenant-1")
    client = TestClient(identity_app)
    assert client.get("/belllabs/async-subagents/served-graphs").status_code == 401
    for bearer in ("other", "offline-secret", claim[:-2] + "zz"):
        wrong = client.get(
            "/belllabs/async-subagents/served-graphs",
            headers={"Authorization": f"Bearer {bearer}"},
        )
        assert wrong.status_code == 401, bearer
    ok = client.get(
        "/belllabs/async-subagents/served-graphs",
        headers={"Authorization": f"Bearer {claim}"},
    )
    assert ok.status_code == 200
    assert ok.json()["graphs"][0]["graph_id"] == TECHNICAL_CHILD_GRAPH_ID
    assert auth_module.verify_bearer(f"Bearer {claim}") == "tenant-1"
    assert auth_module.verify_bearer(f"Basic {claim}") is None
    # The claim is bound to its scope and secret, and lapses.
    other = auth_module.mint_scope_claim("another-secret", "tenant-1")
    assert auth_module.verify_bearer(f"Bearer {other}") is None
    expired = auth_module.mint_scope_claim(
        "offline-secret", "tenant-2", now=datetime.now(UTC) - timedelta(days=1)
    )
    assert auth_module.verify_bearer(f"Bearer {expired}") is None
    assert auth_module.claim_expiry(claim) is not None
    monkeypatch.delenv(auth_module.TOKEN_ENV)
    assert auth_module.verify_bearer(f"Bearer {claim}") is None


def test_hosted_context_defaults_copy_mutable_values_and_order_required_fields_first() -> None:
    """RRM-013 review N9: defaults use factories; partially defaulted schemas still build."""

    from app.domain.operation_execution.contracts import (
        CognitiveRuntimeContextPack,
        CognitiveRuntimeField,
    )
    from app.domain.operation_execution.materialization import compose_cognitive_context_schema
    from app.integrations.agents.deep_agents.materializer import _context_type

    pack = CognitiveRuntimeContextPack.create(
        logical_id="pack.rrm013.context",
        revision=1,
        contributor="base",
        fields=(
            CognitiveRuntimeField(name="a_defaulted_map", value_kind="string_map"),
            CognitiveRuntimeField(name="b_required", value_kind="string"),
        ),
    )
    schema = compose_cognitive_context_schema(schema_id="context.rrm013", packs=(pack,))
    context_type = _context_type(schema, defaults={"a_defaulted_map": {"k": "v"}})
    first = context_type(b_required="x")
    second = context_type(b_required="y")
    assert first.a_defaulted_map == {"k": "v"}
    assert first.a_defaulted_map is not second.a_defaulted_map  # copied per instance
    assert context_type(b_required="z", a_defaulted_map={"o": "p"}).a_defaulted_map == {"o": "p"}
