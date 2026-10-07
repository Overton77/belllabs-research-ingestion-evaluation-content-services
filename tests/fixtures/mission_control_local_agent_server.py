"""Real Agent Server graph with an explicitly distinct deterministic model binding.

Runs the installed Agent Protocol server and the production hosted DeepAgents adapter.
Only cognition is local/scripted; provider threads, checkpoints, cancellation, authentication
and the served identity endpoint are real. No provider API factory is registered.
"""

from __future__ import annotations

import asyncio
import base64
import os
import secrets
import socket
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from tests.fixtures.rrm009_production_stack import SCOPE, _LoggedModel, call_usage

from mission_control.adapters.agent_server.async_subagents.auth import mint_scope_claim
from mission_control.adapters.agent_server.async_subagents.bindings import (
    HostedAsyncSubagentDefinition,
    technical_child_definition,
)
from mission_control.adapters.agent_server.async_subagents.graph import (
    build_hosted_graph,
    hosting_registry,
)
from mission_control.adapters.agent_server.hosting import HostedGraph
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef
from mission_control.domain.execution.contracts import (
    DeepAgentExecutionBinding,
    DeepAgentModelComponent,
    DeepAgentProfile,
)

CANONICAL_CONFIG = Path(__file__).resolve().parents[2] / "agent_server" / "langgraph.json"


def deterministic_child_definition() -> HostedAsyncSubagentDefinition:
    base = technical_child_definition()
    model = DeepAgentModelComponent(
        ref=ExactDefinitionRef(
            kind=DefinitionKind.MODEL,
            logical_id="model.mission-control-local-child",
            revision=1,
            digest=sha256_digest("mission-control-local-child:bounded-wait-and-pong:v1"),
        ),
        provider="openai",  # Framework-neutral model interface, supplied by the local factory.
        model_name="deterministic-local-child",
    )
    profile = DeepAgentProfile.create(
        **{
            **base.profile.model_dump(mode="python", exclude={"profile_digest", "model"}),
            "model": model,
        }
    )
    binding = DeepAgentExecutionBinding.create(
        **{
            **base.binding.model_dump(
                mode="python", exclude={"binding_digest", "profile_digest", "model"}
            ),
            "profile_digest": profile.profile_digest,
            "model": model,
        }
    )
    return replace(base, profile=profile, binding=binding)


class DeterministicHostedChild(_LoggedModel):
    @property
    def _llm_type(self) -> str:
        return "mission-control-local-child"

    def _reply(self, messages: list[BaseMessage]) -> ChatResult:
        human = " ".join(str(item.content) for item in messages if isinstance(item, HumanMessage))
        has_wait_result = any(isinstance(item, ToolMessage) for item in messages)
        if "wait" in human.lower() and not has_wait_result:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "wait_seconds",
                        "args": {"seconds": 90},
                        "id": "bounded-local-wait",
                        "type": "tool_call",
                    }
                ],
                usage_metadata=call_usage(),
            )
        else:
            message = AIMessage(content="PONG", usage_metadata=call_usage())
        return ChatResult(generations=[ChatGeneration(message=message)])


_stack: AsyncExitStack | None = None
_compiled: Any = None
_lock = asyncio.Lock()


def hosting() -> HostedGraph:
    return HostedGraph(deterministic_child_definition(), graph)


async def graph(config: dict) -> Any:
    del config
    global _stack, _compiled
    async with _lock:
        if _compiled is None:
            definition = deterministic_child_definition()
            registry = replace(
                hosting_registry(definition),
                model_factories={
                    definition.binding.model.ref.digest: lambda bound, _secrets: (
                        DeterministicHostedChild(
                            run_id=bound.run_id, operation_id=bound.operation_id, log=[]
                        )
                    )
                },
            )
            _stack = AsyncExitStack()
            _compiled = await build_hosted_graph(
                definition, registry=registry, secrets={}, stack=_stack
            )
        return _compiled


@asynccontextmanager
async def local_agent_server(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    profile: str = "runtime",
    authorization_out: list[str] | None = None,
) -> AsyncIterator[str]:
    await asyncio.to_thread(root.mkdir, parents=True, exist_ok=True)
    # The qualification's reserved loopback port (MISSION_CONTROL_AGENT_SERVER_PORT, default
    # 8133); "0" selects any free port.
    port = int(os.environ.get("MISSION_CONTROL_AGENT_SERVER_PORT", "8133"))
    if port == 0:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    signing_secret = secrets.token_urlsafe(32)
    monkeypatch.setenv("BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN", signing_secret)
    monkeypatch.setenv("AGENT_SERVER_ENDPOINT", endpoint)
    config = CANONICAL_CONFIG
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join((str(config.parents[1] / "src"), str(config.parents[1]))),
        "MISSION_CONTROL_AGENT_SERVER_PROFILE": profile,
        "MISSION_CONTROL_AGENT_HOSTING_FACTORY": (
            "tests.fixtures.mission_control_local_agent_server:hosting"
        ),
        "MISSION_CONTROL_AGENT_AUTHORITY_ENABLED": "0",
        "PYTHONUTF8": "1",
        "LANGSMITH_TRACING": "false",
        "LANGCHAIN_TRACING_V2": "false",
        "LANGSMITH_API_KEY": "",
        "OPENAI_API_KEY": "",
        "LANGGRAPH_DEPLOYMENT_LICENSE": "",
    }
    token = mint_scope_claim(signing_secret, SCOPE)
    readiness_path = "/belllabs/async-subagents/served-graphs"
    if profile != "runtime":
        from joserfc import jwt
        from joserfc.jwk import RSAKey

        key = RSAKey.generate_key(2048)
        now = int(time.time())
        issuer = "https://local-agent-qualification.example"
        environment.update(
            {
                "BELL_LABS_AGENT_AUTH_ISSUER": issuer,
                "BELL_LABS_AGENT_AUTH_PUBLIC_KEY": "",
                "BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64": base64.b64encode(
                    key.as_pem(private=False)
                ).decode(),
                "BELL_LABS_AGENT_AUTH_AUDIENCE": "authenticated",
                "BELL_LABS_AGENT_AUTH_ALGORITHM": "RS256",
            }
        )
        token = jwt.encode(
            {"alg": "RS256"},
            {
                "sub": "local-qualification-operator",
                "iss": issuer,
                "aud": "authenticated",
                "iat": now,
                "exp": now + 600,
                "request_scopes": [SCOPE],
                "roles": ["operator"],
            },
            key,
        )
        readiness_path = "/v2/block-c/qualification"
    if authorization_out is not None:
        authorization_out.append("Bearer " + token)
    with (root / "agent-server.log").open("w", encoding="utf-8") as output:
        process = await asyncio.to_thread(
            subprocess.Popen,
            [
                sys.executable,
                "-m",
                "langgraph_cli",
                "dev",
                "--config",
                str(config.resolve()),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--no-browser",
                "--no-reload",
                "--allow-blocking",
            ],
            cwd=root,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        try:
            async with httpx.AsyncClient(timeout=2) as client, asyncio.timeout(45):
                while True:
                    if process.poll() is not None:
                        raise RuntimeError(
                            f"local Agent Server exited; inspect {root / 'agent-server.log'}"
                        )
                    try:
                        response = await client.get(
                            endpoint + readiness_path,
                            headers={"Authorization": "Bearer " + token},
                        )
                        if response.status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    await asyncio.sleep(0.2)
            yield endpoint
        finally:
            process.terminate()
            try:
                await asyncio.to_thread(process.wait, timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=5)
