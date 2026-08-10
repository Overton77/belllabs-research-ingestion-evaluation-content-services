from __future__ import annotations

import asyncio
import importlib.metadata
import types
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, make_dataclass
from dataclasses import field as dataclass_field
from pathlib import PurePosixPath
from typing import Annotated, Any, cast

from deepagents import DeepAgentState
from deepagents.backends import LangSmithSandbox, StateBackend
from deepagents.backends.protocol import BackendProtocol
from deepagents.backends.utils import create_file_data
from langchain.agents.middleware.types import AgentMiddleware
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.base import BaseStore
from langsmith.sandbox import SandboxClient
from pydantic import SecretStr

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    CognitiveChannelDefinition,
    CognitiveRuntimeContextSchema,
    CognitiveRuntimeField,
    CognitiveStateSchema,
    DeepAgentExecutionBinding,
    DeepAgentMCPServerComponent,
    DeepAgentSandboxComponent,
)
from app.domain.operation_execution.errors import (
    DeepAgentMaterializationError,
    DeepAgentRuntimeDrift,
    DeepAgentUnsupportedPlacement,
)

ResolvedSecrets = Mapping[str, str]
ModelFactory = Callable[[DeepAgentExecutionBinding, ResolvedSecrets], BaseChatModel]
SandboxFactory = Callable[
    [DeepAgentSandboxComponent, ResolvedSecrets],
    AbstractAsyncContextManager[BackendProtocol],
]


@dataclass(frozen=True)
class ResolvedSkillBundle:
    bundle_digest: str
    files: tuple[tuple[str, bytes], ...]

    def verify(self, *, skill_md_digest: str) -> None:
        normalized: list[dict[str, object]] = []
        skill_md: bytes | None = None
        seen: set[str] = set()
        for raw_path, content in self.files:
            path = PurePosixPath(raw_path.replace("\\", "/"))
            if path.is_absolute() or ".." in path.parts or str(path) in seen:
                raise DeepAgentRuntimeDrift("Skill bundle contains an unsafe or duplicate path")
            seen.add(str(path))
            normalized.append(
                {
                    "path": str(path),
                    "digest": sha256_digest(content.decode("utf-8")),
                    "size_bytes": len(content),
                }
            )
            if str(path) == "SKILL.md":
                skill_md = content
        if skill_md is None:
            raise DeepAgentRuntimeDrift("exact Skill bundle does not contain SKILL.md")
        if sha256_digest(skill_md.decode("utf-8")) != skill_md_digest:
            raise DeepAgentRuntimeDrift("SKILL.md content drifted from the exact binding")
        if sha256_digest(sorted(normalized, key=lambda item: str(item["path"]))) != (
            self.bundle_digest
        ):
            raise DeepAgentRuntimeDrift("Skill bundle manifest drifted from the exact binding")


@dataclass(frozen=True)
class ExactComponentRegistry:
    """Run-local exact revisions; keys are immutable definition digests, never aliases."""

    model_factories: Mapping[str, ModelFactory]
    prompts: Mapping[str, str] = dataclass_field(default_factory=dict)
    tools: Mapping[str, BaseTool] = dataclass_field(default_factory=dict)
    middleware: Mapping[str, AgentMiddleware[Any, Any, Any]] = dataclass_field(default_factory=dict)
    skill_bundles: Mapping[str, ResolvedSkillBundle] = dataclass_field(default_factory=dict)
    sandbox_factories: Mapping[str, SandboxFactory] = dataclass_field(default_factory=dict)
    checkpointers: Mapping[str, BaseCheckpointSaver[Any]] = dataclass_field(default_factory=dict)
    stores: Mapping[str, BaseStore] = dataclass_field(default_factory=dict)


@dataclass(frozen=True)
class MaterializedDeepAgentArguments:
    model: BaseChatModel
    tools: tuple[BaseTool, ...]
    middleware: tuple[AgentMiddleware[Any, Any, Any], ...]
    subagents: tuple[dict[str, object], ...]
    skills: tuple[str, ...]
    backend: BackendProtocol
    state_schema: type[DeepAgentState]
    context_schema: type[Any]
    context: object
    checkpointer: BaseCheckpointSaver[Any]
    store: BaseStore
    initial_state: dict[str, object]
    resolved_attachments: tuple[dict[str, str], ...]


class StateSandboxFactory:
    def __call__(
        self,
        component: DeepAgentSandboxComponent,
        _secrets: ResolvedSecrets,
    ) -> AbstractAsyncContextManager[BackendProtocol]:
        if component.backend != "state":
            raise DeepAgentUnsupportedPlacement("state backend factory cannot change placement")

        @asynccontextmanager
        async def context() -> AsyncIterator[BackendProtocol]:
            yield StateBackend()

        return context()


class OpenAIExactModelFactory:
    """Construct ChatOpenAI only from an exact component and resolved credential ref."""

    _allowed_settings = {
        "reasoning_effort",
        "verbosity",
        "max_completion_tokens",
        "temperature",
        "service_tier",
        "use_responses_api",
    }

    def __init__(self, *, secret_key: str = "environment:OPENAI_API_KEY") -> None:
        self._secret_key = secret_key

    def __call__(
        self,
        binding: DeepAgentExecutionBinding,
        secrets: ResolvedSecrets,
    ) -> BaseChatModel:
        unsupported = set(binding.model.settings) - self._allowed_settings
        if unsupported:
            raise DeepAgentMaterializationError(
                "unsupported exact OpenAI model settings: " + ", ".join(sorted(unsupported))
            )
        try:
            api_key = secrets[self._secret_key]
        except KeyError as error:
            raise DeepAgentMaterializationError(
                "OpenAI model credential reference was not resolved"
            ) from error
        return ChatOpenAI(
            model=binding.model.model_name,
            api_key=SecretStr(api_key),
            max_retries=0,
            **binding.model.settings,
        )


class LangSmithSandboxFactory:
    def __call__(
        self,
        component: DeepAgentSandboxComponent,
        secrets: ResolvedSecrets,
    ) -> AbstractAsyncContextManager[BackendProtocol]:
        if component.backend != "langsmith":
            raise DeepAgentUnsupportedPlacement("LangSmith factory cannot change placement")
        api_key = _single_secret(component, secrets)

        @asynccontextmanager
        async def context() -> AsyncIterator[BackendProtocol]:
            client = SandboxClient(api_key=api_key)
            sandbox = await asyncio.to_thread(
                client.create_sandbox,
                snapshot_name=component.snapshot_ref,
                idle_ttl_seconds=component.idle_ttl_seconds,
            )
            backend = LangSmithSandbox(sandbox=sandbox)
            try:
                yield backend
            finally:
                await backend.aclose()
                await asyncio.to_thread(client.delete_sandbox, sandbox.name)

        return context()


class ExactDeepAgentMaterializer:
    """Resolves a frozen binding to framework inputs and owns provider lifecycles."""

    def __init__(self, registry: ExactComponentRegistry) -> None:
        self._registry = registry

    @asynccontextmanager
    async def prepare(
        self,
        binding: DeepAgentExecutionBinding,
        secrets: ResolvedSecrets,
    ) -> AsyncIterator[MaterializedDeepAgentArguments]:
        self._verify_runtime(binding)
        async with AsyncExitStack() as stack:
            model_factory = _exact(
                self._registry.model_factories,
                binding.model.ref.digest,
                "model",
            )
            model = model_factory(binding, secrets)
            backend_factory = _exact(
                self._registry.sandbox_factories,
                binding.sandbox.ref.digest,
                "sandbox",
            )
            backend = await stack.enter_async_context(backend_factory(binding.sandbox, secrets))
            skill_sources, state_skill_files = await self._mount_skills(binding, backend)
            tools = [
                self._resolve_tool(item.ref.digest, item.schema_digest, item.tool_name)
                for item in binding.tools
            ]
            mcp_tools = await self._load_mcp_tools(binding.mcp_servers, secrets)
            tools.extend(mcp_tools)
            middleware = tuple(
                _exact(self._registry.middleware, item.ref.digest, "middleware")
                for item in binding.middleware
            )
            subagents = self._materialize_subagents(binding, secrets, skill_sources)
            checkpointer = _exact(
                self._registry.checkpointers,
                binding.checkpointer_ref.digest,
                "checkpointer",
            )
            store = _exact(self._registry.stores, binding.store_ref.digest, "store")
            state_schema = _state_type(binding.cognitive_state_schema)
            context_schema = _context_type(binding.cognitive_context_schema)
            context = _context_instance(
                context_schema,
                binding.cognitive_context_schema,
                binding.cognitive_context_values,
            )
            initial_state: dict[str, object] = {
                "artifact_index": dict(binding.initial_artifact_index),
                "context_manifest": dict(binding.initial_context_manifest),
                "child_result_index": list(binding.initial_child_result_index),
            }
            if state_skill_files:
                initial_state["files"] = state_skill_files
            yield MaterializedDeepAgentArguments(
                model=model,
                tools=tuple(tools),
                middleware=middleware,
                subagents=subagents,
                skills=skill_sources,
                backend=backend,
                state_schema=state_schema,
                context_schema=context_schema,
                context=context,
                checkpointer=checkpointer,
                store=store,
                initial_state=initial_state,
                resolved_attachments=tuple(
                    item.model_copy(update={"status": "resolved"}).model_dump(mode="json")
                    for item in binding.intended_attachments
                ),
            )

    def _materialize_subagents(
        self,
        binding: DeepAgentExecutionBinding,
        secrets: ResolvedSecrets,
        skill_sources: tuple[str, ...],
    ) -> tuple[dict[str, object], ...]:
        tools_by_ref = {item.ref: item for item in binding.tools}
        skills_by_ref = {item.ref: item for item in binding.skills}
        result: list[dict[str, object]] = []
        for child in binding.sync_subagents:
            model_factory = _exact(
                self._registry.model_factories,
                child.model.ref.digest,
                "subagent model",
            )
            child_binding = binding.model_copy(update={"model": child.model})
            child_tools = [
                self._resolve_tool(
                    tools_by_ref[ref].ref.digest,
                    tools_by_ref[ref].schema_digest,
                    tools_by_ref[ref].tool_name,
                )
                for ref in child.tool_refs
            ]
            child_skill_roots = {
                str(PurePosixPath(skills_by_ref[ref].mount_root).parent) for ref in child.skill_refs
            }
            if not child_skill_roots <= set(skill_sources):
                raise DeepAgentRuntimeDrift("subagent Skill projection is unavailable")
            system_prompt = _exact(
                self._registry.prompts,
                child.system_prompt_ref.digest,
                "subagent prompt",
            )
            result.append(
                {
                    "name": child.name,
                    "description": child.description,
                    "system_prompt": system_prompt,
                    "model": model_factory(child_binding, secrets),
                    "tools": child_tools,
                    "skills": sorted(child_skill_roots),
                    "permissions": [
                        {
                            "operations": ["read", "write"],
                            "paths": list(child.writable_paths),
                            "mode": "allow",
                        },
                        {
                            "operations": ["read", "write"],
                            "paths": ["/"],
                            "mode": "deny",
                        },
                    ],
                }
            )
        return tuple(result)

    @staticmethod
    def _verify_runtime(binding: DeepAgentExecutionBinding) -> None:
        if binding.silent_fallback:
            raise DeepAgentUnsupportedPlacement("silent Deep Agent fallback is forbidden")
        if binding.placement != "local_in_worker":
            raise DeepAgentUnsupportedPlacement(
                "this adapter materializes only local-in-worker Deep Agent placement"
            )
        for package, expected in binding.package_versions.items():
            try:
                actual = importlib.metadata.version(package)
            except importlib.metadata.PackageNotFoundError as error:
                raise DeepAgentRuntimeDrift(
                    f"required runtime package is absent: {package}"
                ) from error
            if actual != expected:
                raise DeepAgentRuntimeDrift(
                    f"runtime package drift for {package}: expected {expected}, observed {actual}"
                )
        if binding.cognitive_state_schema.deepagents_version != "0.7.5":
            raise DeepAgentRuntimeDrift("cognitive schema baseline is not Deep Agents 0.7.5")

    async def _mount_skills(
        self,
        binding: DeepAgentExecutionBinding,
        backend: BackendProtocol,
    ) -> tuple[tuple[str, ...], dict[str, object]]:
        uploads: list[tuple[str, bytes]] = []
        sources: set[str] = set()
        for component in binding.skills:
            bundle = _exact(
                self._registry.skill_bundles,
                component.bundle_digest,
                "Skill bundle",
            )
            if bundle.bundle_digest != component.bundle_digest:
                raise DeepAgentRuntimeDrift("resolved Skill bundle identity drift")
            bundle.verify(skill_md_digest=component.skill_md_digest)
            root = PurePosixPath(component.mount_root)
            sources.add(str(root.parent))
            uploads.extend((str(root / path), content) for path, content in bundle.files)
        state_files: dict[str, object] = {}
        if isinstance(backend, StateBackend):
            state_files = {
                path: create_file_data(content.decode("utf-8")) for path, content in uploads
            }
        elif uploads:
            responses = await backend.aupload_files(uploads)
            failures = [item for item in responses if item.error]
            if failures:
                raise DeepAgentMaterializationError("exact Skill bundle failed sandbox attachment")
        return tuple(sorted(sources)), state_files

    def _resolve_tool(self, digest: str, schema_digest: str, name: str) -> BaseTool:
        tool = _exact(self._registry.tools, digest, "tool")
        if tool.name != name:
            raise DeepAgentRuntimeDrift("resolved tool name drift")
        observed = _tool_schema_digest(tool)
        if observed != schema_digest:
            raise DeepAgentRuntimeDrift("resolved tool schema drift")
        return tool

    async def _load_mcp_tools(
        self,
        servers: tuple[DeepAgentMCPServerComponent, ...],
        secrets: ResolvedSecrets,
    ) -> tuple[BaseTool, ...]:
        if not servers:
            return ()
        connections: dict[str, dict[str, object]] = {}
        expected = {}
        for server in servers:
            if server.transport == "stdio":
                environment = {
                    ref.key: _secret_value(ref.provider, ref.key, secrets)
                    for ref in server.credential_refs
                }
                connections[server.server_name] = {
                    "transport": "stdio",
                    "command": cast(str, server.command),
                    "args": list(server.arguments),
                    "env": environment or None,
                }
            else:
                headers = {}
                if server.credential_refs:
                    headers["Authorization"] = "Bearer " + _secret_value(
                        server.credential_refs[0].provider,
                        server.credential_refs[0].key,
                        secrets,
                    )
                connections[server.server_name] = {
                    "transport": server.transport,
                    "url": cast(str, server.endpoint),
                    "headers": headers or None,
                }
            for expected_tool in server.tools:
                if expected_tool.tool_name in expected:
                    raise DeepAgentMaterializationError("MCP tool namespace collision")
                expected[expected_tool.tool_name] = expected_tool.schema_digest
        # langchain-mcp-adapters 0.3.1 deliberately rejects client-level async
        # context management. get_tools() returns tools that open scoped sessions.
        client = MultiServerMCPClient(cast(Any, connections))
        tools = await client.get_tools()
        observed_names = {tool.name for tool in tools}
        if observed_names != set(expected):
            raise DeepAgentRuntimeDrift("MCP server tool filter drift")
        for loaded_tool in tools:
            observed = _tool_schema_digest(loaded_tool)
            if observed != expected[loaded_tool.name]:
                raise DeepAgentRuntimeDrift(f"MCP tool schema drift: {loaded_tool.name}")
        return tuple(tools)


def _exact(registry: Mapping[str, Any], digest: str, kind: str) -> Any:
    try:
        return registry[digest]
    except KeyError as error:
        raise DeepAgentMaterializationError(
            f"exact {kind} revision is unavailable; runtime lookup/fallback is forbidden"
        ) from error


def _tool_schema_digest(tool: BaseTool) -> str:
    schema_type = tool.get_input_schema()
    schema = (
        schema_type.model_json_schema()
        if hasattr(schema_type, "model_json_schema")
        else schema_type.schema()
    )
    return sha256_digest(schema)


def _single_secret(component: DeepAgentSandboxComponent, secrets: ResolvedSecrets) -> str:
    if len(component.credential_refs) != 1:
        raise DeepAgentMaterializationError("sandbox requires one exact credential reference")
    ref = component.credential_refs[0]
    return _secret_value(ref.provider, ref.key, secrets)


def _secret_value(provider: str, key: str, secrets: ResolvedSecrets) -> str:
    try:
        return secrets[f"{provider}:{key}"]
    except KeyError as error:
        raise DeepAgentMaterializationError(
            "required credential reference was not resolved"
        ) from error


def _merge_by_key(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    return {**left, **right}


def _replace(_left: Any, right: Any) -> Any:
    return right


def _append_unique_by_id(
    left: list[dict[str, Any]], right: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    result = list(left)
    identities = {str(item.get("id", item.get("ref", item))) for item in result}
    for item in right:
        identity = str(item.get("id", item.get("ref", item)))
        if identity not in identities:
            result.append(item)
            identities.add(identity)
    return result


def _channel_type(channel: CognitiveChannelDefinition) -> object:
    if channel.value_kind in {"object", "map"}:
        base: object = dict[str, Any]
    elif channel.value_kind == "string_list":
        base = list[str]
    else:
        base = list[dict[str, Any]]
    reducer = {
        "replace": _replace,
        "merge_by_key": _merge_by_key,
        "append_unique_by_id": _append_unique_by_id,
    }[channel.reducer.value]
    return Annotated[base, reducer]


def _state_type(schema: CognitiveStateSchema) -> type[DeepAgentState]:
    # Framework-owned middleware schemas are composed into the graph by
    # create_deep_agent itself. Redeclaring these channels here would erase
    # PrivateStateAttr and cause an empty reducer default to suppress loading.
    annotations = {
        channel.name: _channel_type(channel)
        for channel in schema.channels
        if channel.name not in {"skills_metadata", "skills_load_errors"}
    }

    def body(namespace: dict[str, object]) -> None:
        namespace["__annotations__"] = annotations
        namespace["__module__"] = __name__

    return cast(
        type[DeepAgentState],
        types.new_class(
            f"BellLabsState_{schema.schema_digest.removeprefix('sha256:')[:12]}",
            (DeepAgentState,),
            {"total": False},
            body,
        ),
    )


def _runtime_python_type(field: CognitiveRuntimeField) -> type[Any]:
    return {
        "string": str,
        "integer": int,
        "boolean": bool,
        "string_list": tuple[str, ...],
        "string_map": dict[str, str],
    }[field.value_kind]


def _context_type(schema: CognitiveRuntimeContextSchema) -> type[Any]:
    return make_dataclass(
        f"BellLabsContext_{schema.schema_digest.removeprefix('sha256:')[:12]}",
        [(item.name, _runtime_python_type(item)) for item in schema.fields],
        frozen=True,
        slots=True,
    )


def _context_instance(
    context_type: type[Any],
    schema: CognitiveRuntimeContextSchema,
    values: Mapping[str, object],
) -> object:
    for context_field in schema.fields:
        expected = _runtime_python_type(context_field)
        value = values[context_field.name]
        if not isinstance(value, expected):
            raise DeepAgentMaterializationError(
                f"runtime context field {context_field.name!r} does not match its frozen value kind"
            )
    return context_type(**values)
