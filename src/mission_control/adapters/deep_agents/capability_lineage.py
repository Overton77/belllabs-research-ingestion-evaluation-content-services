"""Sanitized capability lineage of one bounded Deep Agent invocation (RRM-009, CP-050).

The record answers, for one operation attempt and without any secret value, prompt text or
tool argument value: which exact capabilities were mounted (credential references by name,
the MCP tool filter with every tool's input-schema digest, Skill bundle digests, host tool
schema digests, sync and async subagent slices), where the cognition ran (placement, task
queue, sandbox, checkpointer and store definition digests, package versions), which
capabilities the agent actually invoked (each call's tool, kind, argument names, result digest
and outcome; for a governed browser call only the scheme, host and path it requested, never
its query or fragment) and the model usage observed. Model-chosen strings that are recorded
(a Skill path, a sync subagent name) are bounded; an unknown subagent name is not echoed.
No digest of argument values is kept: an unkeyed digest of a short, guessable argument (a
search query, a URL) would let anyone holding the record confirm a guess (RRM-009 review).

Scope of the "no prompt text" property: this record only. Other runtime event payloads that
are persisted beside it (the adapter's inspection payload) carry the pinned, reviewed Skill
instruction text and bundle manifests the agent was given, never operator input.

It is built from the exact binding and the invocation's own messages only, so the record is
reproducible from the checkpointed state, and it is persisted with the settlement's
digest-bound output payload (`journaled_operation_execution.output_payload`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

from langchain_core.messages import AIMessage, BaseMessage, ToolMessage

from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import ExactDefinitionRef, SecretRef
from mission_control.domain.execution.contracts import DeepAgentExecutionBinding

CAPABILITY_LINEAGE_KIND = "deep_agent.capability_lineage.v1"
SYNC_SUBAGENT_TOOL = "task"
ASYNC_SUBAGENT_TOOLS = frozenset(
    {
        "start_async_task",
        "check_async_task",
        "update_async_task",
        "cancel_async_task",
        "list_async_tasks",
    }
)
BROWSER_TOOL_NAMES = frozenset({"agent_browser_page"})
MAX_RECORDED_PATH_CHARS = 256
UNKNOWN_SUBAGENT = "<not a bound sync subagent>"


def requested_location(url: str) -> str:
    """Scheme, host and path of a requested URL; query, fragment and user info dropped."""

    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if not parts.scheme or not host:
        return "<unparseable>"
    return f"{parts.scheme}://{host}{parts.path[:MAX_RECORDED_PATH_CHARS]}"


def _ref(ref: ExactDefinitionRef) -> dict[str, object]:
    return {
        "kind": ref.kind.value,
        "logical_id": ref.logical_id,
        "revision": ref.revision,
        "digest": ref.digest,
    }


def _text(message: BaseMessage) -> str:
    if isinstance(message.content, str):
        return message.content
    parts: list[str] = []
    for block in message.content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict) and isinstance(block.get("text"), str):
            parts.append(str(block["text"]))
    return "\n".join(parts)


def credential_references(
    binding: DeepAgentExecutionBinding, operation_secret_refs: Iterable[SecretRef] = ()
) -> list[str]:
    """Every credential reference the operation can resolve, by name only."""

    refs = {
        f"{ref.provider}:{ref.key}"
        for ref in (
            *operation_secret_refs,
            *(item for server in binding.mcp_servers for item in server.credential_refs),
            *binding.sandbox.credential_refs,
        )
    }
    refs.update(
        contract.deployment_credential_ref
        for contract in binding.async_subagents
        if contract.deployment_credential_ref is not None
    )
    return sorted(refs)


def mounted_capabilities(binding: DeepAgentExecutionBinding) -> dict[str, object]:
    """The exact capability surface the binding mounts (filters and schema digests)."""

    return {
        "mcp_servers": [
            {
                "server_name": server.server_name,
                "ref": _ref(server.ref),
                "transport": server.transport,
                "schema_digest": server.schema_digest,
                "credential_refs": sorted(
                    f"{ref.provider}:{ref.key}" for ref in server.credential_refs
                ),
                "tool_filter": [
                    {"tool_name": tool.tool_name, "schema_digest": tool.schema_digest}
                    for tool in sorted(server.tools, key=lambda item: item.tool_name)
                ],
            }
            for server in sorted(binding.mcp_servers, key=lambda item: item.server_name)
        ],
        "tools": [
            {
                "tool_name": tool.tool_name,
                "ref": _ref(tool.ref),
                "schema_digest": tool.schema_digest,
            }
            for tool in sorted(binding.tools, key=lambda item: item.tool_name)
        ],
        "skills": [
            {
                "skill_name": skill.skill_name,
                "ref": _ref(skill.ref),
                "bundle_digest": skill.bundle_digest,
                "skill_md_digest": skill.skill_md_digest,
                "mount_root": skill.mount_root,
            }
            for skill in sorted(binding.skills, key=lambda item: item.skill_name)
        ],
        "sync_subagents": [
            {
                "name": child.name,
                "model_ref": _ref(child.model.ref),
                "tool_refs": [_ref(ref) for ref in child.tool_refs],
                "skill_refs": [_ref(ref) for ref in child.skill_refs],
                "state_slice_id": child.state_slice_id,
                "context_slice_id": child.context_slice_id,
                "writable_paths": list(child.writable_paths),
                "budget_limits": dict(sorted(child.budget_limits.items())),
            }
            for child in sorted(binding.sync_subagents, key=lambda item: item.name)
        ],
        "async_subagents": [
            {
                "name": contract.name,
                "contract_digest": contract.contract_digest,
                "graph_id": contract.graph_id,
                "graph_revision": contract.graph_revision,
                "graph_binding_digest": contract.graph_binding_digest,
                "agent_protocol_url": contract.agent_protocol_url,
                "deployment_credential_ref": contract.deployment_credential_ref,
                "budget_limits": dict(sorted(contract.budget_limits.items())),
            }
            for contract in sorted(binding.async_subagents, key=lambda item: item.name)
        ],
        "capability_grant": {
            "capabilities": sorted(binding.capability_grant.capabilities),
            "tool_ids": sorted(binding.capability_grant.tool_ids),
            "mcp_server_ids": sorted(binding.capability_grant.mcp_server_ids),
            "network_hosts": sorted(binding.capability_grant.network_hosts),
        },
    }


def placement(binding: DeepAgentExecutionBinding) -> dict[str, object]:
    return {
        "placement": binding.placement,
        "placement_digest": binding.placement_digest,
        "task_queue": binding.task_queue,
        "model": {
            "ref": _ref(binding.model.ref),
            "provider": binding.model.provider,
            "model_name": binding.model.model_name,
        },
        "sandbox": {
            "ref": _ref(binding.sandbox.ref),
            "backend": binding.sandbox.backend,
            "runtime_digest": binding.sandbox.runtime_digest,
        },
        "checkpointer_ref": _ref(binding.checkpointer_ref),
        "store_ref": _ref(binding.store_ref),
        "package_versions": dict(sorted(binding.package_versions.items())),
    }


def _kind(name: str, arguments: Mapping[str, Any], binding: DeepAgentExecutionBinding) -> str:
    if any(name == tool.tool_name for server in binding.mcp_servers for tool in server.tools):
        return "mcp"
    if name in BROWSER_TOOL_NAMES or any(name == tool.tool_name for tool in binding.tools):
        return "tool"
    if name == SYNC_SUBAGENT_TOOL:
        return "sync_subagent"
    if name in ASYNC_SUBAGENT_TOOLS:
        return "async_subagent"
    path = str(arguments.get("file_path", ""))
    if name == "read_file" and any(
        path.startswith(skill.mount_root.rstrip("/") + "/") for skill in binding.skills
    ):
        return "skill_read"
    return "framework"


def invocations(
    binding: DeepAgentExecutionBinding, messages: Sequence[BaseMessage]
) -> list[dict[str, object]]:
    """Every tool call of the invocation with digests of its arguments and result."""

    results = {
        str(message.tool_call_id): message
        for message in messages
        if isinstance(message, ToolMessage)
    }
    mcp_servers = {
        tool.tool_name: server.server_name
        for server in binding.mcp_servers
        for tool in server.tools
    }
    sync_children = {child.name for child in binding.sync_subagents}
    records: list[dict[str, object]] = []
    for message in messages:
        if not isinstance(message, AIMessage):
            continue
        for call in message.tool_calls:
            name = str(call.get("name", ""))
            arguments = call.get("args") if isinstance(call.get("args"), dict) else {}
            assert isinstance(arguments, dict)
            call_id = str(call.get("id", ""))
            kind = _kind(name, arguments, binding)
            record: dict[str, object] = {
                "tool_call_id": call_id,
                "tool_name": name,
                "kind": kind,
                "argument_names": sorted(str(key)[:64] for key in arguments),
            }
            if kind == "mcp":
                record["mcp_server"] = mcp_servers[name]
            if name in BROWSER_TOOL_NAMES and isinstance(arguments.get("url"), str):
                record["requested_url"] = requested_location(arguments["url"])
            if kind == "sync_subagent":
                child = str(arguments.get("subagent_type", ""))
                record["subagent_type"] = child if child in sync_children else UNKNOWN_SUBAGENT
            if kind == "skill_read":
                record["path"] = str(arguments.get("file_path", ""))[:MAX_RECORDED_PATH_CHARS]
            result = results.get(call_id)
            if result is None:
                record["status"] = "no_result"
            else:
                text = _text(result)
                record["status"] = "error" if result.status == "error" else "success"
                record["result_digest"] = sha256_digest(text)
                record["result_chars"] = len(text)
            records.append(record)
    return records


def capability_lineage(
    binding: DeepAgentExecutionBinding,
    messages: Sequence[BaseMessage],
    *,
    usage_amounts: Mapping[str, int],
    model_calls: Iterable[Mapping[str, object]],
    disclosed_skills: Sequence[Mapping[str, str]],
    operation_secret_refs: Iterable[SecretRef] = (),
) -> dict[str, object]:
    """The sanitized lineage record of one invocation (`messages` are its own messages)."""

    calls = invocations(binding, messages)
    return {
        "kind": CAPABILITY_LINEAGE_KIND,
        "binding_id": binding.binding_id,
        "binding_digest": binding.binding_digest,
        "operation_id": binding.operation_id,
        "placement": placement(binding),
        "credential_refs": credential_references(binding, operation_secret_refs),
        "mounted": mounted_capabilities(binding),
        "disclosed_skills": [dict(item) for item in disclosed_skills],
        "invocations": calls,
        "invoked": {
            kind: sorted({str(item["tool_name"]) for item in calls if item["kind"] == kind})
            for kind in sorted({str(item["kind"]) for item in calls})
        },
        "usage": {
            "amounts": dict(sorted(usage_amounts.items())),
            "model_calls": [dict(item) for item in model_calls],
        },
    }


__all__ = [
    "CAPABILITY_LINEAGE_KIND",
    "capability_lineage",
    "credential_references",
    "invocations",
    "mounted_capabilities",
    "placement",
    "requested_location",
]
