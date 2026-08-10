"""LangSmith tracing bootstrap for provider-neutral BellLabs runtimes."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from langsmith import traceable
from langsmith.wrappers import wrap_openai
from openai import AsyncOpenAI

from app.config import Settings

_CONFIGURED = False


def configure_langsmith_tracing(settings: Settings) -> bool:
    """Export LangSmith environment configuration.

    Returns True when tracing is enabled and an API key is present.
    """
    global _CONFIGURED
    api_key = (
        settings.langsmith_api_key.get_secret_value().strip()
        if settings.langsmith_api_key is not None
        else ""
    )
    enabled = bool(settings.langsmith_tracing and api_key)
    os.environ["LANGSMITH_TRACING"] = "true" if enabled else "false"
    if enabled:
        os.environ["LANGSMITH_API_KEY"] = api_key

    project = settings.langsmith_project.strip()
    if project:
        os.environ["LANGSMITH_PROJECT"] = project
    endpoint = settings.langsmith_endpoint.strip()
    if endpoint:
        os.environ["LANGSMITH_ENDPOINT"] = endpoint
    workspace_id = (settings.langsmith_workspace_id or "").strip()
    if workspace_id:
        os.environ["LANGSMITH_WORKSPACE_ID"] = workspace_id

    _CONFIGURED = True
    return enabled


def create_traced_async_openai(api_key: str) -> AsyncOpenAI:
    """Return an AsyncOpenAI client whose Responses/Chat/Embeddings calls are traced."""
    return wrap_openai(AsyncOpenAI(api_key=api_key))


def runtime_execute_metadata(
    invocation: Any,
    *,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    binding = getattr(invocation, "binding", None)
    model_policy = getattr(binding, "model_policy", None)
    deep_binding = getattr(binding, "deep_agent_binding", None)
    model_component = getattr(deep_binding, "model", None)
    metadata: dict[str, object] = {
        "runtime": "deepagents",
        "run_id": getattr(binding, "run_id", None),
        "binding_id": getattr(binding, "binding_id", None),
        "operation_id": getattr(binding, "operation_id", None),
        "request_scope": getattr(binding, "request_scope", None),
        "configuration_digest": getattr(binding, "effective_configuration_digest", None),
        "model": getattr(model_component, "model_name", None)
        or getattr(model_policy, "model", None),
        "provider": getattr(model_component, "provider", None)
        or getattr(model_policy, "provider", None),
        "workspace_id": getattr(getattr(invocation, "workspace", None), "workspace_id", None),
    }
    if extra:
        metadata.update(dict(extra))
    return {key: value for key, value in metadata.items() if value is not None}


def process_runtime_execute_inputs(inputs: dict[str, Any]) -> dict[str, Any]:
    """Redact secrets and prompt bodies from LangSmith root-span inputs."""
    invocation = inputs.get("invocation")
    binding = getattr(invocation, "binding", None)
    deep_binding = getattr(binding, "deep_agent_binding", None)
    model_component = getattr(deep_binding, "model", None)
    return {
        "run_id": getattr(binding, "run_id", None),
        "binding_id": getattr(binding, "binding_id", None),
        "operation_id": getattr(binding, "operation_id", None),
        "request_scope": getattr(binding, "request_scope", None),
        "model": getattr(model_component, "model_name", None)
        or getattr(getattr(binding, "model_policy", None), "model", None),
        "secret_names": list(getattr(invocation, "resolved_secret_names", ()) or ()),
        "prompt_segment_count": len(getattr(invocation, "prompt_segments", ()) or ()),
        "resolved_secrets": "[redacted]",
    }


def process_runtime_execute_outputs(outputs: Any) -> dict[str, Any]:
    """Summarize RuntimeResult without shipping full model text to LangSmith."""
    if outputs is None:
        return {}
    usage = getattr(outputs, "usage", None)
    output_text = getattr(outputs, "output_text", "") or ""
    return {
        "provider_run_id": getattr(outputs, "provider_run_id", None),
        "output_ref_count": len(getattr(outputs, "output_refs", ()) or ()),
        "has_structured_output": getattr(outputs, "structured_output", None) is not None,
        "usage": getattr(usage, "amounts", None),
        "output_text_chars": len(output_text),
    }


def process_embedding_inputs(inputs: dict[str, Any]) -> dict[str, Any]:
    texts = inputs.get("texts")
    if texts is None and "text" in inputs:
        texts = (inputs.get("text"),)
    if not isinstance(texts, (list, tuple)):
        return {"batch_size": 0, "input_chars": 0}
    return {
        "batch_size": len(texts),
        "input_chars": sum(len(str(item)) for item in texts),
    }


def process_embedding_outputs(outputs: Any) -> dict[str, Any]:
    if outputs is None:
        return {}
    if not isinstance(outputs, tuple):
        outputs = (outputs,)
    first = outputs[0] if outputs else None
    return {
        "embedding_count": len(outputs),
        "model_id": getattr(first, "model_id", None),
        "dimensions": getattr(first, "dimensions", None),
    }


trace_deep_agent_execute = traceable(
    name="belllabs.deep_agent.execute",
    run_type="chain",
    tags=["belllabs", "deepagents", "operation-execution"],
    process_inputs=process_runtime_execute_inputs,
    process_outputs=process_runtime_execute_outputs,
)

trace_capability_embeddings = traceable(
    name="belllabs.capability_embeddings.embed_many",
    run_type="embedding",
    tags=["belllabs", "embeddings", "capability-search"],
    process_inputs=process_embedding_inputs,
    process_outputs=process_embedding_outputs,
)
