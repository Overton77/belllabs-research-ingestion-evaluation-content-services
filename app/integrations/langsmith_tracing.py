"""LangSmith tracing bootstrap for BellLabs runtimes.

The app still executes via the OpenAI Agents SDK. LangSmith captures those runs through:

1. process-env configuration (`LANGSMITH_*`)
2. `OpenAIAgentsTracingProcessor` (agent/tool/handoff spans)
3. wrapped OpenAI clients + `@traceable` entry spans for BellLabs metadata

Secrets and full prompt bodies are never sent as root-span I/O.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, cast

from agents.tracing import TracingProcessor
from langsmith import traceable
from langsmith.integrations.openai_agents_sdk import OpenAIAgentsTracingProcessor
from langsmith.wrappers import wrap_openai
from openai import AsyncOpenAI

from app.config import Settings

_CONFIGURED = False
_PROCESSOR_INSTALLED = False


def configure_langsmith_tracing(settings: Settings) -> bool:
    """Export LangSmith env vars and install OpenAI Agents + client tracing.

    Returns True when tracing is enabled and an API key is present.
    """
    global _CONFIGURED, _PROCESSOR_INSTALLED
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

    openai_key = settings.openai_api_key.get_secret_value().strip()
    if openai_key:
        os.environ.setdefault("OPENAI_API_KEY", openai_key)
        try:
            from agents import set_default_openai_client
        except ImportError:
            pass
        else:
            # use_for_tracing=False: LangSmith owns observability; do not also export
            # OpenAI Agents platform traces with this client key.
            set_default_openai_client(
                create_traced_async_openai(openai_key),
                use_for_tracing=False,
            )

    if enabled and not _PROCESSOR_INSTALLED:
        try:
            from agents import set_trace_processors
        except ImportError:
            pass
        else:
            set_trace_processors(
                [
                    cast(
                        TracingProcessor,
                        OpenAIAgentsTracingProcessor(
                            project_name=project or None,
                            tags=["belllabs", "openai-agents"],
                            metadata={"runtime": "openai_agents"},
                        ),
                    )
                ]
            )
            _PROCESSOR_INSTALLED = True

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
    metadata: dict[str, object] = {
        "runtime": "openai_agents",
        "run_id": getattr(binding, "run_id", None),
        "binding_id": getattr(binding, "binding_id", None),
        "operation_id": getattr(binding, "operation_id", None),
        "request_scope": getattr(binding, "request_scope", None),
        "configuration_digest": getattr(binding, "effective_configuration_digest", None),
        "model": getattr(model_policy, "model", None),
        "provider": getattr(model_policy, "provider", None),
        "workspace_id": getattr(getattr(invocation, "workspace", None), "workspace_id", None),
    }
    if extra:
        metadata.update(dict(extra))
    return {key: value for key, value in metadata.items() if value is not None}


def process_runtime_execute_inputs(inputs: dict[str, Any]) -> dict[str, Any]:
    """Redact secrets and prompt bodies from LangSmith root-span inputs."""
    invocation = inputs.get("invocation")
    binding = getattr(invocation, "binding", None)
    return {
        "run_id": getattr(binding, "run_id", None),
        "binding_id": getattr(binding, "binding_id", None),
        "operation_id": getattr(binding, "operation_id", None),
        "request_scope": getattr(binding, "request_scope", None),
        "model": getattr(getattr(binding, "model_policy", None), "model", None),
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


trace_openai_agents_execute = traceable(
    name="belllabs.openai_agents.execute",
    run_type="chain",
    tags=["belllabs", "openai-agents", "operation-execution"],
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
