from __future__ import annotations

import hashlib
import hmac
import os
import re
from collections.abc import Mapping
from typing import Any

SENSITIVE_KEYS = {
    "authorization",
    "cookie",
    "credentials",
    "environment",
    "headers",
    "secret",
    "token",
}
SIGNED_URL_MARKERS = ("x-amz-signature=", "x-goog-signature=", "sig=")


def require_masked_tracing_posture() -> None:
    tracing_enabled = os.environ.get("LANGSMITH_TRACING", "").lower() in {"1", "true"}
    hide_inputs = os.environ.get("LANGSMITH_HIDE_INPUTS", "").lower() in {"1", "true"}
    hide_outputs = os.environ.get("LANGSMITH_HIDE_OUTPUTS", "").lower() in {"1", "true"}
    if tracing_enabled and not (hide_inputs and hide_outputs):
        raise RuntimeError(
            "Agent Server tracing requires LANGSMITH_HIDE_INPUTS=true and "
            "LANGSMITH_HIDE_OUTPUTS=true"
        )


def configure_agent_server_tracing() -> None:
    """Apply the dedicated trace project before Agent Server handles runs."""

    require_masked_tracing_posture()
    tracing_enabled = os.environ.get("LANGSMITH_TRACING", "").lower() in {"1", "true"}
    if not tracing_enabled:
        return
    project = os.environ.get("AGENT_SERVER_LANGSMITH_PROJECT", "").strip()
    if not project:
        raise RuntimeError(
            "AGENT_SERVER_LANGSMITH_PROJECT is required when Agent Server tracing is enabled"
        )
    os.environ["LANGSMITH_PROJECT"] = project


def correlation_metadata(
    *,
    request_scope: str,
    belllabs_run_id: str,
    graph_id: str,
    graph_assembly_digest: str,
    deployment_endpoint_id: str,
    pseudonym_key: bytes,
) -> dict[str, str]:
    if not pseudonym_key:
        raise ValueError("trace pseudonym key is required")
    return {
        "runtime": "langgraph_agent_server",
        "request_scope_pseudonym": _pseudonym(request_scope, pseudonym_key),
        "belllabs_run_pseudonym": _pseudonym(belllabs_run_id, pseudonym_key),
        "graph_id": graph_id,
        "graph_assembly_digest": graph_assembly_digest,
        "deployment_endpoint_id": deployment_endpoint_id,
    }


def mask_trace_payload(value: Any, *, max_string_chars: int = 512) -> Any:
    if isinstance(value, Mapping):
        masked: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized = re.sub(r"[^a-z]", "", key.lower())
            if any(marker in normalized for marker in SENSITIVE_KEYS):
                masked[key] = "[redacted]"
            else:
                masked[key] = mask_trace_payload(
                    item,
                    max_string_chars=max_string_chars,
                )
        return masked
    if isinstance(value, list | tuple):
        return [
            mask_trace_payload(item, max_string_chars=max_string_chars)
            for item in value
        ]
    if isinstance(value, str):
        lowered = value.lower()
        if any(marker in lowered for marker in SIGNED_URL_MARKERS):
            return "[signed-url-redacted]"
        if len(value) > max_string_chars:
            return {
                "redacted_large_value": True,
                "character_count": len(value),
                "digest": f"sha256:{hashlib.sha256(value.encode()).hexdigest()}",
            }
    return value


def _pseudonym(value: str, key: bytes) -> str:
    return "hmac-sha256:" + hmac.new(key, value.encode(), hashlib.sha256).hexdigest()
