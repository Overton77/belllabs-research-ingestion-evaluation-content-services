"""Frame body handling: canonical JSON, secret redaction, digest and bounded excerpt.

Redaction runs before anything is digested, excerpted or stored, so no secret, token or
credential value reaches the Native Event Store, a transcript or a promoted body. The
digest is computed over the redacted canonical bytes, which is exactly what any stored
excerpt or promoted full body can be verified against. Paths of redacted locations are
recorded with the pattern class that matched, never the matched value.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any
from uuid import UUID

from mission_control.domain.frames.contracts import DEFAULT_EXCERPT_CAP_BYTES, FrameRedaction

REDACTED = "[REDACTED]"
MAX_RECORDED_REDACTIONS = 256
MAX_BODY_DEPTH = 48

# Environment variable names whose values are credentials (SPEC-03 list plus the
# credentials the deployment documents). A `NAME=value` / `NAME: value` occurrence or a
# mapping key equal to the name is redacted.
SECRET_ENV_NAMES: tuple[str, ...] = (
    "CURSOR_API_KEY",
    "TAVILY_API_KEY",
    "FIRECRAWL_API_KEY",
    "NCBI_API_KEY",
    "EXA_API_KEY",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "LANGSMITH_API_KEY",
    "DAYTONA_API_KEY",
    "LINEAR_API_KEY",
    "SUPABASE_SECRET_KEY",
    "TEMPORAL_CLOUD_API_KEY",
    "EDGAR_IDENTITY",
)

# Mapping keys whose values are always credentials, whatever their shape.
SECRET_KEYS: frozenset[str] = frozenset(
    {
        "authorization",
        "proxy_authorization",
        "api_key",
        "apikey",
        "x_api_key",
        "password",
        "passwd",
        "secret",
        "client_secret",
        "access_token",
        "refresh_token",
        "id_token",
        "session_token",
        "cookie",
        "set_cookie",
        "private_key",
        *(name.lower() for name in SECRET_ENV_NAMES),
    }
)

_TEXT_PATTERNS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    ("bearer_token", re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}"), r"\1 " + REDACTED),
    ("basic_auth", re.compile(r"(?i)\b(basic)\s+[A-Za-z0-9+/=]{12,}"), r"\1 " + REDACTED),
    (
        "env_secret",
        re.compile(
            r"\b(" + "|".join(re.escape(name) for name in SECRET_ENV_NAMES) + r")"
            r"(\s*[=:]\s*)[\"']?[^\s\"',;&]+"
        ),
        r"\1\2" + REDACTED,
    ),
    (
        "url_credential",
        re.compile(
            r"(?i)([?&](?:token|key|api_key|apikey|access_token|auth|signature|sig|"
            r"x-amz-signature|x-amz-credential)=)[^&#\s\"']+"
        ),
        r"\1" + REDACTED,
    ),
    (
        "url_userinfo",
        re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^/\s:@]+:[^/\s@]+@"),
        r"\1" + REDACTED + "@",
    ),
    ("sk_key", re.compile(r"\bsk-[A-Za-z0-9_-]{8,}"), REDACTED),
    ("tavily_key", re.compile(r"\btvly-[A-Za-z0-9_-]{8,}"), REDACTED),
    ("firecrawl_key", re.compile(r"\bfc-[A-Za-z0-9]{16,}"), REDACTED),
    ("github_token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}"), REDACTED),
    ("aws_access_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b"), REDACTED),
    (
        "jwt",
        re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
        REDACTED,
    ),
)


@dataclass(frozen=True)
class FrameBody:
    """A redacted canonical body ready to persist."""

    digest: str
    body_bytes: int
    excerpt: str
    redactions: tuple[FrameRedaction, ...]
    canonical: bytes

    @property
    def truncated(self) -> bool:
        return len(self.excerpt.encode("utf-8")) < self.body_bytes


class _Redactor:
    def __init__(self, secret_values: Iterable[str]) -> None:
        values = sorted(
            {value for value in secret_values if isinstance(value, str) and len(value) >= 6},
            key=len,
            reverse=True,
        )
        self._literal = (
            re.compile("|".join(re.escape(value) for value in values)) if values else None
        )
        self.found: list[FrameRedaction] = []

    def _record(self, path: str, pattern: str) -> None:
        if len(self.found) < MAX_RECORDED_REDACTIONS:
            self.found.append(FrameRedaction(path=path[:512], pattern=pattern))

    def text(self, value: str, path: str) -> str:
        result = value
        if self._literal is not None:
            result, count = self._literal.subn(REDACTED, result)
            if count:
                self._record(path, "configured_secret")
        for name, pattern, replacement in _TEXT_PATTERNS:
            result, count = pattern.subn(replacement, result)
            if count:
                self._record(path, name)
        return result

    def value(self, item: Any, path: str, depth: int = 0) -> Any:
        if depth > MAX_BODY_DEPTH:
            self._record(path, "depth_limit")
            return "[TRUNCATED_DEPTH]"
        if isinstance(item, str):
            return self.text(item, path)
        if isinstance(item, Mapping):
            redacted: dict[str, Any] = {}
            for key, nested in item.items():
                name = str(key)
                child = f"{path}.{name}"
                if _secret_key(name) and nested not in (None, "", [], {}):
                    self._record(child, "secret_key")
                    redacted[self.text(name, child)] = REDACTED
                else:
                    redacted[self.text(name, child)] = self.value(nested, child, depth + 1)
            return redacted
        if isinstance(item, list | tuple):
            return [
                self.value(nested, f"{path}[{index}]", depth + 1)
                for index, nested in enumerate(item)
            ]
        return item


def _secret_key(name: str) -> bool:
    normalized = name.strip().lower().replace("-", "_")
    return normalized in SECRET_KEYS or normalized.endswith(
        ("_api_key", "_password", "_secret", "_access_token", "_refresh_token", "_private_key")
    )


def jsonable(value: Any, depth: int = 0) -> Any:
    """Convert provider values to JSON-compatible data deterministically."""

    if depth > MAX_BODY_DEPTH:
        return "[TRUNCATED_DEPTH]"
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return (
            value if value == value and value not in (float("inf"), float("-inf")) else str(value)
        )
    if isinstance(value, Enum):
        return jsonable(value.value, depth + 1)
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes | bytearray | memoryview):
        raw = bytes(value)
        return {
            "bytes_base64": base64.standard_b64encode(raw[:4_096]).decode("ascii"),
            "bytes_length": len(raw),
        }
    if isinstance(value, Mapping):
        return {str(key): jsonable(item, depth + 1) for key, item in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        items = [jsonable(item, depth + 1) for item in value]
        if isinstance(value, set | frozenset):
            items.sort(key=lambda item: json.dumps(item, sort_keys=True, default=str))
        return items
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        return jsonable(dump(mode="json"), depth + 1)
    return str(value)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def excerpt(payload: bytes, cap: int) -> str:
    """The first `cap` bytes of the payload, cut on a UTF-8 character boundary."""

    if cap < 0:
        raise ValueError("excerpt cap must be non-negative")
    return payload[:cap].decode("utf-8", errors="ignore")


def redact_text(
    value: str, secret_values: Iterable[str] = ()
) -> tuple[str, tuple[FrameRedaction, ...]]:
    """Redact one string (render-time defence in depth)."""

    redactor = _Redactor(secret_values)
    return redactor.text(value, "$"), tuple(redactor.found)


def redact_value(
    value: Any, secret_values: Iterable[str] = ()
) -> tuple[Any, tuple[FrameRedaction, ...]]:
    redactor = _Redactor(secret_values)
    return redactor.value(jsonable(value), "$"), tuple(redactor.found)


def frame_body(
    value: Any,
    *,
    excerpt_cap_bytes: int = DEFAULT_EXCERPT_CAP_BYTES,
    secret_values: Iterable[str] = (),
) -> FrameBody:
    """Canonicalize, redact, digest and excerpt one provider payload."""

    redacted, found = redact_value(value, secret_values)
    payload = canonical_bytes(redacted)
    return FrameBody(
        digest="sha256:" + hashlib.sha256(payload).hexdigest(),
        body_bytes=len(payload),
        excerpt=excerpt(payload, excerpt_cap_bytes),
        redactions=found,
        canonical=payload,
    )
