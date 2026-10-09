"""Credential redaction for anything provider-auth code might log, persist or report.

Built on `auth_admission.SECRET_PATTERNS` so the admission's secret check and this
redactor agree on what a credential looks like. Mapping keys that name a secret
(`token`, `api_key`, `authorization`, ...) are redacted whatever their value looks like.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, Final

from mission_control.application.execution.auth_admission import SECRET_PATTERNS

REDACTED: Final = "<redacted>"
_SECRET_KEY: Final = re.compile(
    r"(?i)(token|secret|password|passwd|api[_-]?key|authorization|cookie|credential|"
    r"session[_-]?key|private[_-]?key|account[_-]?id|email)"
)


def redact_text(text: str) -> str:
    """Replace every credential-shaped substring with `<redacted:kind>`."""

    for name, pattern in SECRET_PATTERNS:
        text = pattern.sub(f"<redacted:{name}>", text)
    return text


def redact_value(value: Any) -> Any:
    """Recursively redact a JSON-like value (dicts, lists, strings)."""

    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Mapping):
        return {
            str(key): (REDACTED if _SECRET_KEY.search(str(key)) else redact_value(item))
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        return [redact_value(item) for item in value]
    return value


__all__ = ["REDACTED", "redact_text", "redact_value"]
