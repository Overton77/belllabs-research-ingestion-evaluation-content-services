"""HMAC-signed opaque cursors for runtime inspection pages (REQ-CP-RUN-011).

A cursor is bound to its page kind, request scope, filter digest and an expiry, so a token
issued for one listing cannot page another scope or filter. Pure and provider-neutral.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Final

from app.domain.run_control.inspection import ExpiredInspectionCursor, InvalidInspectionCursor

Clock = Callable[[], datetime]
CURSOR_TTL: Final = timedelta(minutes=15)


class InspectionCursorCodec:
    """HMAC-signed opaque cursors bound to kind, scope, filter, and an expiry."""

    def __init__(
        self,
        key: bytes | None = None,
        *,
        ttl: timedelta = CURSOR_TTL,
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        # A process-local key is the safe default: cursors then expire with the process.
        # Multi-replica deployments supply one shared key through composition.
        self._key = key if key is not None else secrets.token_bytes(32)
        if len(self._key) < 16:
            raise ValueError("inspection cursor key must have at least 16 bytes")
        self._ttl = ttl
        self._clock = clock

    def encode(
        self,
        *,
        kind: str,
        request_scope: str,
        filter_digest: str,
        position: Sequence[str | int],
    ) -> str:
        payload = {
            "v": 1,
            "k": kind,
            "s": _scope_digest(request_scope),
            "f": filter_digest,
            "p": list(position),
            "e": int((self._clock() + self._ttl).timestamp()),
        }
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return f"{_b64(body)}.{_b64(self._sign(body))}"

    def decode(
        self, token: str, *, kind: str, request_scope: str, filter_digest: str
    ) -> list[str | int]:
        try:
            encoded_body, encoded_signature = token.split(".", 1)
            body = _unb64(encoded_body)
            signature = _unb64(encoded_signature)
        except (ValueError, TypeError) as error:
            raise InvalidInspectionCursor("inspection cursor is malformed") from error
        if not hmac.compare_digest(signature, self._sign(body)):
            raise InvalidInspectionCursor("inspection cursor is not valid for this service")
        try:
            payload = json.loads(body)
            valid = (
                payload["v"] == 1
                and payload["k"] == kind
                and payload["s"] == _scope_digest(request_scope)
                and payload["f"] == filter_digest
                and isinstance(payload["p"], list)
            )
            expires_at = int(payload["e"])
        except (KeyError, TypeError, ValueError) as error:
            raise InvalidInspectionCursor("inspection cursor is malformed") from error
        if not valid:
            raise InvalidInspectionCursor("inspection cursor belongs to another scope or filter")
        if self._clock().timestamp() >= expires_at:
            raise ExpiredInspectionCursor("inspection cursor has expired")
        return list(payload["p"])

    def _sign(self, body: bytes) -> bytes:
        return hmac.new(self._key, b"belllabs.inspection-cursor.v1\x00" + body, "sha256").digest()


def _scope_digest(request_scope: str) -> str:
    return hashlib.sha256(request_scope.encode("utf-8")).hexdigest()


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    if not value:
        raise ValueError("empty cursor segment")
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
