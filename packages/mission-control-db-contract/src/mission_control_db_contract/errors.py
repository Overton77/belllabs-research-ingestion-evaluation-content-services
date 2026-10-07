"""Safe, credential-free diagnostics."""

from __future__ import annotations


class ContractError(ValueError):
    """A blocked gate. Messages never contain credentials, DSNs or row contents."""

    def __init__(self, message: str, *, code: str = "BLOCKED") -> None:
        super().__init__(message)
        self.code = code


def sqlstate(exc: BaseException) -> str:
    """Return only the SQLSTATE of a driver error (never its message/detail text)."""
    value = getattr(exc, "sqlstate", None)
    return value if isinstance(value, str) and len(value) == 5 else "unknown"
