"""Parsers for the vendors' read-only auth status commands.

Every parser redacts first and returns only a classification: signed in or not, which
route the stored sign-in is, a short redacted summary and (when an account identifier is
printed) its sha256 fingerprint. Raw output is never returned, stored or logged.

* Codex `codex login status`: messages and exit codes from `run_login_status` in
  https://github.com/openai/codex/blob/main/codex-rs/cli/src/login.rs (retrieved with
  `npx ctx7@latest docs /openai/codex`, 2026-10-08). It loads auth with the API-key
  environment variable disabled, so it reports the *stored* login only.
* Claude Code `claude auth status`: `--json` is the default output (`claude auth status
  --help`, CLI 2.1.295). The JSON field names are NOT documented; the parser accepts
  `loggedIn`/`authMethod` style keys and reports `unknown` for any other shape.
* Cursor `agent status`: documented to show "whether you are authenticated, your account
  information" (https://cursor.com/docs/cli/reference/authentication); the text format is
  not documented, so only explicit "not logged in"/"logged in" phrases are classified.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Final, Literal

from mission_control.adapters.provider_auth.redaction import redact_text
from mission_control.application.execution.auth_admission import AuthRouteKind
from mission_control.domain.authoring.canonical import sha256_digest

StatusParser = Literal["codex_login_status", "claude_auth_status", "cursor_agent_status"]
StoredRoute = Literal["login", "api_key", "access_token", "other"]


@dataclass(frozen=True)
class StatusOutput:
    """A status command's result as captured; consumed only by the parsers below."""

    exit_code: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class StatusReading:
    signed_in: bool | None
    stored_route: StoredRoute | None
    summary: str
    account_fingerprint: str | None = None
    format_documented: bool = True


_CODEX_LINES: Final[tuple[tuple[str, bool, StoredRoute], ...]] = (
    ("Logged in using ChatGPT", True, "login"),
    ("Logged in using an API key", True, "api_key"),
    ("Logged in using access token", True, "access_token"),
    ("Logged in using personal access token", True, "access_token"),
    ("Logged in using workload identity", True, "other"),
)


def parse_codex_login_status(output: StatusOutput) -> StatusReading:
    text = output.stderr + "\n" + output.stdout
    for line, signed_in, route in _CODEX_LINES:
        if line in text:
            # "Logged in using an API key - sk-proj-***ABCDE": only the phrase is kept.
            return StatusReading(signed_in, route, line)
    if "Not logged in" in text:
        return StatusReading(False, None, "Not logged in")
    return StatusReading(None, None, redact_text(f"unrecognized status (exit {output.exit_code})"))


def _truthy(payload: dict[str, Any], *keys: str) -> bool | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool):
            return value
    return None


def parse_claude_auth_status(output: StatusOutput) -> StatusReading:
    try:
        payload = json.loads(output.stdout)
    except json.JSONDecodeError:
        return StatusReading(None, None, "status output is not JSON", format_documented=False)
    if not isinstance(payload, dict):
        return StatusReading(None, None, "status JSON is not an object", format_documented=False)
    signed_in = _truthy(payload, "loggedIn", "logged_in", "authenticated")
    method = payload.get("authMethod") or payload.get("auth_method")
    stored: StoredRoute | None = None
    if isinstance(method, str):
        lowered = method.lower()
        if "api" in lowered and "key" in lowered:
            stored = "api_key"
        elif "oauth_token" in lowered or "setup" in lowered or "token" in lowered:
            stored = "access_token"
        elif "claude.ai" in lowered or "subscription" in lowered or "oauth" in lowered:
            stored = "login"
        else:
            stored = "other"
    account = next(
        (
            payload[key]
            for key in ("email", "accountEmail", "accountUuid", "account_id", "orgId")
            if isinstance(payload.get(key), str) and payload.get(key)
        ),
        None,
    )
    summary = "signed in" if signed_in else ("signed out" if signed_in is False else "unknown")
    if isinstance(method, str):
        summary += f" ({redact_text(method)[:64]})"
    return StatusReading(
        signed_in,
        stored,
        summary,
        account_fingerprint=None if account is None else sha256_digest({"account": account}),
        format_documented=False,
    )


def parse_cursor_agent_status(output: StatusOutput) -> StatusReading:
    lowered = (output.stdout + "\n" + output.stderr).lower()
    if "not logged in" in lowered or "not authenticated" in lowered:
        return StatusReading(False, None, "not logged in", format_documented=False)
    if "logged in" in lowered or "authenticated" in lowered:
        return StatusReading(True, "login", "logged in", format_documented=False)
    return StatusReading(None, None, "unrecognized status", format_documented=False)


PARSERS: Final = {
    "codex_login_status": parse_codex_login_status,
    "claude_auth_status": parse_claude_auth_status,
    "cursor_agent_status": parse_cursor_agent_status,
}


def stored_route_for(reading: StatusReading, login_route: AuthRouteKind) -> AuthRouteKind | None:
    """Map a status reading's stored credential to the profile vocabulary."""

    if reading.stored_route == "login":
        return login_route
    if reading.stored_route == "api_key":
        return "api_key"
    if reading.stored_route == "access_token":
        return "oauth_token_env"
    return None


__all__ = [
    "PARSERS",
    "StatusOutput",
    "StatusParser",
    "StatusReading",
    "parse_claude_auth_status",
    "parse_codex_login_status",
    "parse_cursor_agent_status",
    "stored_route_for",
]
