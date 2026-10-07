"""Explicit app target manifests (``deployments/<app>/target.toml``) and connections.

The target declares the app, verified project identity and the NAME of the environment
variable that holds the database URL. Credentials are never manifest fields, CLI
arguments or report values. There is no code branching on the app.
"""

from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit
from uuid import UUID

from . import SUPPORTED_APPS
from .errors import ContractError

ENVIRONMENTS = ("disposable", "development", "staging", "production")
TARGET_FIELDS = (
    "app",
    "project_label",
    "project_ref",
    "environment",
    "application_id",
    "installation_id",
    "database_host",
    "database_port",
    "database_name",
    "database_user",
    "database_url_env",
)
# Optional: the database session user when a pooler login differs from it (Supabase
# Supavisor logins are `<role>.<project_ref>`). Defaults to `database_user`.
OPTIONAL_TARGET_FIELDS = ("database_session_user",)
APPROVAL_FIELDS = ("approved_by", "identity_evidence")
PROJECT_REF = re.compile(r"[a-z0-9-]{1,63}")
ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]*")
PLACEHOLDER = re.compile(r"REPLACE|PLACEHOLDER|<|>", re.IGNORECASE)


def load_target(path: Path) -> dict[str, str]:
    """Load and validate one target; reject placeholders and unknown fields."""
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError("Explicit complete target configuration is required") from exc
    if data.get("format_version") != 1 or set(data) - {"format_version", "target", "approval"}:
        raise ContractError("Target manifest must be format_version 1 with [target]/[approval]")
    target = data.get("target")
    approval = data.get("approval")
    if not isinstance(target, dict) or not isinstance(approval, dict):
        raise ContractError("Target manifest requires [target] and [approval] tables")
    unknown = (set(target) - set(TARGET_FIELDS) - set(OPTIONAL_TARGET_FIELDS)) | (
        set(approval) - set(APPROVAL_FIELDS)
    )
    if unknown:
        raise ContractError(f"Unknown target fields: {', '.join(sorted(unknown))}")
    result: dict[str, str] = {}
    for name in TARGET_FIELDS:
        value = target.get(name)
        if isinstance(value, int) and name == "database_port":
            value = str(value)
        if not isinstance(value, str) or not value.strip() or PLACEHOLDER.search(value):
            raise ContractError(f"Explicit complete target configuration is required: {name}")
        result[name] = value
    for name in APPROVAL_FIELDS:
        value = approval.get(name)
        if not isinstance(value, str) or not value.strip() or PLACEHOLDER.search(value):
            raise ContractError(f"Target identity approval reference is required: {name}")
        result[name] = value
    if result["app"] not in SUPPORTED_APPS:
        raise ContractError("Target app must be one of the declared supported apps")
    if result["application_id"] != result["app"]:
        raise ContractError("Target application_id must equal the declared app")
    if not PROJECT_REF.fullmatch(result["project_ref"]):
        raise ContractError("Project ref must be a lowercase Supabase-style reference")
    try:
        if str(UUID(result["installation_id"])) != result["installation_id"]:
            raise ValueError
    except ValueError:
        raise ContractError("installation_id must be a canonical lowercase UUID") from None
    if result["environment"] not in ENVIRONMENTS:
        raise ContractError("Unknown target environment")
    if not ENV_NAME.fullmatch(result["database_url_env"]):
        raise ContractError("Database credential must be an environment variable NAME")
    if not result["database_port"].isdigit():
        raise ContractError("Database port must be numeric")
    session_user = target.get("database_session_user", result["database_user"])
    if (
        not isinstance(session_user, str)
        or not session_user.strip()
        or PLACEHOLDER.search(session_user)
    ):
        raise ContractError("database_session_user must be an explicit role name")
    if session_user != result["database_user"] and (
        result["database_user"] != f"{session_user}.{result['project_ref']}"
    ):
        # A differing session user is only admitted for a pooler login that names this
        # exact project; it can never redirect the installer to another project.
        raise ContractError("database_session_user differs without a project-bound pooler login")
    result["database_session_user"] = session_user
    return result


def public_identity(target: dict[str, str]) -> dict[str, str]:
    """Redacted target identity suitable for reports and plan binding."""
    return {
        name: target[name]
        for name in (
            "app",
            "project_label",
            "project_ref",
            "environment",
            "application_id",
            "installation_id",
            "database_host",
            "database_port",
            "database_name",
            "database_user",
            "database_session_user",
            "approved_by",
            "identity_evidence",
        )
    }


def target_dsn(target: dict[str, str]) -> str:
    value = os.environ.get(target["database_url_env"])
    if not value:
        raise ContractError("Database credential environment reference is unset")
    try:
        parsed = urlsplit(value)
        options = parse_qsl(parsed.query, strict_parsing=True) if parsed.query else []
        matches = (
            parsed.scheme in {"postgres", "postgresql"}
            and parsed.hostname == target["database_host"]
            and (parsed.port or 5432) == int(target["database_port"])
            and unquote(parsed.username or "") == target["database_user"]
            and unquote(parsed.path.removeprefix("/")) == target["database_name"]
            and all(
                key in {"sslmode", "sslrootcert", "sslcert", "sslkey"} for key, _value in options
            )
            and len({key for key, _value in options}) == len(options)
            and not parsed.fragment
        )
    except (KeyError, ValueError):
        matches = False
    if not matches:
        raise ContractError(
            "Credential endpoint does not match explicit host/port/user/database target"
        )
    return value


async def connect(
    target: dict[str, str],
    *,
    readonly: bool,
    statement_timeout_ms: int = 60000,
    lock_timeout_ms: int = 10000,
) -> Any:
    import asyncpg

    settings = {
        "application_name": "mission-db",
        "statement_timeout": str(statement_timeout_ms),
        "lock_timeout": str(lock_timeout_ms),
        "idle_in_transaction_session_timeout": str(max(statement_timeout_ms * 2, 60000)),
        "standard_conforming_strings": "on",
    }
    if readonly:
        settings["default_transaction_read_only"] = "on"
    dsn = target_dsn(target)
    try:
        return await asyncpg.connect(
            dsn,
            timeout=10,
            command_timeout=statement_timeout_ms / 1000 + 5,
            server_settings=settings,
        )
    except Exception:
        raise ContractError("Database connection failed; details withheld") from None


async def close_quietly(connection: Any) -> None:
    try:
        await connection.close()
    except Exception:
        raise ContractError("Database connection cleanup failed; details withheld") from None


async def check_database_identity(connection: Any, target: dict[str, str]) -> None:
    database = await connection.fetchval("SELECT current_database()")
    user = await connection.fetchval("SELECT session_user")
    expected_user = target.get("database_session_user", target["database_user"])
    if database != target["database_name"] or user != expected_user:
        raise ContractError("Connected database/user identity differs from explicit target")
