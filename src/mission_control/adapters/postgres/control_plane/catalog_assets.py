"""Shared installation-catalog identities for the common mission_control component.

Published definitions are canonical ``mission_control.asset_version`` rows. Their
legacy identity ``(kind, logical_id, revision)`` is kept as the scoped logical key
``asset_id = definition:<kind>:<logical_id>`` / ``version = <revision>``; the coarse
``asset_version.kind`` category comes from :data:`ASSET_KIND`. Seed bundles and the
search admission filter use the same functions, so there is one mapping.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import DefinitionKind

PUBLISHED_DEFINITION_CONTRACT = "mission-control.published-definition/1"
SKILL_BUNDLE_CONTRACT = "mission-control.skill-bundle.v1"
PUBLICATION_POLICY_REF = "mission-control.catalog-publication/1"
CATALOG_SERVICE_ACTOR = "service:mission-control-catalog"

ASSET_KIND: dict[DefinitionKind, str] = {
    DefinitionKind.WORKFLOW_TYPE: "workflow_template",
    DefinitionKind.WORKFLOW_IMPLEMENTATION: "workflow_template",
    DefinitionKind.BLUEPRINT: "blueprint",
    DefinitionKind.CONTROL_PROFILE: "profile",
    DefinitionKind.RUNTIME_PROFILE: "profile",
    DefinitionKind.WORKSPACE_TEMPLATE: "profile",
    DefinitionKind.EVALUATION_PROFILE: "profile",
    DefinitionKind.WORKFLOW_CONFIGURATION: "profile",
    DefinitionKind.MEMORY_POLICY: "policy",
    DefinitionKind.AGENT_PROFILE: "profile",
    DefinitionKind.CAPABILITY_SELECTION: "policy",
    DefinitionKind.PROMPT: "schema",
    DefinitionKind.SKILL: "skill",
    DefinitionKind.MCP_SERVER: "mcp_server",
    DefinitionKind.MCP_TOOL: "tool",
    DefinitionKind.PLUGIN_PACKAGE: "plugin",
    DefinitionKind.MODEL: "model_route",
    DefinitionKind.MIDDLEWARE: "hook",
    DefinitionKind.SANDBOX_PROFILE: "profile",
    DefinitionKind.TOOL: "tool",
    DefinitionKind.DEEP_AGENT_PLACEMENT: "profile",
}


def definition_asset_id(kind: DefinitionKind | str, logical_id: str) -> str:
    return f"definition:{DefinitionKind(kind).value}:{logical_id}"


def skill_bundle_asset_id(logical_id: str) -> str:
    return f"skill-bundle:{logical_id}"


def definition_manifest_ref(kind: DefinitionKind | str, logical_id: str, revision: int) -> str:
    return f"mc-catalog://definition/{DefinitionKind(kind).value}/{logical_id}/{revision}"


def json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


async def insert_projection_job(
    connection: asyncpg.Connection,
    *,
    installation_id: UUID,
    application_id: str,
    event: dict[str, Any],
    actor_ref: str,
    recorded_at: datetime,
) -> None:
    """Enqueue one leased projection job for an immutable catalog event (idempotent)."""
    await connection.execute(
        """INSERT INTO mission_control.catalog_projection_job
           (installation_id, application_id, projection_job_id, event_key, definition_kind,
            logical_id, revision, source_digest, state, attempt_count, lease_owner,
            lease_expires_at, next_attempt_at, payload, version, updated_at, created_at,
            created_by_actor_ref)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,1,$15,
                   clock_timestamp(),$16)
           ON CONFLICT (installation_id, application_id, event_key) DO NOTHING""",
        installation_id,
        application_id,
        uuid7(),
        event["event_id"],
        event["asset_kind"],
        event["logical_id"],
        int(event["revision"]),
        event["source_digest"],
        event["state"],
        int(event["attempt_count"]),
        event.get("lease_owner"),
        _optional_ts(event.get("lease_expires_at")),
        _ts(event["next_attempt_at"]),
        json.dumps(event, allow_nan=False),
        recorded_at,
        actor_ref,
    )


def payload_digest(payload: Any) -> str:
    return sha256_digest(payload)


def _ts(value: Any) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))


def _optional_ts(value: Any) -> datetime | None:
    return None if value is None else _ts(value)
