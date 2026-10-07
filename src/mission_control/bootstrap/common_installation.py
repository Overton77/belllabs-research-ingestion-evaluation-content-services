"""Read-only verification of an installed common mission_control component.

Readiness requires the persisted installation identity, the complete attested
release, this build's writer compatibility and correctly restricted pool roles.
Missing or mismatched evidence fails closed; there is no transitional fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import asyncpg

from mission_control.application.installations.registry import (
    ApplicationBinding,
    InstallationUnavailable,
)

WRITER_VERSION = "mission-control-runtime/1"
READER_VERSION = "mission-control-runtime/1"
CAPABILITY_ROLES = (
    "mission_control_runtime",
    "mission_control_family_writer",
    "mission_control_catalog_writer",
    "mission_control_outbox_worker",
    "mission_control_readonly",
)
# Required release tables; a missing one means the component is not installed.
REQUIRED_TABLES = frozenset(
    {
        "application_installation",
        "component_release",
        "release_attestation",
        "tenant",
        "mission",
        "mission_run",
        "request_receipt",
        "outbox",
    }
)


@dataclass(frozen=True)
class CommonReadiness:
    storage_mode: Literal["production_common"]
    database_name: str
    component_version: str
    applied_migrations: frozenset[str]
    schema_fingerprint: str
    pool_role: str
    production_ready: bool


async def _role_memberships(connection: asyncpg.Connection) -> tuple[asyncpg.Record, set[str]]:
    role = await connection.fetchrow(
        """
        SELECT current_user AS name, current_database() AS database_name,
               rolsuper, rolbypassrls
        FROM pg_catalog.pg_roles WHERE rolname = current_user
        """
    )
    if role is None:
        raise InstallationUnavailable("pool role is not visible")
    members = {
        row["rolname"]
        for row in await connection.fetch(
            """
            SELECT rolname FROM pg_catalog.pg_roles
            WHERE rolname = ANY($1::text[]) AND pg_has_role(current_user, oid, 'MEMBER')
            """,
            list(CAPABILITY_ROLES),
        )
    }
    return role, members


async def verify_pool_role(connection: asyncpg.Connection, capability_role: str) -> asyncpg.Record:
    """The pool must hold exactly one capability role and no owner/bypass authority."""
    role, members = await _role_memberships(connection)
    owns_objects = await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname IN ('mission_control', 'mission_control_search')
              AND pg_has_role(current_user, c.relowner, 'MEMBER')
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_namespace n
            WHERE n.nspname IN ('mission_control', 'mission_control_search')
              AND pg_has_role(current_user, n.nspowner, 'MEMBER')
        )
        """
    )
    if role["rolsuper"] or role["rolbypassrls"] or owns_objects or members != {capability_role}:
        raise InstallationUnavailable(
            f"pool must hold only {capability_role} without owner or RLS bypass authority"
        )
    return role


async def inspect_common_installation(
    pool: asyncpg.Pool,
    binding: ApplicationBinding,
    *,
    capability_role: str = "mission_control_runtime",
) -> CommonReadiness:
    async with (
        pool.acquire() as connection,
        connection.transaction(isolation="repeatable_read", readonly=True),
    ):
        role = await verify_pool_role(connection, capability_role)
        present = {
            row["relname"]
            for row in await connection.fetch(
                """
                SELECT c.relname FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'mission_control' AND c.relkind IN ('r', 'p')
                """
            )
        }
        if not present >= REQUIRED_TABLES:
            raise InstallationUnavailable("common mission_control component is not installed")
        try:
            installations = await connection.fetch(
                """
                SELECT installation_id, application_id, supabase_project_ref,
                       schema_component_version
                FROM mission_control.application_installation
                WHERE state = 'active'
                """
            )
            receipts = await connection.fetch(
                "SELECT component_version, migration_key FROM mission_control.component_release"
            )
            attestation = await connection.fetchrow(
                """
                SELECT component_version, schema_fingerprint, fingerprint_algorithm,
                       supported_reader_versions, supported_writer_versions
                FROM mission_control.release_attestation
                WHERE component_version = $1
                """,
                binding.required_component_version,
            )
        except asyncpg.InsufficientPrivilegeError as error:
            raise InstallationUnavailable("installation evidence is not readable") from error
        if len(installations) != 1:
            raise InstallationUnavailable("exactly one active application installation is required")
        actual = installations[0]
        if (
            actual["installation_id"] != binding.installation_id
            or actual["application_id"] != binding.application_id
            or actual["supabase_project_ref"] != binding.supabase_project_ref
            or actual["schema_component_version"] != binding.required_component_version
        ):
            raise InstallationUnavailable("persisted installation identity differs from binding")
        if attestation is None:
            raise InstallationUnavailable("installed component release is not attested")
        if (
            WRITER_VERSION not in attestation["supported_writer_versions"]
            or READER_VERSION not in attestation["supported_reader_versions"]
        ):
            raise InstallationUnavailable("this build is not an admitted reader/writer")
        if not receipts or {row["component_version"] for row in receipts} - {
            binding.required_component_version
        }:
            raise InstallationUnavailable("component receipts do not match the pinned release")
        return CommonReadiness(
            storage_mode="production_common",
            database_name=role["database_name"],
            component_version=attestation["component_version"],
            applied_migrations=frozenset(row["migration_key"] for row in receipts),
            schema_fingerprint=attestation["schema_fingerprint"],
            pool_role=role["name"],
            production_ready=attestation["fingerprint_algorithm"] != "fixture-unqualified",
        )
