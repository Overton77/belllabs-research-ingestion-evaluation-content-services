from __future__ import annotations

import asyncio
import json

from app.config import Settings
from app.integrations.postgres import (
    apply_application_migrations,
    create_application_migration_pool,
)


async def _run() -> dict[str, object]:
    pool = await create_application_migration_pool(Settings())
    try:
        await apply_application_migrations(pool)
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT version
                FROM belllabs_control.schema_migrations
                ORDER BY version
                """
            )
            audit_policy_count = await connection.fetchval(
                """
                SELECT count(1)
                FROM pg_policies
                WHERE schemaname = 'belllabs_control'
                  AND tablename = 'coordinator_audit_events'
                  AND policyname = 'coordinator_audit_event_scope_isolation'
                """
            )
            audit_force_rls = await connection.fetchval(
                """
                SELECT relation.relforcerowsecurity
                FROM pg_class AS relation
                JOIN pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'belllabs_control'
                  AND relation.relname = 'coordinator_audit_events'
                """
            )
        versions = tuple(row["version"] for row in rows)
        return {
            "applied_versions": versions,
            "migration_0010_applied": (
                "0010_coordinator_audit_events.sql" in versions
            ),
            "audit_policy_count": audit_policy_count,
            "audit_force_rls": audit_force_rls,
        }
    finally:
        await pool.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run()), sort_keys=True))


if __name__ == "__main__":
    main()
