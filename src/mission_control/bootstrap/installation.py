"""Explicit, idempotent registration of a disposable local installation identity.

Run migrations separately first. Never called by API startup. Existing identity
cannot be changed, and remote PostgreSQL endpoints are rejected.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

import asyncpg

from mission_control.application.installations.registry import ApplicationBinding
from mission_control.contracts.json import parse_json_object


async def register_identity(
    binding: ApplicationBinding,
    *,
    owner_dsn: str,
    expected_database: str,
) -> None:
    if binding.required_component_version != "transitional-local-v1":
        raise ValueError("only transitional-local-v1 identity can be registered")
    parsed = urlsplit(owner_dsn)
    if parsed.scheme not in {"postgres", "postgresql"} or parsed.hostname not in {
        "localhost",
        "127.0.0.1",
        "::1",
    }:
        raise ValueError("identity registration is restricted to loopback PostgreSQL")
    connection = await asyncpg.connect(owner_dsn)
    try:
        async with connection.transaction():
            await connection.execute("SELECT pg_advisory_xact_lock($1)", 0x4D434944)
            actual_database = await connection.fetchval("SELECT current_database()")
            if actual_database != expected_database:
                raise ValueError("database identity differs from the explicit target")
            expected = (
                binding.installation_id,
                binding.application_id,
                binding.supabase_project_ref,
                binding.required_component_version,
                expected_database,
            )
            rows = await connection.fetch("""
                SELECT installation_id, application_id, project_ref,
                       component_version, database_name
                FROM belllabs_control.mission_installation_identity
            """)
            if rows:
                if len(rows) != 1 or tuple(rows[0]) != expected:
                    raise ValueError("existing installation identity differs; refusing replacement")
                return
            await connection.execute(
                """
                INSERT INTO belllabs_control.mission_installation_identity
                    (installation_id, application_id, project_ref, component_version, database_name)
                VALUES ($1,$2,$3,$4,$5)
            """,
                *expected,
            )
    finally:
        await connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding-file", required=True)
    parser.add_argument("--expected-database", required=True)
    parser.add_argument("--owner-dsn-env", required=True)
    args = parser.parse_args()
    try:
        binding = ApplicationBinding.model_validate(
            parse_json_object(Path(args.binding_file).read_bytes())
        )
        dsn = os.environ.get(args.owner_dsn_env)
        if not dsn:
            raise ValueError("owner credential reference is unavailable")
        asyncio.run(
            register_identity(binding, owner_dsn=dsn, expected_database=args.expected_database)
        )
    except Exception:
        # Driver and validation errors can contain connection parameters or input data.
        print(json.dumps({"registered": False, "error": "installation_registration_failed"}))
        return 2
    print(
        json.dumps(
            {
                "registered": True,
                "application_id": binding.application_id,
                "installation_id": str(binding.installation_id),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
