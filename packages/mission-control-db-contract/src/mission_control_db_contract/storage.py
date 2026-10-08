"""Supabase Storage provisioning for capability bundles (ADR-0024, FT-A2).

Seed records of kind ``storage_bucket`` and ``storage_policy`` create the private
``capability-bundles`` bucket and its ``storage.objects`` policies through ``mission-db
seed-apply``; ``mission-db qualify`` verifies them. Immutability is enforced by policy:
writers get INSERT (and SELECT to verify), readers get SELECT, nobody gets UPDATE or DELETE,
and a restrictive policy confines every role to the application's top-level prefix.
A cluster without the Storage schema (a plain local disposable PostgreSQL) reports the
bundle ``blocked``; it is never applied partially.
"""

from __future__ import annotations

from typing import Any

from .errors import ContractError

BUCKET_ID = "capability-bundles"
APPLICATION_TOKEN = "${application_id}"
CLAIM = "mc_capability_role"
POLICY_COMMANDS = ("INSERT", "SELECT", "ALL")
CAPABILITY_ROLES = ("publisher", "reader", "*")


async def storage_available(connection: Any) -> bool:
    """Storage schema, objects table, foldername() and the ``authenticated`` role exist."""
    return bool(
        await connection.fetchval(
            "SELECT to_regclass('storage.buckets') IS NOT NULL "
            "AND to_regclass('storage.objects') IS NOT NULL "
            "AND to_regprocedure('storage.foldername(text)') IS NOT NULL "
            "AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated')"
        )
    )


def _quote_literal(value: str) -> str:
    if "\x00" in value:
        raise ContractError("storage seed literal contains NUL")
    return "'" + value.replace("'", "''") + "'"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def policy_condition(bucket_id: str, application_id: str, capability_role: str) -> str:
    prefix = (
        f"bucket_id = {_quote_literal(bucket_id)} "
        f"AND (storage.foldername(name))[1] = {_quote_literal(application_id)}"
    )
    if capability_role == "*":
        return prefix
    claim = (
        "coalesce(nullif(current_setting('request.jwt.claims', true), '')::jsonb "
        f"->> '{CLAIM}', '')"
    )
    return f"{prefix} AND {claim} = {_quote_literal(capability_role)}"


def policy_statement(fields: dict[str, Any], application_id: str) -> str:
    """The exact ``CREATE POLICY`` for one ``storage_policy`` seed record."""
    command = str(fields["command"])
    role = str(fields["capability_role"])
    bucket = str(fields["bucket_id"])
    if command not in POLICY_COMMANDS or role not in CAPABILITY_ROLES:
        raise ContractError("storage policy command or capability role is not allowed")
    if command in {"UPDATE", "DELETE"}:
        raise ContractError("capability bundle objects are never updated or deleted")
    restrictive = bool(fields.get("restrictive", False))
    if restrictive != (command == "ALL"):
        raise ContractError("only the restrictive prefix guard may cover ALL commands")
    condition = policy_condition(bucket, application_id, role)
    clauses = {
        "INSERT": f"WITH CHECK ({condition})",
        "SELECT": f"USING ({condition})",
        "ALL": f"USING (bucket_id <> {_quote_literal(bucket)} OR ({condition})) "
        f"WITH CHECK (bucket_id <> {_quote_literal(bucket)} OR ({condition}))",
    }
    return (
        f"CREATE POLICY {_quote_ident(str(fields['policy']))} ON storage.objects "
        f"AS {'RESTRICTIVE' if restrictive else 'PERMISSIVE'} FOR {command} "
        f"TO authenticated {clauses[command]}"
    )


def resolve_application(value: str, application_id: str) -> str:
    return value.replace(APPLICATION_TOKEN, application_id)


async def apply_bucket(connection: Any, fields: dict[str, Any]) -> bool:
    """Create the private bucket once; an existing bucket must match exactly."""
    existing = await connection.fetchrow(
        "SELECT public, file_size_limit, allowed_mime_types FROM storage.buckets WHERE id=$1",
        fields["bucket_id"],
    )
    wanted_limit = int(fields["file_size_limit"])
    if existing is not None:
        if (
            bool(existing["public"]) != bool(fields["public"])
            or existing["file_size_limit"] is None
            or int(existing["file_size_limit"]) != wanted_limit
            or existing["allowed_mime_types"] is not None
        ):
            raise ContractError(
                f"storage bucket {fields['bucket_id']} differs from its seed",
                code="SEED_CONFLICT",
            )
        return False
    await connection.execute(
        "INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types) "
        "VALUES ($1, $1, $2, $3, NULL)",
        fields["bucket_id"],
        bool(fields["public"]),
        wanted_limit,
    )
    return True


async def apply_policy(connection: Any, fields: dict[str, Any], application_id: str) -> bool:
    """Create the policy once; an existing policy with that name must be identical."""
    statement = policy_statement(fields, application_id)
    existing = await connection.fetchrow(
        "SELECT cmd, permissive, roles::text[] AS roles FROM pg_policies "
        "WHERE schemaname='storage' AND tablename='objects' AND policyname=$1",
        fields["policy"],
    )
    if existing is not None:
        expected_cmd = str(fields["command"])
        expected_mode = "RESTRICTIVE" if fields.get("restrictive") else "PERMISSIVE"
        if (
            str(existing["cmd"]) != expected_cmd
            or str(existing["permissive"]) != expected_mode
            or list(existing["roles"]) != ["authenticated"]
        ):
            raise ContractError(
                f"storage policy {fields['policy']} differs from its seed", code="SEED_CONFLICT"
            )
        return False
    await connection.execute(statement)
    return True


async def verify_capability_bundles(connection: Any, application_id: str) -> dict[str, Any]:
    """Qualification evidence: bucket private with the limit, exactly the seeded policies."""
    if not await storage_available(connection):
        return {"status": "absent", "bucket": BUCKET_ID}
    bucket = await connection.fetchrow(
        "SELECT public, file_size_limit FROM storage.buckets WHERE id=$1", BUCKET_ID
    )
    rows = await connection.fetch(
        "SELECT policyname, cmd, permissive, qual, with_check FROM pg_policies "
        "WHERE schemaname='storage' AND tablename='objects' "
        "AND (coalesce(qual, '') || coalesce(with_check, '')) LIKE $1",
        f"%{BUCKET_ID}%",
    )
    policies = sorted(
        (
            {"policy": row["policyname"], "cmd": row["cmd"], "mode": row["permissive"]}
            for row in rows
        ),
        key=lambda item: str(item["policy"]),
    )
    problems: list[str] = []
    if bucket is None:
        problems.append("bucket missing")
    elif bucket["public"] or bucket["file_size_limit"] is None:
        problems.append("bucket must be private with a file size limit")
    if any(item["cmd"] in {"UPDATE", "DELETE"} for item in policies):
        problems.append("an UPDATE or DELETE policy exists on capability bundles")
    for row in rows:
        text = (row["qual"] or "") + (row["with_check"] or "")
        if application_id not in text:
            problems.append(f"policy {row['policyname']} is not scoped to {application_id}")
    return {
        "status": "verified" if not problems and bucket is not None else "drift",
        "bucket": BUCKET_ID,
        "policies": policies,
        "problems": problems,
    }
