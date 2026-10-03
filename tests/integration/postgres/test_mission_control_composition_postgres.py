"""Concrete composition against its own disposable local PostgreSQL database."""

from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import pytest

from mission_control.adapters.postgres.connections import apply_application_migrations
from mission_control.adapters.storage.control_plane_payloads import UnavailablePayloadStore
from mission_control.application.execution.service import AdmissionPolicyRegistry
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationUnavailable,
    VerifiedApplicationIdentity,
    request_scope,
)
from mission_control.bootstrap.composition import compose_application_services
from mission_control.domain.authoring.extensions import ExtensionRegistry


@pytest.mark.asyncio
async def test_factory_requires_persisted_identity_and_never_claims_common_schema_readiness(
    test_application_postgres_dsn: str,
) -> None:
    parts = urlsplit(test_application_postgres_dsn)
    if parts.hostname not in {"localhost", "127.0.0.1", "::1"}:
        pytest.fail("composition proof creates its own disposable database on localhost only")
    database = "mc_composition_" + uuid4().hex
    owner = await asyncpg.connect(test_application_postgres_dsn)
    await owner.execute(f'CREATE DATABASE "{database}"')
    runtime = None
    family_writer = None
    migration_pool = None
    try:
        dsn = urlunsplit(parts._replace(path="/" + database))
        migration_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
        await apply_application_migrations(migration_pool)

        async def assume_runtime(connection: asyncpg.Connection) -> None:
            await connection.execute("SET ROLE belllabs_control_runtime")

        runtime = await asyncpg.create_pool(dsn, min_size=1, max_size=2, setup=assume_runtime)

        async def assume_family(connection: asyncpg.Connection) -> None:
            await connection.execute("SET ROLE belllabs_family_repository_writer")

        family_writer = await asyncpg.create_pool(dsn, min_size=1, max_size=1, setup=assume_family)
        installation, tenant = uuid4(), uuid4()
        binding = ApplicationBinding.seal(
            application_id="biotech",
            installation_id=installation,
            binding_version="1",
            supabase_project_ref="biotech-research-ingestion",
            database_secret_ref="TEST_APPLICATION_POSTGRES_DSN",
            accepted_issuers={"https://local.invalid"},
            accepted_audiences={"authenticated"},
            required_component_version="transitional-local-v1",
        )
        identity = VerifiedApplicationIdentity(
            issuer="https://local.invalid",
            audiences=frozenset({"authenticated"}),
            application_id="biotech",
            installation_id=installation,
            tenant_id=tenant,
        )
        registry = ApplicationRegistry((binding,))
        inputs = dict(
            runtime_pool=runtime,
            admission_policies=AdmissionPolicyRegistry(),
            extensions=ExtensionRegistry(),
            payload_store=UnavailablePayloadStore(),
            registry=registry,
            family_writer_pool=family_writer,
        )
        with pytest.raises(InstallationUnavailable, match="not released"):
            await compose_application_services(binding, identity, **inputs)
        with pytest.raises(InstallationUnavailable, match="exactly one"):
            await compose_application_services(
                binding, identity, storage_mode="transitional_local", **inputs
            )
        async with migration_pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO belllabs_control.mission_installation_identity
                    (installation_id, application_id, project_ref, component_version, database_name)
                VALUES($1,'biotech','biotech-research-ingestion','transitional-local-v1',$2)
            """,
                installation,
                database,
            )
        composed = await compose_application_services(
            binding, identity, storage_mode="transitional_local", **inputs
        )
        assert composed.readiness.production_ready is False
        assert composed.readiness.database_name == database
        assert composed.readiness.runtime_role == "belllabs_control_runtime"
        assert composed.lifecycle.request_scope == request_scope(identity)
        assert composed.runtime.request_scope == request_scope(identity)
        assert composed.admission.request_scope == request_scope(identity)
        assert composed.launch is None
        assert registry.resolve("biotech", identity) == binding
        wrong = ApplicationBinding.seal(
            **{
                **binding.model_dump(mode="python", exclude={"binding_digest"}),
                "supabase_project_ref": "other-project",
            }
        )
        with pytest.raises(InstallationUnavailable, match="mismatch"):
            await compose_application_services(
                wrong, identity, storage_mode="transitional_local", **inputs
            )
        with pytest.raises(InstallationUnavailable, match="restricted"):
            await compose_application_services(
                binding,
                identity,
                runtime_pool=migration_pool,
                admission_policies=AdmissionPolicyRegistry(),
                extensions=ExtensionRegistry(),
                payload_store=UnavailablePayloadStore(),
                storage_mode="transitional_local",
            )
    finally:
        if family_writer is not None:
            await family_writer.close()
        if runtime is not None:
            await runtime.close()
        if migration_pool is not None:
            await migration_pool.close()
        # Only the random database created above is removed; configured databases are untouched.
        await owner.execute(f'DROP DATABASE "{database}"')
        await owner.close()
