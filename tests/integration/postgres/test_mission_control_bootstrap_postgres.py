"""Real JWT authentication + factory lifespan over the installed common component."""

import json
import time
from datetime import UTC, datetime
from uuid import uuid4

import asyncpg
import httpx
import pytest
from joserfc import jwt
from joserfc.jwk import RSAKey

from mission_control.adapters.auth.jwt import ActorGrant, ApplicationAuthentication
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    InstallationUnavailable,
)
from mission_control.bootstrap.api import (
    ApplicationDeployment,
    MissionDeployment,
    create_application,
)
from mission_control.bootstrap.common_installation import inspect_common_installation
from tests.fixtures.mission_control_common_db import (
    COMPONENT_VERSION,
    CommonDatabase,
    catalog_scope,
    common_database,
)
from tests.unit.control_plane.test_agentic_asset_definitions import skill_definition

pytestmark = pytest.mark.common_db


def _binding(database: CommonDatabase, **overrides: object) -> ApplicationBinding:
    values: dict[str, object] = {
        "application_id": database.application_id,
        "installation_id": database.installation_id,
        "binding_version": "1",
        "supabase_project_ref": database.project_ref,
        "database_secret_ref": "MC_BOOTSTRAP_TEST_DSN",
        "accepted_issuers": {"https://issuer.invalid"},
        "accepted_audiences": {"authenticated"},
        "required_component_version": COMPONENT_VERSION,
    }
    values.update(overrides)
    return ApplicationBinding.seal(**values)


@pytest.mark.asyncio
async def test_readiness_fails_closed_on_identity_release_and_role_mismatch() -> None:
    async with common_database() as database:
        runtime = await database.pool("mission_control_runtime")
        family = await database.pool("mission_control_family_writer")
        owner = await database.owner_pool()
        try:
            readiness = await inspect_common_installation(runtime, _binding(database))
            assert readiness.storage_mode == "production_common"
            assert readiness.component_version == COMPONENT_VERSION
            # The fixture attestation is not a qualified release fingerprint.
            assert readiness.production_ready is False
            assert readiness.pool_role == database.login_roles["mission_control_runtime"]
            for wrong in (
                _binding(database, supabase_project_ref="another-project"),
                _binding(database, installation_id=uuid4()),
                _binding(database, required_component_version="9.9.9"),
            ):
                with pytest.raises(InstallationUnavailable):
                    await inspect_common_installation(runtime, wrong)
            # Owner and family-writer pools cannot act as the runtime pool.
            with pytest.raises(InstallationUnavailable):
                await inspect_common_installation(owner, _binding(database))
            with pytest.raises(InstallationUnavailable):
                await inspect_common_installation(family, _binding(database))
            # A build whose writer version is not attested refuses to start.
            async with owner.acquire() as connection:
                await connection.execute(
                    "ALTER TABLE mission_control.release_attestation "
                    "DISABLE TRIGGER release_attestation_immutable"
                )
                await connection.execute(
                    "UPDATE mission_control.release_attestation "
                    "SET supported_writer_versions = ARRAY['mission-control-runtime/0']"
                )
            with pytest.raises(InstallationUnavailable, match="admitted reader/writer"):
                await inspect_common_installation(runtime, _binding(database))
        finally:
            await runtime.close()
            await family.close()
            await owner.close()


@pytest.mark.asyncio
async def test_missing_component_fails_readiness_without_fallback() -> None:
    async with common_database(legacy_poison=True) as database:
        owner = await asyncpg.connect(database.owner_dsn)
        try:
            await owner.execute("DROP SCHEMA mission_control_search CASCADE")
            await owner.execute("DROP SCHEMA mission_control CASCADE")
        finally:
            await owner.close()
        runtime = await database.pool("mission_control_runtime")
        try:
            with pytest.raises(InstallationUnavailable):
                await inspect_common_installation(runtime, _binding(database))
        finally:
            await runtime.close()


@pytest.mark.asyncio
async def test_configured_api_authenticates_and_reads_actual_restricted_database(
    tmp_path, monkeypatch
) -> None:
    async with common_database() as database:
        monkeypatch.setenv("MC_BOOTSTRAP_TEST_DSN", database.dsn("mission_control_runtime"))
        tenant = database.tenants["tenant-1"]
        binding = _binding(database)
        key = RSAKey.generate_key(2048, parameters={"kid": "bootstrap"})
        public = tmp_path / "keys.json"
        public.write_text(json.dumps({"keys": [key.as_dict(private=False)]}))
        auth = ApplicationAuthentication(
            binding=binding,
            issuer="https://issuer.invalid",
            audience="authenticated",
            public_jwks_file=public,
            grants=(
                ActorGrant(
                    subject="test-subject",
                    actor_id="operator",
                    tenant_ids={tenant},
                    permissions={"workflow_run.read", "catalog:read"},
                ),
            ),
        )
        deployment = MissionDeployment(
            storage_mode="production_common",
            max_request_bytes=1_000_000,
            applications=(ApplicationDeployment(authentication=auth),),
        )
        scope = catalog_scope(database.application_id)
        publisher = await database.pool("mission_control_runtime")
        try:
            published = await PostgresDefinitionRepository(publisher, catalog_scope=scope).publish(
                skill_definition(),
                actor_id="test-publisher",
                published_at=datetime.now(UTC),
                expected_head_revision=0,
            )
        finally:
            await publisher.close()
        app = create_application(deployment)
        token = jwt.encode(
            {"alg": "RS256", "kid": "bootstrap"},
            {
                "sub": "test-subject",
                "iss": auth.issuer,
                "aud": auth.audience,
                "exp": int(time.time()) + 300,
                "app_metadata": {"application_id": "biotech", "tenant_id": str(tenant)},
            },
            key,
        )
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                ready = await client.get("/health/ready")
                assert ready.status_code == 200, ready.text
                assert ready.json()["storage_mode"] == "production_common"
                assert ready.json()["component_versions"] == [COMPONENT_VERSION]
                assert ready.json()["production_ready"] is False
                assert ready.json()["launch_configured"] is False
                missing = await client.get("/v1/applications/biotech/runs/absent/inspection")
                assert missing.status_code == 401
                headers = {"Authorization": f"Bearer {token}"}
                inspected = await client.get(
                    "/v1/applications/biotech/runs/absent/inspection", headers=headers
                )
                assert inspected.status_code == 404, inspected.text
                listed = await client.get(
                    "/v1/applications/biotech/catalog/definitions", headers=headers
                )
                assert listed.status_code == 200, listed.text
                assert listed.json()["catalog_scope"] == scope
                assert listed.json()["definitions"] == [published.ref.model_dump(mode="json")]
                resolved = await client.post(
                    "/v1/applications/biotech/catalog/resolve",
                    headers=headers,
                    json=published.ref.model_dump(mode="json"),
                )
                assert resolved.status_code == 200, resolved.text
                assert resolved.json()["ref"] == published.ref.model_dump(mode="json")
                unavailable = await client.post(
                    "/v1/applications/biotech/catalog/discover",
                    headers=headers,
                    json={"source": "mcp", "query": "filesystem"},
                )
                assert unavailable.status_code == 403  # read grant cannot discover
                wrong = await client.get(
                    "/v1/applications/other/runs/absent/inspection", headers=headers
                )
                assert wrong.status_code == 403
        assert not app.state.mission_control_ready
