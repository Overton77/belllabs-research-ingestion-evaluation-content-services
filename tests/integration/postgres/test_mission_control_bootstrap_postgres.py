"""Real JWT authentication + factory lifespan + restricted disposable PostgreSQL."""

import json
import time
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import httpx
import pytest
from joserfc import jwt
from joserfc.jwk import RSAKey

from mission_control.adapters.auth.jwt import (
    ActorGrant,
    ApplicationAuthentication,
)
from mission_control.adapters.postgres.connections import apply_application_migrations
from mission_control.application.installations.registry import ApplicationBinding
from mission_control.bootstrap.api import (
    ApplicationDeployment,
    MissionDeployment,
    create_application,
)
from mission_control.bootstrap.installation import register_identity


@pytest.mark.asyncio
async def test_configured_api_authenticates_and_reads_actual_restricted_database(
    test_application_postgres_dsn,
    tmp_path,
    monkeypatch,
):
    parsed = urlsplit(test_application_postgres_dsn)
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        pytest.fail("bootstrap proof only creates disposable databases on loopback")
    name = "mc_bootstrap_" + uuid4().hex
    role = "mc_api_" + uuid4().hex
    owner = await asyncpg.connect(test_application_postgres_dsn)
    await owner.execute(f'CREATE DATABASE "{name}"')
    owner_pool = None
    role_created = False
    try:
        owner_dsn = urlunsplit(parsed._replace(path="/" + name))
        owner_pool = await asyncpg.create_pool(owner_dsn, min_size=1, max_size=1)
        await apply_application_migrations(owner_pool)
        await owner.execute(f'CREATE ROLE "{role}" LOGIN')
        role_created = True
        await owner.execute(f'GRANT belllabs_control_runtime TO "{role}"')
        # This proof's loopback cluster uses trust auth. No deployment credential is copied.
        host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        runtime_dsn = urlunsplit(
            parsed._replace(
                netloc=f"{role}@{host}:{parsed.port or 5432}",
                path="/" + name,
            )
        )
        monkeypatch.setenv("MC_BOOTSTRAP_TEST_DSN", runtime_dsn)
        tenant = uuid4()
        binding = ApplicationBinding.seal(
            application_id="biotech",
            installation_id=uuid4(),
            binding_version="1",
            supabase_project_ref="local-bootstrap-test",
            database_secret_ref="MC_BOOTSTRAP_TEST_DSN",
            accepted_issuers={"https://issuer.invalid"},
            accepted_audiences={"authenticated"},
            required_component_version="transitional-local-v1",
        )
        with pytest.raises(ValueError, match="explicit target"):
            await register_identity(
                binding, owner_dsn=owner_dsn, expected_database="another_database"
            )
        await register_identity(binding, owner_dsn=owner_dsn, expected_database=name)
        await register_identity(binding, owner_dsn=owner_dsn, expected_database=name)
        different = ApplicationBinding.seal(
            **{
                **binding.model_dump(exclude={"binding_digest"}),
                "installation_id": uuid4(),
            }
        )
        with pytest.raises(ValueError, match="refusing replacement"):
            await register_identity(different, owner_dsn=owner_dsn, expected_database=name)
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
            storage_mode="transitional_local",
            max_request_bytes=1_000_000,
            applications=(ApplicationDeployment(authentication=auth),),
        )
        from datetime import UTC, datetime

        from mission_control.adapters.postgres.control_plane.definition_repository import (
            PostgresDefinitionRepository,
        )
        from tests.unit.control_plane.test_agentic_asset_definitions import skill_definition

        catalog_scope = f"mc/{binding.installation_id}/{binding.application_id}/catalog"
        catalog = PostgresDefinitionRepository(owner_pool, catalog_scope=catalog_scope)
        published = await catalog.publish(
            skill_definition(),
            actor_id="test-publisher",
            published_at=datetime.now(UTC),
            expected_head_revision=0,
        )
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
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                ready = await client.get("/health/ready")
                assert ready.status_code == 200, ready.text
                assert ready.json()["production_ready"] is False
                assert ready.json()["launch_configured"] is False
                missing = await client.get("/v1/applications/biotech/runs/absent/inspection")
                assert missing.status_code == 401
                inspected = await client.get(
                    "/v1/applications/biotech/runs/absent/inspection",
                    headers={"Authorization": f"Bearer {token}"},
                )
                assert inspected.status_code == 404, inspected.text
                listed = await client.get(
                    "/v1/applications/biotech/catalog/definitions",
                    headers={"Authorization": f"Bearer {token}"},
                )
                assert listed.status_code == 200, listed.text
                assert listed.json()["catalog_scope"] == catalog_scope
                assert listed.json()["definitions"] == [published.ref.model_dump(mode="json")]
                resolved = await client.post(
                    "/v1/applications/biotech/catalog/resolve",
                    headers={"Authorization": f"Bearer {token}"},
                    json=published.ref.model_dump(mode="json"),
                )
                assert resolved.status_code == 200, resolved.text
                assert resolved.json()["ref"] == published.ref.model_dump(mode="json")
                unavailable = await client.post(
                    "/v1/applications/biotech/catalog/discover",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"source": "mcp", "query": "filesystem"},
                )
                assert unavailable.status_code == 403  # read grant cannot discover
                wrong = await client.get(
                    "/v1/applications/other/runs/absent/inspection",
                    headers={"Authorization": f"Bearer {token}"},
                )
                assert wrong.status_code == 403
        assert not app.state.mission_control_ready
    finally:
        if owner_pool is not None:
            await owner_pool.close()
        await owner.execute(f'DROP DATABASE "{name}"')
        if role_created:
            await owner.execute(f'DROP ROLE "{role}"')
        await owner.close()
