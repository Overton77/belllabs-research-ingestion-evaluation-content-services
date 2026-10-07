"""Concrete composition against a disposable database with the common component."""

import pytest

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
from tests.fixtures.mission_control_common_db import COMPONENT_VERSION, common_database

pytestmark = pytest.mark.common_db


@pytest.mark.asyncio
async def test_factory_requires_attested_installation_and_restricted_pools() -> None:
    async with common_database() as database:
        runtime = await database.pool("mission_control_runtime")
        family_writer = await database.pool("mission_control_family_writer")
        owner = await database.owner_pool()
        try:
            tenant = database.tenants["tenant-1"]
            binding = ApplicationBinding.seal(
                application_id=database.application_id,
                installation_id=database.installation_id,
                binding_version="1",
                supabase_project_ref=database.project_ref,
                database_secret_ref="MC_COMPOSITION_TEST_DSN",
                accepted_issuers={"https://local.invalid"},
                accepted_audiences={"authenticated"},
                required_component_version=COMPONENT_VERSION,
            )
            identity = VerifiedApplicationIdentity(
                issuer="https://local.invalid",
                audiences=frozenset({"authenticated"}),
                application_id=database.application_id,
                installation_id=database.installation_id,
                tenant_id=tenant,
            )
            registry = ApplicationRegistry((binding,))
            inputs = {
                "admission_policies": AdmissionPolicyRegistry(),
                "extensions": ExtensionRegistry(),
                "payload_store": UnavailablePayloadStore(),
                "registry": registry,
            }
            composed = await compose_application_services(
                binding,
                identity,
                runtime_pool=runtime,
                family_writer_pool=family_writer,
                **inputs,
            )
            assert composed.readiness.storage_mode == "production_common"
            assert composed.readiness.production_ready is False  # fixture attestation
            assert composed.readiness.database_name == database.name
            assert composed.readiness.pool_role == database.login_roles["mission_control_runtime"]
            assert composed.lifecycle.request_scope == request_scope(identity)
            assert composed.runtime.request_scope == request_scope(identity)
            assert composed.admission.request_scope == request_scope(identity)
            assert composed.launch is None
            assert registry.resolve(database.application_id, identity) == binding

            wrong = ApplicationBinding.seal(
                **{
                    **binding.model_dump(mode="python", exclude={"binding_digest"}),
                    "supabase_project_ref": "other-project",
                }
            )
            with pytest.raises(InstallationUnavailable, match="differs from binding"):
                await compose_application_services(wrong, identity, runtime_pool=runtime, **inputs)
            # Owner credentials, a swapped family pool and an unbound identity never compose.
            with pytest.raises(InstallationUnavailable, match="mission_control_runtime"):
                await compose_application_services(binding, identity, runtime_pool=owner, **inputs)
            with pytest.raises(InstallationUnavailable, match="mission_control_family_writer"):
                await compose_application_services(
                    binding,
                    identity,
                    runtime_pool=runtime,
                    family_writer_pool=runtime,
                    **inputs,
                )
            with pytest.raises(InstallationUnavailable, match="not bound"):
                await compose_application_services(
                    binding,
                    VerifiedApplicationIdentity(
                        issuer="https://other.invalid",
                        audiences=frozenset({"authenticated"}),
                        application_id=database.application_id,
                        installation_id=database.installation_id,
                        tenant_id=tenant,
                    ),
                    runtime_pool=runtime,
                    **inputs,
                )
        finally:
            await runtime.close()
            await family_writer.close()
            await owner.close()
