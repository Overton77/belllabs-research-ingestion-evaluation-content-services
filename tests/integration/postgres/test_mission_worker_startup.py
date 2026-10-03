"""Worker startup proof against actual persisted identity and three restricted roles."""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import pytest
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.store.postgres import PostgresStore
from pydantic import SecretStr
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

from mission_control.adapters.auth.jwt import ApplicationAuthentication
from mission_control.adapters.postgres.connections import apply_application_migrations
from mission_control.adapters.storage.control_plane_payloads import UnavailablePayloadStore
from mission_control.adapters.temporal.deployment_composition import (
    ProductionWorkerActivityCompositionFactory,
)
from mission_control.adapters.temporal.search_attributes import register_belllabs_search_attributes
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    InstallationUnavailable,
)
from mission_control.bootstrap.api import (
    ApplicationDeployment,
    MissionDeployment,
    RuntimeOptions,
    TemporalDeployment,
)
from mission_control.bootstrap.installation import register_identity
from mission_control.bootstrap.worker import prepare_worker, run_worker, select_application
from mission_control.domain.authoring.extensions import ExtensionRegistry
from tests.fixtures.isolated_settings import isolated_settings
from tests.fixtures.mission_control_production_stack import start_local


@pytest.mark.asyncio
@pytest.mark.parametrize("poll_workers", [False, True])
async def test_worker_rechecks_actual_installation_on_restart_without_startup_mutation(
    test_application_postgres_dsn,
    monkeypatch,
    tmp_path,
    poll_workers,
):
    parts = urlsplit(test_application_postgres_dsn)
    if parts.hostname not in {"127.0.0.1", "localhost", "::1"}:
        pytest.fail("worker proof creates disposable databases on loopback only")
    database = "mc_worker_" + uuid4().hex
    roles = ["mc_w_" + uuid4().hex for _ in range(3)]
    owner = await asyncpg.connect(test_application_postgres_dsn)
    await owner.execute(f'CREATE DATABASE "{database}"')
    owner_pool = None
    created_roles = []
    try:
        owner_dsn = urlunsplit(parts._replace(path="/" + database))
        owner_pool = await asyncpg.create_pool(owner_dsn, min_size=1, max_size=1)
        await apply_application_migrations(owner_pool)
        for role in roles:
            await owner.execute(f'CREATE ROLE "{role}" LOGIN')
            created_roles.append(role)
        await owner.execute(f'GRANT belllabs_control_runtime TO "{roles[0]}"')
        await owner.execute(f'GRANT belllabs_family_repository_writer TO "{roles[1]}"')
        async with owner_pool.acquire() as connection:
            await connection.execute("CREATE SCHEMA mc_worker_recovery")
            await connection.execute(f'GRANT USAGE ON SCHEMA mc_worker_recovery TO "{roles[2]}"')
            await connection.execute(f'GRANT USAGE ON SCHEMA belllabs_control TO "{roles[2]}"')

        runtime_setup_dsn = (
            owner_dsn + "?connect_timeout=5&options=-c%20search_path%3Dmc_worker_recovery"
        )

        def provision_runtime() -> None:
            with PostgresSaver.from_conn_string(runtime_setup_dsn) as saver:
                saver.setup()
            with PostgresStore.from_conn_string(runtime_setup_dsn) as store:
                store.setup()

        await asyncio.to_thread(provision_runtime)
        async with owner_pool.acquire() as connection:
            await connection.execute(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "
                f'mc_worker_recovery TO "{roles[2]}"'
            )

        host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
        dsns = [
            urlunsplit(
                parts._replace(
                    netloc=f"{role}@{host}:{parts.port or 5432}",
                    path="/" + database,
                )
            )
            for role in roles
        ]
        monkeypatch.setenv("MC_WORKER_RUNTIME_DSN", dsns[0])
        monkeypatch.setenv("MC_WORKER_FAMILY_DSN", dsns[1])
        binding = ApplicationBinding.seal(
            application_id="biotech",
            installation_id=uuid4(),
            binding_version="worker-test-v1",
            supabase_project_ref="worker-local-test",
            database_secret_ref="MC_WORKER_RUNTIME_DSN",
            accepted_issuers={"https://worker.invalid"},
            accepted_audiences={"worker"},
            required_component_version="transitional-local-v1",
        )
        authentication = ApplicationAuthentication(
            binding=binding,
            issuer="https://worker.invalid",
            audience="worker",
            public_jwks_file=tmp_path / "unused-worker-jwks.json",
            grants=(),
        )
        item = ApplicationDeployment(
            authentication=authentication,
            family_writer_secret_ref="MC_WORKER_FAMILY_DSN",
            temporal=TemporalDeployment(
                address="127.0.0.1:17233",
                namespace="worker-test",
                root_task_queue="worker-root",
                stagegraph_task_queue="worker-coordinator-family-stagegraph",
                goal_directed_task_queue="worker-coordinator-family-goal-directed",
            ),
        )
        deployment = MissionDeployment(
            storage_mode="transitional_local",
            applications=(item,),
            max_request_bytes=100_000,
        )
        settings = isolated_settings(
            coordinator_launch_enabled=True,
            langgraph_checkpoint_database_direct=SecretStr(dsns[2]),
            langgraph_checkpoint_schema="mc_worker_recovery",
            langgraph_checkpoint_setup=False,
        )
        inputs = dict(
            application_id="biotech", binding_digest=binding.binding_digest, settings=settings
        )
        with pytest.raises(InstallationUnavailable, match="exactly one"):
            async with prepare_worker(deployment, **inputs):
                pytest.fail("worker must not accept an unregistered database")
        await register_identity(binding, owner_dsn=owner_dsn, expected_database=database)
        for _ in range(2):
            async with prepare_worker(deployment, **inputs) as prepared:
                assert prepared.readiness.database_name == database
                assert prepared.readiness.runtime_role == roles[0]
                assert prepared.settings.temporal_task_queue == "worker"
                assert prepared.settings.mission_control_catalog_scope == (
                    f"mc/{binding.installation_id}/biotech/catalog"
                )
                assert not prepared.readiness.production_ready
        with pytest.raises(InstallationUnavailable, match="pin mismatch"):
            select_application(deployment, "biotech", "sha256:" + "0" * 64)
        with pytest.raises(InstallationUnavailable, match="not released"):
            select_application(
                deployment.model_copy(update={"storage_mode": "production_common"}),
                "biotech",
                binding.binding_digest,
            )
        wrong = ApplicationBinding.seal(
            **{
                **binding.model_dump(exclude={"binding_digest"}),
                "installation_id": uuid4(),
            }
        )
        wrong_deployment = deployment.model_copy(
            update={
                "applications": (
                    item.model_copy(
                        update={
                            "authentication": authentication.model_copy(update={"binding": wrong})
                        }
                    ),
                )
            }
        )
        with pytest.raises(InstallationUnavailable, match="mismatch"):
            async with prepare_worker(
                wrong_deployment, **{**inputs, "binding_digest": wrong.binding_digest}
            ):
                pytest.fail("worker must reject a different persisted installation")
        with pytest.raises(InstallationUnavailable, match="checkpoint role"):
            async with prepare_worker(
                deployment,
                **{
                    **inputs,
                    "settings": settings.model_copy(
                        update={"langgraph_checkpoint_database_direct": SecretStr(dsns[0])},
                    ),
                },
            ):
                pytest.fail("business writer cannot serve as checkpoint identity")
        with pytest.raises(InstallationUnavailable, match="operator step"):
            async with prepare_worker(
                deployment,
                **{
                    **inputs,
                    "settings": settings.model_copy(
                        update={"langgraph_checkpoint_setup": True},
                    ),
                },
            ):
                pytest.fail("startup must never initialize checkpoint schema")
        if poll_workers:
            temporal = await start_local(tmp_path / "worker-temporal.sqlite", port=7346)
            stop = asyncio.Event()
            task = None
            try:
                await register_belllabs_search_attributes(temporal.client, "default")
                polling_deployment = deployment.model_copy(
                    update={
                        "applications": (
                            item.model_copy(
                                update={
                                    "temporal": item.temporal.model_copy(
                                        update={
                                            "address": "127.0.0.1:7346",
                                            "namespace": "default",
                                        }
                                    )
                                }
                            ),
                        )
                    }
                )
                polling_settings = settings.model_copy(
                    update={
                        "capability_bundle_backend": "local",
                        "web_research_agent_browser_node": Path(shutil.which("node")),
                        "artifact_payload_root": tmp_path / "artifacts",
                        "deep_agent_sandbox_workspace_root": tmp_path / "workspaces",
                    }
                )
                options = RuntimeOptions(
                    AdmissionPolicyRegistry(),
                    ExtensionRegistry(),
                    UnavailablePayloadStore(),
                    FamilyAdmissionRegistry(),
                )

                class CheckedFactory(ProductionWorkerActivityCompositionFactory):
                    async def build(self, **kwargs):
                        catalog = kwargs["control_plane"]
                        runs = kwargs["run_control"]
                        assert catalog._extensions is options.extensions
                        assert catalog._payload_store is options.payload_store
                        assert runs._policies is options.admission_policies
                        assert runs._family_admissions is options.family_admissions
                        return await super().build(**kwargs)

                task = asyncio.create_task(
                    run_worker(
                        polling_deployment,
                        **{**inputs, "settings": polling_settings},
                        stop=stop,
                        runtime_options=options,
                        composition_factory=CheckedFactory(temporal.client),
                    )
                )
                # Inspect the real server: all family, root and linked queues must
                # have active pollers from the actual entrypoint's worker classes.
                for queue in (
                    "worker-root",
                    "worker-coordinator-family-stagegraph",
                    "worker-coordinator-family-goal-directed",
                    "worker-linked-runs",
                ):
                    async with asyncio.timeout(60):
                        while True:
                            if task.done():
                                await task
                                pytest.fail("worker exited before polling")
                            result = await temporal.client.workflow_service.describe_task_queue(
                                DescribeTaskQueueRequest(
                                    namespace="default",
                                    task_queue=TaskQueue(name=queue),
                                    task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                                )
                            )
                            if result.pollers:
                                break
                            await asyncio.sleep(0.1)
            finally:
                stop.set()
                try:
                    if task is not None:
                        await asyncio.wait_for(task, timeout=30)
                finally:
                    await temporal.shutdown()
    finally:
        if owner_pool is not None:
            await owner_pool.close()
        await owner.execute(f'DROP DATABASE "{database}"')
        for role in reversed(created_roles):
            await owner.execute(f'DROP ROLE "{role}"')
        await owner.close()
