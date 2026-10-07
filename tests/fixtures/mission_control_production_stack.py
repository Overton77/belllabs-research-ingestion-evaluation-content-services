"""Disposable PostgreSQL-only production API/worker stack; deterministic local cognition.

Every stack runs on a fresh database with the common ``mission_control`` component
installed (``tests.fixtures.mission_control_common_db``): the API and workers connect as
restricted ``mission_control_runtime`` / ``mission_control_family_writer`` logins under
forced row-level security, the LangGraph saver/store use the provisioned
``mission_control_runtime`` schema through a checkpointer-only login, and every scope is a
canonical ``mc/{installation}/{application}/{tenant}`` scope. The loopback administrator
DSN (``MISSION_CONTROL_TEST_ADMIN_DSN``) authorizes creating and dropping it; set
``MISSION_CONTROL_E2E_KEEP_DATABASE=1`` to keep it for failure inspection. Missing
prerequisites fail this proof instead of converting it to a successful skip.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
from temporalio.testing import WorkflowEnvironment
from tests.fixtures.mission_control_common_db import (
    CommonDatabase,
    catalog_scope,
    create_common_database,
    drop_common_database,
    provision_runtime,
)
from tests.fixtures.rrm009_production_harness import (
    PRINCIPAL,
    TEMPORAL_PORT,
    ProductionStack,
    _reset_api_state,
)
from tests.fixtures.rrm009_production_stack import (
    LANGGRAPH_SCHEMA,
    SCOPE,
    TASK_QUEUE,
    TechnicalBinding,
    technical_admission_policies,
    technical_binding,
)

from mission_control.adapters.postgres.connections import (
    create_application_family_writer_pool,
    create_application_postgres_pool,
)
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.storage.control_plane_payloads import UnavailablePayloadStore
from mission_control.adapters.temporal.deployment_composition import (
    DeploymentCapabilityComponents,
    ProductionWorkerActivityCompositionFactory,
)
from mission_control.adapters.temporal.search_attributes import register_belllabs_search_attributes
from mission_control.adapters.temporal.worker import (
    compose_worker_run_control_service,
    create_production_workers,
)
from mission_control.application.artifacts.artifact_promotion import (
    StaticArtifactValidationAuthority,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.execution.service import F1RunConfigurationVerifier
from mission_control.application.recovery.run_forks import ForkPatchPolicyRegistry
from mission_control.bootstrap.runtime_control import compose_runtime_control
from mission_control.bootstrap.settings import get_settings
from mission_control.bootstrap.technical_api import api
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.interfaces.http.control_plane import get_control_plane_principal
from mission_control.interfaces.http.run_control import (
    close_run_control_resources,
    initialize_run_control_resources,
)


async def start_local(database: Path, *, port: int = TEMPORAL_PORT) -> WorkflowEnvironment:
    configured_cli = os.environ.get("MISSION_CONTROL_TEMPORAL_CLI")
    sdk_cli = Path(tempfile.gettempdir()) / f"temporal-sdk-python-{version('temporalio')}.exe"
    existing = configured_cli or (str(sdk_cli) if sdk_cli.is_file() else None)
    async with asyncio.timeout(60):
        return await WorkflowEnvironment.start_local(
            port=port,
            dev_server_existing_path=existing,
            dev_server_extra_args=["--db-filename", str(database)],
            dev_server_log_level="error",
        )


async def _fresh_database(*, legacy_poison: bool = False) -> tuple[CommonDatabase, str]:
    """A fresh common-component database and its checkpointer-only runtime DSN."""

    database = await create_common_database(legacy_poison=legacy_poison)
    try:
        checkpoint_dsn = await provision_runtime(database)
    except BaseException:
        await drop_common_database(database)
        raise
    return database, checkpoint_dsn


async def _release_database(database: CommonDatabase) -> None:
    if os.environ.get("MISSION_CONTROL_E2E_KEEP_DATABASE") == "1":
        print(f"E2E: kept database {database.name} for inspection", flush=True)
        return
    await drop_common_database(database)


@asynccontextmanager
async def open_postgres_production_stack(
    *,
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    technical_override: TechnicalBinding | None = None,
    components: DeploymentCapabilityComponents | None = None,
    model_log: list[dict[str, Any]] | None = None,
    extra_environment: dict[str, str] | None = None,
    legacy_poison: bool = False,
) -> AsyncIterator[ProductionStack]:
    node_executable = shutil.which("node")
    if node_executable is None:
        pytest.fail("pinned technical MCP/browser runtime requires an installed Node executable")
    database, checkpoint_dsn = await _fresh_database(legacy_poison=legacy_poison)
    dsn = database.owner_dsn
    print(f"E2E: fresh common database {database.name} installed", flush=True)
    payload_root = root / "payloads"
    temporal_db = root / "temporal.sqlite"
    environment = {
        "APPLICATION_DATABASE_DIRECT": database.dsn("mission_control_runtime"),
        "APPLICATION_MIGRATION_DATABASE_DIRECT": dsn,
        "APPLICATION_FAMILY_WRITER_DATABASE_DIRECT": database.dsn("mission_control_family_writer"),
        "MISSION_CONTROL_CATALOG_SCOPE": catalog_scope(database.application_id),
        "LANGGRAPH_CHECKPOINT_DATABASE_DIRECT": checkpoint_dsn,
        "LANGGRAPH_CHECKPOINT_SCHEMA": LANGGRAPH_SCHEMA,
        "TEMPORAL_ADDRESS": f"127.0.0.1:{TEMPORAL_PORT}",
        "TEMPORAL_NAMESPACE": "default",
        "TEMPORAL_TASK_QUEUE": TASK_QUEUE,
        "RUN_CONTROL_TEMPORAL_ENABLED": "1",
        "COORDINATOR_LAUNCH_ENABLED": "1",
        "BOUNDARY_RELAY_REQUEST_SCOPES": f'["{SCOPE}"]',
        "BOUNDARY_RELAY_INTERVAL_SECONDS": "1",
        "ARTIFACT_PAYLOAD_ROOT": str(payload_root),
        "CAPABILITY_BUNDLE_BACKEND": "local",
        "DEEP_AGENT_SANDBOX_WORKSPACE_ROOT": str(root / "workspaces"),
        "WEB_RESEARCH_AGENT_BROWSER_NODE": node_executable,
        "OPERATION_JOURNAL_CLAIMED_BY": "operation-runtime:mission-control-parity",
        "ASYNC_SUBAGENT_SUBMITTER_IDENTITY": "mission-control-parity",
        "LANGSMITH_TRACING": "false",
        "LANGCHAIN_TRACING_V2": "false",
        "OPENAI_API_KEY": "deterministic-local-model-no-provider-access",
        "S3_BUCKET": "",
    }
    monkeypatch.delenv("LANGGRAPH_CHECKPOINT_SETUP", raising=False)
    for key, value in {**environment, **(extra_environment or {})}.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    settings = get_settings()
    technical = technical_override or technical_binding()
    model_log = model_log if model_log is not None else []
    owner_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    try:
        env = await start_local(temporal_db)
    except BaseException:
        await owner_pool.close()
        await _release_database(database)
        get_settings.cache_clear()
        raise
    print("E2E: local Temporal server connected", flush=True)
    _reset_api_state()
    api.state.admission_policy_registry = technical_admission_policies()
    api.dependency_overrides[get_control_plane_principal] = lambda: PRINCIPAL
    production = None
    try:
        async with AsyncExitStack() as resources:
            await initialize_run_control_resources(api)
            print("E2E: API PostgreSQL identities verified", flush=True)
            await register_belllabs_search_attributes(env.client, settings.temporal_namespace)
            print("E2E: Temporal search attributes registered", flush=True)
            await compose_runtime_control(
                api,
                settings,
                client=env.client,
                stack=resources,
                fork_patch_policies=ForkPatchPolicyRegistry(),
            )
            print("E2E: API runtime services composed", flush=True)
            worker_pool = await create_application_postgres_pool(settings)
            resources.push_async_callback(worker_pool.close)
            writer_pool = await create_application_family_writer_pool(settings)
            resources.push_async_callback(writer_pool.close)
            control_plane = ControlPlaneService(
                PostgresDefinitionRepository(
                    worker_pool, catalog_scope=settings.mission_control_catalog_scope or ""
                ),
                ExtensionRegistry(),
                UnavailablePayloadStore(),
                externalize_above_bytes=15_000_000,
            )
            run_control = compose_worker_run_control_service(
                PostgresRunControlRepository(worker_pool, family_writer_pool=writer_pool),
                F1RunConfigurationVerifier(control_plane),
                technical_admission_policies(),
            )
            factory = ProductionWorkerActivityCompositionFactory(
                env.client,
                additional_components=components or technical.components(model_log),
                artifact_validation=StaticArtifactValidationAuthority(
                    permission_outcomes={
                        ("operation:sandbox-agent@1", "permission:rrm009"): "allowed"
                    },
                    check_outcomes={},
                    required_check_ids={},
                ),
                worker_identity=f"mission-control-parity:{os.getpid()}",
                claim_lease=timedelta(seconds=90),
            )
            composition = await factory.build(
                settings=settings,
                control_plane=control_plane,
                run_control=run_control,
                postgres_pool=worker_pool,
            )
            print("E2E: production activity factory composed", flush=True)
            assert composition.resources is not None
            resources.push_async_callback(composition.resources.aclose)
            workers = create_production_workers(env.client, settings, composition)
            http = await resources.enter_async_context(
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=api), base_url="http://mission-control"
                )
            )
            production = ProductionStack(
                settings=settings,
                env=env,
                client=env.client,
                owner_pool=owner_pool,
                worker_pool=worker_pool,
                control_plane=control_plane,
                technical=technical,
                composition=composition,
                factory=factory,
                http=http,
                payload_root=payload_root,
                temporal_db=temporal_db,
                model_log=model_log,
                worker_queues=tuple(worker.task_queue for worker in workers.workers),
                database=database,
            )
            async with production.worker_stack:
                for worker in workers.workers:
                    await production.worker_stack.enter_async_context(worker)
                print("E2E: production workers polling", flush=True)
                yield production
    finally:
        await close_run_control_resources(api)
        api.dependency_overrides.pop(get_control_plane_principal, None)
        _reset_api_state()
        await owner_pool.close()
        await (production.env if production is not None else env).shutdown()
        get_settings.cache_clear()
        await _release_database(database)
