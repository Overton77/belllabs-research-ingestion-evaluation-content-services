"""Disposable PostgreSQL-only production API/worker stack; deterministic local cognition.

An explicitly supplied loopback owner DSN authorizes creation of a fresh test database.
No existing schema is reset. Databases remain available for failure inspection.
Missing prerequisites fail this proof instead of converting it to a successful skip.
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
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import httpx
import pytest
from temporalio.testing import WorkflowEnvironment

from mission_control.adapters.postgres.connections import (
    apply_application_migrations,
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


async def _fresh_database(owner_dsn: str) -> tuple[str, str, str]:
    parsed = urlsplit(owner_dsn)
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("runtime parity requires an explicitly disposable loopback PostgreSQL")
    if parsed.hostname == "localhost":
        # Docker Desktop's IPv6 localhost can accept then stall libpq negotiation;
        # select the same loopback container explicitly instead of waiting indefinitely.
        authority = parsed.netloc.rsplit("@", 1)[0]
        parsed = parsed._replace(netloc=f"{authority}@127.0.0.1:{parsed.port or 5432}")
    identity = uuid4().hex[:12]
    database = f"mission_control_e2e_{identity}"
    connection = await asyncpg.connect(owner_dsn)
    try:
        await connection.execute(f'CREATE DATABASE "{database}"')
    finally:
        await connection.close()
    dsn = urlunsplit(parsed._replace(path="/" + database))
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    runtime_login = f"mc_e2e_runtime_{identity}"
    writer_login = f"mc_e2e_writer_{identity}"
    try:
        await apply_application_migrations(pool)
        async with pool.acquire() as connection:
            await connection.execute(
                f'CREATE ROLE "{runtime_login}" LOGIN NOSUPERUSER NOBYPASSRLS '
                "IN ROLE belllabs_control_runtime"
            )
            await connection.execute(
                f'CREATE ROLE "{writer_login}" LOGIN NOSUPERUSER NOBYPASSRLS '
                "IN ROLE belllabs_family_repository_writer"
            )
            await connection.execute(f'CREATE SCHEMA "{LANGGRAPH_SCHEMA}"')
    finally:
        await pool.close()
    address = parsed.netloc.split("@", 1)[-1]
    return (
        dsn,
        f"postgresql://{runtime_login}@{address}/{database}",
        (f"postgresql://{writer_login}@{address}/{database}"),
    )


@asynccontextmanager
async def open_postgres_production_stack(
    *,
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    technical_override: TechnicalBinding | None = None,
    components: DeploymentCapabilityComponents | None = None,
    model_log: list[dict[str, Any]] | None = None,
    extra_environment: dict[str, str] | None = None,
) -> AsyncIterator[ProductionStack]:
    configured = os.environ.get("MISSION_CONTROL_E2E_POSTGRES_DSN")
    if not configured:
        pytest.fail("set MISSION_CONTROL_E2E_POSTGRES_DSN to a disposable loopback owner DSN")
    node_executable = shutil.which("node")
    if node_executable is None:
        pytest.fail("pinned technical MCP/browser runtime requires an installed Node executable")
    dsn, runtime_dsn, writer_dsn = await _fresh_database(configured)
    print("E2E: fresh PostgreSQL database migrated", flush=True)
    payload_root = root / "payloads"
    temporal_db = root / "temporal.sqlite"
    environment = {
        "APPLICATION_DATABASE_DIRECT": runtime_dsn,
        "APPLICATION_MIGRATION_DATABASE_DIRECT": dsn,
        "APPLICATION_FAMILY_WRITER_DATABASE_DIRECT": writer_dsn,
        "MISSION_CONTROL_CATALOG_SCOPE": "mission-control-e2e-catalog",
        "LANGGRAPH_CHECKPOINT_DATABASE_DIRECT": dsn
        + ("&" if "?" in dsn else "?")
        + "connect_timeout=10",
        "LANGGRAPH_CHECKPOINT_SCHEMA": LANGGRAPH_SCHEMA,
        "LANGGRAPH_CHECKPOINT_SETUP": "1",
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
