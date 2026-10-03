"""Signed scoped HTTP -> mc.* Temporal -> real PostgreSQL/DeepAgents local proof.

The deterministic model is registered through the production capability hook. No
auth override, memory repository, model-provider network call, or prerequisite skip.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg
import httpx
import pytest
from joserfc import jwt
from joserfc.jwk import RSAKey
from temporalio.client import Client
from temporalio.worker import Replayer

from mission_control.adapters.auth.jwt import ActorGrant, ApplicationAuthentication
from mission_control.adapters.postgres.orchestration.goal_directed_repository import (
    PostgresGoalDirectedDocumentRepository,
)
from mission_control.adapters.postgres.orchestration.stagegraph_repository import (
    PostgresStageGraphOperationTemplateRepository,
)
from mission_control.adapters.storage.control_plane_payloads import UnavailablePayloadStore
from mission_control.adapters.temporal.coordinator_runtime import coordinator_task_queues
from mission_control.adapters.temporal.deployment_composition import (
    ProductionWorkerActivityCompositionFactory,
)
from mission_control.adapters.temporal.search_attributes import register_belllabs_search_attributes
from mission_control.adapters.temporal.worker import create_production_workers
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.goal_directed import GoalDirectedWorkflow
from mission_control.adapters.temporal.workflows.mission_run import MissionRunWorkflow
from mission_control.adapters.temporal.workflows.operation import MissionOperationWorkflow
from mission_control.adapters.temporal.workflows.stagegraph import StageGraphWorkflow
from mission_control.application.execution.service import FamilyAdmissionRegistry
from mission_control.application.installations.registry import ApplicationBinding
from mission_control.application.programs.goal_directed import (
    configure_goal_directed_family_admissions,
)
from mission_control.application.programs.service import register_stagegraph_family_mutations
from mission_control.bootstrap.api import (
    ApplicationDeployment,
    MissionDeployment,
    RuntimeOptions,
    TemporalDeployment,
    create_application,
)
from mission_control.bootstrap.composition import MissionApplicationServices
from mission_control.bootstrap.installation import register_identity
from mission_control.bootstrap.settings import get_settings
from mission_control.contracts.identities import mission_root_id
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.interfaces.http.run_control import ROLE_PERMISSIONS
from tests.fixtures import rrm009_production_stack as technical
from tests.fixtures.mission_control_production_stack import _fresh_database, start_local

PORT = 7343


@dataclass
class ScopedStack:
    http: httpx.AsyncClient
    client: Client
    services: MissionApplicationServices
    pool: asyncpg.Pool
    binding: technical.TechnicalBinding
    model_log: list[dict[str, Any]]
    scope: str


@asynccontextmanager
async def scoped_stack(root: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[ScopedStack]:
    source = os.environ.get("MISSION_CONTROL_E2E_POSTGRES_DSN")
    if not source:
        pytest.fail("MISSION_CONTROL_E2E_POSTGRES_DSN must name an authorized disposable local DB")
    node = shutil.which("node")
    if node is None:
        pytest.fail("the pinned deterministic MCP/browser component requires local Node")
    owner_dsn, runtime_dsn, writer_dsn = await _fresh_database(source)
    installation, tenant = uuid4(), uuid4()
    binding = ApplicationBinding.seal(
        application_id="biotech",
        installation_id=installation,
        binding_version="1",
        supabase_project_ref="local-authenticated-runtime-proof",
        database_secret_ref="MC_SCOPED_RUNTIME_DSN",
        accepted_issuers={"https://local-qualification.invalid"},
        accepted_audiences={"mission-control"},
        required_component_version="transitional-local-v1",
    )
    scope = f"mc/{installation}/biotech/{tenant}"
    monkeypatch.setattr(technical, "SCOPE", scope)
    environment = {
        "MC_SCOPED_RUNTIME_DSN": runtime_dsn,
        "MC_SCOPED_WRITER_DSN": writer_dsn,
        "APPLICATION_DATABASE_DIRECT": runtime_dsn,
        "APPLICATION_MIGRATION_DATABASE_DIRECT": owner_dsn,
        "APPLICATION_FAMILY_WRITER_DATABASE_DIRECT": writer_dsn,
        "MISSION_CONTROL_CATALOG_SCOPE": f"mc/{installation}/biotech/catalog",
        "LANGGRAPH_CHECKPOINT_DATABASE_DIRECT": owner_dsn + "?connect_timeout=10",
        "LANGGRAPH_CHECKPOINT_SCHEMA": technical.LANGGRAPH_SCHEMA,
        "LANGGRAPH_CHECKPOINT_SETUP": "1",
        "TEMPORAL_ADDRESS": f"127.0.0.1:{PORT}",
        "TEMPORAL_NAMESPACE": "default",
        "TEMPORAL_TASK_QUEUE": technical.TASK_QUEUE,
        "RUN_CONTROL_TEMPORAL_ENABLED": "1",
        "COORDINATOR_LAUNCH_ENABLED": "1",
        "BOUNDARY_RELAY_REQUEST_SCOPES": json.dumps([scope]),
        "ARTIFACT_PAYLOAD_ROOT": str(root / "payloads"),
        "CAPABILITY_BUNDLE_BACKEND": "local",
        "DEEP_AGENT_SANDBOX_WORKSPACE_ROOT": str(root / "workspaces"),
        "WEB_RESEARCH_AGENT_BROWSER_NODE": node,
        "OPERATION_JOURNAL_CLAIMED_BY": "operation-runtime:scoped-proof",
        "ASYNC_SUBAGENT_SUBMITTER_IDENTITY": "scoped-proof",
        "LANGSMITH_TRACING": "false",
        "LANGCHAIN_TRACING_V2": "false",
        "OPENAI_API_KEY": "deterministic-local-no-provider-access",
        "S3_BUCKET": "",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    settings = get_settings()
    owner = await asyncpg.connect(owner_dsn)
    try:
        database = await owner.fetchval("SELECT current_database()")
    finally:
        await owner.close()
    await register_identity(binding, owner_dsn=owner_dsn, expected_database=database)
    key = RSAKey.generate_key(2048, parameters={"kid": "scoped-proof"})
    public_key_file = root / "public-jwks.json"
    public_key_file.write_text(json.dumps({"keys": [key.as_dict(private=False)]}))
    authentication = ApplicationAuthentication(
        binding=binding,
        issuer="https://local-qualification.invalid",
        audience="mission-control",
        public_jwks_file=public_key_file,
        grants=(
            ActorGrant(
                subject="qualified-operator",
                actor_id=technical.OPERATOR,
                tenant_ids={tenant},
                permissions=ROLE_PERMISSIONS["operator"] | ROLE_PERMISSIONS["fork_operator"],
                authority_refs={"authority:lifecycle"},
                sponsorship_refs={"sponsorship:test"},
                approval_refs={"approval:test"},
            ),
        ),
    )
    queues = coordinator_task_queues(settings.temporal_task_queue)
    deployment = MissionDeployment(
        storage_mode="transitional_local",
        max_request_bytes=1_000_000,
        applications=(
            ApplicationDeployment(
                authentication=authentication,
                family_writer_secret_ref="MC_SCOPED_WRITER_DSN",
                temporal=TemporalDeployment(
                    address=settings.temporal_address,
                    namespace=settings.temporal_namespace,
                    root_task_queue=queues.stagegraph,
                    stagegraph_task_queue=queues.stagegraph,
                    goal_directed_task_queue=queues.goal_directed,
                ),
            ),
        ),
    )
    family_admissions = FamilyAdmissionRegistry()
    configure_goal_directed_family_admissions(family_admissions)
    register_stagegraph_family_mutations(family_admissions)
    application = create_application(
        deployment,
        runtime_options={
            "biotech": RuntimeOptions(
                technical.technical_admission_policies(),
                ExtensionRegistry(),
                UnavailablePayloadStore(),
                family_admissions=family_admissions,
            )
        },
    )
    env = await start_local(root / "temporal.sqlite", port=PORT)
    print("SCOPED E2E: disposable PostgreSQL and cached Temporal ready", flush=True)
    try:
        await register_belllabs_search_attributes(env.client, settings.temporal_namespace)
        async with AsyncExitStack() as resources:
            await resources.enter_async_context(application.router.lifespan_context(application))
            services = application.state.mission_control_compositions[
                (installation, "biotech", tenant)
            ]
            pool = await asyncpg.create_pool(runtime_dsn, min_size=1, max_size=5)
            resources.push_async_callback(pool.close)
            model_log: list[dict[str, Any]] = []
            binding_assets = technical.technical_binding()
            factory = ProductionWorkerActivityCompositionFactory(
                env.client,
                additional_components=binding_assets.components(model_log),
                worker_identity=f"mission-control-scoped-proof:{os.getpid()}",
                claim_lease=timedelta(seconds=90),
            )
            composition = await factory.build(
                settings=settings,
                control_plane=services.control_plane,
                run_control=services.run_control,
                postgres_pool=pool,
            )
            assert composition.resources is not None
            resources.push_async_callback(composition.resources.aclose)
            workers = create_production_workers(env.client, settings, composition)
            for worker in workers.workers:
                await resources.enter_async_context(worker)
            token = jwt.encode(
                {"alg": "RS256", "kid": "scoped-proof"},
                {
                    "iss": authentication.issuer,
                    "aud": authentication.audience,
                    "sub": "qualified-operator",
                    "exp": int(time.time()) + 1800,
                    "app_metadata": {"application_id": "biotech", "tenant_id": str(tenant)},
                },
                key,
            )
            http = await resources.enter_async_context(
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=application),
                    base_url="http://scoped-proof",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=45,
                )
            )
            print("SCOPED E2E: signed API and actual production workers started", flush=True)
            yield ScopedStack(http, env.client, services, pool, binding_assets, model_log, scope)
    finally:
        await env.shutdown()
        get_settings.cache_clear()


async def until(predicate: Callable[[], Awaitable[bool]], seconds: float = 180) -> None:
    async with asyncio.timeout(seconds):
        for _ in range(int(seconds * 4)):
            if await predicate():
                return
            await asyncio.sleep(0.25)
    raise AssertionError("runtime condition did not become true")


async def inspect(stack: ScopedStack, run_id: str) -> dict[str, Any]:
    response = await stack.http.get(f"/v1/applications/biotech/runs/{run_id}/inspection")
    assert response.status_code == 200, response.text
    return response.json()


async def condition(stack: ScopedStack, run_id: str, field: str, expected: Any) -> bool:
    return (await inspect(stack, run_id))[field] == expected


async def at_wait(stack: ScopedStack, run_id: str) -> bool:
    state = await inspect(stack, run_id)
    if state["lifecycle"] == "completed":
        raise AssertionError(f"run terminated before its declared wait: {state}")
    return bool(state["projection"]["active_waits"])


async def command(stack: ScopedStack, run_id: str, kind: str, payload: dict[str, Any]) -> str:
    state = await inspect(stack, run_id)
    request_id = str(uuid4())
    body = {
        "request_id": request_id,
        "expected_version": state["version"],
        "expected_generation": state["execution_generation"],
        "target": {"kind": "run", "id": run_id},
        "kind": kind,
        "payload": payload,
        "reason": "deterministic scoped HTTP qualification",
    }
    response = await stack.http.post(f"/v1/applications/biotech/runs/{run_id}/commands", json=body)
    assert response.status_code == 202, response.text
    assert response.json()["admission"]["status"] == "accepted", response.text

    async def applied() -> bool:
        result = await stack.http.get(f"/v1/applications/biotech/runs/{run_id}/commands")
        assert result.status_code == 200, result.text
        match = next(
            item
            for item in result.json()["commands"]
            if item["command"]["command_id"] == request_id
        )
        return [item["state"] for item in match["receipts"]][-1:] == ["applied"]

    await until(applied)
    replay = await stack.http.post(f"/v1/applications/biotech/runs/{run_id}/commands", json=body)
    assert replay.status_code == 200 and replay.json()["replay"] is True, replay.text
    return request_id


async def admit_and_launch(stack: ScopedStack, family: str) -> tuple[str, str]:
    catalog = await technical.publish_technical_catalog(
        stack.services.control_plane,
        family=family,
        now=datetime.now(UTC),
    )
    request = technical.admission_request(catalog, str(uuid4())).model_dump(mode="json")
    for field in (
        "schema_version",
        "request_scope",
        "idempotency_issuer",
        "actor",
        "requested_at",
        "correlation_id",
        "causation_id",
    ):
        request.pop(field, None)
    admission = await stack.http.post("/v1/applications/biotech/run-requests", json=request)
    assert admission.status_code == 201, admission.text
    run_id = admission.json()["admission"]["run_id"]
    binding_ref = f"semantic-input:scoped-proof:{run_id}"
    if family == "StageGraph":
        await PostgresStageGraphOperationTemplateRepository(stack.pool).persist_templates(
            request_scope=stack.scope,
            semantic_input_binding_ref=binding_ref,
            templates={
                name: value.model_copy(update={"request_scope": stack.scope})
                for name, value in technical.stage_templates(stack.binding, catalog).items()
            },
            recorded_at=datetime.now(UTC),
        )
        family_input = {
            "stagegraph": asdict(technical.stage_input(catalog, run_id, binding_ref, 1))
        }
    else:
        templates = {
            name: value.model_copy(update={"request_scope": stack.scope})
            for name, value in technical.goal_templates(stack.binding, catalog).items()
        }
        await PostgresGoalDirectedDocumentRepository(stack.pool).persist_templates(
            request_scope=stack.scope,
            semantic_input_binding_ref=binding_ref,
            executor=templates["executor"],
            verifier=templates["verifier"],
            recorded_at=datetime.now(UTC),
        )
        family_input = {
            "goal_directed": asdict(
                technical.goal_input(
                    catalog,
                    run_id,
                    binding_ref,
                    1,
                    "Produce one independently verified technical record.",
                )
            )
        }
    launched = await stack.http.post(
        f"/v1/applications/biotech/runs/{run_id}/launch",
        json={"family": family, **family_input},
    )
    assert launched.status_code == 202, launched.text
    workflow_id = launched.json()["workflow_id"]
    assert workflow_id == mission_root_id(stack.scope, run_id)
    print(f"SCOPED E2E: admitted and launched {family}", flush=True)
    return run_id, workflow_id


@pytest.mark.asyncio
async def test_signed_scoped_stagegraph_goal_and_lifecycle_reach_mc_operations(
    tmp_path, monkeypatch
):
    async with scoped_stack(tmp_path, monkeypatch) as stack:
        unauthenticated = await stack.http.get(
            "/v1/applications/biotech/runs/absent/inspection",
            headers={"Authorization": ""},
        )
        assert unauthenticated.status_code == 401
        other = await stack.http.get("/v1/applications/other/runs/absent/inspection")
        assert other.status_code == 403
        stage_run, stage_root = await admit_and_launch(stack, "StageGraph")
        await until(lambda: at_wait(stack, stage_run))
        pause_decision = str(uuid4())
        await command(
            stack,
            stage_run,
            "pause",
            {
                "decision": {
                    "decision_id": pause_decision,
                    "scope": ["run"],
                    "reason": "hold at declared boundary",
                    "authority_ref": "authority:lifecycle",
                },
                "runnable_work_remains": False,
            },
        )
        assert (await inspect(stack, stage_run))["lifecycle"] == "paused"
        await command(
            stack,
            stage_run,
            "resume",
            {
                "decision": {
                    "decision_id": str(uuid4()),
                    "pause_decision_id": pause_decision,
                    "reason": "release operator hold",
                    "authority_ref": "authority:lifecycle",
                },
                "runnable_work_remains": False,
            },
        )
        state = await inspect(stack, stage_run)
        wait = state["projection"]["active_waits"][0]
        await command(
            stack,
            stage_run,
            "satisfy_wait",
            {
                "condition_id": wait["condition_id"],
                "verification_evidence_ref": "evidence:scoped-review",
                "runnable_work_remains": True,
            },
        )
        await until(lambda: condition(stack, stage_run, "lifecycle", "completed"))
        stage_terminal = await inspect(stack, stage_run)
        assert stage_terminal["execution_outcome"] == "completed", stage_terminal
        settlements = stage_terminal["projection"]["accepted_operation_settlement_evidence"]
        # Each stage records its operation journal settlement and family result acceptance.
        assert len(settlements) == 4
        assert (
            sum(item["settlement_id"].startswith("stagegraph-result:") for item in settlements) == 2
        )
        assert len(stage_terminal["projection"]["accepted_output_evidence"]) == 2

        goal_run, goal_root = await admit_and_launch(stack, "GoalDirected")
        await until(lambda: condition(stack, goal_run, "lifecycle", "completed"), seconds=300)
        goal_terminal = await inspect(stack, goal_run)
        assert goal_terminal["execution_outcome"] == "completed", goal_terminal
        assert len(goal_terminal["projection"]["accepted_operation_settlement_evidence"]) >= 4

        replayer = Replayer(
            workflows=[
                MissionRunWorkflow,
                MissionOperationWorkflow,
                StageGraphWorkflow,
                GoalDirectedWorkflow,
            ],
            workflow_runner=coordinator_workflow_runner(),
        )
        evidence: list[dict[str, Any]] = []
        for run_id, root_id in ((stage_run, stage_root), (goal_run, goal_root)):
            root_history = await stack.client.get_workflow_handle(root_id).fetch_history()
            root_start = root_history.events[0].workflow_execution_started_event_attributes
            assert root_start.workflow_type.name == "mc.mission_run.v1"
            await replayer.replay_workflow(root_history)
            operation_ids: list[str] = []
            async for execution in stack.client.list_workflows(f"BellLabsRunId = '{run_id}'"):
                if execution.workflow_type != "mc.operation.v1":
                    continue
                assert execution.id.startswith(root_id + "/operation/")
                operation_ids.append(execution.id)
                history = await stack.client.get_workflow_handle(execution.id).fetch_history()
                assert (
                    history.events[0].workflow_execution_started_event_attributes.workflow_type.name
                    == "mc.operation.v1"
                )
                await replayer.replay_workflow(history)
            assert len(operation_ids) >= (2 if run_id == stage_run else 4), operation_ids
            evidence.append(
                {
                    "run_id": run_id,
                    "root_type": "mc.mission_run.v1",
                    "operation_type": "mc.operation.v1",
                    "operations": len(operation_ids),
                }
            )
        assert stack.model_log, "actual deterministic DeepAgents model must have been invoked"
        print(
            "SCOPED MISSION CONTROL EVIDENCE:",
            json.dumps(
                {
                    "runs": evidence,
                    "model_events": len(stack.model_log),
                    "authenticated": True,
                    "lifecycle_controls": ["pause", "resume", "satisfy_wait"],
                    "replayed": True,
                    "production_ready": False,
                }
            ),
            flush=True,
        )
