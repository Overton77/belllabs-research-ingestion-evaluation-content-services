"""RRM-009 production stack harness: the API, workers, Temporal dev server and facade helpers.

The BellLabs API (`app.server.api`) is composed exactly as a deployment composes it and the
workers are the deployment factory's, over the disposable application PostgreSQL, MongoDB and a
persistent Temporal dev server. The RRM-009 qualifications (composition, live capabilities,
object store, cancellation drill) share this harness; the technical models, catalog and
templates it runs are in `tests.fixtures.rrm009_production_stack`.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import asyncpg
import httpx
import pytest
from pymongo import AsyncMongoClient
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer

from app.api.control_plane import ControlPlanePrincipal, get_control_plane_principal
from app.api.run_control import (
    close_run_control_resources,
    initialize_run_control_resources,
)
from app.api.runtime_composition import compose_runtime_control
from app.application.control_plane.control_plane_repository import BeanieDefinitionRepository
from app.application.control_plane.service import ControlPlaneService
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.application.run_control.service import F1RunConfigurationVerifier
from app.application.runtime.run_forks import ForkPatchPolicyRegistry
from app.application.workspaces.artifact_promotion import (
    ArtifactPayloadAddress,
    StaticArtifactValidationAuthority,
)
from app.config import Settings, get_settings
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.operation_execution.contracts import (
    ArtifactPromotionPlan,
    GenericArtifactWorkflowRequest,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    WorkspaceOwner,
    WorkspaceOwnerKind,
)
from app.domain.run_control.contracts import (
    ActorContext,
    LifecycleCommand,
    ReserveBudgetAction,
    SatisfyWaitAction,
    StartAction,
)
from app.integrations.control_plane_payloads import UnavailablePayloadStore
from app.integrations.mongodb import create_mongodb
from app.integrations.operation_runtime_ports import FilesystemArtifactPayloadStore
from app.integrations.postgres import (
    create_application_family_writer_pool,
    create_application_postgres_pool,
)
from app.server import api
from app.temporal.deployment_composition import (
    DeploymentCapabilityComponents,
    ProductionWorkerActivityCompositionFactory,
)
from app.temporal.search_attributes import (
    BELLLABS_SEARCH_ATTRIBUTE_KEYS,
    SearchAttributeRegistrationError,
    register_belllabs_search_attributes,
)
from app.temporal.worker import (
    WorkerActivityComposition,
    compose_worker_run_control_service,
    create_production_workers,
)
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.operation import OperationWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow, wait_condition_id
from tests.fixtures.checkpoint_lineage import bind_unit, stage_unit
from tests.fixtures.rrm009_production_stack import (
    LANGGRAPH_SCHEMA,
    NODE_EXECUTABLE,
    OPERATOR,
    REPORT_PATH,
    SCOPE,
    TASK_QUEUE,
    WAIT_ID,
    TechnicalBinding,
    TechnicalCatalog,
    admission_request,
    prepare_disposable_identities,
    publish_technical_catalog,
    runtime_environment,
    stage_templates,
    technical_admission_policies,
)
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)

TEMPORAL_PORT = 7341
PRINCIPAL = ControlPlanePrincipal(
    actor_id=OPERATOR,
    roles=frozenset({"operator", "fork_operator", "relay", "auditor", "state_inspector"}),
    tenant_scopes=frozenset({SCOPE}),
    authority_refs=frozenset({"authority:lifecycle"}),
    sponsorship_refs=frozenset({"sponsorship:test"}),
    approval_refs=frozenset({"approval:test"}),
)
STAGE_OBJECTIVE = "Review the draft against the stricter technical checklist."
API_STATE_ATTRIBUTES = (
    "run_control_service",
    "boundary_intervention_service",
    "unit_reconciliation_service",
    "runtime_inspection_service",
    "run_fork_services",
    "admission_policy_registry",
    "run_control_family_admission_registry",
    "control_plane_service",
    "control_plane_mongodb_client",
    "run_launch_service",
    "generic_artifact_submitter",
    "runtime_control",
    "boundary_command_relay",
    "workflow_submitter",
    "fork_patch_policies",
    "temporal_client",
    "temporal_visibility_reader",
    "inspection_checkpoint_reader",
    "inspection_async_child_details",
    "inspection_cursor_key",
    "boundary_command_transport",
    "unit_reconciliation_nudge",
    "unit_reconciliation_verifier",
    "family_liability_hints",
    "async_child_usage_reconciliation",
)


@dataclass
class ProductionStack:
    settings: Settings
    env: WorkflowEnvironment
    client: Client
    owner_pool: asyncpg.Pool
    worker_pool: asyncpg.Pool
    control_plane: ControlPlaneService
    technical: TechnicalBinding
    composition: WorkerActivityComposition
    factory: ProductionWorkerActivityCompositionFactory
    http: httpx.AsyncClient
    payload_root: Path
    temporal_db: Path
    model_log: list[dict[str, Any]] = field(default_factory=list)
    worker_stack: AsyncExitStack = field(default_factory=AsyncExitStack)
    worker_queues: tuple[str, ...] = ()

    async def restart_temporal(self) -> None:
        """Stop the dev server and start it again on the same port and database file."""

        await self.env.shutdown()
        self.env = await _start_local(self.temporal_db)


async def _start_local(database: Path) -> WorkflowEnvironment:
    try:
        # No Search Attributes at start: the deployment's administrative step registers them
        # (REQ-CP-EXEC-015), and the database file keeps them across the restart drill.
        return await WorkflowEnvironment.start_local(
            port=TEMPORAL_PORT,
            dev_server_extra_args=["--db-filename", str(database)],
            dev_server_log_level="error",
        )
    except RuntimeError as error:
        pytest.skip(f"Temporal dev server is unavailable: {error}")


def _reset_api_state() -> None:
    for name in API_STATE_ATTRIBUTES:
        if hasattr(api.state, name):
            delattr(api.state, name)


@pytest.fixture
async def mongo_database(test_mongodb_uri: str) -> AsyncIterator[str]:
    name = f"rrm009_{uuid4().hex[:12]}"
    yield name
    client: AsyncMongoClient[Any] = AsyncMongoClient(test_mongodb_uri)
    try:
        await client.drop_database(name)
    finally:
        await client.close()


@asynccontextmanager
async def open_production_stack(
    *,
    dsn: str,
    mongo_uri: str,
    mongo_database: str,
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    technical: TechnicalBinding,
    components: DeploymentCapabilityComponents | None,
    model_log: list[dict[str, Any]],
    extra_environment: dict[str, str] | None = None,
) -> AsyncIterator[ProductionStack]:
    """The deployment's API and workers over the disposable stores and a persistent namespace.

    `components` are the exact components the deployment registers beside its pins (the
    deterministic qualification models); `None` serves the pinned catalog only (live).
    """

    require_disposable_postgres(dsn)
    if not NODE_EXECUTABLE.exists():
        pytest.skip(f"pinned agent-browser tool requires node at {NODE_EXECUTABLE}")
    payload_root = root / "payloads"
    workspace_root = root / "workspaces"
    temporal_db = root / "temporal.sqlite"
    environment = runtime_environment(
        owner_dsn=dsn,
        mongo_uri=mongo_uri,
        mongo_database=mongo_database,
        temporal_address=f"127.0.0.1:{TEMPORAL_PORT}",
        task_queue=TASK_QUEUE,
        payload_root=payload_root,
        workspace_root=workspace_root,
    )
    for name, value in {**environment, **(extra_environment or {})}.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    settings = get_settings()
    owner_pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=3)
    await reset_application_schema(owner_pool)
    await prepare_disposable_identities(dsn)
    mongo_client, _database = await create_mongodb(settings)
    env = await _start_local(temporal_db)
    _reset_api_state()
    api.state.admission_policy_registry = technical_admission_policies()
    api.dependency_overrides[get_control_plane_principal] = lambda: PRINCIPAL
    production: ProductionStack | None = None
    try:
        async with AsyncExitStack() as resources:
            await initialize_run_control_resources(api)
            policies = ForkPatchPolicyRegistry()
            # Readiness only verifies: before the administrative registration the API refuses to
            # compose, and registration is idempotent.
            with pytest.raises(SearchAttributeRegistrationError):
                await compose_runtime_control(
                    api, settings, client=env.client, stack=resources, fork_patch_policies=policies
                )
            assert set(
                await register_belllabs_search_attributes(env.client, settings.temporal_namespace)
            ) == {key.name for key in BELLLABS_SEARCH_ATTRIBUTE_KEYS}
            assert (
                await register_belllabs_search_attributes(env.client, settings.temporal_namespace)
                == ()
            )
            await compose_runtime_control(
                api, settings, client=env.client, stack=resources, fork_patch_policies=policies
            )
            worker_pool = await create_application_postgres_pool(settings)
            writer_pool = await create_application_family_writer_pool(settings)
            control_plane = ControlPlaneService(
                BeanieDefinitionRepository(),
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
                additional_components=components,
                artifact_validation=StaticArtifactValidationAuthority(
                    permission_outcomes={
                        ("operation:sandbox-agent@1", "permission:rrm009"): "allowed"
                    },
                    check_outcomes={},
                    required_check_ids={},
                ),
                worker_identity=f"rrm009-worker:{os.getpid()}",
                claim_lease=timedelta(seconds=90),
            )
            composition = await factory.build(
                settings=settings,
                control_plane=control_plane,
                run_control=run_control,
                postgres_pool=worker_pool,
            )
            assert composition.resources is not None
            resources.push_async_callback(composition.resources.aclose)
            workers = create_production_workers(env.client, settings, composition)
            http = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api), base_url="http://belllabs"
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
            try:
                async with production.worker_stack:
                    for worker in workers.workers:
                        await production.worker_stack.enter_async_context(worker)
                    yield production
            finally:
                await http.aclose()
                await writer_pool.close()
                await worker_pool.close()
    finally:
        # Always torn down, also when the qualification fails: the dev server, the API
        # state and the pools never leak into the next run.
        await close_run_control_resources(api)
        api.dependency_overrides.pop(get_control_plane_principal, None)
        _reset_api_state()
        await mongo_client.close()
        await owner_pool.close()
        await (production.env if production is not None else env).shutdown()
        get_settings.cache_clear()


# --- Facade helpers --------------------------------------------------------------------------


async def _until(predicate: Callable[[], Awaitable[bool]], seconds: float = 180) -> None:
    async with asyncio.timeout(seconds):
        for _ in range(int(seconds * 4)):
            if await predicate():
                return
            await asyncio.sleep(0.25)
    raise AssertionError("condition not reached")


async def _diagnose(stack: ProductionStack, run_id: str) -> str:
    """Compact history tails of a run's root, family and operations plus the model log
    (printed on failure only, before the dev server is torn down)."""

    lines: list[str] = [f"model_log={json.dumps(stack.model_log)[:2000]}"]
    budget = await stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    lines.append(f"budget={budget.text[:3000]}")
    async for execution in stack.client.list_workflows(f"BellLabsRunId = '{run_id}'"):
        try:
            history = await stack.client.get_workflow_handle(execution.id).fetch_history()
        except Exception as error:  # noqa: BLE001 - diagnostics only
            lines.append(f"{execution.id}: {type(error).__name__}")
            continue
        lines.append(
            f"{execution.id} [{execution.status}]: "
            + ", ".join(str(event.event_type) for event in history.events[-6:])
        )
        for event in history.events:
            if event.event_type == 10:  # ActivityTaskScheduled: the queue the worker must poll
                attributes = event.activity_task_scheduled_event_attributes
                pollers = await stack.client.workflow_service.describe_task_queue(
                    DescribeTaskQueueRequest(
                        namespace=stack.client.namespace,
                        task_queue=TaskQueue(name=attributes.task_queue.name),
                        task_queue_type=TaskQueueType.Value("TASK_QUEUE_TYPE_ACTIVITY"),
                    )
                )
                lines.append(
                    f"scheduled {attributes.activity_type.name} on {attributes.task_queue.name} "
                    f"(activity pollers: {len(pollers.pollers)})"
                )
            if event.event_type in {3, 14, 24, 26, 42}:  # failed runs, activities and tasks
                lines.append(str(event)[:900])
    return " | ".join(lines)


async def _wait_for(
    stack: ProductionStack, run_id: str, predicate: Callable[[], Awaitable[bool]], seconds: float
) -> None:
    try:
        await _until(predicate, seconds)
    except (TimeoutError, AssertionError) as error:
        raise AssertionError(f"{error}: {await _diagnose(stack, run_id)}") from error


async def _admit(stack: ProductionStack, catalog: TechnicalCatalog, request_id: str) -> str:
    response = await stack.http.post(
        "/run-control/v1/run-requests",
        json=admission_request(catalog, request_id).model_dump(mode="json"),
    )
    assert response.status_code == 201, response.text
    assert response.json()["status"] == "accepted", response.text
    return cast(str, response.json()["run_id"])


async def _launch(stack: ProductionStack, run_id: str, body: dict[str, Any]) -> dict[str, Any]:
    response = await stack.http.post(f"/run-control/v1/runs/{run_id}/launch", json=body)
    assert response.status_code == 202, response.text
    return cast(dict[str, Any], response.json())


async def _run(stack: ProductionStack, run_id: str) -> dict[str, Any]:
    response = await stack.http.get(
        f"/run-control/v1/runs/{run_id}", params={"request_scope": SCOPE}
    )
    assert response.status_code == 200, response.text
    return cast(dict[str, Any], response.json())


async def _holds_wait(stack: ProductionStack, run_id: str) -> bool:
    run = await _run(stack, run_id)
    return any(item["condition_id"] == wait_condition_id(WAIT_ID) for item in run["active_waits"])


async def _terminal(stack: ProductionStack, run_id: str) -> bool:
    return (await _run(stack, run_id))["phase"] == "terminal"


def _command(run_id: str, version: int, command_id: str, action: Any, permission: str) -> dict:
    return LifecycleCommand(
        command_id=command_id,
        idempotency_issuer=OPERATOR,
        request_scope=SCOPE,
        run_id=run_id,
        expected_run_version=version,
        actor=ActorContext(actor_id=OPERATOR, permissions=frozenset({permission})),
        action=action,
        reason=f"RRM-009 qualification {command_id}",
        occurred_at=datetime.now(UTC),
        correlation_id=f"rrm009:{run_id}",
    ).model_dump(mode="json")


async def _send(stack: ProductionStack, run_id: str, command: dict[str, Any]) -> dict[str, Any]:
    response = await stack.http.post(f"/run-control/v1/runs/{run_id}/commands", json=command)
    assert response.status_code == 200, response.text
    return cast(dict[str, Any], response.json())


async def _receipt_states(stack: ProductionStack, run_id: str, command_id: str) -> list[str]:
    response = await stack.http.get(
        f"/run-control/v1/runs/{run_id}/boundary-commands", params={"request_scope": SCOPE}
    )
    assert response.status_code == 200, response.text
    for status in response.json():
        if status["command"]["command_id"] == command_id:
            return [receipt["state"] for receipt in status["receipts"]]
    return []


async def _release_wait(stack: ProductionStack, run_id: str) -> None:
    await _until(lambda: _holds_wait(stack, run_id))
    run = await _run(stack, run_id)
    command_id = f"release:{run_id[:8]}"
    result = await _send(
        stack,
        run_id,
        _command(
            run_id,
            run["version"],
            command_id,
            SatisfyWaitAction(
                condition_id=wait_condition_id(WAIT_ID),
                verification_evidence_ref="evidence:rrm009-review",
            ),
            "workflow_run.observe_wait",
        ),
    )
    assert result["reason_code"] == "accepted_pending_application", result

    async def applied() -> bool:
        return (await _receipt_states(stack, run_id, command_id))[-1:] == ["applied"]

    await _until(applied)


async def _visible(client: Client, query: str, expected: int) -> int:
    """The Visibility count once it reaches `expected`, or the last count after 60 s."""

    count = -1
    for _ in range(240):
        count = (await client.count_workflows(query)).count
        if count == expected:
            return count
        await asyncio.sleep(0.25)
    return count


async def _replay(client: Client, workflow_ids: list[str]) -> int:
    replayer = Replayer(
        workflows=[
            BellLabsRunWorkflow,
            StageGraphWorkflow,
            GoalDirectedWorkflow,
            OperationWorkflow,
        ],
        workflow_runner=coordinator_workflow_runner(),
    )
    events = 0
    for workflow_id in workflow_ids:
        history = await client.get_workflow_handle(workflow_id).fetch_history()
        await replayer.replay_workflow(history)
        events += len(history.events)
    return events


async def _operation_payloads(stack: ProductionStack, run_id: str) -> list[dict[str, Any]]:
    """The digest-bound output payloads of the run's journaled settlements.

    Read exactly as the journal restores them: the settlement row's result manifest address,
    then the manifest's output payload address, both verified by the payload store.
    """

    store = FilesystemArtifactPayloadStore(stack.payload_root)
    async with stack.owner_pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT s.result_manifest_ref, s.result_manifest_digest, s.result_manifest_size_bytes
            FROM belllabs_control.operation_settlements s
            JOIN belllabs_control.operation_effect_claims c
              ON c.request_scope = s.request_scope AND c.effect_claim_id = s.effect_claim_id
            WHERE c.belllabs_run_id = $1 AND s.result_manifest_ref IS NOT NULL
            ORDER BY s.settled_at
            """,
            run_id,
        )
    payloads: list[dict[str, Any]] = []
    for row in rows:
        manifest = json.loads(
            await store.retrieve(
                ArtifactPayloadAddress(
                    object_ref=row["result_manifest_ref"],
                    content_digest=row["result_manifest_digest"],
                    size_bytes=row["result_manifest_size_bytes"],
                )
            )
        )
        if manifest.get("output_payload_ref") is None:
            continue
        payloads.append(
            json.loads(
                await store.retrieve(
                    ArtifactPayloadAddress(
                        object_ref=manifest["output_payload_ref"],
                        content_digest=manifest["output_payload_digest"],
                        size_bytes=manifest["output_payload_size_bytes"],
                    )
                )
            )
        )
    return payloads


def _pinned_summary(stack: ProductionStack) -> dict[str, Any]:
    """The worker's mounted pins by digest (the full disclosure is in the factory)."""

    assert stack.factory.operation is not None
    disclosure = stack.factory.operation.capabilities.disclosure()
    return {
        "mcp_servers": {
            item["server_id"]: {
                "module_digest": item["module_digest"],
                "schema_digest": item["schema_digest"],
                "tools": len(item["tools"]),
                "credential_ref": item["credential_ref"],
            }
            for item in cast(list[dict[str, Any]], disclosure["mcp_servers"])
        },
        "skills": {
            item["skill_name"]: item["bundle_digest"]
            for item in cast(list[dict[str, Any]], disclosure["skills"])
        },
        "tools": {
            item["tool_name"]: item["entrypoint_digest"]
            for item in cast(list[dict[str, Any]], disclosure["tools"])
        },
        "checkpointer_digests": disclosure["checkpointer_digests"],
        "store_digests": disclosure["store_digests"],
    }


def _lineages(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        event["capability_lineage"]
        for payload in payloads
        for event in payload.get("event_payloads", ())
        if "capability_lineage" in event
    ]


async def _saver_checkpoints(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as connection:
        return int(
            await connection.fetchval(f"SELECT count(*) FROM {LANGGRAPH_SCHEMA}.checkpoints")
        )


def _calls(stack: ProductionStack, run_id: str) -> dict[str, dict[str, int]]:
    calls: dict[str, dict[str, int]] = {}
    for item in stack.model_log:
        if item["run_id"] != run_id:
            continue
        per_operation = calls.setdefault(item["operation"], {"parent": 0, "child": 0})
        per_operation[item["model"]] += 1
    return calls


# --- Durable outputs through the generic artifact path --------------------------------------


async def promote_generic_artifact(stack: ProductionStack) -> tuple[str, dict[str, Any]]:
    """One governed operation through `POST /runs/{run_id}/operations` (GenericArtifactWorkflow:
    `operation.execute`, candidate capture, `artifact.promote`); returns the run and result.
    Shared by the filesystem and the S3 object-store qualifications."""

    catalog = await publish_technical_catalog(
        stack.control_plane, family="StageGraph", now=datetime.now(UTC)
    )
    run_id = await _admit(stack, catalog, "rrm009-artifact-run")
    run = await _run(stack, run_id)
    started = await _send(
        stack,
        run_id,
        _command(
            run_id, run["version"], f"start:{run_id[:8]}", StartAction(), "workflow_run.start"
        ),
    )
    assert started["status"] == "accepted"
    unit = stage_unit(
        request_scope=SCOPE,
        run_id=run_id,
        operation_id=(
            "execution-epoch:1:stage:report:mapped:none:workflow-cycle:0:stage-cycle:0:slot:default"
        ),
        stage_id="report",
    )
    reservation_id = f"reservation:{unit.unit_key}"
    run = await _run(stack, run_id)
    reserved = await _send(
        stack,
        run_id,
        _command(
            run_id,
            run["version"],
            f"reserve:{run_id[:8]}",
            ReserveBudgetAction(
                reservation_id=reservation_id, amounts={"tokens.total": 40, "model.turns": 8}
            ),
            "workflow_run.reserve_budget",
        ),
    )
    assert reserved["status"] == "accepted"
    template = stage_templates(stack.technical, catalog)["draft/execute/default"]
    workspace = template.workspace.model_copy(
        update={
            "namespace_id": f"workspace-namespace:{run_id}",
            "workspace_id": f"workspace:{run_id}:report",
        }
    )
    revision = reserved["resulting_run_version"]
    deep_binding = bind_unit(
        cast(Any, template.deep_agent_binding),
        unit,
        control_revision=revision,
        reservation_id=reservation_id,
        workspace=workspace,
        erc_digest=catalog.erc.digest,
    )
    operation = OperationExecutionRequest.model_validate(
        {
            **template.model_dump(mode="python"),
            "identity": OperationAttemptIdentity(
                run_id=run_id,
                operation_id=unit.semantic_operation_id,
                operation_attempt=unit.semantic_attempt,
            ),
            "effective_configuration_digest": catalog.erc.digest,
            "run_control_revision": revision,
            "workspace": workspace,
            "deep_agent_binding": deep_binding,
            "runtime_unit": unit,
            "budget_reservation_id": reservation_id,
            "budget_limits": {"tokens.total": 40, "model.turns": 8},
            "requested_at": datetime.now(UTC),
            "idempotency_key": f"rrm009-artifact:{unit.unit_key}",
        }
    )
    submission = GenericArtifactWorkflowRequest(
        request_scope=SCOPE,
        run_id=run_id,
        operation=operation,
        promotion=ArtifactPromotionPlan(
            namespace_id=workspace.namespace_id,
            workspace_id=workspace.workspace_id,
            output_slot="output",
            logical_path=REPORT_PATH,
            owner=WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="stage:draft"),
            permission_ref="permission:rrm009",
            permission_outcome="allowed",
            output_contract_ref=operation.operation_contract_ref,
        ),
    )
    response = await stack.http.post(
        f"/run-control/v1/runs/{run_id}/operations", json=submission.model_dump(mode="json")
    )
    assert response.status_code == 201, response.text
    result = cast(dict[str, Any], response.json())
    assert result["operation"]["status"] == "completed"
    artifact = result["artifact"]
    assert artifact["status"] == "admitted"
    assert artifact["durable_reference"].startswith(f"artifact://{SCOPE}/{run_id}/")
    return run_id, result
