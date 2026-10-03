"""RRM-009 technical qualification: the production composition from the API to Temporal.

The BellLabs API (`app.server.api`) is composed exactly as a deployment composes it
(`initialize_run_control_resources`, `compose_runtime_control`); the workers are the
deployment factory's (`ProductionWorkerActivityCompositionFactory`, `create_production_workers`)
over the disposable application PostgreSQL, MongoDB and a persistent Temporal dev server
(`start_local` with a database file, kept across the qualification and restarted once).
The catalog is published and compiled through the real control plane; admission, launch,
interventions, snapshots, forks, inspection and artifact promotion all go through the facade.

Both families run bounded technical inputs. Cognition is a real `create_deep_agent` graph with
a deterministic technical model that invokes one in-process sync subagent, writes its report
into the governed writable slot, and calls the exact qualification MCP tool. No company input,
no live model. Opt-in through `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI`.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import asdict, dataclass, field
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
from app.application.orchestration.mongo_goal_directed_repository import (
    MongoGoalDirectedDocumentRepository,
)
from app.application.orchestration.mongo_stagegraph_repository import (
    MongoStageGraphOperationTemplateRepository,
)
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.application.run_control.run_launch import fork_semantic_input_binding_ref
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
    PauseAction,
    PauseDecision,
    ReserveBudgetAction,
    ResumeAction,
    ResumeDecision,
    RunOutcome,
    SatisfyWaitAction,
    StartAction,
)
from app.domain.run_control.forks import ForkPatchPolicy, PatchablePath, stage_objective_path
from app.integrations.control_plane_payloads import UnavailablePayloadStore
from app.integrations.mongodb import create_mongodb
from app.integrations.operation_runtime_ports import FilesystemArtifactPayloadStore
from app.integrations.postgres import (
    create_application_family_writer_pool,
    create_application_postgres_pool,
)
from app.models import WorkspaceCandidateDocument
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
    AGENT_COGNITIVE_QUEUE,
    ANSWER_MARKER,
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
    goal_input,
    goal_templates,
    prepare_disposable_identities,
    publish_technical_catalog,
    runtime_environment,
    stage_input,
    stage_templates,
    technical_admission_policies,
    technical_binding,
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


@pytest.fixture
async def stack(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[ProductionStack]:
    technical = technical_binding()
    model_log: list[dict[str, Any]] = []
    async with open_production_stack(
        dsn=test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical=technical,
        components=technical.components(model_log),
        model_log=model_log,
    ) as production:
        yield production


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


async def _assert_completed_with_baseline_released(stack: ProductionStack, run_id: str) -> None:
    """RRM-021: a run admitted with a non-empty baseline completed, with nothing reserved."""

    run = await _run(stack, run_id)
    assert run["terminal_outcome"] == "completed", run
    budget = await stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    assert budget.status_code == 200, budget.text
    body = budget.json()
    assert not any(body["reserved"].values()), body
    assert "baseline" not in body["reservations"], body


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


# --- StageGraph --------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stagegraph_runs_through_the_production_composition_with_fork_relay_and_inspection(
    stack: ProductionStack,
) -> None:
    catalog = await publish_technical_catalog(
        stack.control_plane, family="StageGraph", now=datetime.now(UTC)
    )
    templates = MongoStageGraphOperationTemplateRepository()
    policies = cast(ForkPatchPolicyRegistry, api.state.fork_patch_policies)
    policies.register(
        catalog.workflow_ref.digest,
        ForkPatchPolicy(
            policy_id="fork-patch-policy:rrm009-technical-stagegraph",
            family="stage_graph",
            patchable=(
                PatchablePath(path=stage_objective_path("review"), invalidates=("review",)),
            ),
        ),
    )
    source_run = await _admit(stack, catalog, "rrm009-stagegraph-source")
    source_binding = f"semantic-input:rrm009:{source_run}"
    await templates.persist_templates(
        request_scope=SCOPE,
        semantic_input_binding_ref=source_binding,
        templates=stage_templates(stack.technical, catalog),
        recorded_at=datetime.now(UTC),
    )
    persisted = await templates.get_template(
        semantic_input_binding_ref=source_binding,
        operation_request_key="draft/execute/default",
        request_scope=SCOPE,
        run_id=source_run,
    )
    assert persisted.deep_agent_binding is not None
    assert persisted.deep_agent_binding.task_queue == AGENT_COGNITIVE_QUEUE
    assert AGENT_COGNITIVE_QUEUE in stack.worker_queues, stack.worker_queues
    # A launch that does not bind the admitted authority is refused before Temporal.
    stale = await stack.http.post(
        f"/run-control/v1/runs/{source_run}/launch",
        json={
            "request_scope": SCOPE,
            "run_id": source_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, source_run, source_binding, 7)),
        },
    )
    assert stale.status_code == 422 and stale.json()["detail"]["code"] == "stale_run_version"
    receipt = await _launch(
        stack,
        source_run,
        {
            "request_scope": SCOPE,
            "run_id": source_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, source_run, source_binding, 1)),
        },
    )
    assert receipt["workflow_id"] == f"belllabs-run/{source_run}"
    assert receipt["parent_run_id"] is None
    # A repeated launch of the same pending run is idempotent at the facade.
    again = await stack.http.post(
        f"/run-control/v1/runs/{source_run}/launch",
        json={
            "request_scope": SCOPE,
            "run_id": source_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, source_run, source_binding, 1)),
        },
    )
    # While the run is still pending the duplicate start resolves to the same execution;
    # once the family has started it the launch is refused, never started twice.
    if again.status_code == 202:
        assert (again.json()["workflow_id"], again.json()["temporal_run_id"]) == (
            receipt["workflow_id"],
            receipt["temporal_run_id"],
        ), again.text
    else:
        assert again.status_code == 409, again.text
        assert again.json()["detail"]["code"] == "run_not_pending", again.text

    # `draft` settles (sync subagent, report written, MCP called); the run holds its wait.
    await _wait_for(stack, source_run, lambda: _holds_wait(stack, source_run), 150)
    snapshot = await stack.http.post(
        f"/run-control/v1/runs/{source_run}/snapshots", json={"request_scope": SCOPE}
    )
    assert snapshot.status_code == 201, snapshot.text
    manifest = snapshot.json()
    assert manifest["boundary_kind"] == "stage_settled"
    fork = await stack.http.post(
        f"/run-control/v1/runs/{source_run}/forks",
        json={
            "request_scope": SCOPE,
            "request_id": f"fork-{source_run[:8]}",
            "idempotency_key": f"fork-{source_run[:8]}",
            "snapshot_id": manifest["snapshot_id"],
            "snapshot_digest": manifest["snapshot_digest"],
            "changes": [{"path": stage_objective_path("review"), "value": STAGE_OBJECTIVE}],
            "invalidation_frontier": ["review"],
            "baseline_reservations": {"tokens.total": 20},
            "sponsorship_ref": "sponsorship:test",
            "approval_refs": ["approval:test"],
            "reason": "RRM-009 technical fork",
        },
    )
    assert fork.status_code == 201, fork.text
    derived_run = fork.json()["target_run_id"]
    fork_request_id = fork.json()["request_id"]
    derived_binding = fork_semantic_input_binding_ref(fork_request_id)
    # The governed launch of the derived run: parent from the receipt, patched templates.
    # RRM-021: the governed launch still refuses a family input whose baseline differs from
    # the admitted one (RRM-009), now that the StageGraph settles the baseline it carries.
    wrong_baseline = await stack.http.post(
        f"/run-control/v1/runs/{derived_run}/launch",
        json={
            "request_scope": SCOPE,
            "run_id": derived_run,
            "family": "StageGraph",
            "stagegraph": {
                **asdict(stage_input(catalog, derived_run, derived_binding, 1)),
                "baseline_reservation": {"tokens.total": 21},
            },
            "source_semantic_input_binding_ref": source_binding,
        },
    )
    assert wrong_baseline.status_code == 422, wrong_baseline.text
    assert wrong_baseline.json()["detail"]["code"] == "budget_mismatch"
    wrong_binding = await stack.http.post(
        f"/run-control/v1/runs/{derived_run}/launch",
        json={
            "request_scope": SCOPE,
            "run_id": derived_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, derived_run, "semantic-input:other", 1)),
            "source_semantic_input_binding_ref": source_binding,
        },
    )
    assert wrong_binding.status_code == 422
    assert wrong_binding.json()["detail"]["code"] == "fork_binding_mismatch"
    derived_receipt = await _launch(
        stack,
        derived_run,
        {
            "request_scope": SCOPE,
            "run_id": derived_run,
            "family": "StageGraph",
            "stagegraph": asdict(stage_input(catalog, derived_run, derived_binding, 1)),
            "source_semantic_input_binding_ref": source_binding,
        },
    )
    assert derived_receipt["parent_run_id"] == source_run
    assert derived_receipt["fork_request_id"] == fork_request_id
    derived_templates = await templates.list_templates(
        request_scope=SCOPE, semantic_input_binding_ref=derived_binding
    )
    assert (
        derived_templates["review/execute/default"].prompt_segments[-1].content == STAGE_OBJECTIVE
    )
    assert (
        derived_templates["draft/execute/default"]
        == (
            await templates.list_templates(
                request_scope=SCOPE, semantic_input_binding_ref=source_binding
            )
        )["draft/execute/default"]
    )
    assert await _visible(stack.client, f"BellLabsParentRunId = '{source_run}'", 1) == 1
    await _release_wait(stack, derived_run)
    await _wait_for(stack, derived_run, lambda: _terminal(stack, derived_run), 240)
    await _assert_completed_with_baseline_released(stack, derived_run)

    # RRM-007 relay drill on a persistent namespace: the pause is accepted while the Temporal
    # transport is down, and delivered and applied once the server is back and the relay runs.
    await stack.env.shutdown()
    run = await _run(stack, source_run)
    pause_id = f"pause:{source_run[:8]}"
    paused = await _send(
        stack,
        source_run,
        _command(
            source_run,
            run["version"],
            pause_id,
            PauseAction(
                decision=PauseDecision(
                    decision_id=pause_id,
                    scope=frozenset({"run"}),
                    reason="RRM-009 relay drill: operator hold while the transport is down",
                    authority_ref="authority:lifecycle",
                ),
                # The source holds its declared wait, so no admissible work remains.
                runnable_work_remains=False,
            ),
            "workflow_run.pause",
        ),
    )
    assert (
        paused["status"] == "accepted" and paused["reason_code"] == "accepted_pending_application"
    )
    assert await _receipt_states(stack, source_run, pause_id) == ["accepted"]
    stack.env = await _start_local(stack.temporal_db)
    relay = api.state.boundary_command_relay

    async def pause_applied() -> bool:
        await relay.run_once()
        return (await _receipt_states(stack, source_run, pause_id))[-1:] == ["applied"]

    await _until(pause_applied, seconds=240)
    assert await _receipt_states(stack, source_run, pause_id) == [
        "accepted",
        "delivered",
        "applied",
    ]
    assert (await _run(stack, source_run))["phase"] == "paused"
    run = await _run(stack, source_run)
    resume_id = f"resume:{source_run[:8]}"
    await _send(
        stack,
        source_run,
        _command(
            source_run,
            run["version"],
            resume_id,
            ResumeAction(
                decision=ResumeDecision(
                    decision_id=resume_id,
                    pause_decision_id=pause_id,
                    reason="RRM-009 relay drill: operator release",
                    authority_ref="authority:lifecycle",
                )
            ),
            "workflow_run.resume",
        ),
    )

    async def resumed() -> bool:
        return (await _receipt_states(stack, source_run, resume_id))[-1:] == ["applied"]

    await _until(resumed)
    await _release_wait(stack, source_run)
    await _wait_for(stack, source_run, lambda: _terminal(stack, source_run), 240)
    await _assert_completed_with_baseline_released(stack, source_run)

    # Visibility (REQ-CP-EXEC-015) on the persistent namespace: root, family and operations.
    assert await _visible(stack.client, f"BellLabsRunId = '{source_run}'", 4) == 4
    # The derived run's `draft` unit also runs its operation workflow, which reuses the
    # source's settled result by immutable ref (RRM-006) without any cognition.
    assert await _visible(stack.client, f"BellLabsRunId = '{derived_run}'", 4) == 4
    derived_ids = sorted(
        [
            execution.id
            async for execution in stack.client.list_workflows(f"BellLabsRunId = '{derived_run}'")
        ]
    )
    assert derived_ids == sorted(
        [
            f"belllabs-run/{derived_run}",
            f"family/{derived_run}/1",
            *(
                f"operation/{derived_run}:operation:execution-epoch:1:stage:{stage}:mapped:none:"
                "workflow-cycle:0:stage-cycle:0:slot:execute:attempt:1"
                for stage in ("draft", "review")
            ),
        ]
    )
    source = await _run(stack, source_run)
    derived = await _run(stack, derived_run)
    assert (source["phase"], source["terminal_outcome"]) == ("terminal", "completed")
    assert (derived["phase"], derived["terminal_outcome"]) == ("terminal", "completed")
    source_outputs = sorted(item["output_ref"] for item in source["accepted_output_evidence"])
    derived_outputs = sorted(item["output_ref"] for item in derived["accepted_output_evidence"])
    by_stage = {
        run_id: {ref.split(":")[2]: ref for ref in outputs}
        for run_id, outputs in ((source_run, source_outputs), (derived_run, derived_outputs))
    }
    assert by_stage[derived_run]["draft"] == by_stage[source_run]["draft"]  # reused by ref
    assert by_stage[derived_run]["review"] != by_stage[source_run]["review"]  # patched

    # Sync subagent, writable-slot capture and the exact MCP tool, per operation.
    calls = _calls(stack, source_run)
    assert sorted(calls) and all(value == {"parent": 4, "child": 1} for value in calls.values())
    assert {"parent": 4, "child": 1} == next(iter(_calls(stack, derived_run).values()))
    payloads = [
        *await _operation_payloads(stack, source_run),
        *await _operation_payloads(stack, derived_run),
    ]
    facts = [(payload["structured_output"] or {}).get("facts") for payload in payloads]
    # Source draft and review, the derived draft's reused result (the source draft's output,
    # with no cognition of its own) and the derived review.
    assert len(facts) == 4 and all(item == {"child": True, "mcp": True} for item in facts), facts
    lineages = _lineages(payloads)
    assert len(lineages) == 3  # a reused result carries no lineage of its own
    for lineage in lineages:
        # The sync subagent's model call is charged to the parent operation (REQ-CP-DA-007).
        assert lineage["usage"]["amounts"] == {"model.turns": 5, "tokens.total": 25}
        assert [call["scope"] for call in lineage["usage"]["model_calls"]].count("subordinate") == 1
        assert lineage["invoked"] == {
            "framework": ["write_file"],
            "mcp": ["lookup_binding_marker"],
            "sync_subagent": ["task"],
        }
        assert lineage["credential_refs"] == ["environment:OPENAI_API_KEY"]
        assert lineage["placement"]["task_queue"] == AGENT_COGNITIVE_QUEUE
    candidates = await WorkspaceCandidateDocument.find(
        WorkspaceCandidateDocument.logical_path == REPORT_PATH
    ).to_list()
    assert len(candidates) >= 3  # draft and review of the source, review of the derived run
    assert all(
        (stack.payload_root / item.object_ref.split("://")[1]).exists() for item in candidates
    )

    # Inspection (REQ-CP-RUN-011/012) over the composed sources: Temporal current, the
    # persistent saver's history served, async-child detail composed.
    read = await stack.http.get(
        f"/run-control/v1/inspection/runs/{source_run}", params={"request_scope": SCOPE}
    )
    assert read.status_code == 200, read.text
    sections = read.json()["sections"]
    assert sections["temporal"]["freshness"] == "current", sections["temporal"]
    assert sections["async_children_detail"]["freshness"] == "current", sections
    unit_key = read.json()["data"]["units"][0]["unit_key"]
    # REQ-CP-EXEC-015: the unit's operation execution is listed by its unit key, and the
    # run's root, family and operations by their kind, on the persistent namespace.
    assert await _visible(stack.client, f"BellLabsUnitKey = '{unit_key}'", 1) == 1
    kinds = sorted(
        [
            str(execution.search_attributes.get("BellLabsWorkflowKind", ["?"])[0])
            async for execution in stack.client.list_workflows(f"BellLabsRunId = '{source_run}'")
        ]
    )
    assert kinds == ["family", "operation", "operation", "root"], kinds
    history = await stack.http.get(
        f"/run-control/v1/inspection/runs/{source_run}/units/{unit_key}/checkpoints",
        params={"request_scope": SCOPE},
    )
    assert history.status_code == 200, history.text
    history_sections = history.json()["sections"]
    assert history_sections["checkpoints"]["freshness"] == "current", history_sections
    assert len(history.json()["data"]["entries"]) >= 4
    assert await _saver_checkpoints(stack.owner_pool) > 0
    replayed = await _replay(
        stack.client,
        [
            f"belllabs-run/{source_run}",
            f"family/{source_run}/1",
            f"belllabs-run/{derived_run}",
            f"family/{derived_run}/1",
        ],
    )
    ready = await stack.http.get("/health/ready")
    # RRM-009 review: the unauthenticated probe discloses status and mode only.
    assert ready.status_code == 200 and ready.json() == {
        "status": "ready",
        "mode": "runtime-control",
    }, ready.text
    print(
        "RRM-009 EVIDENCE stagegraph:",
        json.dumps(
            {
                "source_run": source_run,
                "derived_run": derived_run,
                "fork_request_id": fork_request_id,
                "pause_receipts": await _receipt_states(stack, source_run, pause_id),
                "outputs": {"source": source_outputs, "derived": derived_outputs},
                "model_calls": calls,
                "facts": facts,
                "candidates": len(candidates),
                "saver_checkpoints": await _saver_checkpoints(stack.owner_pool),
                "inspection": {name: item["freshness"] for name, item in sections.items()},
                "replayed_events": replayed,
                "readiness": api.state.runtime_control.readiness,
                "capabilities": _pinned_summary(stack),
                "lineage": lineages[0],
            },
            sort_keys=True,
            default=str,
        ),
    )


# --- GoalDirected ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_goal_directed_runs_two_iterations_through_the_production_composition(
    stack: ProductionStack,
) -> None:
    catalog = await publish_technical_catalog(
        stack.control_plane, family="GoalDirected", now=datetime.now(UTC)
    )
    run_id = await _admit(stack, catalog, "rrm009-goal-source")
    binding_ref = f"semantic-input:rrm009:{run_id}"
    templates = goal_templates(stack.technical, catalog)
    await MongoGoalDirectedDocumentRepository().persist_templates(
        request_scope=SCOPE,
        semantic_input_binding_ref=binding_ref,
        executor=templates["executor"],
        verifier=templates["verifier"],
        recorded_at=datetime.now(UTC),
    )
    objective = "Produce one independently verified technical record."
    receipt = await _launch(
        stack,
        run_id,
        {
            "request_scope": SCOPE,
            "run_id": run_id,
            "family": "GoalDirected",
            "goal_directed": asdict(goal_input(catalog, run_id, binding_ref, 1, objective)),
        },
    )
    assert receipt["workflow_id"] == f"belllabs-run/{run_id}"
    await _wait_for(stack, run_id, lambda: _terminal(stack, run_id), 300)
    run = await _run(stack, run_id)
    assert (run["phase"], run["terminal_outcome"]) == ("terminal", RunOutcome.COMPLETED.value)
    # RRM-019: only the final executor's verified outputs are promoted.
    assert [item["output_ref"] for item in run["accepted_output_evidence"]] == [
        "artifact:rrm009-goal:2"
    ]
    assert len(run["accepted_operation_settlement_evidence"]) == 4
    calls = _calls(stack, run_id)
    assert len(calls) == 4 and all(value == {"parent": 4, "child": 1} for value in calls.values())
    assert await _visible(stack.client, f"BellLabsRunId = '{run_id}'", 6) == 6
    budget = await stack.http.get(
        f"/run-control/v1/runs/{run_id}/budget", params={"request_scope": SCOPE}
    )
    assert budget.status_code == 200
    replayed = await _replay(stack.client, [f"belllabs-run/{run_id}", f"family/{run_id}/1"])
    print(
        "RRM-009 EVIDENCE goal_directed:",
        json.dumps(
            {
                "run": run_id,
                "accepted_outputs": [
                    item["output_ref"] for item in run["accepted_output_evidence"]
                ],
                "settlements": len(run["accepted_operation_settlement_evidence"]),
                "model_calls": calls,
                "consumed": budget.json()["consumed"],
                "replayed_events": replayed,
            },
            sort_keys=True,
        ),
    )


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
    try:
        response = await stack.http.post(
            f"/run-control/v1/runs/{run_id}/operations", json=submission.model_dump(mode="json")
        )
    except Exception:
        async for execution in stack.client.list_workflows():
            history = await stack.client.get_workflow_handle(execution.id).fetch_history()
            for event in history.events:
                if event.event_type in (11, 12, 13):
                    print("DEBUGEVENT", execution.id, str(event)[:6000])
        raise
    assert response.status_code == 201, response.text
    result = cast(dict[str, Any], response.json())
    assert result["operation"]["status"] == "completed"
    artifact = result["artifact"]
    assert artifact["status"] == "admitted"
    assert artifact["durable_reference"].startswith(f"artifact://{SCOPE}/{run_id}/")
    return run_id, result


@pytest.mark.asyncio
async def test_generic_artifact_operation_promotes_the_captured_report_durably(
    stack: ProductionStack,
) -> None:
    run_id, result = await promote_generic_artifact(stack)
    artifact = result["artifact"]
    object_path = stack.payload_root / artifact["object_ref"].split("://")[1]
    assert object_path.exists()
    assert b"RRM-009 report" in object_path.read_bytes()
    async with stack.owner_pool.acquire() as connection:
        durable = await connection.fetchval(
            "SELECT count(*) FROM belllabs_control.durable_artifact_references WHERE run_id = $1",
            run_id,
        )
    assert durable == 1
    assert ANSWER_MARKER in result["operation"]["output_text"]
    assert _calls(stack, run_id) == {
        "execution-epoch:1:stage:report:mapped:none:workflow-cycle:0:stage-cycle:0:slot:default": {
            "parent": 4,
            "child": 1,
        }
    }
    assert json.loads(result["operation"]["output_text"])["facts"] == {
        "child": True,
        "mcp": True,
    }
    print(
        "RRM-009 EVIDENCE artifact:",
        json.dumps(
            {
                "run": run_id,
                "artifact_id": artifact["artifact_id"],
                "durable_reference": artifact["durable_reference"],
                "object_ref": artifact["object_ref"],
                "content_digest": artifact["content_digest"],
                "model_calls": _calls(stack, run_id),
            },
            sort_keys=True,
        ),
    )
