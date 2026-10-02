"""Production runtime composition of the BellLabs API (RRM-009, CP-050 prerequisite).

`compose_runtime_control` attaches, on `app.state` and before the first request, everything
the governed facade needs to drive and observe the macro runtime through Temporal:

* the production root submitter (`TemporalWorkflowSubmitter.for_production`, Search
  Attribute policy `required`, REQ-CP-EXEC-015) and the `RunLaunchService` behind
  `POST /run-control/v1/runs/{run_id}/launch`;
* inspection sources (REQ-CP-RUN-011/012): the Temporal Visibility reader, the registered
  persistent saver's checkpoint history reader, the Mongo async-child detail repository and
  one shared cursor key;
* boundary intervention delivery (RRM-007, review F6): the Temporal transport for inline
  delivery, the reconciliation nudge and verifier, and the delivery relay that re-drives
  accepted commands whose inline delivery failed;
* fork patch policies (RRM-006) and the generic artifact submitter.

Readiness verifies the namespace's Search Attributes and never mutates it; registration is
the separate administrative step (`scripts/register_belllabs_search_attributes.py`).
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass

from fastapi import FastAPI
from starlette.requests import Request
from temporalio.client import Client

from app.api.run_control import (
    get_boundary_intervention_service,
    get_run_control_service,
)
from app.application.async_subagents.mongo_async_subagent_repository import (
    MongoAsyncSubagentDetailRepository,
)
from app.application.async_subagents.parent_effects import RunControlAsyncChildEffects
from app.application.async_subagents.postgres_async_subagents import PostgresAsyncSubagentAuthority
from app.application.async_subagents.service import AsyncSubagentService
from app.application.async_subagents.usage_reconciliation import (
    AsyncChildUsageReconciliation,
    ProviderNotComposed,
)
from app.application.orchestration.fork_templates import StageGraphForkTemplateDerivation
from app.application.orchestration.mongo_stagegraph_repository import (
    MongoStageGraphOperationTemplateRepository,
)
from app.application.orchestration.service import orchestration_lifecycle_actor
from app.application.run_control.boundary_relay import BoundaryCommandRelay
from app.application.run_control.liability_hints import FamilyLiabilityHints
from app.application.run_control.run_launch import RunLaunchService
from app.application.runtime.postgres_run_forks import PostgresForkMaterializationStore
from app.application.runtime.postgres_stage3_kernel_repository import PostgresForkRepository
from app.application.runtime.run_forks import ForkPatchPolicyRegistry
from app.config import Settings
from app.integrations.agents.deep_agents.checkpoint_history import (
    LangGraphCheckpointHistoryReader,
)
from app.integrations.agents.deep_agents.checkpoint_verifier import (
    LangGraphCheckpointDescendantVerifier,
)
from app.integrations.capability_pins import CapabilityPins
from app.integrations.langgraph_persistence import StandalonePersistenceLifespan
from app.integrations.temporal_boundary_commands import TemporalBoundaryCommandTransport
from app.integrations.temporal_operation_submission import TemporalGenericArtifactSubmitter
from app.integrations.temporal_unit_reconciliation import (
    TemporalFamilyLiabilityHint,
    TemporalUnitReconciliationNudge,
)
from app.integrations.temporal_visibility import TemporalVisibilityInspectionReader
from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter
from app.temporal.coordinator_runtime import coordinator_task_queues
from app.temporal.registration.task_queues import generic_artifact_task_queue
from app.temporal.search_attributes import (
    SearchAttributeRegistrationError,
    verify_belllabs_search_attributes,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuntimeControlComposition:
    """What the API composed. `readiness` stays in process and in the log; `/health/ready`
    reports status and mode only (RRM-009 review)."""

    submitter: TemporalWorkflowSubmitter
    launch: RunLaunchService
    relay: BoundaryCommandRelay
    checkpointer_digests: tuple[str, ...]
    readiness: dict[str, object]


def startup_request(app: FastAPI) -> Request:
    """A Starlette request bound to the application, for the lazily composed dependencies."""

    return Request({"type": "http", "app": app, "headers": [], "method": "GET", "path": "/"})


async def compose_runtime_control(
    app: FastAPI,
    settings: Settings,
    *,
    client: Client,
    stack: AsyncExitStack,
    fork_patch_policies: ForkPatchPolicyRegistry | None = None,
    start_relay: bool = True,
) -> RuntimeControlComposition:
    state = app.state
    pool = getattr(state, "run_control_postgres_pool", None)
    if pool is None:
        raise RuntimeError("RUN_CONTROL_TEMPORAL_ENABLED requires application PostgreSQL")
    readiness: dict[str, object] = {"temporal_namespace": settings.temporal_namespace}
    try:
        await verify_belllabs_search_attributes(client, settings.temporal_namespace)
        readiness["search_attributes"] = "verified"
    except SearchAttributeRegistrationError as error:
        readiness["search_attributes"] = f"missing: {error}"
        raise
    persistence = await stack.enter_async_context(
        StandalonePersistenceLifespan(settings.langgraph_checkpoint_dsn)
    )
    pins = CapabilityPins.from_settings(settings)
    checkpointers = {item.ref.digest: persistence.saver for item in pins.checkpointers}
    queues = coordinator_task_queues(settings.temporal_task_queue)
    submitter = TemporalWorkflowSubmitter.for_production(
        client,
        stagegraph_task_queue=queues.stagegraph,
        goal_directed_task_queue=queues.goal_directed,
        search_attribute_policy="required",
    )
    # Inspection sources (RRM-005).
    state.temporal_client = client
    state.temporal_visibility_reader = TemporalVisibilityInspectionReader(client)
    state.inspection_checkpoint_reader = LangGraphCheckpointHistoryReader(checkpointers)
    state.inspection_async_child_details = MongoAsyncSubagentDetailRepository()
    state.inspection_cursor_key = settings.inspection_cursor_secret
    # Boundary interventions (RRM-007) and unit reconciliation (RRM-004).
    state.boundary_command_transport = TemporalBoundaryCommandTransport(client)
    state.unit_reconciliation_nudge = TemporalUnitReconciliationNudge(client)
    state.unit_reconciliation_verifier = LangGraphCheckpointDescendantVerifier(checkpointers)
    # Forks (RRM-006) and the governed launch (RRM-009).
    state.fork_patch_policies = fork_patch_policies or ForkPatchPolicyRegistry()
    state.workflow_submitter = submitter
    request = startup_request(app)
    run_control = await get_run_control_service(request)
    launch = RunLaunchService(
        run_control=run_control,
        submitter=submitter,
        forks=PostgresForkRepository(pool),
        materializations=PostgresForkMaterializationStore(pool),
        fork_templates=StageGraphForkTemplateDerivation(
            MongoStageGraphOperationTemplateRepository()
        ),
    )
    state.run_launch_service = launch
    state.generic_artifact_submitter = TemporalGenericArtifactSubmitter(
        client, task_queue=generic_artifact_task_queue(settings.temporal_task_queue)
    )
    interventions = await get_boundary_intervention_service(request)
    # RRM-008 composed: the `liability_reconciled` hint after operator decisions, and the
    # privileged usage reconciliation of cancelled async children (no provider access).
    hints = FamilyLiabilityHints(run_control, TemporalFamilyLiabilityHint(client))
    state.family_liability_hints = hints
    state.async_child_usage_reconciliation = AsyncChildUsageReconciliation(
        AsyncSubagentService(
            MongoAsyncSubagentDetailRepository(),
            PostgresAsyncSubagentAuthority(pool),
            ProviderNotComposed(),
            parent_effects=RunControlAsyncChildEffects(
                run_control, actor=orchestration_lifecycle_actor()
            ),
        ),
        hints,
    )
    relay = BoundaryCommandRelay(
        run_control=run_control,
        interventions=interventions,
        request_scopes=settings.boundary_relay_request_scopes,
        interval_seconds=settings.boundary_relay_interval_seconds,
    )
    state.boundary_command_relay = relay
    if start_relay and relay.request_scopes:
        task = asyncio.create_task(relay.run_forever(), name="belllabs-boundary-relay")

        async def stop_relay() -> None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        stack.push_async_callback(stop_relay)
    readiness.update(
        {
            "launch": "composed",
            "boundary_relay_scopes": list(relay.request_scopes),
            "checkpointer_digests": sorted(checkpointers),
        }
    )
    composition = RuntimeControlComposition(
        submitter=submitter,
        launch=launch,
        relay=relay,
        checkpointer_digests=tuple(sorted(checkpointers)),
        readiness=readiness,
    )
    state.runtime_control = composition
    logger.info("runtime control composed", extra={"readiness": readiness})
    return composition


__all__ = ["RuntimeControlComposition", "compose_runtime_control", "startup_request"]
