from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

import asyncpg
from temporalio.client import Client
from temporalio.worker import Worker, WorkerDeploymentConfig

from mission_control.adapters.temporal.artifact_activities import (
    ArtifactPromotionActivities,
    create_generic_artifact_worker,
)
from mission_control.adapters.temporal.coordinator_runtime import (
    CoordinatorWorkerActivities,
    CoordinatorWorkerSet,
    coordinator_task_queues,
    create_coordinator_workers,
)
from mission_control.adapters.temporal.operation_activities import (
    OperationExecutionActivities,
    create_agent_cognitive_worker,
)
from mission_control.adapters.temporal.registration.task_queues import (
    BellLabsTaskQueues,
    generic_artifact_task_queue,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.execution.harness.hook_callbacks import HookCallbackService
from mission_control.application.execution.run_control_repository import RunControlRepository
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
    RunConfigurationVerifier,
    RunControlService,
)
from mission_control.application.programs.goal_directed import (
    configure_goal_directed_family_admissions,
)
from mission_control.application.programs.service import register_stagegraph_family_mutations
from mission_control.bootstrap.settings import Settings
from mission_control.domain.execution.heartbeats import OperationHeartbeatPolicy


@dataclass(frozen=True)
class WorkerActivityComposition:
    """Deployment-supplied, fully wired activity adapters.

    `artifacts` serves `GenericArtifactWorkflow` (candidate capture and governed promotion)
    on its own queue; `resources` holds provider lifespans the composition opened (the
    persistent LangGraph saver and store) and is closed when the workers stop.
    """

    coordinator: CoordinatorWorkerActivities
    operation: OperationExecutionActivities
    artifacts: ArtifactPromotionActivities | None = None
    resources: AsyncExitStack | None = None
    # FT-G3: the Kernel Hook callback service the worker serves on its loopback listener
    # (present when a file-based lane such as `cursor_local` is composed).
    hook_callbacks: HookCallbackService | None = None


class WorkerActivityCompositionFactory(Protocol):
    async def build(
        self,
        *,
        settings: Settings,
        control_plane: ControlPlaneService,
        run_control: RunControlService,
        postgres_pool: asyncpg.Pool,
    ) -> WorkerActivityComposition: ...


@dataclass(frozen=True)
class ProductionWorkerSet:
    """The workers a launch-enabled deployment runs for one composition (RRM-009)."""

    coordinator: CoordinatorWorkerSet
    operation: Worker
    artifacts: Worker | None

    @property
    def workers(self) -> tuple[Worker, ...]:
        return (
            *self.coordinator.workers,
            self.operation,
            *((self.artifacts,) if self.artifacts is not None else ()),
        )


def operation_heartbeat_policy(settings: Settings) -> OperationHeartbeatPolicy:
    """The deployment's heartbeat timeout per operation class (RRM-008 composed by RRM-009)."""

    return OperationHeartbeatPolicy(
        deep_agent_seconds=settings.operation_heartbeat_timeout_seconds,
        deep_agent_async_children_seconds=(
            settings.operation_async_children_heartbeat_timeout_seconds
        ),
        bound_seconds=settings.operation_bound_heartbeat_timeout_seconds,
        deep_agent_segment_loop=settings.mission_control_lane_segment_loop,
    )


def create_production_workers(
    client: Client,
    settings: Settings,
    composition: WorkerActivityComposition,
    *,
    deployment_config: WorkerDeploymentConfig | None = None,
) -> ProductionWorkerSet:
    """Both family workers, the cognitive worker and the generic artifact worker, on the
    canonical queues derived from `TEMPORAL_TASK_QUEUE`.

    The workers that serve `operation.execute` also serve `operation.cancel` (RRM-008) and
    drain with `WORKER_GRACEFUL_SHUTDOWN_SECONDS`, which must be shorter than every operation
    heartbeat timeout the families declare: the composition refuses otherwise.

    FT-G7: `deployment_config` (from `worker_deployment_config`) puts every worker of the
    set into one Worker Deployment Version; `None` keeps unversioned workers.
    """

    operation_heartbeat_policy(settings).verify_graceful_shutdown(
        settings.worker_graceful_shutdown_seconds
    )
    drain = timedelta(seconds=settings.worker_graceful_shutdown_seconds)
    return ProductionWorkerSet(
        coordinator=create_coordinator_workers(
            client,
            task_queues=coordinator_task_queues(settings.temporal_task_queue),
            activities=composition.coordinator,
            deployment_config=deployment_config,
        ),
        operation=create_agent_cognitive_worker(
            client,
            task_queue=BellLabsTaskQueues.from_base(settings.temporal_task_queue).agent_cognitive,
            activities=composition.operation,
            graceful_shutdown_timeout=drain,
            deployment_config=deployment_config,
        ),
        artifacts=(
            create_generic_artifact_worker(
                client,
                task_queue=generic_artifact_task_queue(settings.temporal_task_queue),
                operations=composition.operation,
                artifacts=composition.artifacts,
                graceful_shutdown_timeout=drain,
                deployment_config=deployment_config,
            )
            if composition.artifacts is not None
            else None
        ),
    )


async def production_workers_or_close(
    client: Client,
    settings: Settings,
    composition: WorkerActivityComposition,
    *,
    deployment_config: WorkerDeploymentConfig | None = None,
) -> ProductionWorkerSet:
    """`create_production_workers`, closing the composition's resources (the persistent saver
    and store) when the worker set refuses to start, for example on a drain that is not
    shorter than a heartbeat timeout (RRM-009 review)."""

    try:
        return create_production_workers(
            client, settings, composition, deployment_config=deployment_config
        )
    except BaseException:
        if composition.resources is not None:
            await composition.resources.aclose()
        raise


def compose_worker_run_control_service(
    repository: RunControlRepository,
    configuration_verifier: RunConfigurationVerifier,
    policies: AdmissionPolicyRegistry,
    family_admission_registry: FamilyAdmissionRegistry | None = None,
) -> RunControlService:
    """Build worker run control with an optional exact family-policy registry."""

    if family_admission_registry is None:
        registry = FamilyAdmissionRegistry()
        configure_goal_directed_family_admissions(registry)
        register_stagegraph_family_mutations(registry)
    else:
        registry = family_admission_registry
    return RunControlService(
        repository,
        configuration_verifier,
        policies,
        registry,
    )


async def main(
    composition_factory: WorkerActivityCompositionFactory | None = None,
    family_admission_registry: FamilyAdmissionRegistry | None = None,
) -> None:
    """Compatibility module entrypoint; the trusted Mission Control bootstrap owns startup."""
    from mission_control.bootstrap.worker import main as mission_control_main

    await mission_control_main(composition_factory, family_admission_registry)


if __name__ == "__main__":
    from mission_control.bootstrap.worker import run_cli

    run_cli()
