"""Worker Deployment versioning for Mission Control workers (FT-G7).

temporalio 1.34 replaces build-id-only routing with Worker Deployments: every worker of one
release polls as `WorkerDeploymentVersion(deployment_name, build_id)`. Mission Control's
families, roots and operations are long-running and already evolve through `workflow.patched`
with replay tests over captured histories, so the default versioning behavior is
`AUTO_UPGRADE`: when a new version becomes current, running executions move to it on their
next workflow task and the old version drains without pinning workers for the lifetime of
a mission. A workflow that must not move declares `PINNED` on its own `@workflow.defn`.

A versioned worker receives new executions only after its version is *current*; until then
the server routes them to unversioned pollers. `promote_worker_deployment_version` sets the
current version once the release's pollers are registered, which is the local development
roll (start the new worker, promote it, stop the old one). Production operators may keep
`TEMPORAL_PROMOTE_ON_START=false` and promote with the CLI instead.
"""

from __future__ import annotations

import asyncio
from importlib.metadata import PackageNotFoundError, version
from typing import Final

from temporalio.api.workflowservice.v1 import (
    DescribeWorkerDeploymentRequest,
    SetWorkerDeploymentCurrentVersionRequest,
)
from temporalio.client import Client
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import WorkerDeploymentConfig

from mission_control.bootstrap.settings import Settings

DEFAULT_VERSIONING_BEHAVIOR: Final = VersioningBehavior.AUTO_UPGRADE
_DISTRIBUTION: Final = "mission-control"


class WorkerVersioningError(RuntimeError):
    """The release's deployment version could not be made current."""


def release_build_id(settings: Settings) -> str:
    """`TEMPORAL_BUILD_ID`, else the installed distribution version of this release."""

    if settings.temporal_build_id:
        return settings.temporal_build_id
    try:
        return version(_DISTRIBUTION)
    except PackageNotFoundError:  # pragma: no cover - source checkouts are installed
        return "0.0.0+source"


def worker_deployment_version(settings: Settings) -> WorkerDeploymentVersion:
    return WorkerDeploymentVersion(
        deployment_name=settings.temporal_deployment_name,
        build_id=release_build_id(settings),
    )


def worker_deployment_config(settings: Settings) -> WorkerDeploymentConfig | None:
    """The release's deployment config, or `None` when versioning is switched off."""

    if not settings.temporal_worker_versioning:
        return None
    return WorkerDeploymentConfig(
        version=worker_deployment_version(settings),
        use_worker_versioning=True,
        default_versioning_behavior=DEFAULT_VERSIONING_BEHAVIOR,
    )


async def current_deployment_build_id(
    client: Client, namespace: str, deployment_name: str
) -> str | None:
    """The build id of the deployment's current version (`None`: unset or no deployment)."""

    try:
        response = await client.workflow_service.describe_worker_deployment(
            DescribeWorkerDeploymentRequest(namespace=namespace, deployment_name=deployment_name)
        )
    except RPCError as error:
        if error.status == RPCStatusCode.NOT_FOUND:
            return None
        raise
    current = response.worker_deployment_info.routing_config.current_deployment_version
    return current.build_id or None


async def promote_worker_deployment_version(
    client: Client,
    namespace: str,
    deployment_version: WorkerDeploymentVersion,
    *,
    identity: str = "mission-control-worker",
    attempts: int = 30,
    interval_seconds: float = 1.0,
) -> bool:
    """Make `deployment_version` current; idempotent. Returns whether this call changed it.

    The server accepts the change only after the version's pollers registered their task
    queues, so a freshly started worker set is retried for `attempts * interval_seconds`.
    """

    name = deployment_version.deployment_name
    if await current_deployment_build_id(client, namespace, name) == (deployment_version.build_id):
        return False
    last: RPCError | None = None
    for _ in range(attempts):
        try:
            await client.workflow_service.set_worker_deployment_current_version(
                SetWorkerDeploymentCurrentVersionRequest(
                    namespace=namespace,
                    deployment_name=name,
                    build_id=deployment_version.build_id,
                    identity=identity,
                )
            )
        except RPCError as error:
            # The version (or the deployment) exists only once a poller registered it.
            if error.status not in {
                RPCStatusCode.NOT_FOUND,
                RPCStatusCode.FAILED_PRECONDITION,
                RPCStatusCode.INVALID_ARGUMENT,
            }:
                raise
            last = error
            await asyncio.sleep(interval_seconds)
            continue
        return True
    raise WorkerVersioningError(
        f"worker deployment version {name}:{deployment_version.build_id} did not become "
        f"current: {last}"
    )


__all__ = [
    "DEFAULT_VERSIONING_BEHAVIOR",
    "WorkerVersioningError",
    "current_deployment_build_id",
    "promote_worker_deployment_version",
    "release_build_id",
    "worker_deployment_config",
    "worker_deployment_version",
]
