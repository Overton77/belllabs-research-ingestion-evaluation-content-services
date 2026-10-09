"""MP-22 acceptance: a cloud-cluster outage drill never launches a second copy of a mission.

Real local Temporal (``127.0.0.1:7233``): two scratch namespaces stand for the two clusters,
``cloud`` (the active cluster) and ``local`` (the fallback new runs may target). The outage is
the ``cloud`` binding pointing at a closed loopback port: its frontend is unreachable while
its history (the scratch namespace) still holds the active execution. No worker polls either
namespace, so the active run never leaves ``pending`` in Mission Control: exactly the window in
which a phase check alone cannot tell that the run was started.

The launcher is the guard plus a plain ``start_workflow`` under the run-derived workflow id
(``belllabs-run/{run_id}``), which ``RunLaunchService`` uses; it starts only what the guard
admits. Both namespaces are deleted afterwards.
"""

from __future__ import annotations

import asyncio
import socket
import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from pathlib import Path

import pytest
from google.protobuf.duration_pb2 import Duration
from temporalio.api.operatorservice.v1 import DeleteNamespaceRequest
from temporalio.api.workflowservice.v1 import (
    DescribeNamespaceRequest,
    RegisterNamespaceRequest,
)
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.service import RPCError, RPCStatusCode

from mission_control.bootstrap.preflight import (
    ClusterBinding,
    ClusterBindingLedger,
    ClusterLaunchDecision,
    guard_cluster_launch,
    run_workflow_id,
    temporal_presence,
)

ADDRESS = "127.0.0.1:7233"
WORKFLOW_TYPE = "BellLabsRunWorkflow"
QUEUE = "mp22-outage-drill"


def closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


async def connect(cluster: ClusterBinding) -> Client:
    return await Client.connect(cluster.address, namespace=cluster.namespace)


@dataclass(frozen=True)
class Drill:
    admin: Client
    cloud: ClusterBinding
    cloud_down: ClusterBinding
    local: ClusterBinding

    async def client(self, cluster: ClusterBinding) -> Client:
        return await connect(cluster)


async def register(admin: Client, namespace: str) -> None:
    await admin.workflow_service.register_namespace(
        RegisterNamespaceRequest(
            namespace=namespace,
            workflow_execution_retention_period=Duration(seconds=86_400),
        )
    )
    for _ in range(60):
        try:
            await admin.workflow_service.describe_namespace(
                DescribeNamespaceRequest(namespace=namespace)
            )
            client = await Client.connect(ADDRESS, namespace=namespace)
            await client.get_workflow_handle("mp22-namespace-ready").describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND and "workflow" in str(error).lower():
                return
            await asyncio.sleep(0.5)
            continue
        return
    raise AssertionError(f"namespace {namespace} did not become ready")


@pytest.fixture
async def drill() -> AsyncIterator[Drill]:
    admin = await Client.connect(ADDRESS)
    suffix = uuid.uuid4().hex[:10]
    names = {"cloud": f"mp22-drill-cloud-{suffix}", "local": f"mp22-drill-local-{suffix}"}
    for name in names.values():
        await register(admin, name)
    cloud = ClusterBinding(
        cluster_id="cloud",
        target="local",
        address=ADDRESS,
        namespace=names["cloud"],
        task_queue=QUEUE,
    )
    try:
        yield Drill(
            admin=admin,
            cloud=cloud,
            cloud_down=cloud.model_copy(update={"address": f"127.0.0.1:{closed_port()}"}),
            local=ClusterBinding(
                cluster_id="local",
                target="local",
                address=ADDRESS,
                namespace=names["local"],
                task_queue=QUEUE,
            ),
        )
    finally:
        for name in names.values():
            client = await Client.connect(ADDRESS, namespace=name)
            async for execution in client.list_workflows():
                if execution.status == WorkflowExecutionStatus.RUNNING:
                    await client.get_workflow_handle(execution.id).terminate("mp22 drill cleanup")
            await admin.operator_service.delete_namespace(DeleteNamespaceRequest(namespace=name))


async def launch(
    drill: Drill,
    run_id: str,
    target: ClusterBinding,
    *,
    clusters: Sequence[ClusterBinding],
    ledger: ClusterBindingLedger,
    run_phase: str = "pending",
) -> ClusterLaunchDecision:
    """The guarded launcher: the guard decides, and only an admitted launch starts."""

    decision = await guard_cluster_launch(
        run_id,
        target,
        clusters=clusters,
        ledger=ledger,
        run_phase=run_phase,
        probe=temporal_presence(connect, timeout_s=5),
    )
    if decision.admitted:
        client = await drill.client(target)
        await client.start_workflow(
            WORKFLOW_TYPE,
            {"run_id": run_id},
            id=run_workflow_id(run_id),
            task_queue=target.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
    return decision


async def executions(drill: Drill, cluster: ClusterBinding, run_id: str) -> list[str]:
    """Temporal run ids of the run's workflow in one cluster (describe is consistent)."""

    client = await drill.client(cluster)
    try:
        description = await client.get_workflow_handle(run_workflow_id(run_id)).describe()
    except RPCError as error:
        if error.status != RPCStatusCode.NOT_FOUND:
            raise
        return []
    return [description.run_id]


@pytest.mark.asyncio
async def test_an_outage_drill_never_launches_a_second_copy_in_another_cluster(
    drill: Drill, tmp_path: Path
) -> None:
    ledger = ClusterBindingLedger(tmp_path / "ledger")
    active = f"run-active-{uuid.uuid4().hex[:8]}"

    # Before the outage: the mission starts on the cloud cluster and is bound there.
    started = await launch(
        drill, active, drill.cloud, clusters=(drill.cloud, drill.local), ledger=ledger
    )
    assert (started.admitted, started.code) == (True, "BOUND")
    original = await executions(drill, drill.cloud, active)
    assert len(original) == 1

    # Outage: the cloud frontend is unreachable; operators try to move the mission locally.
    during = (drill.cloud_down, drill.local)
    refused = await launch(drill, active, drill.local, clusters=during, ledger=ledger)
    assert (refused.admitted, refused.code) == (False, "RUN_BOUND_TO_OTHER_CLUSTER")
    assert refused.bound_cluster_id == "cloud"

    # A second operator host without the ledger: the cloud probe finds the execution.
    elsewhere = ClusterBindingLedger(tmp_path / "other-host-ledger")
    found = await launch(
        drill, active, drill.local, clusters=(drill.cloud, drill.local), ledger=elsewhere
    )
    assert (found.admitted, found.code) == (False, "ACTIVE_IN_OTHER_CLUSTER")

    # Once the run left `pending` no guarded launcher starts it anywhere.
    moved_on = await launch(
        drill, active, drill.local, clusters=during, ledger=elsewhere, run_phase="active"
    )
    assert (moved_on.admitted, moved_on.code) == (False, "RUN_NOT_PENDING")

    # New runs may target the local cluster during the outage.
    fresh = f"run-new-{uuid.uuid4().hex[:8]}"
    admitted = await launch(drill, fresh, drill.local, clusters=during, ledger=ledger)
    assert (admitted.admitted, admitted.code) == (True, "BOUND")
    assert admitted.unverified_clusters == ("cloud",)

    # Evidence: the active mission has no execution in the local cluster, still exactly one in
    # the cloud cluster; the new run exists only locally.
    assert await executions(drill, drill.local, active) == []
    assert await executions(drill, drill.cloud, active) == original
    assert len(await executions(drill, drill.local, fresh)) == 1
    assert await executions(drill, drill.cloud, fresh) == []

    # Recovery: the original cluster is back; relaunching there attaches to the same execution.
    recovered = await launch(
        drill, active, drill.cloud, clusters=(drill.cloud, drill.local), ledger=ledger
    )
    assert (recovered.admitted, recovered.code) == (True, "BOUND_HERE")
    assert await executions(drill, drill.cloud, active) == original
    assert await executions(drill, drill.local, active) == []
