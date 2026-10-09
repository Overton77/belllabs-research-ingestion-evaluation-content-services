"""`run_cluster_binding` (migration 0032): the Temporal cluster a run was bound to (MP-22).

Written once by `RunLaunchService.launch` before the submit, under the run's tenant scope
(forced RLS), keyed by the application `run_key` the launch service holds as `run_id`. A
second bind of the same run returns the stored row unchanged whatever cluster the caller
names; the launch service compares and refuses. Rows are immutable (trigger) and only the
runtime role inserts.
"""

from __future__ import annotations

from datetime import datetime

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import begin
from mission_control.application.execution.run_launch import (
    RunClusterBinding,
    TemporalClusterIdentity,
)

_COLUMNS = (
    "installation_id, application_id, tenant_id, run_key, workflow_id, cluster_id, "
    "temporal_target, temporal_address, temporal_namespace, task_queue, recorded_at"
)


class PostgresRunClusterBindings:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def bind(
        self,
        request_scope: str,
        run_id: str,
        *,
        workflow_id: str,
        cluster: TemporalClusterIdentity,
        now: datetime,
    ) -> RunClusterBinding:
        async with self._pool.acquire() as connection, connection.transaction():
            scope = await begin(connection, request_scope)
            await connection.execute(
                f"INSERT INTO mission_control.run_cluster_binding ({_COLUMNS}) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT DO NOTHING",
                *scope,
                run_id,
                workflow_id,
                cluster.cluster_id,
                cluster.target,
                cluster.address,
                cluster.namespace,
                cluster.task_queue,
                now,
            )
            row = await connection.fetchrow(
                "SELECT run_key, workflow_id, cluster_id, temporal_target, temporal_address, "
                "temporal_namespace, task_queue, recorded_at "
                "FROM mission_control.run_cluster_binding "
                "WHERE installation_id = $1 AND application_id = $2 AND tenant_id = $3 "
                "AND run_key = $4",
                *scope,
                run_id,
            )
        if row is None:
            raise RuntimeError("run cluster binding was not persisted")
        return RunClusterBinding(
            run_id=str(row["run_key"]),
            workflow_id=row["workflow_id"],
            cluster=TemporalClusterIdentity(
                cluster_id=row["cluster_id"],
                target=row["temporal_target"],
                address=row["temporal_address"],
                namespace=row["temporal_namespace"],
                task_queue=row["task_queue"],
            ),
            recorded_at=row["recorded_at"],
        )

    async def get(self, request_scope: str, run_id: str) -> RunClusterBinding | None:
        async with self._pool.acquire() as connection, connection.transaction():
            scope = await begin(connection, request_scope)
            row = await connection.fetchrow(
                "SELECT run_key, workflow_id, cluster_id, temporal_target, temporal_address, "
                "temporal_namespace, task_queue, recorded_at "
                "FROM mission_control.run_cluster_binding "
                "WHERE installation_id = $1 AND application_id = $2 AND tenant_id = $3 "
                "AND run_key = $4",
                *scope,
                run_id,
            )
        if row is None:
            return None
        return RunClusterBinding(
            run_id=str(row["run_key"]),
            workflow_id=row["workflow_id"],
            cluster=TemporalClusterIdentity(
                cluster_id=row["cluster_id"],
                target=row["temporal_target"],
                address=row["temporal_address"],
                namespace=row["temporal_namespace"],
                task_queue=row["task_queue"],
            ),
            recorded_at=row["recorded_at"],
        )


__all__ = ["PostgresRunClusterBindings"]
