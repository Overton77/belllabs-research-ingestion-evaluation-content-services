"""The live context an approval is revalidated against, read from ``harness_execution`` (MP-11).

No new column: the execution generation is the one MP-06 records as the session owner's
generation (``native_identity.mc_session_owner.generation``; every ``lane.turn`` claims the
owner and admits its dispatches through the Stop Fence with this same generation). The
policy digest is the execution's binding digest (``actual_binding_digest``, else
``intended_binding_digest``: the ``mc.execution_binding`` digest covers effective
permissions and the policy digest, so any binding change conservatively invalidates an
approval). A lost or missing execution, or one with no recorded owner yet, reports
``granted=False`` so nothing is applied.

Lane adapters therefore bind native requests with ``policy_digest`` = the execution's binding
digest and ``generation`` = the turn request's generation.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.lanes.execution_state import OWNER_KEY
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import scoped
from mission_control.application.execution.approvals import ApprovalContextState

_UNKNOWN_DIGEST = "sha256:" + "0" * 64


class PostgresApprovalContextProbe:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def current(
        self, request_scope: str, *, run_id: str, harness_execution_id: str
    ) -> ApprovalContextState:
        try:
            execution = UUID(harness_execution_id)
        except ValueError:
            return _refused()
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await connection.fetchrow(
                f"""
                SELECT execution.native_identity, execution.recovery_state,
                       execution.intended_binding_digest, execution.actual_binding_digest,
                       run.run_key
                FROM mission_control.harness_execution execution
                JOIN mission_control.mission_run run
                  ON run.installation_id = execution.installation_id
                 AND run.application_id = execution.application_id
                 AND run.tenant_id = execution.tenant_id
                 AND run.run_id = execution.run_id
                WHERE {scoped("execution")} AND execution.harness_execution_id = $4
                """,
                *args,
                execution,
            )
        if row is None or row["run_key"] != run_id:
            return _refused()
        owner = _identity(row["native_identity"]).get(OWNER_KEY)
        generation = owner.get("generation") if isinstance(owner, Mapping) else None
        if not isinstance(generation, int) or generation < 1:
            return _refused()
        return ApprovalContextState(
            generation=generation,
            policy_digest=row["actual_binding_digest"] or row["intended_binding_digest"],
            granted=row["recovery_state"] != "lost",
        )


def _identity(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value) if isinstance(value, Mapping) else {}


def _refused() -> ApprovalContextState:
    return ApprovalContextState(generation=1, policy_digest=_UNKNOWN_DIGEST, granted=False)


__all__ = ["PostgresApprovalContextProbe"]
