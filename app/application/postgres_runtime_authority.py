from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

import asyncpg

from app.application.postgres_run_control_repository import PostgresRunControlRepository
from app.application.postgres_runtime_execution_repository import (
    PostgresRuntimeCoordinationRepository,
)
from app.application.postgres_stage3_kernel_repository import PostgresDecisionRepository
from app.application.runtime_bootstrap import (
    AuthoritativeRuntimeProjection,
    BootstrapRequest,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import ExecutionEpochKey
from app.domain.graph_runtime.kernel import DecisionRequest
from app.domain.run_control.contracts import RunPhase


class PostgresBootstrapAuthority:
    """Loads bootstrap authority only from BellLabs application PostgreSQL."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._bindings = PostgresRuntimeCoordinationRepository(pool)
        self._runs = PostgresRunControlRepository(pool)

    async def load(self, epoch: ExecutionEpochKey) -> AuthoritativeRuntimeProjection:
        binding = await self._bindings.get_binding(epoch)
        if binding is None:
            raise LookupError("authoritative runtime binding is unavailable")
        run = await self._runs.get_run(epoch.request_scope, epoch.belllabs_run_id)
        budget = await self._runs.get_budget(epoch.request_scope, epoch.belllabs_run_id)
        lifecycle_digest = sha256_digest(run.model_dump(mode="json"))
        budget_digest = sha256_digest(budget.model_dump(mode="json"))
        decision_digest = sha256_digest(
            {
                "active_waits": run.active_waits,
                "active_pauses": run.active_pauses,
                "resume_decisions": run.resume_decisions,
            }
        )
        return AuthoritativeRuntimeProjection(
            binding=binding,
            lifecycle_version=run.version,
            lifecycle_projection_ref=f"run:{run.run_id}:version:{run.version}",
            lifecycle_projection_digest=lifecycle_digest,
            budget_projection_ref=f"budget:{budget.account_id}:{budget_digest}",
            decision_projection_ref=f"decisions:{run.run_id}:{decision_digest}",
            cancellation_requested=run.phase == RunPhase.CANCELLING,
            terminal=run.phase == RunPhase.TERMINAL,
        )


class PostgresBootstrapDecisionBridge:
    """Persists a stable BellLabs decision before LangGraph can interrupt."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._decisions = PostgresDecisionRepository(pool)

    async def persist_reconciliation_decision(
        self,
        request: BootstrapRequest,
        current: AuthoritativeRuntimeProjection,
        reason_code: str,
    ) -> str:
        decision_id = "decision-" + str(
            uuid5(
                NAMESPACE_URL,
                ":".join(
                    (
                        "runtime-bootstrap-reconciliation",
                        request.epoch.canonical_key,
                        current.binding.binding_id,
                        str(current.binding.version),
                        str(current.lifecycle_version),
                        reason_code,
                    )
                ),
            )
        )
        values = {
            "decision_id": decision_id,
            "request_scope": request.epoch.request_scope,
            "binding_id": current.binding.binding_id,
            "decision_type": "runtime_reconciliation",
            "schema_ref": "schema:runtime-reconciliation-decision.v1",
            "choices_ref": "choices:runtime-reconciliation-actions.v1",
            "evidence_refs": (
                f"binding:{current.binding.binding_id}",
                f"reason:{reason_code}",
            ),
            "expected_lifecycle_version": current.lifecycle_version,
            "policy_ref": "policy:runtime-reconciliation-operator.v1",
            "requested_at": current.binding.updated_at,
            "expires_at": None,
        }
        decision = DecisionRequest(
            **values,
            request_digest=sha256_digest(values),
        )
        await self._decisions.create(decision)
        return decision_id
