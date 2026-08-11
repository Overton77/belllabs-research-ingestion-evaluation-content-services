from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from langgraph.types import interrupt

from app.agent_server.context import require_runtime_scope
from app.agent_server.runtime_composition import get_bootstrap_reconciler
from app.application.runtime.runtime_bootstrap import (
    BootstrapRequest,
    CheckpointRuntimeProjection,
    RuntimeBootstrapReconciler,
)
from app.domain.graph_runtime.identities import ExecutionEpochKey


def make_bootstrap_node(
    reconciler: RuntimeBootstrapReconciler | None,
):
    async def bootstrap_runtime_authority(
        state: Mapping[str, Any],
        runtime: Any,
    ) -> dict[str, object]:
        require_runtime_scope(runtime, state["request_scope"])
        active_reconciler = reconciler or get_bootstrap_reconciler()
        checkpoint = _checkpoint_projection(state)
        decision = await active_reconciler.reconcile(
            BootstrapRequest(
                epoch=ExecutionEpochKey(
                    request_scope=state["request_scope"],
                    belllabs_run_id=state["belllabs_run_id"],
                    execution_epoch=state["execution_epoch"],
                ),
                runtime_binding_ref=state["runtime_binding_ref"],
                run_plan_digest=state["run_plan_digest"],
                graph_assembly_digest=state["graph_assembly_digest"],
                state_schema_digest=state["state_schema_digest"],
                checkpoint_projection=checkpoint,
            )
        )
        if decision.action == "interrupt_for_reconciliation":
            if decision.decision_id is None:
                raise RuntimeError(
                    "runtime reconciliation decision was not persisted before interrupt"
                )
            interrupt(
                {
                    "decision_id": decision.decision_id,
                    "decision_type": "runtime_reconciliation",
                    "binding_id": decision.binding_id,
                    "reason_code": decision.reason_code,
                }
            )
        return {
            "runtime_binding_ref": decision.binding_id,
            "lifecycle_projection_ref": decision.lifecycle_projection_ref,
            "lifecycle_projection_version": decision.lifecycle_version,
            "lifecycle_projection_digest": decision.lifecycle_projection_digest,
            "event_refs": (f"runtime-bootstrap:{decision.reason_code}",),
        }

    return bootstrap_runtime_authority


def _checkpoint_projection(
    state: Mapping[str, Any],
) -> CheckpointRuntimeProjection | None:
    required = {
        "checkpoint_binding_version",
        "lifecycle_projection_version",
        "lifecycle_projection_digest",
    }
    if not required <= state.keys():
        return None
    return CheckpointRuntimeProjection(
        binding_id=state["runtime_binding_ref"],
        binding_version=state["checkpoint_binding_version"],
        lifecycle_version=state["lifecycle_projection_version"],
        lifecycle_projection_digest=state["lifecycle_projection_digest"],
        run_plan_digest=state["run_plan_digest"],
        graph_assembly_digest=state["graph_assembly_digest"],
        state_schema_digest=state["state_schema_digest"],
        deployment_endpoint_id=state.get("deployment_endpoint_id"),
        deployment_revision=state.get("deployment_revision"),
        graph_id=state.get("graph_id"),
    )
