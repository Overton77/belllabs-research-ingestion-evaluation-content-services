from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.agent_server.bootstrap import make_bootstrap_node
from app.agent_server.goal_directed.graph import (
    NODE_BOOTSTRAP_RUNTIME_AUTHORITY as GOAL_BOOTSTRAP,
)
from app.agent_server.goal_directed.graph import build_goal_directed_graph
from app.agent_server.runtime_composition import (
    configure_bootstrap_reconciler,
    reset_bootstrap_reconciler,
)
from app.agent_server.stagegraph.graph import (
    NODE_BOOTSTRAP_RUNTIME_AUTHORITY as STAGE_BOOTSTRAP,
)
from app.agent_server.stagegraph.graph import build_stagegraph
from app.application.runtime_bootstrap import (
    AuthoritativeRuntimeProjection,
    RuntimeBootstrapReconciler,
)
from app.domain.graph_runtime.contracts import RuntimeExecutionBinding
from app.domain.graph_runtime.identities import ExecutionEpochKey

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def binding() -> RuntimeExecutionBinding:
    return RuntimeExecutionBinding(
        binding_id="binding-1",
        epoch=ExecutionEpochKey(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            execution_epoch=1,
        ),
        submission_id="submission-1",
        submission_idempotency_key="submission-1",
        submission_digest=DIGEST,
        run_plan_digest=DIGEST,
        graph_assembly_digest=DIGEST,
        state_schema_digest=DIGEST,
        runtime_provider="legacy_temporal",
        created_at=NOW,
        updated_at=NOW,
    )


class Authority:
    async def load(self, _epoch):  # type: ignore[no-untyped-def]
        return AuthoritativeRuntimeProjection(
            binding=binding(),
            lifecycle_version=3,
            lifecycle_projection_ref="lifecycle:3",
            lifecycle_projection_digest=DIGEST,
            budget_projection_ref="budget:3",
            decision_projection_ref="decisions:3",
        )


class DecisionBridge:
    def __init__(self) -> None:
        self.reasons: list[str] = []

    async def persist_reconciliation_decision(
        self,
        _request,  # type: ignore[no-untyped-def]
        _current,  # type: ignore[no-untyped-def]
        reason_code: str,
    ) -> str:
        self.reasons.append(reason_code)
        return "decision-bootstrap-1"


def runtime(scope: str = "tenant-1"):  # type: ignore[no-untyped-def]
    return SimpleNamespace(
        server_info=SimpleNamespace(
            user=SimpleNamespace(permissions=(f"request_scope:{scope}",))
        )
    )


@pytest.mark.asyncio
async def test_common_bootstrap_node_rebuilds_projection_from_authority() -> None:
    node = make_bootstrap_node(RuntimeBootstrapReconciler(Authority()))
    state = {
        "request_scope": "tenant-1",
        "belllabs_run_id": "run-1",
        "execution_epoch": 1,
        "runtime_binding_ref": "binding-1",
        "run_plan_digest": DIGEST,
        "graph_assembly_digest": DIGEST,
        "state_schema_digest": DIGEST,
    }

    update = await node(state, runtime())

    assert update["runtime_binding_ref"] == "binding-1"
    assert update["lifecycle_projection_version"] == 3
    assert update["event_refs"] == ("runtime-bootstrap:checkpoint_projection_missing",)


@pytest.mark.asyncio
async def test_common_bootstrap_requires_scope_and_configured_authority() -> None:
    state = {
        "request_scope": "tenant-1",
        "belllabs_run_id": "run-1",
        "execution_epoch": 1,
        "runtime_binding_ref": "binding-1",
        "run_plan_digest": DIGEST,
        "graph_assembly_digest": DIGEST,
        "state_schema_digest": DIGEST,
    }

    with pytest.raises(PermissionError, match="outside the authenticated request scope"):
        await make_bootstrap_node(RuntimeBootstrapReconciler(Authority()))(
            state,
            runtime("tenant-2"),
        )
    with pytest.raises(RuntimeError, match="not configured"):
        await make_bootstrap_node(None)(state, runtime())


def test_both_graph_families_start_with_the_common_bootstrap_node() -> None:
    stage_graph = build_stagegraph().get_graph()
    goal_graph = build_goal_directed_graph().get_graph()

    assert STAGE_BOOTSTRAP in stage_graph.nodes
    assert GOAL_BOOTSTRAP in goal_graph.nodes
    assert STAGE_BOOTSTRAP in stage_graph.edges[0].target
    assert GOAL_BOOTSTRAP in goal_graph.edges[0].target


@pytest.mark.asyncio
async def test_incompatible_bootstrap_persists_decision_before_compact_interrupt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge = DecisionBridge()
    captured: list[dict[str, str]] = []
    monkeypatch.setattr(
        "app.agent_server.bootstrap.interrupt",
        lambda payload: captured.append(payload),
    )
    state = {
        "request_scope": "tenant-1",
        "belllabs_run_id": "run-1",
        "execution_epoch": 1,
        "runtime_binding_ref": "wrong-binding",
        "run_plan_digest": DIGEST,
        "graph_assembly_digest": DIGEST,
        "state_schema_digest": DIGEST,
    }

    await make_bootstrap_node(RuntimeBootstrapReconciler(Authority(), bridge))(
        state,
        runtime(),
    )

    assert bridge.reasons == ["runtime_binding_identity_mismatch"]
    assert captured == [
        {
            "decision_id": "decision-bootstrap-1",
            "decision_type": "runtime_reconciliation",
            "binding_id": "binding-1",
            "reason_code": "runtime_binding_identity_mismatch",
        }
    ]


@pytest.mark.asyncio
async def test_published_graph_node_uses_process_lifespan_composition() -> None:
    configure_bootstrap_reconciler(RuntimeBootstrapReconciler(Authority()))
    try:
        node = make_bootstrap_node(None)
        update = await node(
            {
                "request_scope": "tenant-1",
                "belllabs_run_id": "run-1",
                "execution_epoch": 1,
                "runtime_binding_ref": "binding-1",
                "run_plan_digest": DIGEST,
                "graph_assembly_digest": DIGEST,
                "state_schema_digest": DIGEST,
            },
            runtime(),
        )
    finally:
        reset_bootstrap_reconciler()

    assert update["runtime_binding_ref"] == "binding-1"
