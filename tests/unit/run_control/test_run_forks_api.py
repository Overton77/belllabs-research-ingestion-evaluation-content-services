"""RRM-006: `/run-control/v1` snapshot and fork routes (scope, permissions, typed errors)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from app.api.control_plane import ControlPlanePrincipal, get_control_plane_principal
from app.api.run_forks import RunForkServices, get_run_fork_services
from app.application.runtime.run_forks import ForkPatchPolicyRegistry
from app.domain.run_control.contracts import ReserveBudgetAction
from app.server import api
from tests.fixtures.checkpoint_recovery import recovery_harness, stage_recovery_unit
from tests.fixtures.run_forks import (
    FakeForkSourceReader,
    compose_in_memory_forks,
    inspection_reads,
    stage_policy,
    stagegraph_head,
)
from tests.unit.run_control.test_run_control import WORKFLOW_DIGEST, command

SCOPE = "tenant-1"


def principal(
    *,
    roles: frozenset[str] = frozenset({"operator", "fork_operator"}),
    scopes: frozenset[str] = frozenset({SCOPE}),
) -> ControlPlanePrincipal:
    return ControlPlanePrincipal(
        actor_id="operator",
        roles=roles,
        tenant_scopes=scopes,
        authority_refs=frozenset({"authority:lifecycle"}),
        sponsorship_refs=frozenset({"sponsorship:test"}),
        approval_refs=frozenset({"approval:test"}),
    )


@pytest.fixture
async def world() -> AsyncIterator[dict[str, Any]]:
    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id, "draft")
    result = await harness.run(await harness.request(unit))
    assert result.status == "completed"
    sources = FakeForkSourceReader(
        harness.run_control,
        heads={harness.run_id: (stagegraph_head(stages={"draft": "completed"}),)},
    )
    policies = ForkPatchPolicyRegistry()
    policies.register(WORKFLOW_DIGEST, stage_policy())
    forks = compose_in_memory_forks(
        harness.run_control,
        harness.repository,
        inspection_reads(harness.repository, harness.lineage, harness.journal),
        sources,
        policies=policies,
    )
    services = RunForkServices(
        snapshots=forks.snapshots,
        forks=forks.forks,
        receipts=forks.repository,
        materializations=forks.store,
    )
    caller = {"principal": principal()}
    api.dependency_overrides[get_run_fork_services] = lambda: services
    api.dependency_overrides[get_control_plane_principal] = lambda: caller["principal"]
    try:
        yield {"harness": harness, "forks": forks, "caller": caller}
    finally:
        api.dependency_overrides.pop(get_run_fork_services, None)
        api.dependency_overrides.pop(get_control_plane_principal, None)


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=api), base_url="http://forks")


def _fork_body(snapshot: dict[str, Any], **values: Any) -> dict[str, Any]:
    return {
        "request_scope": SCOPE,
        "request_id": "fork-api-1",
        "idempotency_key": "fork-api-1",
        "snapshot_id": snapshot["snapshot_id"],
        "snapshot_digest": snapshot["snapshot_digest"],
        "changes": [{"path": "stage_objectives.review", "value": "Review strictly."}],
        "invalidation_frontier": ["review"],
        "baseline_reservations": {"tokens.total": 20},
        "sponsorship_ref": "sponsorship:test",
        "approval_refs": ["approval:test"],
        "reason": "technical fork through the facade",
        **values,
    }


@pytest.mark.asyncio
async def test_snapshot_and_fork_through_the_run_control_facade(world: dict[str, Any]) -> None:
    harness = world["harness"]
    run_id = harness.run_id
    run = await harness.run_control.get_run(SCOPE, run_id)
    base = f"/run-control/v1/runs/{run_id}"
    async with _client() as client:
        stale = await client.post(
            f"{base}/snapshots",
            json={"request_scope": SCOPE, "expected_run_version": run.version + 5},
        )
        assert stale.status_code == 409
        assert stale.json()["detail"]["code"] == "stale_expected_version"

        taken = await client.post(
            f"{base}/snapshots", json={"request_scope": SCOPE, "expected_run_version": run.version}
        )
        assert taken.status_code == 201, taken.text
        snapshot = taken.json()
        assert snapshot["schema_version"] == "belllabs.run-snapshot.v1"
        assert snapshot["boundary_kind"] == "stage_settled"
        read = await client.get(
            f"{base}/snapshots/{snapshot['snapshot_id']}", params={"request_scope": SCOPE}
        )
        assert read.json() == snapshot
        elsewhere = await client.get(
            f"/run-control/v1/runs/other/snapshots/{snapshot['snapshot_id']}",
            params={"request_scope": SCOPE},
        )
        assert elsewhere.status_code == 404

        protected = await client.post(
            f"{base}/forks",
            json=_fork_body(snapshot, changes=[{"path": "budget.tokens.total", "value": 1}]),
        )
        assert protected.status_code == 422
        assert protected.json()["detail"] == {
            "code": "protected_field",
            "message": "a fork patch cannot change protected fields",
            "reasons": ["budget.tokens.total"],
        }
        stale_snapshot = await client.post(
            f"{base}/forks", json=_fork_body(snapshot, snapshot_digest="sha256:" + "0" * 64)
        )
        assert stale_snapshot.status_code == 409
        assert stale_snapshot.json()["detail"]["code"] == "stale_snapshot"
        unknown = await client.post(
            f"{base}/forks", json=_fork_body(snapshot, snapshot_id="run-snapshot:" + "f" * 64)
        )
        assert unknown.status_code == 404
        unsponsored = await client.post(
            f"{base}/forks", json=_fork_body(snapshot, sponsorship_ref="sponsorship:other")
        )
        assert unsponsored.status_code == 403

        forked = await client.post(f"{base}/forks", json=_fork_body(snapshot))
        assert forked.status_code == 201, forked.text
        receipt = forked.json()
        assert receipt["target_execution_epoch"] == 1
        assert receipt["source_run_id"] == run_id
        assert receipt["lineage"]["snapshot_digest"] == snapshot["snapshot_digest"]
        replay = await client.post(f"{base}/forks", json=_fork_body(snapshot))
        assert replay.status_code == 201 and replay.json() == receipt
        conflict = await client.post(
            f"{base}/forks", json=_fork_body(snapshot, reason="another intent")
        )
        assert conflict.status_code == 409
        assert conflict.json()["detail"]["code"] == "idempotency_conflict"

        view = await client.get("/run-control/v1/forks/fork-api-1", params={"request_scope": SCOPE})
        assert view.status_code == 200
        assert view.json()["receipt"] == receipt
        assert [item["decision"] for item in view.json()["reuse_decisions"]] == ["reuse"]
        assert (
            await client.get("/run-control/v1/forks/fork-api-1", params={"request_scope": "x"})
        ).status_code == 404


@pytest.mark.asyncio
async def test_scope_permission_and_quiescence_are_enforced(world: dict[str, Any]) -> None:
    harness = world["harness"]
    run_id = harness.run_id
    base = f"/run-control/v1/runs/{run_id}"
    async with _client() as client:
        world["caller"]["principal"] = principal(scopes=frozenset({"tenant-2"}))
        assert (
            await client.post(f"{base}/snapshots", json={"request_scope": SCOPE})
        ).status_code == 404
        world["caller"]["principal"] = principal(roles=frozenset({"operator", "auditor"}))
        denied = await client.post(f"{base}/snapshots", json={"request_scope": SCOPE})
        assert denied.status_code == 403
        assert denied.json()["detail"] == "workflow_run.snapshot permission required"
        assert (
            await client.post(
                f"{base}/forks",
                json=_fork_body({"snapshot_id": "s", "snapshot_digest": "sha256:" + "0" * 64}),
            )
        ).status_code == 403
        world["caller"]["principal"] = principal()
        assert (
            await client.post(
                "/run-control/v1/runs/unknown/snapshots", json={"request_scope": SCOPE}
            )
        ).status_code == 404

        run = await harness.run_control.get_run(SCOPE, run_id)
        await harness.run_control.execute(
            command(
                run_id,
                run.version,
                "open-reservation",
                ReserveBudgetAction(reservation_id="reservation:open", amounts={"tokens.total": 1}),
            )
        )
        busy = await client.post(f"{base}/snapshots", json={"request_scope": SCOPE})
        assert busy.status_code == 409
        assert busy.json()["detail"]["code"] == "snapshot_not_quiescent"
        assert busy.json()["detail"]["reasons"] == ["budget_reservation_open:reservation:open"]
