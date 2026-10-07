"""Exercise the public transport against the actual snapshot/fork state machine."""

from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from tests.fixtures.checkpoint_recovery import recovery_harness, stage_recovery_unit
from tests.fixtures.run_forks import (
    FakeForkSourceReader,
    compose_in_memory_forks,
    inspection_reads,
    stage_policy,
    stagegraph_head,
)
from tests.unit.run_control.test_run_control import WORKFLOW_DIGEST

from mission_control.application.missions.runtime import MissionControlRuntimeService
from mission_control.application.recovery.run_forks import ForkPatchPolicyRegistry
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    get_mission_principal,
    get_runtime_service,
    router,
)


@pytest.mark.asyncio
async def test_safe_snapshot_and_semantic_fork_over_scoped_http():
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
    service = MissionControlRuntimeService(
        forks.snapshots,
        forks.forks,
        request_scope="tenant-1",
    )
    actor = ActorContext(
        actor_id="operator",
        permissions={
            "workflow_run.snapshot",
            "workflow_run.fork",
            "workflow_run.admit",
        },
        authority_refs={"authority:lifecycle"},
    )
    caller = MissionPrincipal(
        application_id="biotech",
        installation_id=uuid4(),
        tenant_id=uuid4(),
        issuer="https://issuer.invalid",
        audiences={"authenticated"},
        actor=actor,
        sponsorship_refs={"sponsorship:test"},
        approval_refs={"approval:test"},
    )
    app = FastAPI()
    app.include_router(router)
    # The registered real service is bound to the legacy fixture scope. Installation
    # authorization and selection are covered separately; no domain service is mocked.
    app.dependency_overrides[get_runtime_service] = lambda: service
    app.dependency_overrides[get_mission_principal] = lambda: caller
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    base = f"/v1/applications/biotech/runs/{harness.run_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        stale = await client.post(f"{base}/snapshots", json={"expected_version": run.version + 1})
        assert stale.status_code == 409, stale.text
        taken = await client.post(f"{base}/snapshots", json={"expected_version": run.version})
        assert taken.status_code == 201, taken.text
        envelope = taken.json()
        snapshot = envelope["snapshot"]
        assert envelope["application_id"] == "biotech"
        assert snapshot["boundary_kind"] == "stage_settled"
        read = await client.get(f"{base}/snapshots/{snapshot['snapshot_id']}")
        assert read.json() == envelope
        wrong_run = await client.get(
            f"/v1/applications/biotech/runs/other/snapshots/{snapshot['snapshot_id']}",
        )
        assert wrong_run.status_code == 404
        body = {
            "request_id": str(uuid4()),
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot_digest": snapshot["snapshot_digest"],
            "changes": [{"path": "stage_objectives.review", "value": "Review strictly."}],
            "invalidation_frontier": ["review"],
            "baseline_reservations": {"tokens.total": 20},
            "sponsorship_ref": "sponsorship:test",
            "approval_refs": ["approval:test"],
            "reason": "Authorized semantic fork",
        }
        mismatch = await client.post(
            f"{base}/forks",
            json=body,
            headers={"Idempotency-Key": str(uuid4())},
        )
        assert mismatch.status_code == 409
        forked = await client.post(f"{base}/forks", json=body)
        assert forked.status_code == 202, forked.text
        replay = await client.post(f"{base}/forks", json=body)
        assert replay.json() == forked.json()
        assert forked.json()["request_id"] == body["request_id"]
        assert forked.json()["receipt"]["target_run_id"] != harness.run_id
        changed = await client.post(f"{base}/forks", json={**body, "reason": "Different intent"})
        assert changed.status_code == 409, changed.text
