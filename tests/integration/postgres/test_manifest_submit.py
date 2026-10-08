"""FT-E3: Mission Manifest submit and start on a disposable PostgreSQL 17 (common component).

Compile resolves against the seeded catalog fixture (in memory, lexical); submit publishes the
lowered definitions and the Compiled Program into the installation catalog in PostgreSQL and
commits the revision, typed definition rows, authoring provenance and the admitted run (or the
chain) in one transaction under the restricted runtime role. Start goes through the governed
``RunLaunchService`` with a recording submitter (no Temporal, no provider).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.control_plane.manifest_submission import (
    PostgresManifestSubmissionRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.application.authoring.manifest_service import (
    ManifestCompileService,
    ManifestProgramCompiler,
)
from mission_control.application.authoring.manifest_submit import (
    ManifestBlocked,
    ManifestIdempotencyConflict,
    ManifestSubmitService,
    SubmitRequest,
    register_manifest_admission_policies,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.execution.run_launch import RunLaunchService
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    F1RunConfigurationVerifier,
    RunControlService,
)
from mission_control.application.programs.service import GoalDirectedLaunchService
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.domain.authoring.manifest import canonical_manifest_bytes, load_manifest_yaml
from mission_control.domain.coordinator.launch import BlueprintFamily, WorkflowSubmission
from mission_control.domain.policies.contracts import ActorContext
from tests.fixtures.catalog.fast_track_catalog import fast_track_catalog
from tests.fixtures.mission_control_common_db import CommonDatabase, catalog_scope
from tests.integration.postgres.runtime_common import common_db as common_db

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
MINIMAL = ROOT / "tests/fixtures/manifests/minimal-stage-graph.yml"
MISSION_2 = (
    ROOT / "docs/specs/fast-track-2026-10/missions/02-research-ingestion-cursor-cloud-chain.yml"
)
OWNER = ActorContext(
    actor_id="owner",
    authority_refs=frozenset({"authority:owner"}),
    permissions=frozenset({"workflow_run.admit", "workflow_run.start", "workflow_run.read"}),
)


class RecordingSubmitter:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def submit(
        self,
        workflow_input: object,
        *,
        workflow_id: str,
        blueprint_family: BlueprintFamily,
        parent_run_id: str | None = None,
        mission_id: str | None = None,
    ) -> WorkflowSubmission:
        self.calls.append(
            {"family": blueprint_family.value, "mission_id": mission_id, "input": workflow_input}
        )
        return WorkflowSubmission(workflow_id=f"wf:{workflow_id}", temporal_run_id="run-1")


class GoalInputs:
    """The admitted run's GoalDirected input, with a test semantic binding reference."""

    def __init__(self, launches: GoalDirectedLaunchService) -> None:
        self._launches = launches

    async def family_input(
        self, *, request_scope: str, run_id: str, family: str, initial_goal: str | None
    ) -> dict[str, Any]:
        assert family == "GoalDirected" and initial_goal is not None
        prepared = await self._launches.prepare(
            request_scope,
            run_id,
            initial_goal=initial_goal,
            semantic_input_binding_ref="semantic-input:manifest-test",
        )
        return asdict(prepared)


class Stack:
    def __init__(self, db: CommonDatabase, pool: asyncpg.Pool) -> None:
        self.db = db
        self.pool = pool
        self.scope = db.scope()
        self.submitter = RecordingSubmitter()

    async def build(self) -> ManifestSubmitService:
        definitions, search = await fast_track_catalog()
        catalog = PostgresDefinitionRepository(self.pool, catalog_scope=catalog_scope("biotech"))
        programs = ManifestProgramCompiler(catalog, ExtensionRegistry(), InMemoryPayloadStore())
        control_plane = ControlPlaneService(catalog, ExtensionRegistry(), InMemoryPayloadStore())
        policies = AdmissionPolicyRegistry()
        register_manifest_admission_policies(policies)
        register_manifest_admission_policies(policies)  # idempotent composition
        run_control = RunControlService(
            PostgresRunControlRepository(self.pool),
            F1RunConfigurationVerifier(control_plane),
            policies,
        )
        self.run_control = run_control
        self.submissions = PostgresManifestSubmissionRepository(self.pool)
        return ManifestSubmitService(
            compiler=ManifestCompileService(
                definitions=definitions, search=search, programs=programs
            ),
            programs=programs,
            run_control=run_control,
            submissions=self.submissions,
            request_scope=self.scope,
            launches=RunLaunchService(run_control=run_control, submitter=self.submitter),
            launch_inputs=GoalInputs(GoalDirectedLaunchService(run_control, control_plane)),
            subscriptions=SubscriptionService(PostgresSubscriptionStore(self.pool, self.scope)),
        )

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        async with self.pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, self.scope)
            return list(await connection.fetch(query, *args))

    async def val(self, query: str, *args: Any) -> Any:
        async with self.pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, self.scope)
            return await connection.fetchval(query, *args)


@pytest_asyncio.fixture
async def stack(common_db: CommonDatabase) -> AsyncIterator[Stack]:
    pool = await common_db.pool("mission_control_runtime")
    try:
        yield Stack(common_db, pool)
    finally:
        await pool.close()


def request(manifest_yaml: str, request_id: UUID | None = None) -> SubmitRequest:
    return SubmitRequest(
        manifest_yaml=manifest_yaml,
        request_id=request_id or uuid4(),
        actor=OWNER,
        sponsorship_refs=frozenset({"sponsorship:test"}),
    )


def goal_loop_manifest(title: str = "Goal loop sweep") -> str:
    document = load_manifest_yaml(MINIMAL.read_text(encoding="utf-8"))
    mission = document["mission"]
    mission["key"] = "goal-sweep"
    mission["title"] = title
    mission["program"] = {
        "key": "root",
        "behavior": "goal_loop",
        "objective": "evidence_map",
        "action_space": ["pubmed"],
        "outputs": [{"name": "claims", "schema": "claim_table@1"}],
    }
    mission["controls"]["subscriptions"] = [
        {"events": ["run.completed", "chain_link.released"], "channel": "stream"}
    ]
    return canonical_manifest_bytes(document).decode("utf-8")


@pytest.mark.asyncio
async def test_submit_commits_the_revision_and_admits_the_run_idempotently(stack: Stack) -> None:
    service = await stack.build()
    manifest = MINIMAL.read_text(encoding="utf-8")
    first_id = uuid4()
    receipt, replayed = await service.submit(request(manifest, first_id))
    assert not replayed and not receipt.unchanged and receipt.chain_id is None
    (mission,) = receipt.missions
    assert mission.family == "StageGraph" and mission.revision_no == 1
    assert mission.run_id is not None
    (run,) = await stack.fetch(
        "SELECT phase, mission_id, revision_id, created_by_actor_ref "
        "FROM mission_control.mission_run WHERE run_key = $1",
        mission.run_id,
    )
    assert (run["phase"], run["mission_id"], run["revision_id"]) == (
        "pending",
        mission.mission_id,
        mission.revision_id,
    )
    (snapshot,) = await stack.fetch(
        "SELECT snapshot.definition_contract_version, snapshot.definition_digest, "
        "snapshot.definition FROM mission_control.definition_snapshot AS snapshot "
        "JOIN mission_control.mission_revision AS revision "
        "ON revision.definition_snapshot_id = snapshot.definition_snapshot_id "
        "WHERE revision.revision_id = $1",
        mission.revision_id,
    )
    assert snapshot["definition_contract_version"] == "mc.mission_definition.v1"
    assert json.loads(snapshot["definition"])["mission_key"] == "minimal-sweep"
    program = json.loads(
        await stack.val(
            "SELECT program FROM mission_control.compiled_program WHERE revision_id = $1",
            mission.revision_id,
        )
    )
    assert program["effective_configuration_digest"] == mission.effective_configuration_digest
    assert program["definition_digest"] == snapshot["definition_digest"]
    # Typed definition rows of mig/0002.
    goals = await stack.fetch(
        "SELECT goal_key FROM mission_control.goal WHERE revision_id = $1", mission.revision_id
    )
    nodes = await stack.fetch(
        "SELECT node_key, parent_node_key, behavior_kind FROM mission_control.program_node "
        "WHERE revision_id = $1",
        mission.revision_id,
    )
    assert [row["goal_key"] for row in goals] == ["evidence_map"]
    assert {(row["node_key"], row["parent_node_key"], row["behavior_kind"]) for row in nodes} == {
        ("root", None, "stage_graph"),
        ("collect", "root", "goal_directed"),
        ("synthesize", "root", "operation"),
    }
    assert (
        await stack.val(
            "SELECT count(*) FROM mission_control.success_criterion WHERE revision_id = $1",
            mission.revision_id,
        )
        == 1
    )
    # Frozen run request: the admission receipt names the run.
    assert (
        await stack.val(
            "SELECT count(*) FROM mission_control.request_receipt "
            "WHERE action = 'mc.run.admit' AND resource_ref = $1",
            mission.run_id,
        )
        == 1
    )
    # Provenance with the revision; queryable and immutable.
    provenance = await stack.submissions.provenance(stack.scope, mission.revision_id)
    assert provenance is not None
    assert provenance["manifest_yaml"] == manifest
    assert provenance["resolution"]["schema_version"] == "mc.manifest_resolution.v1"
    assert provenance["resolution"]["catalog_resolution"] == "resolved"
    with pytest.raises(asyncpg.PostgresError):
        async with stack.pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, stack.scope)
            await connection.execute(
                "UPDATE mission_control.authoring_provenance SET manifest_yaml = 'x' "
                "WHERE revision_id = $1",
                mission.revision_id,
            )

    # Replay: the same ids. A changed digest under the same id: IDEMPOTENCY_CONFLICT.
    again, replayed = await service.submit(request(manifest, first_id))
    assert replayed and again == receipt
    changed = manifest.replace("Minimal literature sweep", "Minimal literature sweep v2")
    with pytest.raises(ManifestIdempotencyConflict):
        await service.submit(request(changed, first_id))
    # A new request id with an equal digest returns the head unchanged.
    unchanged, replayed = await service.submit(request(manifest))
    assert not replayed and unchanged.unchanged
    assert unchanged.missions[0].revision_id == mission.revision_id
    assert unchanged.missions[0].run_id == mission.run_id
    # A changed manifest is the next revision of the same mission, with its own run.
    revised, _ = await service.submit(request(changed))
    assert revised.missions[0].mission_id == mission.mission_id
    assert revised.missions[0].revision_no == 2
    assert revised.missions[0].run_id not in {None, mission.run_id}
    head = await stack.val(
        "SELECT scheduling_head_revision_id FROM mission_control.mission WHERE mission_id = $1",
        mission.mission_id,
    )
    assert head == revised.missions[0].revision_id


@pytest.mark.asyncio
async def test_blockers_and_foreign_applications_write_nothing(stack: Stack) -> None:
    service = await stack.build()
    document = load_manifest_yaml(MINIMAL.read_text(encoding="utf-8"))
    document["mission"]["application"] = "ai-engineer"
    before = await stack.val("SELECT count(*) FROM mission_control.mission")
    with pytest.raises(ManifestBlocked) as blocked:
        await service.submit(request(canonical_manifest_bytes(document).decode("utf-8")))
    assert blocked.value.issues[0].code.value == "APPLICATION_FORBIDDEN"
    assert await stack.val("SELECT count(*) FROM mission_control.mission") == before
    with pytest.raises(ManifestBlocked) as unsponsored:
        await service.submit(
            SubmitRequest(
                manifest_yaml=MINIMAL.read_text(encoding="utf-8"),
                request_id=uuid4(),
                actor=OWNER,
                sponsorship_refs=frozenset(),
            )
        )
    assert unsponsored.value.issues[0].reason == "no_sponsorship"
    assert await stack.val("SELECT count(*) FROM mission_control.mission") == before


@pytest.mark.asyncio
async def test_chain_submit_arms_links_and_admits_only_the_first_mission(stack: Stack) -> None:
    service = await stack.build()
    receipt, _ = await service.submit(request(MISSION_2.read_text(encoding="utf-8")))
    assert receipt.chain_id is not None
    research, ingestion = receipt.missions
    assert (research.mission_key, ingestion.mission_key) == ("research", "ingestion")
    assert research.run_id is not None and ingestion.run_id is None
    links = await stack.fetch(
        "SELECT link_key, state, from_mission_id, to_mission_id FROM mission_control.chain_link "
        "WHERE chain_id = $1 ORDER BY link_key",
        receipt.chain_id,
    )
    assert [(row["link_key"], row["state"]) for row in links] == [
        ("research->ingestion:depends_on", "armed"),
        ("research->ingestion:supplies", "armed"),
    ]
    assert {row["to_mission_id"] for row in links} == {ingestion.mission_id}
    (frozen,) = await stack.fetch(
        "SELECT mission_key, family, initial_goal, autostart, run_request "
        "FROM mission_control.chain_member_admission WHERE chain_id = $1",
        receipt.chain_id,
    )
    assert (frozen["mission_key"], frozen["family"], frozen["autostart"]) == (
        "ingestion",
        "GoalDirected",
        True,
    )
    assert "knowledge graph" in frozen["initial_goal"]
    assert (
        await stack.val(
            "SELECT count(*) FROM mission_control.mission_run WHERE mission_id = $1",
            ingestion.mission_id,
        )
        == 0
    )
    provenance_chains = await stack.fetch(
        "SELECT mission_key, chain_id FROM mission_control.authoring_provenance "
        "WHERE chain_id = $1 ORDER BY mission_key",
        receipt.chain_id,
    )
    assert [row["mission_key"] for row in provenance_chains] == ["ingestion", "research"]


@pytest.mark.asyncio
async def test_start_launches_with_the_mission_id_and_registers_subscriptions(
    stack: Stack,
) -> None:
    service = await stack.build()
    receipt, _ = await service.submit(request(goal_loop_manifest()))
    (mission,) = receipt.missions
    assert mission.family == "GoalDirected" and mission.run_id is not None
    started = await service.start(mission.run_id, OWNER)
    (call,) = stack.submitter.calls
    assert call["mission_id"] == str(mission.mission_id)
    assert call["family"] == "GoalDirected"
    assert started.family == "GoalDirected" and started.workflow_id.startswith("wf:")
    (subscription,) = started.subscriptions
    assert subscription.channel == "stream_ticket"
    rows = await stack.fetch(
        "SELECT target_kind, run_id FROM mission_control.mission_subscription "
        "WHERE mission_id = $1",
        mission.mission_id,
    )
    assert [(row["target_kind"], row["run_id"]) for row in rows] == [("run", mission.run_uuid)]
    # A second start re-registers nothing (the submitter fake leaves the run pending).
    again = await service.start(mission.run_id, OWNER)
    assert again.subscriptions[0].subscription_id == subscription.subscription_id
    assert (
        await stack.val(
            "SELECT count(*) FROM mission_control.mission_subscription WHERE mission_id = $1",
            mission.mission_id,
        )
        == 1
    )
    assert await service.head_run(mission.mission_id) == mission.run_id
