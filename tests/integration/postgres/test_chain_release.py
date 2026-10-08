"""FT-D2: the chain reducer releases the consumer inside the supplier's ledger transaction.

Real PostgreSQL 17 (forced RLS, restricted ``mission_control_runtime`` role, migration 0028):
the supplier run is driven through ``RunControlService`` with the Postgres repository, so every
event reaches ``canonical.append_events`` and its registered chain hook. Assertions read the
persisted rows only: link state, the consumer run (``pending``), the ``mc.chain.start_run``
outbox intent, the sealed chain-link packet, the Mission Graph rows and the chain events in
both member streams with one envelope ``event_id``.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from mission_control.adapters.operations.runtime_ports import FilesystemArtifactPayloadStore
from mission_control.adapters.postgres.chains.store import (
    HOOK_NAME,
    ChainReleaseHook,
    PostgresChainIntentStore,
    PostgresChainReader,
    insert_chain,
)
from mission_control.adapters.postgres.control_plane.mission_revisions import (
    insert_revision,
    lock_or_create_mission,
)
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.adapters.postgres.workspace_candidate_contents import (
    PostgresWorkspaceCandidateContents,
)
from mission_control.application.chains.relay import ChainIntentRelay, ChainStartReceipt
from mission_control.application.execution.service import RunControlService
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.manifest import load_manifest_yaml, parse_manifest
from mission_control.domain.composition.chain import (
    ChainMemberAdmission,
    ChainScope,
    MissionChain,
    build_mission_chain,
    compile_chain,
)
from mission_control.domain.context.packet import ContextPacket, ExpansionTier
from mission_control.domain.context.refs import workspace_candidate_ref
from mission_control.domain.execution.contracts import CapturedWorkspaceCandidate, WorkspaceOwner
from mission_control.domain.policies.contracts import (
    AcceptedObligationEvidence,
    AcceptedOutputEvidence,
    CancelAction,
    CommandStatus,
    RecordObligationEvidenceAction,
    RecordOutputEvidenceAction,
    RecordUsageAction,
    RunOutcome,
    StartAction,
    TerminalizationProposal,
    TerminalizeAction,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.unit.run_control.test_run_control import (
    WORKFLOW_DIGEST,
    command,
    request,
    service,
)

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "tests/fixtures/manifests/two-mission-chain.yml"
NOW = datetime(2026, 10, 8, 12, tzinfo=UTC)
EVIDENCE_MAP = json.dumps({"claims": [{"pmid": "1", "effect": "up"}]}).encode()


@pytest.fixture
def no_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate the ledger writer from hooks a production composition registered earlier."""

    monkeypatch.setattr(mc, "_POST_APPEND_HOOKS", {})


@pytest.fixture
def chain_hook(no_hooks: None) -> None:
    mc.register_post_append_hook(HOOK_NAME, ChainReleaseHook())


class Stack:
    def __init__(self, db: CommonDatabase, pool: asyncpg.Pool, payload_root: Path) -> None:
        self.db = db
        self.pool = pool
        self.scope = db.scope()
        repository = PostgresRunControlRepository(pool)
        self.research, _ = service(repository, required_obligations=frozenset({"evidence_map"}))  # type: ignore[arg-type]
        self.ingestion, _ = service(repository, required_obligations=frozenset({"ingested"}))  # type: ignore[arg-type]
        self.payloads = FilesystemArtifactPayloadStore(payload_root)
        self.chain: MissionChain | None = None
        self.supplier_run = ""

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        async with self.pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, self.scope)
            return list(await connection.fetch(query, *args))

    async def val(self, query: str, *args: Any) -> Any:
        async with self.pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, self.scope)
            return await connection.fetchval(query, *args)


def scoped(command_value: Any, scope: str) -> Any:
    return command_value.model_copy(update={"request_scope": scope})


async def submit(stack: Stack, *, autostart: bool = True) -> MissionChain:
    """The chain a manifest submit commits (FT-E3): supplier admitted, consumer frozen."""

    admitted = await stack.research.admit(
        request(request_scope=stack.scope, request_id=f"research-{uuid4()}")
    )
    assert admitted.run_id is not None
    stack.supplier_run = admitted.run_id
    research_mission, research_revision = (
        await stack.fetch(
            "SELECT mission_id, revision_id FROM mission_control.mission_run WHERE run_key = $1",
            admitted.run_id,
        )
    )[0]
    consumer_request = request(request_scope=stack.scope, request_id=f"ingestion-{uuid4()}")
    verified = await stack.ingestion.verify_admission(consumer_request)
    compilation = compile_chain(
        parse_manifest(load_manifest_yaml(FIXTURE.read_text(encoding="utf-8")))
    )
    assert compilation.resolution is not None
    parsed = parse_request_scope(stack.scope)
    async with stack.pool.acquire() as connection, connection.transaction():
        args = await mc.begin(connection, stack.scope)
        head = await lock_or_create_mission(
            connection,
            args,
            mission_key=f"ingestion-{uuid4().hex[:8]}",
            title="Ingestion",
            actor_ref="operator",
            at=NOW,
        )
        revision = await insert_revision(
            connection,
            args,
            mission_id=head.mission_id,
            definition_contract="mc.mission_definition.v1",
            definition={"mission_key": "ingestion"},
            definition_digest=sha256_digest({"mission_key": "ingestion"}),
            compiler_version="test",
            program_schema_version="mc.admitted-run-program/1",
            program={"program": "ingestion"},
            program_digest=sha256_digest({"program": "ingestion"}),
            policy_digest=consumer_request.effective_configuration_digest,
            binding_digest=sha256_digest({"binding": "ingestion"}),
            actor_ref="operator",
            at=NOW,
        )
        chain = build_mission_chain(
            resolution=compilation.resolution,
            chain_id=uuid4(),
            scope=ChainScope(
                installation_id=parsed.installation_id,
                application_id=parsed.application_id,
                tenant_id=parsed.tenant_id,
            ),
            chain_key=f"chain-{uuid4().hex[:8]}",
            title="Research then ingestion",
            manifest_digest="sha256:" + "c" * 64,
            members={
                "research": (research_mission, research_revision),
                "ingestion": (head.mission_id, revision.revision_id),
            },
            created_at=NOW,
            created_by_actor_ref="operator",
        )
        await insert_chain(
            connection,
            args,
            chain,
            [
                ChainMemberAdmission(
                    chain_id=chain.chain_id,
                    mission_id=head.mission_id,
                    mission_key="ingestion",
                    revision_id=revision.revision_id,
                    family="GoalDirected",
                    initial_goal="Ingest the supplied evidence map",
                    autostart=autostart,
                    run_request=consumer_request,
                    verified_configuration=verified,
                )
            ],
        )
    stack.chain = chain
    return chain


async def run_command(
    authority: RunControlService, stack: Stack, run_id: str, version: int, name: str, action: Any
) -> int:
    result = await authority.execute(
        scoped(command(run_id, version, name, action), stack.scope).model_copy(
            update={"occurred_at": NOW + timedelta(minutes=version)}
        )
    )
    assert result.status == CommandStatus.ACCEPTED, result
    return result.resulting_run_version


async def accept_goal_and_output(stack: Stack, run_id: str, version: int) -> tuple[int, str]:
    candidate = CapturedWorkspaceCandidate(
        namespace_id=f"run/{run_id}",
        workspace_id="workspace:research",
        output_slot="output",
        logical_path="/work/output/evidence_map.json",
        owner=WorkspaceOwner(kind="run", owner_id="research"),
        candidate_id=f"cand-{uuid4().hex[:12]}",
        content_digest=f"sha256:{sha256(EVIDENCE_MAP).hexdigest()}",
        media_type="application/json",
        size_bytes=len(EVIDENCE_MAP),
    )
    await PostgresWorkspaceCandidateContents(
        stack.pool, stack.payloads, request_scope=stack.scope
    ).put(candidate, EVIDENCE_MAP)
    output_ref = workspace_candidate_ref(candidate.candidate_id)
    version = await run_command(
        stack.research,
        stack,
        run_id,
        version,
        "accept-goal",
        RecordObligationEvidenceAction(evidence=OBLIGATION),
    )
    version = await run_command(
        stack.research,
        stack,
        run_id,
        version,
        "accept-output",
        RecordOutputEvidenceAction(
            evidence=AcceptedOutputEvidence(
                output_ref=output_ref,
                evidence_digest="sha256:" + "f" * 64,
                accepted_by_authority_ref="authority:lifecycle",
            )
        ),
    )
    return version, output_ref


OBLIGATION = AcceptedObligationEvidence(
    obligation_ref="evidence_map",
    evidence_digest="sha256:" + "e" * 64,
    accepted_by_authority_ref="authority:lifecycle",
)


async def terminalize(
    authority: RunControlService,
    stack: Stack,
    run_id: str,
    version: int,
    *,
    obligations: tuple[AcceptedObligationEvidence, ...],
    outputs: tuple[str, ...] = (),
    name: str = "terminalize",
    release: bool = True,
    cancelled: bool = False,
) -> int:
    if release:
        version = await release_baseline(authority, stack, run_id, version, name)
    projection = await authority.get_run(stack.scope, run_id)
    return await run_command(
        authority,
        stack,
        run_id,
        version,
        name,
        TerminalizeAction(
            proposal=TerminalizationProposal(
                proposal_id=f"{name}-proposal",
                expected_run_version=version,
                workflow_type_digest=WORKFLOW_DIGEST,
                obligation_revision="obligations:1",
                evidence_frontier_digest=projection.evidence_frontier_digest,
                accepted_obligation_evidence_digest=sha256_digest(
                    [item.model_dump(mode="json") for item in obligations]
                ),
                proposing_execution_binding_ref="execution:test",
                required_obligations_accepted=not cancelled,
                valid_output_refs=outputs,
                cancellation_settled=cancelled,
                budget_settled=True,
                effects_settled=True,
                proposed_at=NOW,
            )
        ),
    )


async def release_baseline(
    authority: RunControlService, stack: Stack, run_id: str, version: int, name: str
) -> int:
    return await run_command(
        authority,
        stack,
        run_id,
        version,
        f"{name}-release",
        RecordUsageAction(
            usage_id=f"{name}-release",
            reservation_id="baseline",
            actual_amounts={},
            release_amounts={"tokens.total": 20},
        ),
    )


async def link_rows(stack: Stack) -> dict[str, asyncpg.Record]:
    rows = await stack.fetch(
        "SELECT * FROM mission_control.chain_link WHERE chain_id = $1",
        stack.chain.chain_id if stack.chain else None,
    )
    return {row["link_key"]: row for row in rows}


async def chain_events(stack: Stack, event_type: str) -> list[asyncpg.Record]:
    assert stack.chain is not None
    return await stack.fetch(
        "SELECT mission_id, seq, payload FROM mission_control.mission_event "
        "WHERE event_type = $1 AND mission_id = ANY($2::uuid[]) ORDER BY mission_id, seq",
        event_type,
        [member.mission_id for member in stack.chain.members],
    )


@pytest_asyncio.fixture
async def stack(common_db: CommonDatabase, tmp_path: Path) -> AsyncIterator[Stack]:
    pool = await common_db.pool("mission_control_runtime")
    try:
        yield Stack(common_db, pool, tmp_path / "payloads")
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_release_admits_the_consumer_with_a_packet_and_intent_in_one_transaction(
    stack: Stack, chain_hook: None
) -> None:
    chain = await submit(stack)
    supplies, depends = "research->ingestion:supplies", "research->ingestion:depends_on"
    run_id = stack.supplier_run
    version = await run_command(stack.research, stack, run_id, 1, "start", StartAction())
    assert {row["state"] for row in (await link_rows(stack)).values()} == {"armed"}

    # Goal acceptance with an accepted output releases `supplies`; `depends_on` still waits.
    version, output_ref = await accept_goal_and_output(stack, run_id, version)
    rows = await link_rows(stack)
    assert (rows[supplies]["state"], rows[depends]["state"]) == ("released", "armed")
    assert rows[supplies]["released_run_id"] is None
    consumer_mission = chain.members[1].mission_id
    assert (
        await stack.val(
            "SELECT count(*) FROM mission_control.mission_run WHERE mission_id = $1",
            consumer_mission,
        )
        == 0
    )

    # Terminal acceptance: `depends_on` releases and the consumer is admitted, in this commit.
    await terminalize(
        stack.research, stack, run_id, version, obligations=(OBLIGATION,), outputs=(output_ref,)
    )
    rows = await link_rows(stack)
    assert {row["state"] for row in rows.values()} == {"released"}
    (consumer,) = await stack.fetch(
        "SELECT run_id, run_key, phase, created_by_actor_ref FROM mission_control.mission_run "
        "WHERE mission_id = $1",
        consumer_mission,
    )
    assert consumer["phase"] == "pending"
    assert consumer["created_by_actor_ref"] == f"chain:{chain.chain_id}"
    assert {row["released_run_id"] for row in rows.values()} == {consumer["run_id"]}
    packet_digests = {row["packet_digest"] for row in rows.values()}
    assert len(packet_digests) == 1 and None not in packet_digests

    (intent,) = await stack.fetch(
        "SELECT delivery_key, delivery_state, payload FROM mission_control.outbox "
        "WHERE destination_kind = 'mc.chain.start_run'"
    )
    assert intent["delivery_key"] == f"chain-start:{consumer['run_key']}"
    assert intent["delivery_state"] == "pending"
    payload = json.loads(intent["payload"])
    assert payload["family"] == "GoalDirected" and payload["actor_ref"].startswith("chain:")

    (selection,) = await stack.fetch(
        "SELECT packet, packet_digest FROM mission_control.context_selection "
        "WHERE run_key = $1 AND purpose = 'chain_link'",
        consumer["run_key"],
    )
    packet = ContextPacket.model_validate(json.loads(selection["packet"]))
    assert selection["packet_digest"] in packet_digests
    supplied = [item for item in packet.items if item.binding_name == "research.evidence_map"]
    assert len(supplied) == 1 and supplied[0].tier is ExpansionTier.MATERIALIZE
    assert supplied[0].materialize is not None
    assert supplied[0].materialize.path == "/inputs/research.evidence_map/evidence_map.json"
    assert supplied[0].content_digest == f"sha256:{sha256(EVIDENCE_MAP).hexdigest()}"
    assert any(
        item.reference is not None
        and item.reference.retrieval.command == f"missionctl run transcript {run_id} --since 0"
        for item in packet.items
    )

    relationships = await stack.fetch(
        "SELECT kind, source_run_id, target_run_id FROM mission_control.mission_relationship"
    )
    assert sorted(row["kind"] for row in relationships) == ["depends_on", "supplies"]
    assert {row["target_run_id"] for row in relationships} == {consumer["run_id"]}

    # Same envelope event id in every member stream, one seq per mission.
    released = await chain_events(stack, "chain_link.released")
    by_mission: dict[UUID, set[str]] = {}
    for row in released:
        by_mission.setdefault(row["mission_id"], set()).add(json.loads(row["payload"])["event_id"])
    assert set(by_mission) == {member.mission_id for member in chain.members}
    assert len({frozenset(ids) for ids in by_mission.values()}) == 1
    assert len(next(iter(by_mission.values()))) == 2

    # The inspection projection shows the running chain and the admitted consumer.
    inspection = await PostgresChainReader(stack.pool).inspect(stack.scope, chain.chain_id)
    assert inspection is not None
    assert inspection.chain.lifecycle.value == "running"
    assert [member.status for member in inspection.members] == ["terminal", "active"]

    # Replaying the hook over the same commit decides nothing new.
    before = await stack.val("SELECT count(*) FROM mission_control.mission_event")
    async with stack.pool.acquire() as connection, connection.transaction():
        args = await mc.begin(connection, stack.scope)
        research_row = (
            await connection.fetch(
                "SELECT run_id, mission_id FROM mission_control.mission_run WHERE run_key = $1",
                run_id,
            )
        )[0]
        envelope = await connection.fetchval(
            "SELECT payload FROM mission_control.mission_event WHERE run_id = $1 "
            "AND event_type = 'workflow_run.terminalize'",
            research_row["run_id"],
        )
        from mission_control.domain.policies.contracts import DomainEventEnvelope

        await ChainReleaseHook()(
            connection,
            args,
            mc.AppendedEvents(
                run_key=run_id,
                run_id=research_row["run_id"],
                mission_id=research_row["mission_id"],
                ledger_commit_id=uuid4(),
                events=(DomainEventEnvelope.model_validate(json.loads(envelope)),),
                actor_ref="operator",
            ),
        )
    assert await stack.val("SELECT count(*) FROM mission_control.mission_event") == before

    # The consumer accepts: the chain completes `accepted` in both streams with one event id.
    consumer_version = await run_command(
        stack.ingestion, stack, consumer["run_key"], 1, "start", StartAction()
    )
    ingested = AcceptedObligationEvidence(
        obligation_ref="ingested",
        evidence_digest="sha256:" + "a" * 64,
        accepted_by_authority_ref="authority:lifecycle",
    )
    consumer_version = await run_command(
        stack.ingestion,
        stack,
        consumer["run_key"],
        consumer_version,
        "accept-ingested",
        RecordObligationEvidenceAction(evidence=ingested),
    )
    await terminalize(
        stack.ingestion,
        stack,
        consumer["run_key"],
        consumer_version,
        obligations=(ingested,),
        name="terminalize-ingestion",
    )
    completed = await chain_events(stack, "chain.completed")
    assert len(completed) == 2
    payloads = [json.loads(row["payload"]) for row in completed]
    assert len({item["event_id"] for item in payloads}) == 1
    assert payloads[0]["payload"]["terminal_outcome"] == "accepted"
    assert (
        await stack.val(
            "SELECT terminal_outcome FROM mission_control.mission_chain WHERE chain_id = $1",
            chain.chain_id,
        )
        == "accepted"
    )


class FailingHook:
    async def __call__(self, *_args: Any) -> None:
        raise RuntimeError("injected failure after the event write")


@pytest.mark.asyncio
async def test_injected_failure_after_the_event_write_rolls_back_the_release(
    stack: Stack, chain_hook: None
) -> None:
    await submit(stack)
    run_id = stack.supplier_run
    version = await run_command(stack.research, stack, run_id, 1, "start", StartAction())
    version, output_ref = await accept_goal_and_output(stack, run_id, version)
    version = await release_baseline(stack.research, stack, run_id, version, "crash")
    events_before = await stack.val("SELECT count(*) FROM mission_control.mission_event")
    unregister: Callable[[], None] = mc.register_post_append_hook("zz.failing", FailingHook())
    try:
        with pytest.raises(RuntimeError, match="injected failure"):
            await terminalize(
                stack.research,
                stack,
                run_id,
                version,
                obligations=(OBLIGATION,),
                outputs=(output_ref,),
                name="terminalize-crash",
                release=False,
            )
    finally:
        unregister()
    rows = await link_rows(stack)
    assert rows["research->ingestion:depends_on"]["state"] == "armed"
    assert (await stack.research.get_run(stack.scope, run_id)).terminal_outcome is None
    assert (
        await stack.val(
            "SELECT count(*) FROM mission_control.outbox "
            "WHERE destination_kind = 'mc.chain.start_run'"
        )
        == 0
    )
    assert stack.chain is not None
    assert (
        await stack.val(
            "SELECT count(*) FROM mission_control.mission_run WHERE mission_id = $1",
            stack.chain.members[1].mission_id,
        )
        == 0
    )
    # The failing terminalization left no event behind.
    assert await stack.val("SELECT count(*) FROM mission_control.mission_event") == events_before


@pytest.mark.asyncio
async def test_cancelled_supplier_cancels_armed_links_and_completes_the_chain(
    stack: Stack, chain_hook: None
) -> None:
    chain = await submit(stack)
    run_id = stack.supplier_run
    version = await run_command(stack.research, stack, run_id, 1, "start", StartAction())
    version = await run_command(stack.research, stack, run_id, version, "cancel", CancelAction())
    await terminalize(
        stack.research, stack, run_id, version, obligations=(), name="cancelled", cancelled=True
    )
    assert (await stack.research.get_run(stack.scope, run_id)).terminal_outcome is (
        RunOutcome.CANCELLED
    )
    assert {row["state"] for row in (await link_rows(stack)).values()} == {"cancelled"}
    completed = await chain_events(stack, "chain.completed")
    assert {json.loads(row["payload"])["payload"]["terminal_outcome"] for row in completed} == {
        "cancelled"
    }
    assert (
        await stack.val(
            "SELECT count(*) FROM mission_control.mission_run WHERE mission_id = $1",
            chain.members[1].mission_id,
        )
        == 0
    )


class RecordingStarter:
    def __init__(self) -> None:
        self.started: list[str] = []

    async def start(self, intent: Any) -> ChainStartReceipt:
        self.started.append(intent.run_key)
        return ChainStartReceipt(workflow_id=f"wf:{intent.run_key}", temporal_run_id="r1")


@pytest.mark.asyncio
async def test_relay_delivers_the_start_intent_once_and_honours_autostart_false(
    stack: Stack, chain_hook: None
) -> None:
    await submit(stack)
    run_id = stack.supplier_run
    version = await run_command(stack.research, stack, run_id, 1, "start", StartAction())
    version, output_ref = await accept_goal_and_output(stack, run_id, version)
    await terminalize(
        stack.research, stack, run_id, version, obligations=(OBLIGATION,), outputs=(output_ref,)
    )
    starter = RecordingStarter()
    relay = ChainIntentRelay(store=PostgresChainIntentStore(stack.pool), starter=starter)
    first = await relay.relay_once(stack.scope, now=NOW + timedelta(hours=1))
    second = await relay.relay_once(stack.scope, now=NOW + timedelta(hours=2))
    assert len(first.delivered) == 1 and second.delivered == ()
    assert len(starter.started) == 1
    assert (
        await stack.val(
            "SELECT delivery_state FROM mission_control.outbox "
            "WHERE destination_kind = 'mc.chain.start_run'"
        )
        == "delivered"
    )


@pytest.mark.asyncio
async def test_autostart_false_admits_the_consumer_but_writes_no_start_intent(
    stack: Stack, chain_hook: None
) -> None:
    chain = await submit(stack, autostart=False)
    run_id = stack.supplier_run
    version = await run_command(stack.research, stack, run_id, 1, "start", StartAction())
    version, output_ref = await accept_goal_and_output(stack, run_id, version)
    await terminalize(
        stack.research, stack, run_id, version, obligations=(OBLIGATION,), outputs=(output_ref,)
    )
    assert (
        await stack.val(
            "SELECT phase FROM mission_control.mission_run WHERE mission_id = $1",
            chain.members[1].mission_id,
        )
        == "pending"
    )
    assert (
        await stack.val(
            "SELECT count(*) FROM mission_control.outbox WHERE destination_kind LIKE 'mc.chain.%'"
        )
        == 0
    )


@pytest.mark.asyncio
async def test_release_runs_under_the_family_writer_role(
    stack: Stack, common_db: CommonDatabase, no_hooks: None
) -> None:
    """StageGraph terminalization is a family admission: the hook runs on the family writer
    connection, so the release needs exactly the 0028 grants of that role."""

    chain = await submit(stack)
    run_id = stack.supplier_run
    version = await run_command(stack.research, stack, run_id, 1, "start", StartAction())
    version, output_ref = await accept_goal_and_output(stack, run_id, version)
    # No hook registered yet: the supplier is terminal and nothing was released.
    await terminalize(
        stack.research, stack, run_id, version, obligations=(OBLIGATION,), outputs=(output_ref,)
    )
    assert {row["state"] for row in (await link_rows(stack)).values()} == {"armed"}
    family = await common_db.pool("mission_control_family_writer")
    try:
        async with family.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, stack.scope)
            research = (
                await connection.fetch(
                    "SELECT run_id, mission_id FROM mission_control.mission_run WHERE run_key = $1",
                    run_id,
                )
            )[0]
            envelope = await connection.fetchval(
                "SELECT payload FROM mission_control.mission_event WHERE run_id = $1 "
                "AND event_type = 'workflow_run.terminalize'",
                research["run_id"],
            )
            from mission_control.domain.policies.contracts import DomainEventEnvelope

            await ChainReleaseHook()(
                connection,
                args,
                mc.AppendedEvents(
                    run_key=run_id,
                    run_id=research["run_id"],
                    mission_id=research["mission_id"],
                    ledger_commit_id=uuid4(),
                    events=(DomainEventEnvelope.model_validate(json.loads(envelope)),),
                    actor_ref="family-writer",
                ),
            )
    finally:
        await family.close()
    assert {row["state"] for row in (await link_rows(stack)).values()} == {"released"}
    assert (
        await stack.val(
            "SELECT phase FROM mission_control.mission_run WHERE mission_id = $1",
            chain.members[1].mission_id,
        )
        == "pending"
    )
    assert (
        await stack.val(
            "SELECT count(*) FROM mission_control.outbox "
            "WHERE destination_kind = 'mc.chain.start_run'"
        )
        == 1
    )
