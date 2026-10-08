"""FT-D2: the pure chain reducer and the chain-link packet (SPEC-04 "Chain reducer").

The reducer is driven with chain rows and member run facts only; persisted effects are proven
in tests/integration/postgres/test_chain_release.py.
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

import pytest

from mission_control.application.chains.packet import (
    SuppliedArtifact,
    chain_pack_request,
    match_supplied,
)
from mission_control.application.chains.reducer import (
    ChainReducer,
    ChainSnapshot,
    MemberRun,
    chain_event_id,
    member_statuses,
)
from mission_control.domain.authoring.manifest import load_manifest_yaml, parse_manifest
from mission_control.domain.composition.chain import (
    ChainBinding,
    ChainLifecycle,
    ChainLink,
    ChainLinkState,
    ChainPhase,
    ChainScope,
    ChainTerminalOutcome,
    ChainTransition,
    MissionChain,
    build_mission_chain,
    compile_chain,
)
from mission_control.domain.context.packet import (
    ContextPacket,
    ContextSourceKind,
    ExpansionTier,
    pack,
)
from mission_control.domain.policies.contracts import RunOutcome, RunPhase

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "tests/fixtures/manifests/two-mission-chain.yml"
NOW = datetime(2026, 10, 8, 12, tzinfo=UTC)
CHAIN_ID = UUID("01920000-0000-7000-8000-000000000101")
SCOPE = ChainScope(
    installation_id=UUID("01920000-0000-7000-8000-0000000000aa"),
    application_id="biotech",
    tenant_id=UUID("01920000-0000-7000-8000-0000000000bb"),
)
REQUEST_SCOPE = f"mc/{SCOPE.installation_id}/biotech/{SCOPE.tenant_id}"
CAUSE = UUID("01920000-0000-7000-8000-0000000000cc")
reducer = ChainReducer()


def mission_id(key: str) -> UUID:
    return uuid5(CHAIN_ID, f"mission:{key}")


def document() -> dict[str, Any]:
    return load_manifest_yaml(FIXTURE.read_text(encoding="utf-8"))


def three_missions(*, report_links: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """research -> ingestion -> report, plus optional extra links into report."""

    doc = document()
    third = copy.deepcopy(doc["missions"][1])
    third["key"] = "report"
    third["program"]["inputs"] = [
        {"name": "receipt", "from": "ingestion.ingestion_receipt", "expand": "reference"}
    ]
    doc["missions"].append(third)
    doc["links"].append(
        {"from": "ingestion", "to": "report", "kind": "supplies", "outputs": ["ingestion_receipt"]}
    )
    doc["links"].extend(report_links or [])
    return doc


def build(doc: dict[str, Any]) -> MissionChain:
    compilation = compile_chain(parse_manifest(doc))
    assert compilation.ok, compilation.blockers
    assert compilation.resolution is not None
    return build_mission_chain(
        resolution=compilation.resolution,
        chain_id=CHAIN_ID,
        scope=SCOPE,
        chain_key="chain",
        title="Chain",
        manifest_digest="sha256:" + "a" * 64,
        members={
            key: (mission_id(key), uuid5(CHAIN_ID, f"revision:{key}"))
            for key in compilation.resolution.order
        },
        created_at=NOW,
        created_by_actor_ref="actor:owner",
    )


def run(
    key: str,
    *,
    phase: RunPhase = RunPhase.ACTIVE,
    outcome: RunOutcome | None = None,
    obligations: frozenset[str] = frozenset(),
    required: frozenset[str] = frozenset(),
    outputs: tuple[str, ...] = (),
) -> MemberRun:
    return MemberRun(
        mission_id=mission_id(key),
        run_id=uuid5(CHAIN_ID, f"run:{key}"),
        run_key=f"run-{key}",
        phase=phase,
        terminal_outcome=outcome,
        required_obligations=required,
        accepted_obligations=obligations,
        accepted_outputs=outputs,
        evidence_frontier_digest="sha256:" + "f" * 64,
    )


def accepted(key: str, goal: str, *, outputs: tuple[str, ...] = ("artifact://x/evidence_map",)):
    return run(
        key,
        phase=RunPhase.TERMINAL,
        outcome=RunOutcome.COMPLETED,
        obligations=frozenset({goal}),
        required=frozenset({goal}),
        outputs=outputs,
    )


def snapshot(chain: MissionChain, *runs: MemberRun) -> ChainSnapshot:
    return ChainSnapshot(
        chain=chain,
        link_versions={link.link_id: 1 for link in chain.links},
        runs={item.mission_id: item for item in runs},
    )


def reduce(chain: MissionChain, trigger: str, *runs: MemberRun) -> ChainTransition:
    return reducer.on_events(
        snapshot(chain, *runs),
        cause_event_id=CAUSE,
        trigger_mission_id=mission_id(trigger),
        occurred_at=NOW,
    )


def apply(chain: MissionChain, transition: ChainTransition) -> MissionChain:
    """Fold a transition into the chain the way the store persists it."""

    links = []
    by_id = {update.link_id: update for update in transition.link_updates}
    for link in chain.links:
        update = by_id.get(link.link_id)
        if update is None:
            links.append(link)
            continue
        links.append(
            link.model_copy(
                update={
                    "state": update.state,
                    "blocked_reason": update.blocked_reason,
                    "released_at": NOW if update.state is ChainLinkState.RELEASED else None,
                }
            )
        )
    values: dict[str, Any] = {"links": tuple(links)}
    for update in transition.chain_updates:
        values.update(
            lifecycle=update.lifecycle,
            phase=update.phase,
            terminal_outcome=update.terminal_outcome,
            version=chain.version + 1,
        )
    return MissionChain.model_validate({**chain.model_dump(), **values})


def states(transition: ChainTransition) -> dict[str, tuple[str, str | None]]:
    return {
        str(update.link_id): (update.state.value, update.blocked_reason)
        for update in transition.link_updates
    }


def link(chain: MissionChain, key: str) -> ChainLink:
    return next(item for item in chain.links if item.link_key == key)


SUPPLIES = "research->ingestion:supplies"
DEPENDS = "research->ingestion:depends_on"


# --- release conditions --------------------------------------------------------------------


def test_goal_accepted_releases_supplies_but_the_consumer_waits_for_every_incoming_link():
    chain = build(document())
    active = run(
        "research",
        obligations=frozenset({"evidence_map"}),
        outputs=("artifact://x/evidence_map",),
    )
    transition = reduce(chain, "research", active)
    assert states(transition) == {str(link(chain, SUPPLIES).link_id): ("released", None)}
    assert transition.admissions == ()
    assert [event.event_type for event in transition.events] == ["chain_link.released"]
    # The chain starts running with its first release.
    assert [(u.lifecycle, u.phase) for u in transition.chain_updates] == [
        (ChainLifecycle.RUNNING, ChainPhase.RELEASING)
    ]


def test_goal_accepted_without_accepted_outputs_does_not_release_a_supplies_link():
    chain = build(document())
    transition = reduce(chain, "research", run("research", obligations=frozenset({"evidence_map"})))
    assert transition.link_updates == ()


def test_goal_accepted_also_matches_a_goal_prefixed_obligation():
    chain = build(document())
    active = run(
        "research", obligations=frozenset({"goal:evidence_map"}), outputs=("artifact://x/e",)
    )
    assert states(reduce(chain, "research", active)) == {
        str(link(chain, SUPPLIES).link_id): ("released", None)
    }


def test_terminal_acceptance_releases_both_links_and_admits_the_consumer_once():
    chain = build(document())
    transition = reduce(chain, "research", accepted("research", "evidence_map"))
    assert set(states(transition).values()) == {("released", None)}
    (admission,) = transition.admissions
    assert admission.to_mission_key == "ingestion"
    assert set(admission.link_ids) == {item.link_id for item in chain.links}
    assert sorted(event.event_type for event in transition.events) == [
        "chain_link.released",
        "chain_link.released",
    ]
    # Replaying the same facts over the persisted result decides nothing new.
    after = apply(chain, transition)
    assert reduce(after, "research", accepted("research", "evidence_map")).is_noop


def test_execution_complete_releases_on_any_terminal_outcome_but_cancelled():
    doc = document()
    doc["links"] = [doc["links"][1]]
    doc["missions"][1]["program"].pop("inputs")
    chain = build(doc)
    failed = run("research", phase=RunPhase.TERMINAL, outcome=RunOutcome.FAILED)
    transition = reduce(chain, "research", failed)
    assert set(states(transition).values()) == {("released", None)}
    assert len(transition.admissions) == 1


def test_mission_accepted_waits_for_every_required_obligation():
    doc = document()
    doc["links"] = [doc["links"][1] | {"on": "mission_accepted"}]
    doc["missions"][1]["program"].pop("inputs")
    chain = build(doc)
    partial = run(
        "research",
        phase=RunPhase.TERMINAL,
        outcome=RunOutcome.COMPLETED,
        obligations=frozenset({"evidence_map"}),
        required=frozenset({"evidence_map", "reviewed"}),
    )
    transition = reduce(chain, "research", partial)
    assert set(states(transition).values()) == {("blocked", "upstream_not_accepted")}
    assert transition.admissions == ()
    assert len(reduce(chain, "research", accepted("research", "evidence_map")).admissions) == 1


# --- blocking and cancellation ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("outcome", "reason", "chain_outcome"),
    [
        (RunOutcome.PARTIALLY_COMPLETED, "upstream_not_accepted", "not_accepted"),
        (RunOutcome.FAILED, "upstream_execution_failed", "execution_failed"),
        (RunOutcome.COMPLETED, "upstream_not_accepted", "not_accepted"),
    ],
)
def test_terminal_supplier_that_misses_the_goal_blocks_and_completes_the_chain(
    outcome: RunOutcome, reason: str, chain_outcome: str
):
    chain = build(document())
    supplier = run("research", phase=RunPhase.TERMINAL, outcome=outcome)
    transition = reduce(chain, "research", supplier)
    supplies = str(link(chain, SUPPLIES).link_id)
    assert states(transition)[supplies] == ("blocked", reason)
    assert transition.admissions == ()
    (update,) = transition.chain_updates
    assert update.lifecycle is ChainLifecycle.COMPLETED
    assert update.phase is ChainPhase.BLOCKED
    assert update.terminal_outcome == ChainTerminalOutcome(chain_outcome)
    types = [event.event_type for event in transition.events]
    assert "chain_link.blocked" in types and types[-1] == "chain.completed"
    completed = transition.events[-1].payload
    assert completed["terminal_outcome"] == chain_outcome
    assert [member["mission_key"] for member in completed["members"]] == ["research", "ingestion"]


def test_cancel_downstream_cancels_armed_links_and_completes_cancelled():
    chain = build(document())
    cancelled = run("research", phase=RunPhase.TERMINAL, outcome=RunOutcome.CANCELLED)
    transition = reduce(chain, "research", cancelled)
    assert set(states(transition).values()) == {("cancelled", None)}
    assert [event.event_type for event in transition.events] == [
        "chain_link.cancelled",
        "chain_link.cancelled",
        "chain.completed",
    ]
    assert transition.chain_updates[0].terminal_outcome is ChainTerminalOutcome.CANCELLED


def test_cancel_downstream_cancels_a_released_consumer_run_and_detach_leaves_it_running():
    chain = build(document())
    after = apply(chain, reduce(chain, "research", accepted("research", "evidence_map")))
    consumer = run("ingestion")
    admitted = MissionChain.model_validate(
        {
            **after.model_dump(),
            "links": tuple(
                item.model_copy(update={"released_run_id": consumer.run_id}) for item in after.links
            ),
        }
    )
    cancelled = run("research", phase=RunPhase.TERMINAL, outcome=RunOutcome.CANCELLED)
    transition = reduce(admitted, "research", cancelled, consumer)
    assert {item.run_key for item in transition.cancellations} == {"run-ingestion"}
    assert transition.link_updates == ()

    doc = document()
    for item in doc["links"]:
        item["on_upstream_cancel"] = "detach"
    detached_chain = build(doc)
    after = apply(
        detached_chain, reduce(detached_chain, "research", accepted("research", "evidence_map"))
    )
    detached = reduce(after, "research", cancelled, consumer)
    assert transition.cancellations and not detached.cancellations
    assert set(states(detached).values()) == {("detached", None)}
    assert {event.event_type for event in detached.events} == {"chain_link.detached"}


def test_consumer_already_started_guard_blocks_instead_of_admitting_a_second_run():
    chain = build(document())
    manual = run("ingestion", phase=RunPhase.ACTIVE)
    transition = reduce(chain, "research", accepted("research", "evidence_map"), manual)
    assert transition.admissions == ()
    assert set(states(transition).values()) == {("blocked", "consumer_already_started")}


# --- all incoming links and completion -------------------------------------------------------


def test_a_consumer_with_several_suppliers_is_admitted_only_when_all_are_released():
    doc = three_missions(
        report_links=[
            {"from": "research", "to": "report", "kind": "depends_on", "on": "mission_accepted"}
        ]
    )
    chain = build(doc)
    research = accepted("research", "evidence_map")
    first = reduce(chain, "research", research)
    assert {item.to_mission_key for item in first.admissions} == {"ingestion"}
    chain = apply(chain, first)
    ingestion = accepted("ingestion", "ingested", outputs=("artifact://x/ingestion_receipt",))
    second = reduce(chain, "ingestion", research, ingestion)
    assert {item.to_mission_key for item in second.admissions} == {"report"}
    (admission,) = second.admissions
    assert len(admission.link_ids) == 2


def test_chain_completes_accepted_only_when_every_member_is_mission_accepted():
    chain = build(document())
    research = accepted("research", "evidence_map")
    chain = apply(chain, reduce(chain, "research", research))
    consumer = run("ingestion", phase=RunPhase.ACTIVE)
    running = reduce(chain, "ingestion", research, consumer)
    assert all(item.lifecycle is not ChainLifecycle.COMPLETED for item in running.chain_updates)
    done = accepted("ingestion", "ingested")
    transition = reduce(chain, "ingestion", research, done)
    (update,) = transition.chain_updates
    assert update.terminal_outcome is ChainTerminalOutcome.ACCEPTED
    assert transition.events[-1].event_type == "chain.completed"
    rejected = run("ingestion", phase=RunPhase.TERMINAL, outcome=RunOutcome.PARTIALLY_COMPLETED)
    assert (
        reduce(chain, "ingestion", research, rejected).chain_updates[0].terminal_outcome
        is ChainTerminalOutcome.NOT_ACCEPTED
    )


FAILED_RESEARCH = run("research", phase=RunPhase.TERMINAL, outcome=RunOutcome.FAILED)


def test_completed_chain_is_final_and_member_statuses_follow_topology():
    chain = build(three_missions())
    blocked = apply(
        chain,
        reduce(
            chain, "research", run("research", phase=RunPhase.TERMINAL, outcome=RunOutcome.FAILED)
        ),
    )
    assert blocked.lifecycle is ChainLifecycle.COMPLETED
    assert reduce(blocked, "research", accepted("research", "evidence_map")).is_noop
    statuses = member_statuses(
        blocked,
        {item.link_id: item.state for item in blocked.links},
        {
            mission_id("research"): run(
                "research", phase=RunPhase.TERMINAL, outcome=RunOutcome.FAILED
            )
        },
    )
    assert list(statuses.values()) == ["terminal", "unreachable", "unreachable"]


def test_event_identity_is_shared_and_deterministic():
    chain = build(document())
    transition = reduce(chain, "research", accepted("research", "evidence_map"))
    ids = {chain_event_id(chain.chain_id, event) for event in transition.events}
    assert len(ids) == len(transition.events)
    again = reduce(chain, "research", accepted("research", "evidence_map"))
    assert ids == {chain_event_id(chain.chain_id, event) for event in again.events}


# --- chain-link packet -------------------------------------------------------------------------

EVIDENCE = SuppliedArtifact(
    source_ref="workspace-candidate://cand-1",
    content_digest="sha256:" + "e" * 64,
    size_bytes=812,
    media_type="application/json",
    durable_ref="objects/cand-1#sha256:" + "e" * 64 + ":812",
    logical_path="/work/output/evidence_map.json",
)
NOTES = SuppliedArtifact(
    source_ref="workspace-candidate://cand-2",
    content_digest="sha256:" + "d" * 64,
    size_bytes=40,
    media_type="text/markdown",
    durable_ref="objects/cand-2#sha256:" + "d" * 64 + ":40",
    logical_path="/work/output/notes.md",
)


def test_supplied_outputs_match_by_name_and_fall_back_for_a_single_binding():
    binding = ChainBinding(
        output_name="evidence_map", input_name="evidence_map", schema_ref="claim_table@1"
    )
    assert match_supplied(binding, (EVIDENCE, NOTES), single_binding=False) == (EVIDENCE,)
    other = binding.model_copy(update={"output_name": "branch_ref"})
    assert match_supplied(other, (NOTES,), single_binding=False) == ()
    assert match_supplied(other, (NOTES,), single_binding=True) == (NOTES,)


def sealed_packet() -> ContextPacket:
    chain = build(document())
    research = accepted("research", "evidence_map", outputs=(EVIDENCE.source_ref, NOTES.source_ref))
    request = chain_pack_request(
        chain=chain,
        consumer_mission_id=mission_id("ingestion"),
        consumer_revision_id=uuid5(CHAIN_ID, "revision:ingestion"),
        consumer_run_key="run-ingestion",
        links=chain.links,
        supplier_runs={research.mission_id: research},
        supplied={research.mission_id: (EVIDENCE, NOTES)},
        sealed_at=NOW,
        request_scope=REQUEST_SCOPE,
    )
    packet = pack(request)
    assert isinstance(packet, ContextPacket), packet
    return packet


def test_chain_packet_materializes_the_supplied_output_with_disposition_and_references():
    packet = sealed_packet()
    assert packet.target.purpose.value == "chain_link"
    supplied = [item for item in packet.items if item.binding_name == "research.evidence_map"]
    assert [item.source_ref for item in supplied] == [EVIDENCE.source_ref]
    (item,) = supplied
    assert item.tier is ExpansionTier.MATERIALIZE and item.materialize is not None
    assert item.materialize.path == "/inputs/research.evidence_map/evidence_map.json"
    assert item.content_digest == EVIDENCE.content_digest
    assert item.provenance.chain_link_id is not None and not item.provenance.provisional
    kinds = {entry.source_kind for entry in packet.items}
    assert {
        ContextSourceKind.CONTINUATION_CHECKPOINT,
        ContextSourceKind.JOURNAL_DIGEST,
    } <= kinds
    references = [entry for entry in packet.items if entry.reference is not None]
    assert all(
        entry.reference is not None
        and entry.reference.retrieval.command is not None
        and entry.reference.retrieval.command.startswith("missionctl run transcript run-research")
        for entry in references
    )
    disposition = next(entry for entry in packet.items if entry.source_ref.endswith("/disposition"))
    assert disposition.inline is not None and "evidence_map" in disposition.inline.text


def test_chain_packet_digest_is_deterministic():
    assert sealed_packet().packet_digest == sealed_packet().packet_digest


def test_consumer_first_packet_carries_the_chain_supply_under_its_mount_root():
    from mission_control.application.context.pack_service import chain_supply_from_packet
    from mission_control.domain.context.packet import (
        ContextPurpose,
        LaneFileSupport,
        PacketTarget,
        PackRequest,
    )

    chain_packet = sealed_packet()
    supply = chain_supply_from_packet(chain_packet)
    assert {binding.binding_name for binding in supply.bindings} == {"research.evidence_map"}
    request = PackRequest(
        packet_id="iteration-1",
        sealed_at=NOW,
        context_selection_ref="context_selection:iteration-1",
        scope=chain_packet.scope,
        target=PacketTarget(
            mission_id="ingestion",
            run_id="run-ingestion",
            revision_id="goal-revision:1",
            node_key="goal/executor",
            activation_id="goal:1:executor",
            attempt_no=1,
            generation=1,
            purpose=ContextPurpose.ITERATION_START,
        ),
        profile=chain_pack_request(
            chain=build(document()),
            consumer_mission_id=mission_id("ingestion"),
            consumer_revision_id=uuid5(CHAIN_ID, "revision:ingestion"),
            consumer_run_key="run-ingestion",
            links=(),
            supplier_runs={},
            supplied={},
            sealed_at=NOW,
            request_scope=REQUEST_SCOPE,
        ).profile,
        bindings=supply.bindings,
        candidates=supply.candidates,
        lane=LaneFileSupport(writable_workspace=True, mount_root="/work/goal/1/executor"),
    )
    packet = pack(request)
    assert isinstance(packet, ContextPacket), packet
    (item,) = [entry for entry in packet.items if entry.binding_name == "research.evidence_map"]
    assert item.materialize is not None
    assert item.materialize.path == (
        "/work/goal/1/executor/inputs/research.evidence_map/evidence_map.json"
    )
    assert item.content_digest == EVIDENCE.content_digest
    assert {entry.source_kind for entry in packet.items} >= {
        ContextSourceKind.CHAIN_SUPPLY,
        ContextSourceKind.CONTINUATION_CHECKPOINT,
        ContextSourceKind.JOURNAL_DIGEST,
    }
