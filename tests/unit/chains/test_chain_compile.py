"""FT-D1: chain contracts and compile-time link validation (SPEC-04 "Compile")."""

from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from mission_control.application.chains.service import ManifestStructureService
from mission_control.domain.authoring.manifest import (
    ManifestErrorCode,
    load_manifest_yaml,
    parse_manifest,
)
from mission_control.domain.composition.chain import (
    CHAIN_LINK_SCHEMA_VERSION,
    CHAIN_SCHEMA_VERSION,
    ChainLink,
    ChainLinkKind,
    ChainLinkState,
    ChainScope,
    ChainTransition,
    LinkUpdate,
    MissionChain,
    ReleaseCondition,
    build_mission_chain,
    chain_contract_schemas,
    chain_digest,
    chain_link_id,
    compile_chain,
    link_transition_allowed,
)
from mission_control.interfaces.cli.main import contract_schema_texts
from mission_control.interfaces.cli.main import main as missionctl

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "tests/fixtures/manifests/two-mission-chain.yml"
MISSION_2 = (
    ROOT / "docs/specs/fast-track-2026-10/missions/02-research-ingestion-cursor-cloud-chain.yml"
)
SCHEMAS = ROOT / "src/mission_control/contracts/schemas"
CHAIN_ID = UUID("01920000-0000-7000-8000-000000000001")
SCOPE = ChainScope(
    installation_id=UUID("01920000-0000-7000-8000-0000000000aa"),
    application_id="biotech",
    tenant_id=UUID("01920000-0000-7000-8000-0000000000bb"),
)


def chain_document() -> dict[str, Any]:
    return load_manifest_yaml(FIXTURE.read_text(encoding="utf-8"))


def three_missions() -> dict[str, Any]:
    document = chain_document()
    third = copy.deepcopy(document["missions"][1])
    third["key"] = "report"
    third["program"]["inputs"] = [
        {"name": "receipt", "from": "ingestion.ingestion_receipt", "expand": "reference"}
    ]
    document["missions"].append(third)
    document["links"].append(
        {"from": "ingestion", "to": "report", "kind": "supplies", "outputs": ["ingestion_receipt"]}
    )
    return document


def blockers(document: dict[str, Any]) -> set[tuple[str, str | None]]:
    compilation = compile_chain(parse_manifest(document))
    assert all(issue.code is ManifestErrorCode.INVALID_DEFINITION for issue in compilation.blockers)
    return {(issue.pointer, issue.reason) for issue in compilation.blockers}


# --- compile -----------------------------------------------------------------------------


def test_fixture_chain_compiles_with_topological_order_and_bindings():
    compilation = compile_chain(parse_manifest(chain_document()))
    assert compilation.ok
    assert compilation.resolution is not None
    assert compilation.resolution.order == ("research", "ingestion")
    supplies, depends = compilation.resolution.links
    assert supplies.kind is ChainLinkKind.SUPPLIES
    assert supplies.on == ReleaseCondition(kind="goal_accepted", goal_key="evidence_map")
    assert [
        (b.output_name, b.input_name, b.schema_ref, b.expand.value) for b in supplies.bindings
    ] == [("evidence_map", "evidence_map", "claim_table@1", "materialize")]
    assert depends.kind is ChainLinkKind.DEPENDS_ON
    assert depends.on == ReleaseCondition(kind="execution_complete")
    assert depends.bindings == ()


def test_owner_mission_2_compiles_as_a_chain():
    compilation = compile_chain(parse_manifest(load_manifest_yaml(MISSION_2.read_text("utf-8"))))
    assert compilation.blockers == ()
    assert compilation.resolution is not None
    assert compilation.resolution.order == ("research", "ingestion")
    assert [link.link_key for link in compilation.resolution.links] == [
        "research->ingestion:supplies",
        "research->ingestion:depends_on",
    ]


def test_three_mission_chain_orders_topologically_regardless_of_file_order():
    document = three_missions()
    document["missions"] = [
        document["missions"][2],
        document["missions"][0],
        document["missions"][1],
    ]
    compilation = compile_chain(parse_manifest(document))
    assert compilation.blockers == ()
    assert compilation.resolution is not None
    assert compilation.resolution.order == ("research", "ingestion", "report")


def test_default_release_condition_is_the_goal_owning_the_first_bound_output():
    document = chain_document()
    document["links"][0].pop("on")
    compilation = compile_chain(parse_manifest(document))
    assert compilation.resolution is not None
    assert compilation.resolution.links[0].on == ReleaseCondition(
        kind="goal_accepted", goal_key="evidence_map"
    )


def test_chain_cycle():
    document = three_missions()
    document["links"].append(
        {"from": "report", "to": "research", "kind": "depends_on", "on": "mission_accepted"}
    )
    found = blockers(document)
    assert ("/links/0", "chain_cycle") in found or any(
        reason == "chain_cycle" for _, reason in found
    )
    document = chain_document()
    document["links"].append(
        {"from": "research", "to": "research", "kind": "depends_on", "on": "mission_accepted"}
    )
    assert ("/links/2/to", "chain_cycle") in blockers(document)


def test_unknown_mission_key():
    document = chain_document()
    document["links"][1]["to"] = "nowhere"
    assert ("/links/1/to", "unknown_mission_key") in blockers(document)


def test_unbound_chain_output_when_supplier_root_does_not_project_it():
    document = chain_document()
    document["links"][0]["outputs"] = ["claims"]
    found = blockers(document)
    assert ("/links/0/outputs/0", "unbound_chain_output") in found
    # The consumer's input is then not bound by any supplies link either.
    assert ("/missions/1/program/inputs/0/from", "unbound_chain_output") in found


def test_consumer_input_needs_a_supplies_link_that_binds_it():
    document = chain_document()
    document["links"] = [document["links"][1]]
    assert blockers(document) == {("/missions/1/program/inputs/0/from", "unbound_chain_output")}


def test_supplies_link_needs_a_matching_consumer_input():
    document = chain_document()
    document["missions"][1]["program"]["inputs"] = []
    assert blockers(document) == {("/links/0/outputs/0", "unbound_chain_output")}


def test_schema_mismatch():
    document = chain_document()
    document["missions"][1]["program"]["inputs"][0]["schema"] = "source_manifest@1"
    assert blockers(document) == {("/missions/1/program/inputs/0/schema", "schema_mismatch")}
    document["missions"][1]["program"]["inputs"][0]["schema"] = "claim_table@1"
    assert blockers(document) == set()


def test_missing_incoming_link_and_first_mission_rule():
    document = three_missions()
    document["links"] = document["links"][:2]
    document["missions"][2]["program"]["inputs"] = []
    assert blockers(document) == {("/missions/2/key", "missing_incoming_link")}


def test_unknown_release_goal():
    document = chain_document()
    document["links"][0]["on"] = {"goal_accepted": {"goal_key": "ingested"}}
    assert ("/links/0/on/goal_accepted/goal_key", "unknown_goal") in blockers(document)


def test_single_mission_has_no_chain_section():
    document = chain_document()
    single = {"manifest": "mission/v1", "mission": document["missions"][0]}
    assert compile_chain(parse_manifest(single)).resolution is None


def test_compile_is_deterministic():
    first = compile_chain(parse_manifest(chain_document()))
    second = compile_chain(parse_manifest(chain_document()))
    assert chain_digest(first) == chain_digest(second)


# --- contracts -----------------------------------------------------------------------------


def built_chain() -> MissionChain:
    compilation = compile_chain(parse_manifest(chain_document()))
    assert compilation.resolution is not None
    return build_mission_chain(
        resolution=compilation.resolution,
        chain_id=CHAIN_ID,
        scope=SCOPE,
        chain_key="two-mission-chain",
        title="Research then ingestion",
        manifest_digest="sha256:" + "a" * 64,
        members={
            "research": (UUID(int=1), UUID(int=11)),
            "ingestion": (UUID(int=2), UUID(int=12)),
        },
        created_at=datetime(2026, 10, 7, tzinfo=UTC),
        created_by_actor_ref="actor:owner",
    )


def test_build_mission_chain_is_deterministic_and_links_are_armed():
    chain = built_chain()
    assert chain.schema_version == CHAIN_SCHEMA_VERSION
    assert [member.mission_key for member in chain.members] == ["research", "ingestion"]
    assert {link.state for link in chain.links} == {ChainLinkState.ARMED}
    assert chain.links[0].link_id == chain_link_id(CHAIN_ID, "research->ingestion:supplies")
    assert chain.links[0].schema_version == CHAIN_LINK_SCHEMA_VERSION
    assert chain_digest(chain) == chain_digest(built_chain())
    assert MissionChain.model_validate_json(chain.model_dump_json()) == chain


def test_contract_invariants():
    chain = built_chain()
    link = chain.links[0]
    with pytest.raises(ValidationError):
        ChainLink.model_validate({**link.model_dump(), "state": "released"})
    with pytest.raises(ValidationError):
        ChainLink.model_validate({**link.model_dump(), "state": "blocked"})
    with pytest.raises(ValidationError):
        ChainLink.model_validate({**link.model_dump(), "bindings": []})
    with pytest.raises(ValidationError):
        ReleaseCondition(kind="mission_accepted", goal_key="x")
    with pytest.raises(ValidationError):
        MissionChain.model_validate({**chain.model_dump(), "lifecycle": "completed"})
    with pytest.raises(ValidationError):
        MissionChain.model_validate(
            {**chain.model_dump(), "members": chain.model_dump()["members"][:1]}
        )


def test_forward_only_link_transitions():
    assert link_transition_allowed(ChainLinkState.ARMED, ChainLinkState.RELEASED)
    assert link_transition_allowed(ChainLinkState.RELEASED, ChainLinkState.DETACHED)
    assert not link_transition_allowed(ChainLinkState.RELEASED, ChainLinkState.ARMED)
    assert not link_transition_allowed(ChainLinkState.BLOCKED, ChainLinkState.RELEASED)
    transition = ChainTransition(cause_event_id=UUID(int=7))
    assert transition.is_noop
    transition = ChainTransition(
        cause_event_id=UUID(int=7),
        link_updates=(
            LinkUpdate(link_id=UUID(int=8), expected_version=1, state=ChainLinkState.BLOCKED),
        ),
    )
    assert not transition.is_noop


def test_release_condition_event_mapping():
    assert (
        ReleaseCondition(kind="goal_accepted", goal_key="g").satisfying_event
        == "disposition.recorded"
    )
    assert ReleaseCondition(kind="mission_accepted").satisfying_event == "mission.closed"
    assert ReleaseCondition(kind="execution_complete").satisfying_event == "run.completed"


@pytest.mark.parametrize("name", [CHAIN_SCHEMA_VERSION, CHAIN_LINK_SCHEMA_VERSION])
def test_committed_chain_json_schemas_are_generated(name: str):
    committed = (SCHEMAS / f"{name}.json").read_text(encoding="utf-8").replace("\r\n", "\n")
    assert committed == contract_schema_texts()[name]
    assert json.loads(committed)["title"] in {"MissionChain", "ChainLink"}
    assert set(chain_contract_schemas()) == {CHAIN_SCHEMA_VERSION, CHAIN_LINK_SCHEMA_VERSION}


# --- structural report and CLI ----------------------------------------------------------------


def test_structural_report_includes_the_chain_section():
    report = ManifestStructureService().compile(MISSION_2.read_text(encoding="utf-8"))
    assert report.ok
    assert report.resolution.schema_version == "mc.manifest_resolution.v1"
    assert report.resolution.chain is not None
    assert report.resolution.chain.order == ("research", "ingestion")
    assert [item.mission_key for item in report.definitions] == ["research", "ingestion"]
    assert report.resolution.catalog_resolution == "deferred"


def test_structural_report_reports_blockers_with_pointers():
    text = FIXTURE.read_text(encoding="utf-8").replace(
        "- { from: research, to: ingestion, kind: depends_on, on: execution_complete }",
        "- { from: research, to: elsewhere, kind: depends_on, on: execution_complete }",
    )
    report = ManifestStructureService().compile(text)
    assert not report.ok
    assert [(issue.pointer, issue.reason) for issue in report.blockers] == [
        ("/links/1/to", "unknown_mission_key")
    ]


def test_structural_report_on_unparseable_manifest():
    report = ManifestStructureService().compile("manifest: mission/v1\nmission: {key: x}\n")
    assert not report.ok
    assert report.resolution is None
    assert all(issue.pointer.startswith("/mission") for issue in report.blockers)


def test_missionctl_mission_compile(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    assert missionctl(["mission", "compile", str(MISSION_2), "--json"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["ok"] is True
    assert printed["resolution"]["chain"]["order"] == ["research", "ingestion"]
    broken = tmp_path / "broken.yml"
    broken.write_text(
        FIXTURE.read_text(encoding="utf-8").replace(
            "outputs: [evidence_map]", "outputs: [nothing]"
        ),
        encoding="utf-8",
    )
    assert missionctl(["mission", "compile", str(broken), "--json"]) == 2
    printed = json.loads(capsys.readouterr().out)
    assert {"pointer": "/links/0/outputs/0", "reason": "unbound_chain_output"}.items() <= printed[
        "blockers"
    ][0].items()
