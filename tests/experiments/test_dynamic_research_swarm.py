from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.experiments.dynamic_research_swarm.contracts import (
    AgentClaim,
    MissionPlan,
    ResearchUnitResult,
    ResearchUnitSpec,
    SourceSnapshot,
    UnitAnalysis,
    sha256_text,
)
from app.experiments.dynamic_research_swarm.evaluators import evaluate_unit
from app.experiments.dynamic_research_swarm.temporal_activities import _repair_mojibake


def _source(text: str, *, digest: str | None = None) -> SourceSnapshot:
    return SourceSnapshot(
        source_id="unit_a:S1",
        title="Fixture source",
        url="https://example.org/study",
        retrieved_at=datetime.now(UTC),
        text=text,
        text_sha256=digest or sha256_text(text),
    )


def _result(source: SourceSnapshot, claim: AgentClaim) -> ResearchUnitResult:
    return ResearchUnitResult(
        unit=ResearchUnitSpec(
            unit_id="unit_a",
            question="What did the bounded fixture study report?",
            search_query="bounded fixture study sample size",
            rationale="Exercise deterministic evidence gates.",
        ),
        sources=(source,),
        analysis=UnitAnalysis(answer="Fixture answer grounded in S1.", claims=(claim,)),
    )


def test_faithful_numeric_claim_is_accepted_deterministically() -> None:
    quote = "The study enrolled 34 participants in 2021 and reported no serious adverse events."
    source = _source(f"Background. {quote} End.")
    claim = AgentClaim(
        claim_text="The study enrolled 34 participants in 2021.",
        claim_type="numeric",
        source_id=source.source_id,
        source_quote=quote,
        numeric_mentions=("34", "2021"),
    )
    first = evaluate_unit("run-a", _result(source, claim))[0]
    second = evaluate_unit("run-a", _result(source, claim))[0]
    assert first.disposition == "ACCEPT"
    assert first.report_sha256 == second.report_sha256
    assert first.evaluation_id == second.evaluation_id


@pytest.mark.parametrize(
    ("claim_text", "declared"),
    [
        ("The study enrolled 43 participants in 2021.", ("43", "2021")),
        ("The study enrolled 34 participants in 2022.", ("34", "2022")),
        ("The study enrolled 34 participants in 2021.", ("34",)),
    ],
)
def test_numeric_mutations_and_omissions_are_rejected(
    claim_text: str, declared: tuple[str, ...]
) -> None:
    quote = "The study enrolled 34 participants in 2021."
    source = _source(quote)
    claim = AgentClaim(
        claim_text=claim_text,
        claim_type="numeric",
        source_id=source.source_id,
        source_quote=quote,
        numeric_mentions=declared,
    )
    assert evaluate_unit("run-a", _result(source, claim))[0].disposition == "REJECT"


def test_inserted_quote_and_altered_snapshot_hash_are_rejected() -> None:
    source = _source("The intervention may improve the measured marker.", digest="0" * 64)
    claim = AgentClaim(
        claim_text="The intervention may improve the measured marker.",
        claim_type="direct_quote",
        source_id=source.source_id,
        source_quote="The intervention may significantly improve the measured marker.",
    )
    evaluation = evaluate_unit("run-a", _result(source, claim))[0]
    assert evaluation.disposition == "REJECT"
    assert not next(gate for gate in evaluation.gates if gate.gate == "source_hash").passed
    assert not next(gate for gate in evaluation.gates if gate.gate == "exact_evidence_span").passed


def test_source_modality_cannot_be_strengthened() -> None:
    quote = "The intervention may improve the measured marker in mice."
    source = _source(quote)
    claim = AgentClaim(
        claim_text="The intervention improves the measured marker in mice.",
        claim_type="fact",
        source_id=source.source_id,
        source_quote=quote,
    )
    evaluation = evaluate_unit("run-a", _result(source, claim))[0]
    assert evaluation.disposition == "REJECT"
    assert not next(gate for gate in evaluation.gates if gate.gate == "modality_guard").passed


def test_mission_plan_rejects_duplicate_unit_identities() -> None:
    unit = ResearchUnitSpec(
        unit_id="same_unit",
        question="What distinct evidence should this unit find?",
        search_query="distinct evidence query",
        rationale="Bounded decomposition.",
    )
    with pytest.raises(ValueError, match="unique"):
        MissionPlan(
            objective="A sufficiently long research objective.",
            plan_summary="A sufficiently long plan summary.",
            units=(unit, unit),
        )


def test_planner_unit_id_is_normalized_before_safe_pattern_validation() -> None:
    unit = ResearchUnitSpec(
        unit_id="U1",
        question="What distinct evidence should this normalized unit find?",
        search_query="normalized unit evidence query",
        rationale="Normalize an otherwise safe planner identity.",
    )
    assert unit.unit_id == "u1"


def test_known_utf8_cp1252_mojibake_is_repaired() -> None:
    assert _repair_mojibake("a â€œquotedâ€ phrase") == "a “quoted” phrase"
