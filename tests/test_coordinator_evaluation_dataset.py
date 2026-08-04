from __future__ import annotations

import json
from pathlib import Path

DATASET = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "coordinator_retrieval_evaluation.json"
)


def test_coordinator_evaluation_dataset_covers_required_adversarial_cases() -> None:
    payload = json.loads(DATASET.read_text(encoding="utf-8"))
    cases = {case["case_id"]: case for case in payload["cases"]}

    assert len(cases) >= 20
    assert {
        "schema-direct-id",
        "schema-paraphrase-minimal-context",
        "schema-paraphrase-grounded-fields",
        "tenant-forbidden-capability",
        "skill-description-injection",
        "skill-unsafe-script",
        "alias-moves-before-prepare",
        "goal-directed-contract-negatives",
        "web-natural-language-no-provider-names",
        "web-sibling-tool-rejection",
        "browser-runtime-grant-missing",
        "projection-and-embedding-negatives",
    } <= cases.keys()
    web_case = cases["web-natural-language-no-provider-names"]
    normalized_query = web_case["query"].casefold()
    assert all(
        term.casefold() not in normalized_query
        for term in web_case["forbidden_query_terms"]
    )
    assert len(web_case["expected_capability_assets"]) == 7
