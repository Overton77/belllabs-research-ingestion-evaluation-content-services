from __future__ import annotations

from pathlib import Path

from app.domain.schema_context.canonicalization import write_json
from app.experiments.schema_context_selection.comparison import (
    compare_schema_context_runs,
    comparison_markdown,
)


def _run(root: Path, run_id: str, *, candidate: bool) -> None:
    inputs = {"schema": "s", "report": "r", "structured_candidates": "c"}
    write_json(
        root / "result.json",
        {
            "run_id": run_id,
            "status": "completed",
            "catalog_digest": f"catalog-{run_id}",
            "input_digests": inputs,
            "compatibility_decision": {"compatible": True},
        },
    )
    write_json(
        root / "metrics.json",
        {
            "workspace": {
                "catalog_resource_count": 67 if candidate else 1046,
                "total_catalog_bytes": 400_000 if candidate else 3_650_000,
            },
            "selection": {"revision_count": 1 if candidate else 2},
            "runtime": {
                "input_tokens": 100 if candidate else 400,
                "total_tokens": 120 if candidate else 450,
                "total_elapsed_ms": 1000 if candidate else 2000,
            },
            "query": {
                "known_oracle_entity_recall": 1.0,
                "oracle_terms_recovered": ["TruAge", "TruHealth", "TruAge + TruHealth"],
                "intent_count": 5,
                "successful_count": 5,
                "rejected_count": 0,
                "failed_count": 0,
                "total_records": 32,
            },
        },
    )
    write_json(
        root / "selection/accepted.json",
        {
            "acceptance_decision": "accepted",
            "selection": {
                "selected_node_types": sorted(_CORE_NODES),
                "selected_relationship_types": ["IMPLEMENTS", "OFFERS"],
            },
        },
    )


_CORE_NODES = {
    "LabTest",
    "Organization",
    "PanelDefinition",
    "Product",
    "TechnologyPlatform",
}


def test_comparison_accepts_preserved_workload_with_efficiency_improvement(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    _run(baseline, "baseline", candidate=False)
    _run(candidate, "candidate", candidate=True)

    comparison = compare_schema_context_runs(baseline, candidate)

    assert comparison["accepted"]
    assert comparison["metrics"]["catalog_bytes"]["percent"] < -80
    assert all(gate["passed"] for gate in comparison["gates"])
    assert "Schema context A/B comparison: PASS" in comparison_markdown(comparison)


def test_comparison_rejects_missing_required_relationship(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    _run(baseline, "baseline", candidate=False)
    _run(candidate, "candidate", candidate=True)
    accepted = candidate / "selection/accepted.json"
    payload = __import__("json").loads(accepted.read_text(encoding="utf-8"))
    payload["selection"]["selected_relationship_types"] = ["IMPLEMENTS_PLATFORM"]
    write_json(accepted, payload)

    comparison = compare_schema_context_runs(baseline, candidate)

    assert not comparison["accepted"]
    gate = next(
        item for item in comparison["gates"] if item["id"] == "product_platform_relationship"
    )
    assert not gate["passed"]
