from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.domain.schema_context.canonicalization import sha256_digest

_CORE_NODES = {
    "LabTest",
    "Organization",
    "PanelDefinition",
    "Product",
    "TechnologyPlatform",
}
_OFFERED_PRODUCTS = {"TruAge", "TruHealth", "TruAge + TruHealth"}


def _read_json(root: Path, relative: str) -> dict[str, Any]:
    path = root / relative
    if not path.is_file():
        raise FileNotFoundError(path)
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _metric(payload: dict[str, Any], *path: str, default: Any = None) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _change(baseline: Any, candidate: Any) -> dict[str, Any]:
    result = {"baseline": baseline, "candidate": candidate, "delta": None, "percent": None}
    if isinstance(baseline, int | float) and isinstance(candidate, int | float):
        result["delta"] = candidate - baseline
        if baseline != 0:
            result["percent"] = ((candidate - baseline) / baseline) * 100
    return result


def compare_schema_context_runs(baseline_root: Path, candidate_root: Path) -> dict[str, Any]:
    """Compare two completed run artifacts and evaluate the tuned workload gates."""
    baseline_root = baseline_root.resolve()
    candidate_root = candidate_root.resolve()
    baseline_result = _read_json(baseline_root, "result.json")
    candidate_result = _read_json(candidate_root, "result.json")
    baseline_metrics = _read_json(baseline_root, "metrics.json")
    candidate_metrics = _read_json(candidate_root, "metrics.json")
    baseline_accepted = _read_json(baseline_root, "selection/accepted.json")
    candidate_accepted = _read_json(candidate_root, "selection/accepted.json")

    baseline_selection = baseline_accepted["selection"]
    candidate_selection = candidate_accepted["selection"]
    baseline_nodes = set(baseline_selection["selected_node_types"])
    candidate_nodes = set(candidate_selection["selected_node_types"])
    baseline_relationships = set(baseline_selection["selected_relationship_types"])
    candidate_relationships = set(candidate_selection["selected_relationship_types"])
    recovered = set(_metric(candidate_metrics, "query", "oracle_terms_recovered", default=[]))

    gates: list[dict[str, Any]] = []

    def gate(gate_id: str, passed: bool, detail: str) -> None:
        gates.append({"id": gate_id, "passed": passed, "detail": detail})

    same_inputs = all(
        baseline_result["input_digests"].get(name) == candidate_result["input_digests"].get(name)
        for name in ("schema", "report", "structured_candidates")
    )
    gate("same_workload_inputs", same_inputs, "Schema, report, and candidate digests match.")
    gate(
        "candidate_completed",
        candidate_result.get("status") == "completed",
        f"Candidate status is {candidate_result.get('status')!r}.",
    )
    gate(
        "independent_acceptance",
        candidate_accepted.get("acceptance_decision") == "accepted",
        "Candidate has a persisted independently accepted selection.",
    )
    gate(
        "core_semantic_membership",
        _CORE_NODES <= candidate_nodes,
        f"Missing core nodes: {sorted(_CORE_NODES - candidate_nodes)}.",
    )
    gate(
        "product_platform_relationship",
        "IMPLEMENTS" in candidate_relationships,
        "Product.implementsPlatforms requires the exact IMPLEMENTS relationship type.",
    )
    candidate_recall = float(
        _metric(candidate_metrics, "query", "known_oracle_entity_recall", default=0.0)
    )
    baseline_recall = float(
        _metric(baseline_metrics, "query", "known_oracle_entity_recall", default=0.0)
    )
    gate(
        "oracle_recall_preserved",
        candidate_recall >= baseline_recall and candidate_recall == 1.0,
        f"Candidate recall {candidate_recall:.3f}; baseline {baseline_recall:.3f}.",
    )
    gate(
        "all_offered_products_recovered",
        _OFFERED_PRODUCTS <= recovered,
        f"Missing products: {sorted(_OFFERED_PRODUCTS - recovered)}.",
    )
    rejected = int(_metric(candidate_metrics, "query", "rejected_count", default=-1))
    failed = int(_metric(candidate_metrics, "query", "failed_count", default=-1))
    successful = int(_metric(candidate_metrics, "query", "successful_count", default=0))
    intents = int(_metric(candidate_metrics, "query", "intent_count", default=0))
    gate(
        "safe_successful_queries",
        rejected == 0 and failed == 0 and successful == intents and intents > 0,
        f"intents={intents}, successful={successful}, rejected={rejected}, failed={failed}.",
    )
    compatibility = candidate_result.get("compatibility_decision") or {}
    gate(
        "schema_compatibility",
        compatibility.get("compatible") is True,
        "Candidate passed the exact deployed-schema compatibility gate.",
    )

    comparison = {
        "baseline_run_id": baseline_result["run_id"],
        "candidate_run_id": candidate_result["run_id"],
        "baseline_catalog_digest": baseline_result["catalog_digest"],
        "candidate_catalog_digest": candidate_result["catalog_digest"],
        "input_digests": candidate_result["input_digests"],
        "gates": gates,
        "accepted": all(item["passed"] for item in gates),
        "selection_membership": {
            "nodes_added": sorted(candidate_nodes - baseline_nodes),
            "nodes_removed": sorted(baseline_nodes - candidate_nodes),
            "relationships_added": sorted(candidate_relationships - baseline_relationships),
            "relationships_removed": sorted(baseline_relationships - candidate_relationships),
        },
        "metrics": {
            "catalog_resources": _change(
                _metric(baseline_metrics, "workspace", "catalog_resource_count"),
                _metric(candidate_metrics, "workspace", "catalog_resource_count"),
            ),
            "catalog_bytes": _change(
                _metric(baseline_metrics, "workspace", "total_catalog_bytes"),
                _metric(candidate_metrics, "workspace", "total_catalog_bytes"),
            ),
            "selection_revisions": _change(
                _metric(baseline_metrics, "selection", "revision_count"),
                _metric(candidate_metrics, "selection", "revision_count"),
            ),
            "input_tokens": _change(
                _metric(baseline_metrics, "runtime", "input_tokens"),
                _metric(candidate_metrics, "runtime", "input_tokens"),
            ),
            "total_tokens": _change(
                _metric(baseline_metrics, "runtime", "total_tokens"),
                _metric(candidate_metrics, "runtime", "total_tokens"),
            ),
            "elapsed_ms": _change(
                _metric(baseline_metrics, "runtime", "total_elapsed_ms"),
                _metric(candidate_metrics, "runtime", "total_elapsed_ms"),
            ),
            "oracle_recall": _change(baseline_recall, candidate_recall),
            "query_records": _change(
                _metric(baseline_metrics, "query", "total_records"),
                _metric(candidate_metrics, "query", "total_records"),
            ),
        },
    }
    return {**comparison, "comparison_digest": sha256_digest(comparison)}


def comparison_markdown(comparison: dict[str, Any]) -> str:
    status = "PASS" if comparison["accepted"] else "FAIL"
    lines = [
        f"# Schema context A/B comparison: {status}",
        "",
        f"Baseline: `{comparison['baseline_run_id']}`  ",
        f"Candidate: `{comparison['candidate_run_id']}`  ",
        f"Comparison digest: `{comparison['comparison_digest']}`",
        "",
        "## Acceptance gates",
        "",
    ]
    for gate in comparison["gates"]:
        marker = "x" if gate["passed"] else " "
        lines.append(f"- [{marker}] `{gate['id']}` — {gate['detail']}")
    lines.extend(
        [
            "",
            "## Metric changes",
            "",
            "| Metric | Baseline | Candidate | Change |",
            "|---|---:|---:|---:|",
        ]
    )
    for name, value in comparison["metrics"].items():
        percent = value["percent"]
        change = "n/a" if percent is None else f"{percent:+.1f}%"
        lines.append(f"| {name} | {value['baseline']} | {value['candidate']} | {change} |")
    return "\n".join(lines) + "\n"
