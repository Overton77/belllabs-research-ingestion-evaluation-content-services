from __future__ import annotations

import json
from typing import Any

from app.domain.schema_context.contracts import QueryExecutionResult, SchemaContextSelection

EXPECTED_NODES = {
    "Organization",
    "Product",
    "LabTest",
    "Biomarker",
    "PanelDefinition",
    "TechnologyPlatform",
}
EXPECTED_RELATIONSHIPS = {
    "OFFERS",
    "DELIVERS_LABTEST",
    "MEASURES",
    "IMPLEMENTS",
    "IMPLEMENTS_PANEL",
    "INCLUDES_BIOMARKER",
    "INCLUDES_LABTEST",
    "USES_PLATFORM",
    "DEVELOPS_PLATFORM",
}
ORACLE_TERMS = {
    "64720458-3328-5439-b6de-1624bd5b60ae",
    "TruDiagnostic",
    "TruAge",
    "TruHealth",
    "TruAge + TruHealth",
    "TruAge Epigenetic Biological Age Test",
    "TruHealth Epigenetic Biomarker Proxy Test",
    "TruDiagnostic Immune Cell Deconvolution Test",
    "TruAge Aging Panel",
    "SymphonyAge Organ Systems Panel",
    "Methylation Screening Array",
    "OMICmAge",
    "SymphonyAge",
    "DunedinPACE",
}


def evaluate_selection(selection: SchemaContextSelection) -> dict[str, Any]:
    expected = EXPECTED_NODES | EXPECTED_RELATIONSHIPS
    selected = set(selection.selected_node_types) | set(selection.selected_relationship_types)
    return {
        "expected_core_recall": len(selected & expected) / len(expected),
        "expected_core_missing": sorted(expected - selected),
        "selected_node_count": len(selection.selected_node_types),
        "selected_relationship_count": len(selection.selected_relationship_types),
        "unresolved_mapping_count": len(selection.unresolved_mappings),
        "near_miss_count": len(selection.near_miss_candidates),
    }


def evaluate_query_results(results: list[QueryExecutionResult]) -> dict[str, Any]:
    text = "\n".join(str(item.records) for item in results if item.status == "succeeded")
    recovered = sorted(term for term in ORACLE_TERMS if term in text)
    record_keys = [
        json.dumps(record, ensure_ascii=False, sort_keys=True, default=str)
        for result in results
        if result.status == "succeeded"
        for record in result.records
    ]
    return {
        "oracle_terms_recovered": recovered,
        "known_oracle_entity_recall": len(recovered) / len(ORACLE_TERMS),
        "successful_count": sum(item.status == "succeeded" for item in results),
        "rejected_count": sum(item.status == "rejected" for item in results),
        "failed_count": sum(item.status == "failed" for item in results),
        "total_records": sum(item.record_count for item in results),
        "duplicate_record_count": len(record_keys) - len(set(record_keys)),
        "truncated_result_count": sum(item.truncated for item in results),
    }
