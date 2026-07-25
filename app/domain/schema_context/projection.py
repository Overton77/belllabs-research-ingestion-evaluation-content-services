from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from app.domain.schema_context.canonicalization import sha256_digest
from app.domain.schema_context.contracts import (
    AcceptedSchemaContextSelection,
    ExpandedSchemaSlice,
    SchemaOperationProjection,
)

PROJECTION_VERSION = "read-query-reconciliation-v2"


def _index_name(index: dict) -> str | None:
    value = index.get("name") or index.get("indexName")
    return str(value) if value else None


def build_operation_projection(
    accepted: AcceptedSchemaContextSelection,
    expanded: ExpandedSchemaSlice,
    *,
    live_indexes: tuple[dict, ...] = (),
    allow_vector: bool = False,
) -> SchemaOperationProjection:
    online = {
        str(item.get("name")): item
        for item in live_indexes
        if str(item.get("state", "")).upper() == "ONLINE"
    }
    fulltext: list[dict] = []
    vector: list[dict] = []
    diagnostics: list[str] = []
    for declaration in expanded.fulltext_declarations:
        name = _index_name(declaration)
        admitted = bool(name and name in online)
        fulltext.append({**declaration, "live_online": admitted})
        if name and not admitted:
            diagnostics.append(f"full-text index unavailable or offline: {name}")
    for declaration in expanded.vector_declarations:
        name = _index_name(declaration)
        admitted = bool(allow_vector and name and name in online)
        if admitted:
            vector.append({**declaration, "live_online": True})
        elif name:
            diagnostics.append(f"vector index not admitted or unavailable: {name}")

    semantic_labels = set(accepted.selection.selected_node_types)
    labels = tuple(
        sorted(semantic_labels & set(expanded.selected_node_definitions))
    )
    relationships = tuple(sorted(expanded.selected_relationship_definitions))
    properties = {
        label: tuple(field["name"] for field in node["fields"])
        for label, node in sorted(expanded.selected_node_definitions.items())
        if label in semantic_labels
    }
    relationship_properties = {
        rel_type: tuple(
            sorted(
                {
                    field["name"]
                    for property_type in rel["property_types"]
                    for field in expanded.relationship_property_types.get(property_type, {}).get(
                        "fields", []
                    )
                }
            )
        )
        for rel_type, rel in sorted(expanded.selected_relationship_definitions.items())
    }
    traversals = tuple(
        {
            "source": endpoint["source"],
            "relationship_type": rel_type,
            "target": endpoint["target"],
            "direction": endpoint["direction"],
        }
        for rel_type, rel in sorted(expanded.selected_relationship_definitions.items())
        for endpoint in rel["endpoints"]
        if endpoint["source"] in semantic_labels and endpoint["target"] in semantic_labels
    )
    logical = {
        "version": PROJECTION_VERSION,
        "purpose": "read_query_reconciliation",
        "source_schema_digest": expanded.source_schema_digest,
        "accepted_selection_digest": accepted.accepted_selection_digest,
        "expanded_slice_digest": expanded.expanded_slice_digest,
        "allowed_node_labels": labels,
        "allowed_relationship_types": relationships,
        "allowed_properties_by_label": properties,
        "allowed_relationship_properties": relationship_properties,
        "allowed_traversals": traversals,
        "identity_fields_by_label": {
            label: fields
            for label, fields in expanded.identity_candidates.items()
            if label in semantic_labels
        },
        "exact_range_search_capabilities": properties,
        "fulltext_capabilities": tuple(fulltext),
        "vector_capabilities": tuple(vector),
        "permitted_query_kinds": (
            "exact_identity",
            "fulltext_search",
            "bounded_neighborhood",
            "entity_details",
        )
        + (("vector_search",) if allow_vector and vector else ()),
        "procedure_allowlist": ("db.index.fulltext.queryNodes",)
        + (("db.index.vector.queryNodes",) if allow_vector and vector else ()),
        "default_limit": 20,
        "maximum_limit": 100,
        "maximum_traversal_depth": 1,
        "timeout_seconds": 15.0,
        "result_policy": {
            "max_records": 100,
            "max_total_bytes": 250000,
            "max_string_chars": 4000,
            "max_list_items": 50,
            "max_map_keys": 50,
        },
        "live_capability_diagnostics": tuple(sorted(diagnostics)),
    }
    projection_digest = sha256_digest(logical)
    projection_id = str(uuid5(NAMESPACE_URL, f"schema-projection:{projection_digest}"))
    return SchemaOperationProjection(
        projection_id=projection_id,
        projection_digest=projection_digest,
        **logical,
    )
