from __future__ import annotations

import re
from typing import Any

from app.domain.schema_context.canonicalization import sha256_digest
from app.domain.schema_context.contracts import QueryExecutionIntent, SchemaOperationProjection
from app.domain.schema_context.errors import QueryIntentRejected

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FORBIDDEN_CYPHER = re.compile(
    r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|ALTER|GRANT|DENY|REVOKE|LOAD\s+CSV)\b",
    re.IGNORECASE,
)


def intent_digest(intent: QueryExecutionIntent) -> str:
    return sha256_digest(intent.model_dump(mode="json"))


def _contains_secret_or_embedding(value: Any, path: str = "") -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            name = f"{path}.{key}".lower()
            if any(term in name for term in ("password", "credential", "api_key", "token", "uri")):
                return True
            if "embedding" in name and isinstance(item, list):
                return True
            if _contains_secret_or_embedding(item, name):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_secret_or_embedding(item, path) for item in value)
    return False


def validate_query_intent(
    intent: QueryExecutionIntent, projection: SchemaOperationProjection
) -> None:
    if intent.projection_id != projection.projection_id:
        raise QueryIntentRejected("projection identity mismatch")
    if intent.projection_digest != projection.projection_digest:
        raise QueryIntentRejected("projection digest mismatch")
    if intent.schema_definition_digest != projection.source_schema_digest:
        raise QueryIntentRejected("schema digest mismatch")
    if intent.selection_digest != projection.accepted_selection_digest:
        raise QueryIntentRejected("selection digest mismatch")
    if intent.query_kind not in projection.permitted_query_kinds:
        raise QueryIntentRejected(f"query kind is not admitted: {intent.query_kind}")
    if intent.limit > projection.maximum_limit:
        raise QueryIntentRejected("requested limit exceeds projection maximum")
    if intent.max_depth > projection.maximum_traversal_depth:
        raise QueryIntentRejected("requested traversal depth exceeds projection maximum")
    unknown_labels = set(intent.labels) - set(projection.allowed_node_labels)
    unknown_relationships = set(intent.relationship_types) - set(
        projection.allowed_relationship_types
    )
    if unknown_labels:
        raise QueryIntentRejected(f"labels are not admitted: {sorted(unknown_labels)}")
    if unknown_relationships:
        raise QueryIntentRejected(
            f"relationships are not admitted: {sorted(unknown_relationships)}"
        )
    for label in intent.labels:
        allowed = set(projection.allowed_properties_by_label[label])
        invalid = {
            field
            for field in intent.requested_fields
            if field not in allowed and field not in {"*", "labels", "elementId"}
        }
        if invalid:
            raise QueryIntentRejected(f"requested fields are not admitted: {sorted(invalid)}")
    if _contains_secret_or_embedding(intent.parameters):
        raise QueryIntentRejected("intent contains secret-bearing or raw embedding parameters")
    if intent.proposed_cypher:
        if _FORBIDDEN_CYPHER.search(intent.proposed_cypher):
            raise QueryIntentRejected("proposed Cypher contains a write/admin operation")
        raise QueryIntentRejected("custom Cypher is not admitted; use a host-compiled query kind")
    if intent.query_kind in {"exact_identity", "entity_details", "bounded_neighborhood"}:
        if len(intent.labels) != 1:
            raise QueryIntentRejected("query kind requires exactly one anchor label")
        field = intent.parameters.get("field")
        if (
            not isinstance(field, str)
            or field not in projection.allowed_properties_by_label[intent.labels[0]]
        ):
            raise QueryIntentRejected("anchor field is not admitted for the label")
        if "value" not in intent.parameters:
            raise QueryIntentRejected("anchor value is required")
    if intent.query_kind == "bounded_neighborhood" and not intent.relationship_types:
        raise QueryIntentRejected("neighborhood intent requires relationship types")
    if intent.query_kind == "fulltext_search":
        admitted_indexes = {
            str(item.get("name") or item.get("indexName"))
            for item in projection.fulltext_capabilities
            if item.get("live_online")
        }
        if intent.parameters.get("index") not in admitted_indexes:
            raise QueryIntentRejected("full-text index is not admitted and online")
        if not isinstance(intent.parameters.get("query"), str):
            raise QueryIntentRejected("full-text query text is required")
    if intent.query_kind == "vector_search":
        if not intent.semantic_query_text:
            raise QueryIntentRejected("vector intent requires semantic query text")
        if any("embedding" in key.lower() for key in intent.parameters):
            raise QueryIntentRejected("vector intent cannot contain a raw embedding")


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise QueryIntentRejected("unsafe schema identifier")
    return f"`{value}`"


def compile_query_intent(intent: QueryExecutionIntent) -> tuple[str, dict[str, Any]]:
    label = _identifier(intent.labels[0]) if intent.labels else ""
    parameters: dict[str, Any] = {"limit": intent.limit}
    if intent.query_kind in {"exact_identity", "entity_details"}:
        field = _identifier(str(intent.parameters["field"]))
        parameters["value"] = intent.parameters["value"]
        return (
            f"MATCH (n:{label}) WHERE n.{field} = $value "
            "RETURN n{.*, __labels: labels(n), __element_id: elementId(n)} AS entity "
            "LIMIT $limit",
            parameters,
        )
    if intent.query_kind == "fulltext_search":
        parameters.update(
            {"index": intent.parameters["index"], "query": intent.parameters["query"]}
        )
        return (
            "CALL db.index.fulltext.queryNodes($index, $query, {limit: $limit}) "
            "YIELD node, score "
            "RETURN node{.*, __labels: labels(node), __element_id: elementId(node)} AS entity, "
            "score LIMIT $limit",
            parameters,
        )
    if intent.query_kind == "bounded_neighborhood":
        field = _identifier(str(intent.parameters["field"]))
        rel_types = "|".join(_identifier(value) for value in intent.relationship_types)
        parameters["value"] = intent.parameters["value"]
        return (
            f"MATCH (n:{label}) WHERE n.{field} = $value "
            f"MATCH (n)-[r:{rel_types}]-(m) "
            "RETURN n{.*, __labels: labels(n), __element_id: elementId(n)} AS source, "
            "type(r) AS relationship_type, properties(r) AS relationship_properties, "
            "m{.*, __labels: labels(m), __element_id: elementId(m)} AS target "
            "LIMIT $limit",
            parameters,
        )
    if intent.query_kind == "vector_search":
        raise QueryIntentRejected("vector compilation requires a host-generated embedding")
    raise QueryIntentRejected("unsupported baseline query kind")
