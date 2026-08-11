from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from graphql import GraphQLError, parse
from graphql.language.ast import DocumentNode
from pydantic import BaseModel, ConfigDict, Field

from app.domain.schema_catalog import (
    CATALOG_CORE_GENERATOR_VERSION,
    CatalogParseError,
    SemanticOverlay,
    derive_catalog,
    load_semantic_overlay,
    parse_physical_schema,
    render_tier_zero,
    require_valid_catalog_overlay,
)
from app.domain.schema_context.canonicalization import (
    canonical_json_bytes,
    safe_relative_path,
    sha256_digest,
    write_json,
    write_text,
)
from app.domain.schema_context.errors import SchemaParseError

CATALOG_GENERATOR_VERSION = CATALOG_CORE_GENERATOR_VERSION
DEFAULT_SEMANTIC_OVERLAY = (
    Path(__file__).resolve().parents[2] / "schema-catalog" / "semantic-overlay.v1.json"
)


class SchemaCatalog(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_ref: str
    source_digest: str
    source_bytes: int
    generator_version: str
    catalog_digest: str
    nodes: dict[str, dict[str, Any]]
    relationships: dict[str, dict[str, Any]]
    enums: dict[str, tuple[str, ...]]
    unions: dict[str, tuple[str, ...]]
    interfaces: dict[str, dict[str, Any]]
    other_definitions: dict[str, str]
    fulltext_indexes: tuple[dict[str, Any], ...]
    vector_indexes: tuple[dict[str, Any], ...]
    semantic_modules: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    tier_zero: dict[str, Any] | None = None


def parse_schema_catalog(
    source: bytes,
    source_ref: str,
    *,
    semantic_overlay: SemanticOverlay | Path | None = None,
) -> SchemaCatalog:
    try:
        physical = parse_physical_schema(source, source_ref)
    except CatalogParseError as error:
        raise SchemaParseError(str(error)) from error
    loaded_overlay: SemanticOverlay | None = None
    derived = None
    if semantic_overlay is not None:
        loaded_overlay = (
            semantic_overlay
            if isinstance(semantic_overlay, SemanticOverlay)
            else load_semantic_overlay(semantic_overlay)
        )
        require_valid_catalog_overlay(physical, loaded_overlay)
        derived = derive_catalog(physical, loaded_overlay)
    else:
        derived = derive_catalog(
            physical,
            SemanticOverlay(overlay_version="1", modules=(), elements={}),
        )

    def legacy_directives(values: Any) -> list[dict[str, Any]]:
        return [value.model_dump(mode="json") for value in values]

    def legacy_field(value: Any) -> dict[str, Any]:
        return {
            "name": value.name,
            "description": value.description,
            "type": value.type_expression,
            "named_type": value.named_type,
            "nullable": value.nullable,
            "list": value.is_list,
            "directives": legacy_directives(value.directives),
        }

    typed_objects = {**physical.nodes, **physical.relationship_property_types}
    nodes = {
        name: {
            "name": value.name,
            "description": value.description,
            "interfaces": list(value.interfaces),
            "directives": legacy_directives(value.directives),
            "fields": [legacy_field(field) for field in value.fields],
            "identity_candidates": list(value.identity_candidates),
            "search_candidates": list(value.search_candidates),
            "aliases": list(value.aliases),
            "relationship_properties": name in physical.relationship_property_types,
            "sdl": value.sdl,
            "semantic": (
                loaded_overlay.elements[
                    (
                        f"relationship-property:{name}"
                        if name in physical.relationship_property_types
                        else f"node:{name}"
                    )
                ].model_dump(mode="json")
                if loaded_overlay is not None
                and (
                    f"relationship-property:{name}"
                    if name in physical.relationship_property_types
                    else f"node:{name}"
                )
                in loaded_overlay.elements
                else None
            ),
        }
        for name, value in typed_objects.items()
    }
    relationships = {
        name: {
            "type": name,
            "endpoints": [
                {
                    "source": endpoint.declaring_type,
                    "target": endpoint.related_type,
                    "field": endpoint.field_name,
                    "direction": endpoint.direction,
                    "properties_type": endpoint.properties_type,
                    "field_type": endpoint.field_type,
                    "directives": legacy_directives(endpoint.directives),
                    "element_id": endpoint.element_id,
                    "physical_start": endpoint.physical_start_type,
                    "physical_end": endpoint.physical_end_type,
                    "semantic": (
                        loaded_overlay.elements[endpoint.element_id].model_dump(mode="json")
                        if loaded_overlay is not None
                        and endpoint.element_id in loaded_overlay.elements
                        else None
                    ),
                }
                for endpoint in relationship.endpoints
            ],
            "property_types": list(relationship.property_types),
            "semantic": (
                loaded_overlay.elements[f"relationship-type:{name}"].model_dump(mode="json")
                if loaded_overlay is not None
                and f"relationship-type:{name}" in loaded_overlay.elements
                else None
            ),
        }
        for name, relationship in physical.relationships.items()
    }
    interfaces = {
        name: {
            "name": value.name,
            "description": value.description,
            "fields": [legacy_field(field) for field in value.fields],
            "directives": legacy_directives(value.directives),
            "sdl": value.sdl,
        }
        for name, value in physical.interfaces.items()
    }
    fulltext = [
        {"node": value.node_type, "directive": value.directive, **value.arguments}
        for value in physical.fulltext_indexes
    ]
    vector = [
        {"node": value.node_type, "directive": value.directive, **value.arguments}
        for value in physical.vector_indexes
    ]
    semantic_modules = derived.modules
    tier_zero = render_tier_zero(derived).model_dump(
        mode="json", exclude_defaults=True, exclude_none=True
    )
    logical = {
        "source_digest": physical.source_digest,
        "source_bytes": physical.source_bytes,
        "generator_version": physical.generator_version,
        "nodes": nodes,
        "relationships": relationships,
        "enums": physical.enums,
        "unions": physical.unions,
        "interfaces": interfaces,
        "other_definitions": physical.other_definitions,
        "fulltext_indexes": fulltext,
        "vector_indexes": vector,
        "semantic_modules": semantic_modules,
        "tier_zero": tier_zero,
    }
    return SchemaCatalog(
        source_ref=source_ref,
        **logical,
        catalog_digest=sha256_digest(logical),
    )


_MODULE_TERMS = {
    "organizations-and-people": {
        "Organization",
        "OrganizationSnapshot",
        "Person",
        "Team",
        "Role",
    },
    "products-and-commerce": {"Product", "ProductSnapshot", "Offering", "Brand"},
    "diagnostics-and-biomarkers": {
        "LabTest",
        "Biomarker",
        "PanelDefinition",
        "TechnologyPlatform",
        "Metric",
    },
    "studies-and-evidence": {"Study", "Evidence", "ClinicalTrial", "Publication"},
    "documents-and-provenance": {"Document", "Source", "Citation", "Artifact"},
}


def _modules(catalog: SchemaCatalog) -> dict[str, dict[str, Any]]:
    if catalog.semantic_modules:
        governed: dict[str, dict[str, Any]] = {}
        for module, members in sorted(catalog.semantic_modules.items()):
            governed[module] = {
                "name": module,
                "nodes": sorted(
                    member.removeprefix("node:") for member in members if member.startswith("node:")
                ),
                "relationships": sorted(
                    member.removeprefix("relationship-type:")
                    for member in members
                    if member.startswith("relationship-type:")
                ),
                "element_ids": list(members),
            }
        return governed
    modules: dict[str, dict[str, Any]] = {}
    assigned: set[str] = set()
    for module, seeds in _MODULE_TERMS.items():
        node_names = sorted(
            name
            for name in catalog.nodes
            if name in seeds or any(seed.lower() in name.lower() for seed in seeds)
        )
        assigned.update(node_names)
        modules[module] = {
            "name": module,
            "nodes": node_names,
            "relationships": sorted(
                rel_type
                for rel_type, rel in catalog.relationships.items()
                if any(
                    endpoint["source"] in node_names or endpoint["target"] in node_names
                    for endpoint in rel["endpoints"]
                )
            ),
        }
    modules["remaining-schema"] = {
        "name": "remaining-schema",
        "nodes": sorted(set(catalog.nodes) - assigned),
        "relationships": sorted(catalog.relationships),
    }
    return modules


def _markdown_card(title: str, payload: dict[str, Any]) -> str:
    lines = [f"# {title}", ""]
    if payload.get("description"):
        lines.extend([str(payload["description"]), ""])
    lines.extend(["```json", canonical_json_bytes(payload).decode().rstrip(), "```"])
    return "\n".join(lines)


def materialize_schema_catalog(
    catalog: SchemaCatalog,
    source: bytes,
    schema_root: Path,
) -> dict[str, Any]:
    resources: list[dict[str, Any]] = []

    def record(path: str, payload: Any, kind: str, media_type: str = "application/json") -> None:
        relative = safe_relative_path(path)
        destination = schema_root / relative
        digest = (
            write_json(destination, payload)
            if media_type == "application/json"
            else write_text(destination, str(payload))
        )
        resources.append(
            {
                "logical_path": f"schema/{relative.as_posix()}",
                "content_digest": digest,
                "source_schema_digest": catalog.source_digest,
                "catalog_digest": catalog.catalog_digest,
                "media_type": media_type,
                "resource_kind": kind,
                "read_only": True,
                "size_bytes": destination.stat().st_size,
            }
        )

    source_path = schema_root / "source" / "neo4jbiotechschema.graphql"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(source)
    resources.append(
        {
            "logical_path": "schema/source/neo4jbiotechschema.graphql",
            "content_digest": sha256_digest(source),
            "source_schema_digest": catalog.source_digest,
            "catalog_digest": catalog.catalog_digest,
            "media_type": "application/graphql",
            "resource_kind": "authoritative_sdl_copy",
            "read_only": True,
            "size_bytes": len(source),
        }
    )
    compact = catalog.model_dump(mode="json", exclude={"other_definitions"})
    record("global/compact-schema.json", compact, "compact_schema")
    record(
        "global/compact-schema.md",
        _markdown_card("Compact schema", compact),
        "compact_schema",
        "text/markdown",
    )
    modules = _modules(catalog)
    record("global/module-index.json", modules, "module_index")
    record(
        "global/module-index.md",
        _markdown_card("Module index", modules),
        "module_index",
        "text/markdown",
    )
    topology = {
        rel_type: rel["endpoints"] for rel_type, rel in sorted(catalog.relationships.items())
    }
    record("global/topology-index.json", topology, "topology_index")
    record(
        "global/topology-index.md",
        _markdown_card("Topology index", topology),
        "topology_index",
        "text/markdown",
    )
    search_index = {name: node["search_candidates"] for name, node in sorted(catalog.nodes.items())}
    identity_index = {
        name: node["identity_candidates"] for name, node in sorted(catalog.nodes.items())
    }
    record("global/search-index.json", search_index, "search_index")
    record("global/identity-index.json", identity_index, "identity_index")
    for module, payload in modules.items():
        record(f"modules/{module}.json", payload, "schema_module")

    incoming: dict[str, list[dict[str, Any]]] = defaultdict(list)
    outgoing: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rel_type, rel in catalog.relationships.items():
        for endpoint in rel["endpoints"]:
            item = {"relationship_type": rel_type, **endpoint}
            outgoing[endpoint.get("physical_start", endpoint["source"])].append(item)
            incoming[endpoint.get("physical_end", endpoint["target"])].append(item)

    for name, node in sorted(catalog.nodes.items()):
        card = {
            **node,
            "outgoing_relationships": sorted(outgoing[name], key=str),
            "incoming_relationships": sorted(incoming[name], key=str),
            "fulltext_indexes": [item for item in catalog.fulltext_indexes if item["node"] == name],
            "vector_indexes": [item for item in catalog.vector_indexes if item["node"] == name],
            "source_schema_digest": catalog.source_digest,
            "catalog_digest": catalog.catalog_digest,
            "recommended_drilldown_paths": [f"schema/drilldown/nodes/{name}.json"],
        }
        record(f"cards/nodes/{name}.json", card, "node_card")
        record(f"cards/nodes/{name}.md", _markdown_card(name, card), "node_card", "text/markdown")
        record(f"drilldown/nodes/{name}.json", card, "node_drilldown")
    for rel_type, relationship in sorted(catalog.relationships.items()):
        property_fields = {
            name: catalog.nodes[name]["fields"]
            for name in relationship["property_types"]
            if name in catalog.nodes
        }
        card = {
            **relationship,
            "relationship_property_fields": property_fields,
            "immediate_neighboring_types": sorted(
                {
                    endpoint[key]
                    for endpoint in relationship["endpoints"]
                    for key in ("source", "target")
                }
            ),
            "source_schema_digest": catalog.source_digest,
            "catalog_digest": catalog.catalog_digest,
            "example_read_patterns": [
                f"MATCH (a:{endpoint.get('physical_start', endpoint['source'])})"
                f"-[r:{rel_type}]->"
                f"(b:{endpoint.get('physical_end', endpoint['target'])}) "
                "RETURN a, r, b LIMIT $limit"
                for endpoint in relationship["endpoints"]
            ],
        }
        record(f"cards/relationships/{rel_type}.json", card, "relationship_card")
        record(
            f"cards/relationships/{rel_type}.md",
            _markdown_card(rel_type, card),
            "relationship_card",
            "text/markdown",
        )
        record(f"drilldown/relationships/{rel_type}.json", card, "relationship_drilldown")
    for kind, values in (
        ("enums", catalog.enums),
        ("unions", catalog.unions),
        ("interfaces", catalog.interfaces),
    ):
        for name, card_payload in sorted(values.items()):
            record(f"cards/{kind}/{name}.json", card_payload, f"{kind[:-1]}_card")
    record("indexes/fulltext.json", catalog.fulltext_indexes, "fulltext_indexes")
    record("indexes/vector.json", catalog.vector_indexes, "vector_indexes")
    record(
        "indexes/aliases.json",
        {name: node["aliases"] for name, node in catalog.nodes.items() if node["aliases"]},
        "alias_index",
    )
    record(
        "indexes/lexical-terms.json",
        sorted(set(catalog.nodes) | set(catalog.relationships) | set(catalog.enums)),
        "lexical_index",
    )
    skill = """# Schema navigation

1. Read `schema/manifest.json`, then the compact schema and module index.
2. Choose candidate modules before opening node or relationship cards.
3. Inspect only relevant cards; use drill-down files when cards are insufficient.
4. Never invent schema names. Record exclusions, near misses, and unresolved mappings.
5. Select semantic membership only; deterministic host expansion adds structural closure.
6. Schema files provide context, not graph authority. Do not attempt Neo4j access.
7. Avoid reading the full authoritative SDL unless the indexed resources are insufficient.
"""
    record("skills/schema-navigation/SKILL.md", skill, "navigation_skill", "text/markdown")
    manifest = {
        "schema_definition_ref": catalog.source_ref,
        "schema_definition_digest": catalog.source_digest,
        "catalog_digest": catalog.catalog_digest,
        "generator_version": catalog.generator_version,
        "resources": sorted(resources, key=lambda item: item["logical_path"]),
    }
    write_json(schema_root / "manifest.json", manifest)
    return manifest


def parse_document(source: str) -> DocumentNode:
    try:
        return parse(source)
    except GraphQLError as error:
        raise SchemaParseError(f"SDL parse failed: {type(error).__name__}") from error
