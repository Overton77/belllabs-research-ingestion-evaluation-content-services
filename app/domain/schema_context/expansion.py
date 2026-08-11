from __future__ import annotations

from graphql import parse

from app.application.schema.schema_catalog import SchemaCatalog
from app.domain.schema_context.canonicalization import sha256_digest
from app.domain.schema_context.contracts import (
    AcceptedSchemaContextSelection,
    ExpandedSchemaSlice,
)

EXPANSION_POLICY_VERSION = "structural-closure-v2"
_BUILTINS = {"String", "ID", "Int", "Float", "Boolean", "Date", "DateTime", "Time", "LocalDateTime"}


def expand_selection(
    accepted: AcceptedSchemaContextSelection,
    catalog: SchemaCatalog,
) -> ExpandedSchemaSlice:
    selection = accepted.selection
    nodes = set(selection.selected_node_types)
    relationships: dict[str, dict] = {}
    diagnostics: list[str] = []
    abstract_endpoint_types: set[str] = set()

    def concrete_endpoint_names(name: str) -> tuple[str, ...]:
        if name in catalog.nodes:
            return (name,)
        if name in catalog.unions:
            abstract_endpoint_types.add(name)
            members = tuple(catalog.unions[name])
            selected_members = tuple(member for member in members if member in nodes)
            return selected_members or members
        if name in catalog.interfaces:
            abstract_endpoint_types.add(name)
            implementations = tuple(
                node_name
                for node_name, node in sorted(catalog.nodes.items())
                if name in node["interfaces"]
            )
            selected_implementations = tuple(
                node_name for node_name in implementations if node_name in nodes
            )
            return selected_implementations or implementations
        diagnostics.append(f"ignored unresolved relationship endpoint type {name}")
        return ()

    for rel_type in selection.selected_relationship_types:
        relationship = catalog.relationships[rel_type]
        endpoints: list[dict] = []
        for endpoint in relationship["endpoints"]:
            sources = concrete_endpoint_names(endpoint["source"])
            targets = concrete_endpoint_names(endpoint["target"])
            if not (set(sources) & nodes or set(targets) & nodes):
                continue
            for source in sources:
                for target in targets:
                    resolved = {**endpoint, "source": source, "target": target}
                    if source != endpoint["source"]:
                        resolved["abstract_source"] = endpoint["source"]
                    if target != endpoint["target"]:
                        resolved["abstract_target"] = endpoint["target"]
                    endpoints.append(resolved)
        relationships[rel_type] = {**relationship, "endpoints": endpoints}
        for endpoint in endpoints:
            for key in ("source", "target"):
                if endpoint[key] not in nodes:
                    diagnostics.append(
                        f"added endpoint node {endpoint[key]} required by {rel_type}"
                    )
                    nodes.add(endpoint[key])

    relationship_properties = {
        name: catalog.nodes[name]
        for relationship in relationships.values()
        for name in relationship["property_types"]
        if name in catalog.nodes
    }
    required_names: set[str] = set(abstract_endpoint_types)
    for name in nodes:
        node = catalog.nodes[name]
        required_names.update(node["interfaces"])
        required_names.update(field["named_type"] for field in node["fields"])
    for node in relationship_properties.values():
        required_names.update(field["named_type"] for field in node["fields"])
    enums = {name: values for name, values in catalog.enums.items() if name in required_names}
    unions = {name: values for name, values in catalog.unions.items() if name in required_names}
    interfaces = {
        name: value for name, value in catalog.interfaces.items() if name in required_names
    }
    directives: dict[str, list[dict]] = {}
    for name in sorted(nodes):
        directives[name] = catalog.nodes[name]["directives"]
    selected_definitions = {name: catalog.nodes[name] for name in sorted(nodes)}
    pieces = [node["sdl"] for node in selected_definitions.values()]
    pieces.extend(node["sdl"] for node in relationship_properties.values())
    pieces.extend(interface["sdl"] for interface in interfaces.values())
    pieces.extend(f"enum {name} {{ {' '.join(values)} }}" for name, values in enums.items())
    pieces.extend(f"union {name} = {' | '.join(values)}" for name, values in unions.items())
    selected_sdl = "\n\n".join(dict.fromkeys(pieces)) + "\n"
    parse(selected_sdl)
    logical = {
        "selected_node_definitions": selected_definitions,
        "selected_relationship_definitions": relationships,
        "relationship_property_types": relationship_properties,
        "required_enums": enums,
        "required_unions": unions,
        "implemented_interfaces": interfaces,
        "relevant_directives": {key: tuple(value) for key, value in directives.items()},
        "fulltext_declarations": tuple(
            item for item in catalog.fulltext_indexes if item["node"] in nodes
        ),
        "vector_declarations": tuple(
            item for item in catalog.vector_indexes if item["node"] in nodes
        ),
        "identity_candidates": {
            name: tuple(catalog.nodes[name]["identity_candidates"]) for name in sorted(nodes)
        },
        "selected_sdl": selected_sdl,
        "closure_diagnostics": tuple(sorted(set(diagnostics))),
        "accepted_selection_digest": accepted.accepted_selection_digest,
        "source_schema_digest": catalog.source_digest,
        "expansion_policy_version": EXPANSION_POLICY_VERSION,
    }
    return ExpandedSchemaSlice(
        **logical,
        expanded_slice_digest=sha256_digest(logical),
    )
