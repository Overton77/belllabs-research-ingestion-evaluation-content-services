from __future__ import annotations

from collections import defaultdict

from app.domain.schema_catalog.models import (
    DerivedCatalog,
    DerivedElement,
    PhysicalSchemaCatalog,
    SemanticOverlay,
)
from app.domain.schema_catalog.validation import require_valid_catalog_overlay


def derive_catalog(physical: PhysicalSchemaCatalog, overlay: SemanticOverlay) -> DerivedCatalog:
    """Combine physical truth and governed semantics into deterministic navigation indexes."""
    require_valid_catalog_overlay(physical, overlay)
    incoming: dict[str, set[str]] = defaultdict(set)
    outgoing: dict[str, set[str]] = defaultdict(set)
    neighbors: dict[str, set[str]] = defaultdict(set)
    elements: dict[str, DerivedElement] = {}

    for name in physical.nodes:
        element_id = f"node:{name}"
        elements[element_id] = DerivedElement(
            element_id=element_id,
            physical_name=name,
            kind="node",
            semantics=overlay.elements.get(element_id),
        )
    for relationship in physical.relationships.values():
        relationship_id = f"relationship-type:{relationship.name}"
        for endpoint in relationship.endpoints:
            outgoing[endpoint.physical_start_type].add(endpoint.element_id)
            incoming[endpoint.physical_end_type].add(endpoint.element_id)
            source_id = f"node:{endpoint.physical_start_type}"
            target_id = f"node:{endpoint.physical_end_type}"
            neighbors[source_id].add(target_id)
            neighbors[target_id].add(source_id)
            elements[endpoint.element_id] = DerivedElement(
                element_id=endpoint.element_id,
                physical_name=f"{endpoint.declaring_type}.{endpoint.field_name}",
                kind="relationship-field",
                semantics=overlay.elements.get(endpoint.element_id),
                neighboring_elements=tuple(sorted({source_id, target_id, relationship_id})),
            )
        elements[relationship_id] = DerivedElement(
            element_id=relationship_id,
            physical_name=relationship.name,
            kind="relationship-type",
            semantics=overlay.elements.get(relationship_id),
            neighboring_elements=tuple(
                sorted(
                    {f"node:{endpoint.physical_start_type}" for endpoint in relationship.endpoints}
                    | {f"node:{endpoint.physical_end_type}" for endpoint in relationship.endpoints}
                )
            ),
        )
    for element_id, value in tuple(elements.items()):
        if value.kind == "node":
            elements[element_id] = value.model_copy(
                update={"neighboring_elements": tuple(sorted(neighbors[element_id]))}
            )

    modules: dict[str, tuple[str, ...]] = {}
    for module in overlay.modules:
        members = set(module.seed_elements)
        members.update(
            element_id
            for element_id, semantics in overlay.elements.items()
            if module.name in semantics.modules
        )
        if module.closure_policy in {"incident-relationships", "one-hop"}:
            seed_nodes = {value for value in members if value.startswith("node:")}
            for relationship in physical.relationships.values():
                for endpoint in relationship.endpoints:
                    endpoint_nodes = {
                        f"node:{endpoint.physical_start_type}",
                        f"node:{endpoint.physical_end_type}",
                    }
                    if endpoint_nodes & seed_nodes:
                        members.add(endpoint.element_id)
                        members.add(f"relationship-type:{relationship.name}")
                        if module.closure_policy == "one-hop":
                            members.update(endpoint_nodes)
        modules[module.name] = tuple(sorted(members))

    return DerivedCatalog(
        physical=physical,
        overlay=overlay,
        elements=dict(sorted(elements.items())),
        modules=dict(sorted(modules.items())),
        incoming_by_node={name: tuple(sorted(values)) for name, values in sorted(incoming.items())},
        outgoing_by_node={name: tuple(sorted(values)) for name, values in sorted(outgoing.items())},
    )
