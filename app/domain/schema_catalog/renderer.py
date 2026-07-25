from __future__ import annotations

from app.domain.schema_catalog.models import (
    DerivedCatalog,
    TierZeroCatalog,
    TierZeroNodeMetadata,
)


def render_tier_zero(catalog: DerivedCatalog) -> TierZeroCatalog:
    """Render a deterministic navigation surface without copying fields or SDL bodies."""
    node_metadata: dict[str, TierZeroNodeMetadata] = {}
    for name, physical in sorted(catalog.physical.nodes.items()):
        semantics = catalog.overlay.elements.get(f"node:{name}")
        description = semantics.description if semantics else physical.description
        if semantics is not None or description:
            node_metadata[name] = TierZeroNodeMetadata(
                description=description,
                archetypes=semantics.archetypes if semantics else (),
                modules=semantics.modules if semantics else (),
                aliases=semantics.aliases if semantics else (),
            )
    relationships = {
        name: tuple(
            (
                endpoint.declaring_type,
                endpoint.field_name,
                endpoint.direction,
                endpoint.related_type,
            )
            for endpoint in relationship.endpoints
        )
        for name, relationship in sorted(catalog.physical.relationships.items())
    }
    return TierZeroCatalog(
        catalog_digest=catalog.physical.catalog_digest,
        source_digest=catalog.physical.source_digest,
        node_names=tuple(sorted(catalog.physical.nodes)),
        relationship_property_types=tuple(sorted(catalog.physical.relationship_property_types)),
        node_metadata=node_metadata,
        endpoint_columns=("declaring_type", "field", "direction", "related_type"),
        relationships=relationships,
        module_definitions=catalog.overlay.modules,
    )
