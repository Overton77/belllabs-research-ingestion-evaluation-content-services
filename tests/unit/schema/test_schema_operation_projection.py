from app.domain.schema_context.expansion import expand_selection
from app.domain.schema_context.projection import build_operation_projection
from tests.schema_context_helpers import accepted, catalog


def test_projection_uses_sdl_plus_live_online_indexes_and_does_not_invent_vectors() -> None:
    value = catalog()
    admitted = accepted(value)
    expanded = expand_selection(admitted, value)
    projection = build_operation_projection(
        admitted,
        expanded,
        live_indexes=(
            {"name": "OrganizationName", "state": "ONLINE"},
            {"name": "ProductEmbedding", "state": "OFFLINE"},
        ),
        allow_vector=True,
    )

    assert projection.allowed_node_labels == ("Organization", "Product")
    assert projection.fulltext_capabilities[0]["live_online"] is True
    assert projection.vector_capabilities == ()
    assert "vector_search" not in projection.permitted_query_kinds
    assert projection.source_schema_digest == value.source_digest
