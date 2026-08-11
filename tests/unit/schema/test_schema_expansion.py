from graphql import parse

from app.application.schema.schema_catalog import parse_schema_catalog
from app.domain.schema_context.expansion import expand_selection
from tests.schema_context_helpers import accepted, catalog


def test_expansion_is_complete_parseable_and_deterministic() -> None:
    value = catalog()
    admitted = accepted(value)
    first = expand_selection(admitted, value)
    second = expand_selection(admitted, value)

    assert first.expanded_slice_digest == second.expanded_slice_digest
    assert set(first.selected_node_definitions) == {"Organization", "Product"}
    assert first.selected_node_definitions["Organization"]["fields"]
    assert "OFFERS" in first.selected_relationship_definitions
    parse(first.selected_sdl)
    assert admitted.selection.selected_node_types == ("Organization", "Product")


def test_expansion_resolves_union_endpoint_to_selected_concrete_member(tmp_path) -> None:
    schema = tmp_path / "schema.graphql"
    schema.write_text(
        """
        directive @node on OBJECT
        directive @relationship(type: String!, direction: String!) on FIELD_DEFINITION
        directive @id on FIELD_DEFINITION
        type Organization @node {
          id: ID! @id
          offers: [Producible!]! @relationship(type: "OFFERS", direction: "OUT")
        }
        type Product @node { id: ID! @id }
        type FoodProduct @node { id: ID! @id }
        union Producible = Product | FoodProduct
        """,
        encoding="utf-8",
    )
    value = parse_schema_catalog(schema.read_bytes(), str(schema))
    admitted = accepted(value)

    expanded = expand_selection(admitted, value)

    endpoint = expanded.selected_relationship_definitions["OFFERS"]["endpoints"][0]
    assert endpoint["source"] == "Organization"
    assert endpoint["target"] == "Product"
    assert endpoint["abstract_target"] == "Producible"
    assert "Producible" in expanded.required_unions
    assert "Producible" not in expanded.selected_node_definitions
