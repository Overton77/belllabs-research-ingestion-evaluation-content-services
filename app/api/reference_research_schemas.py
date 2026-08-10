"""Transport schema export for the immutable Q/D reference result contracts."""

from app.domain.reference_research.contracts import (
    DaveFixtureInput,
    DaveOwnershipResult,
    QualiaCatalogResult,
    QualiaFixtureInput,
)


def reference_research_contract_schemas() -> dict[str, object]:
    return {
        "qualia_fixture_input": QualiaFixtureInput.model_json_schema(),
        "qualia_catalog_result": QualiaCatalogResult.model_json_schema(),
        "dave_fixture_input": DaveFixtureInput.model_json_schema(),
        "dave_ownership_result": DaveOwnershipResult.model_json_schema(),
    }
