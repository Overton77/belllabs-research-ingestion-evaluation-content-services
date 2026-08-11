from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.application.schema.schema_catalog import parse_schema_catalog
from app.application.schema.schema_workspace import (
    TIER0_MAX_BYTES,
    build_tier0,
    materialize_schema_workspace,
    select_workspace_candidates,
    workspace_profile_paths,
)
from tests.schema_context_helpers import SDL


def test_profiles_mount_one_representation_per_candidate(tmp_path: Path) -> None:
    catalog = parse_schema_catalog(SDL, "fixture.graphql")
    root = tmp_path / "schema"
    manifest = materialize_schema_workspace(
        catalog,
        SDL,
        root,
        report="Organization offers a Product using a TechnologyPlatform.",
    )

    paths = workspace_profile_paths(tmp_path, "selection-candidates")
    assert "schema/overview/tier0.json" in paths
    assert not any("drilldown" in path for path in paths)
    assert not any(path.endswith(".md") and "/elements/" in path for path in paths)
    detail_stems = [path.removesuffix("/detail.json") for path in paths if "/elements/" in path]
    assert len(detail_stems) == len(set(detail_stems))
    assert manifest["tier0_size_bytes"] < TIER0_MAX_BYTES
    assert not (root / "source/neo4jbiotechschema.graphql").exists()


def test_authoritative_schema_tier0_is_bounded_and_workload_candidates_are_present() -> None:
    repository = Path(__file__).resolve().parents[2]
    schema_path = repository / "biotech-kg/src/schema/neo4jbiotechschema.graphql"
    report_path = (
        Path(__file__).resolve().parents[1]
        / ".scratch/schema-context-selection-runs/live-windows-bind-9/inputs/report.md"
    )
    source = schema_path.read_bytes()
    catalog = parse_schema_catalog(source, str(schema_path))

    encoded = json.dumps(build_tier0(catalog), sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    nodes, relationships = select_workspace_candidates(catalog, report_path.read_bytes())

    assert len(encoded) < TIER0_MAX_BYTES
    assert {"LabTest", "PanelDefinition", "Metric", "TechnologyPlatform"} <= set(nodes)
    assert {"IMPLEMENTS", "IMPLEMENTS_PANEL", "INCLUDES_LABTEST", "OFFERS"} <= set(relationships)


def test_tier0_exposes_governed_ontological_categories() -> None:
    from app.application.schema.schema_catalog import DEFAULT_SEMANTIC_OVERLAY

    repository = Path(__file__).resolve().parents[2]
    schema_path = repository / "biotech-kg/src/schema/neo4jbiotechschema.graphql"
    catalog = parse_schema_catalog(
        schema_path.read_bytes(),
        str(schema_path),
        semantic_overlay=DEFAULT_SEMANTIC_OVERLAY,
    )

    metadata = build_tier0(catalog)["node_metadata"]["LabTest"]
    assert metadata["archetypes"] == ["entity"]
    assert metadata["modules"] == ["diagnostics-and-biomarkers"]
    assert metadata["description"].startswith("A laboratory analysis")


def test_workspace_materialization_rejects_stale_destination(tmp_path: Path) -> None:
    catalog = parse_schema_catalog(SDL, "fixture.graphql")
    root = tmp_path / "schema"
    root.mkdir()
    (root / "stale.json").write_text("{}", encoding="utf-8")

    with pytest.raises(FileExistsError, match="must be empty"):
        materialize_schema_workspace(catalog, SDL, root, report="Organization")

    assert (root / "stale.json").exists()
