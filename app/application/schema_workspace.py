from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Literal

from app.application.schema_catalog import SchemaCatalog
from app.domain.schema_context.canonicalization import (
    canonical_json_bytes,
    sha256_digest,
    write_json,
    write_text,
)

WorkspaceProfile = Literal["selection-tier0", "selection-candidates"]
TIER0_MAX_BYTES = 50 * 1024

# Governed starting vocabulary for the first official reconciliation profile. These are
# discovery candidates, not mandatory selections; the selector must still justify membership.
_RECONCILIATION_NODE_SEEDS = {
    "Biomarker",
    "Document",
    "LabTest",
    "Metric",
    "Organization",
    "OrganizationSnapshot",
    "PanelDefinition",
    "Product",
    "ProductSnapshot",
    "TechnologyPlatform",
}
_RECONCILIATION_RELATIONSHIP_SEEDS = {
    "DELIVERS_LABTEST",
    "DEVELOPS_PLATFORM",
    "IMPLEMENTS",
    "IMPLEMENTS_PANEL",
    "INCLUDES_BIOMARKER",
    "INCLUDES_LABTEST",
    "MEASURES",
    "OFFERS",
    "USES_PLATFORM",
}


def _words(value: str) -> set[str]:
    expanded = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    return {
        token[:-1] if token.endswith("s") and len(token) > 4 else token
        for token in re.findall(r"[a-z0-9]+", expanded.lower())
        if len(token) > 2
    }


def build_tier0(catalog: SchemaCatalog) -> dict[str, Any]:
    """Build the complete, bounded discovery surface used before drill-down."""
    if catalog.tier_zero is None:
        raise ValueError("catalog is missing its typed Tier 0 projection")
    payload: dict[str, Any] = {
        **json.loads(json.dumps(catalog.tier_zero)),
        "profile": "selection-tier0",
        "source_schema_digest": catalog.source_digest,
        "catalog_digest": catalog.catalog_digest,
        "counts": {
            "nodes": len(catalog.tier_zero["node_names"]),
            "relationship_property_types": len(catalog.tier_zero["relationship_property_types"]),
            "relationships": len(catalog.tier_zero["relationships"]),
        },
    }
    size = len(canonical_json_bytes(payload))
    if size > TIER0_MAX_BYTES:
        # Descriptions aid discovery but names and topology are the non-negotiable Tier 0 core.
        for node in payload["node_metadata"].values():
            node.pop("description", None)
        size = len(canonical_json_bytes(payload))
    if size > TIER0_MAX_BYTES:
        raise ValueError(f"Tier 0 schema overview is {size} bytes; maximum is {TIER0_MAX_BYTES}")
    return payload


def select_workspace_candidates(
    catalog: SchemaCatalog,
    report: bytes | str,
    *,
    max_nodes: int = 28,
    max_relationships: int = 48,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Deterministically shortlist detail cards from report vocabulary and topology."""
    report_text = report.decode("utf-8", errors="replace") if isinstance(report, bytes) else report
    report_words = _words(report_text)
    report_lower = report_text.lower()
    if catalog.tier_zero is None:
        raise ValueError("catalog is missing its typed Tier 0 projection")
    selectable_nodes = set(catalog.tier_zero["node_names"])
    ranked: list[tuple[int, str]] = []
    for name in sorted(selectable_nodes):
        node = catalog.nodes[name]
        terms = _words(name)
        semantic = node.get("semantic") if isinstance(node, dict) else None
        if isinstance(semantic, dict):
            for key in ("aliases", "synonyms", "acronyms", "legacy_names", "task_cues"):
                terms.update(_words(" ".join(str(x) for x in semantic.get(key, ()))))
            terms.update(_words(str(semantic.get("description", ""))))
        score = 6 * len(_words(name) & report_words) + len(terms & report_words)
        if name.lower() in report_lower:
            score += 20
        ranked.append((score, name))
    positive = [(score, name) for score, name in ranked if score > 0]
    available_node_seeds = _RECONCILIATION_NODE_SEEDS & selectable_nodes
    chosen_nodes = available_node_seeds | {
        name for _, name in sorted(positive, key=lambda item: (-item[0], item[1]))[:max_nodes]
    }
    if len(chosen_nodes) > max_nodes:
        ranked_names = [name for _, name in sorted(positive, key=lambda item: (-item[0], item[1]))]
        chosen_nodes = available_node_seeds | set(
            ranked_names[: max_nodes - len(available_node_seeds)]
        )

    rel_ranked: list[tuple[int, str]] = []
    for rel_type, relationship in catalog.relationships.items():
        endpoint_score = sum(
            3
            for endpoint in relationship["endpoints"]
            for name in (endpoint["source"], endpoint["target"])
            if name in chosen_nodes
        )
        lexical_score = 5 * len(_words(rel_type) & report_words)
        rel_ranked.append((endpoint_score + lexical_score, rel_type))
    available_relationship_seeds = _RECONCILIATION_RELATIONSHIP_SEEDS & set(catalog.relationships)
    chosen_relationships = available_relationship_seeds | {
        name
        for score, name in sorted(rel_ranked, key=lambda item: (-item[0], item[1]))[
            :max_relationships
        ]
        if score > 0
    }
    if len(chosen_relationships) > max_relationships:
        ranked_names = [
            name
            for score, name in sorted(rel_ranked, key=lambda item: (-item[0], item[1]))
            if score > 0
        ]
        chosen_relationships = available_relationship_seeds | set(
            ranked_names[: max_relationships - len(available_relationship_seeds)]
        )
    return tuple(sorted(chosen_nodes)), tuple(sorted(chosen_relationships))


def _node_detail(catalog: SchemaCatalog, name: str) -> dict[str, Any]:
    incoming: list[dict[str, Any]] = []
    outgoing: list[dict[str, Any]] = []
    for rel_type, relationship in catalog.relationships.items():
        for endpoint in relationship["endpoints"]:
            item = {"relationship_type": rel_type, **endpoint}
            direction = endpoint.get("direction", "OUT")
            if endpoint["source"] == name:
                (incoming if direction == "IN" else outgoing).append(item)
            if endpoint["target"] == name:
                (outgoing if direction == "IN" else incoming).append(item)
    return {
        **catalog.nodes[name],
        "incoming_relationships": sorted(incoming, key=str),
        "outgoing_relationships": sorted(outgoing, key=str),
        "source_schema_digest": catalog.source_digest,
        "catalog_digest": catalog.catalog_digest,
    }


def materialize_schema_workspace(
    catalog: SchemaCatalog,
    source: bytes,
    schema_root: Path,
    *,
    report: bytes | str = "",
) -> dict[str, Any]:
    """Render an audit catalog and purpose-specific, non-duplicated selection profiles."""
    if schema_root.exists() and any(path.is_file() for path in schema_root.rglob("*")):
        raise FileExistsError(f"schema workspace destination must be empty: {schema_root}")
    schema_root.mkdir(parents=True, exist_ok=True)
    tier0 = build_tier0(catalog)
    candidates = select_workspace_candidates(catalog, report)
    node_names, relationship_names = candidates

    write_json(schema_root / "catalog/catalog.json", catalog.model_dump(mode="json"))
    write_json(schema_root / "overview/tier0.json", tier0)
    for name in node_names:
        write_json(schema_root / f"elements/nodes/{name}/detail.json", _node_detail(catalog, name))
    for rel_type in relationship_names:
        write_json(
            schema_root / f"elements/relationships/{rel_type}/detail.json",
            {
                **catalog.relationships[rel_type],
                "source_schema_digest": catalog.source_digest,
                "catalog_digest": catalog.catalog_digest,
            },
        )

    navigation = """# Schema navigation

1. Read `schema/overview/tier0.json` for the complete vocabulary and topology.
2. Read `schema/profiles/selection-candidates.json` for the deterministic shortlist.
3. Open only relevant `schema/elements/**/detail.json` files.
4. Never invent schema names. Record exclusions, near misses, and unresolved mappings.
5. Select semantic membership only; trusted host expansion adds structural closure.
6. Schema files provide context, not graph authority. Do not attempt Neo4j access.
"""
    write_text(schema_root / "skills/schema-navigation/SKILL.md", navigation)

    tier0_paths = (
        "schema/manifest.json",
        "schema/overview/tier0.json",
        "schema/profiles/selection-tier0.json",
        "schema/skills/schema-navigation/SKILL.md",
    )
    candidate_paths = tuple(
        [f"schema/elements/nodes/{name}/detail.json" for name in node_names]
        + [f"schema/elements/relationships/{name}/detail.json" for name in relationship_names]
    )
    profiles = {
        "selection-tier0": tier0_paths,
        "selection-candidates": tier0_paths
        + ("schema/profiles/selection-candidates.json",)
        + candidate_paths,
    }
    write_json(
        schema_root / "profiles/selection-tier0.json",
        {"profile": "selection-tier0", "paths": tier0_paths},
    )
    write_json(
        schema_root / "profiles/selection-candidates.json",
        {
            "profile": "selection-candidates",
            "nodes": node_names,
            "relationships": relationship_names,
            "paths": candidate_paths,
        },
    )

    resources = []
    for path in sorted(p for p in schema_root.rglob("*") if p.is_file()):
        relative = path.relative_to(schema_root).as_posix()
        content = path.read_bytes()
        resources.append(
            {
                "logical_path": f"schema/{relative}",
                "content_digest": sha256_digest(content),
                "media_type": "text/markdown" if path.suffix == ".md" else "application/json",
                "read_only": True,
                "size_bytes": len(content),
            }
        )
    manifest = {
        "schema_definition_ref": catalog.source_ref,
        "schema_definition_digest": catalog.source_digest,
        "catalog_digest": catalog.catalog_digest,
        "generator_version": catalog.generator_version,
        "tier0_size_bytes": (schema_root / "overview/tier0.json").stat().st_size,
        "profiles": {name: list(paths) for name, paths in profiles.items()},
        "resource_count": len(resources),
        "resources_digest": sha256_digest(resources),
    }
    write_json(schema_root / "manifest.json", manifest)
    return {**manifest, "resources": resources}


def workspace_profile_paths(run_root: Path, profile: WorkspaceProfile) -> tuple[str, ...]:
    payload = json.loads(
        (run_root / "schema/profiles" / f"{profile}.json").read_text(encoding="utf-8")
    )
    base = tuple(payload["paths"])
    if profile == "selection-candidates":
        return (
            "schema/manifest.json",
            "schema/overview/tier0.json",
            "schema/profiles/selection-tier0.json",
            "schema/profiles/selection-candidates.json",
            "schema/skills/schema-navigation/SKILL.md",
        ) + base
    return base
