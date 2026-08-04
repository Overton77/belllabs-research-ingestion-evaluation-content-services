from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from neo4j import READ_ACCESS, AsyncDriver
from pydantic import BaseModel

from app.config import PROJECT_ROOT, get_settings
from app.domain.schema_context.canonicalization import sha256_digest
from app.domain.schema_grounding.authority import live_schema_compatibility_diff
from app.domain.schema_grounding.contracts import (
    LiveSchemaCompatibilityDiff,
    SchemaDeploymentEvidenceProvisioningRequest,
)
from app.integrations.neo4j import create_neo4j
from app.integrations.neo4j_schema_deployment import Neo4jLiveSchemaDeploymentReader


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only diagnostic for canonical SDL versus the live Neo4j "
            "label/relationship/index snapshot. This never provisions evidence."
        )
    )
    parser.add_argument("--schema-definition", type=Path, required=True)
    parser.add_argument("--schema-definition-ref", required=True)
    parser.add_argument("--schema-definition-digest", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--database", default="neo4j")
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--issued-at", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def _checked_output(path: Path) -> Path:
    resolved = path.resolve()
    if not resolved.is_relative_to(PROJECT_ROOT.resolve()):
        raise ValueError("--output must stay inside the project directory")
    return resolved


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _jsonable(value.model_dump(mode="python"))
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return [_jsonable(item) for item in sorted(value)]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


async def _unexpected_token_usage(
    driver: AsyncDriver,
    diff: LiveSchemaCompatibilityDiff,
) -> dict[str, dict[str, int]]:
    node_counts: dict[str, int] = {}
    relationship_counts: dict[str, int] = {}
    async with driver.session(
        database=diff.observed_database,
        default_access_mode=READ_ACCESS,
    ) as session:
        for label in sorted(diff.unexpected_node_labels):
            cursor = await session.run(
                "MATCH (n) WHERE $label IN labels(n) RETURN count(n) AS count",
                {"label": label},
            )
            row = await cursor.single(strict=True)
            node_counts[label] = int(row["count"])
        for relationship_type in sorted(diff.unexpected_relationship_types):
            cursor = await session.run(
                "MATCH ()-[r]->() WHERE type(r) = $relationship_type "
                "RETURN count(r) AS count",
                {"relationship_type": relationship_type},
            )
            row = await cursor.single(strict=True)
            relationship_counts[relationship_type] = int(row["count"])
    return {
        "unexpected_node_label_counts": node_counts,
        "unexpected_relationship_type_counts": relationship_counts,
    }


async def _unexpected_index_details(
    driver: AsyncDriver,
    diff: LiveSchemaCompatibilityDiff,
) -> tuple[dict[str, Any], ...]:
    async with driver.session(
        database=diff.observed_database,
        default_access_mode=READ_ACCESS,
    ) as session:
        cursor = await session.run(
            """
            SHOW INDEXES
            YIELD name, state, type, entityType, labelsOrTypes, properties,
                  owningConstraint
            RETURN name, state, type, entityType, labelsOrTypes, properties,
                   owningConstraint
            ORDER BY name
            """
        )
        rows = await cursor.data()
    return tuple(
        row
        for row in rows
        if str(row["name"]) in diff.unexpected_index_names
    )


async def _unexpected_label_traces(
    driver: AsyncDriver,
    diff: LiveSchemaCompatibilityDiff,
    usage: dict[str, dict[str, int]],
    unexpected_index_details: tuple[dict[str, Any], ...],
) -> dict[str, dict[str, Any]]:
    async with driver.session(
        database=diff.observed_database,
        default_access_mode=READ_ACCESS,
    ) as session:
        constraint_cursor = await session.run(
            """
            SHOW CONSTRAINTS
            YIELD name, type, entityType, labelsOrTypes, properties, ownedIndex
            RETURN name, type, entityType, labelsOrTypes, properties, ownedIndex
            ORDER BY name
            """
        )
        constraints = await constraint_cursor.data()
        traces: dict[str, dict[str, Any]] = {}
        for label in sorted(diff.unexpected_node_labels):
            property_cursor = await session.run(
                """
                MATCH (n)
                WHERE $label IN labels(n)
                UNWIND keys(n) AS property_key
                WITH DISTINCT property_key
                ORDER BY property_key
                RETURN collect(property_key) AS property_keys
                """,
                {"label": label},
            )
            property_row = await property_cursor.single(strict=True)
            outgoing_cursor = await session.run(
                """
                MATCH (n)-[r]->(other)
                WHERE $label IN labels(n)
                RETURN type(r) AS relationship_type,
                       labels(other) AS counterparty_labels,
                       count(r) AS count
                ORDER BY relationship_type, counterparty_labels
                """,
                {"label": label},
            )
            incoming_cursor = await session.run(
                """
                MATCH (other)-[r]->(n)
                WHERE $label IN labels(n)
                RETURN type(r) AS relationship_type,
                       labels(other) AS counterparty_labels,
                       count(r) AS count
                ORDER BY relationship_type, counterparty_labels
                """,
                {"label": label},
            )
            node_count = usage["unexpected_node_label_counts"][label]
            associated_indexes = tuple(
                row
                for row in unexpected_index_details
                if label in tuple(row.get("labelsOrTypes") or ())
            )
            associated_constraints = tuple(
                row
                for row in constraints
                if label in tuple(row.get("labelsOrTypes") or ())
            )
            if node_count > 0:
                classification = "active_noncanonical_data"
            elif associated_indexes or associated_constraints:
                classification = "zero_count_token_with_schema_artifacts"
            else:
                classification = "zero_count_persistent_token"
            traces[label] = {
                "classification": classification,
                "node_count": node_count,
                "sample_property_keys": tuple(property_row["property_keys"]),
                "associated_indexes": associated_indexes,
                "associated_constraints": associated_constraints,
                "relationships": {
                    "outgoing": tuple(await outgoing_cursor.data()),
                    "incoming": tuple(await incoming_cursor.data()),
                },
            }
    return traces


def _likely_causes(
    diff: LiveSchemaCompatibilityDiff,
    usage: dict[str, dict[str, int]],
) -> tuple[str, ...]:
    causes = []
    if diff.unexpected_node_labels or diff.unexpected_relationship_types:
        active_tokens = {
            token: count
            for counts in usage.values()
            for token, count in counts.items()
            if count > 0
        }
        if active_tokens:
            causes.append(
                "The live graph contains active elements using tokens outside the "
                "canonical SDL, consistent with legacy data, an out-of-band loader, "
                "or schema-version drift."
            )
        else:
            causes.append(
                "The unexpected names are zero-count persistent Neo4j tokens left by "
                "historical data; token-catalog comparison must distinguish residue "
                "from active graph elements."
            )
    if diff.missing_index_names:
        causes.append(
            "One or more canonical @fulltext/@vector index declarations are not "
            "materialized under the declared indexName in the live database."
        )
    if (
        diff.expected_but_unobserved_node_labels
        or diff.expected_but_unobserved_relationship_types
    ):
        causes.append(
            "Expected-but-unobserved labels and relationship types are informational: "
            "Neo4j token discovery does not prove absence from the deployed contract "
            "when no instance currently uses that token."
        )
    if not causes:
        causes.append(
            "No label/relationship/index incompatibility was found; investigate "
            "database identity or snapshot-digest integrity."
        )
    return tuple(causes)


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    schema_path = args.schema_definition.resolve(strict=True)
    schema_bytes = await asyncio.to_thread(schema_path.read_bytes)
    canonical_sdl = schema_bytes.decode("utf-8")
    actual_schema_digest = sha256_digest(schema_bytes)
    request = SchemaDeploymentEvidenceProvisioningRequest(
        environment=args.environment,
        database=args.database,
        deployment_id=args.deployment_id,
        schema_definition_ref=args.schema_definition_ref,
        schema_definition_digest=args.schema_definition_digest,
        canonical_sdl=canonical_sdl,
        issued_at=datetime.fromisoformat(args.issued_at.replace("Z", "+00:00")),
    )
    settings = get_settings()
    driver = await create_neo4j(settings)
    try:
        snapshot = await Neo4jLiveSchemaDeploymentReader(driver).read_schema_snapshot(
            database=args.database
        )
        diff = live_schema_compatibility_diff(request, snapshot)
        usage = await _unexpected_token_usage(driver, diff)
        unexpected_index_details = await _unexpected_index_details(driver, diff)
        unexpected_label_traces = await _unexpected_label_traces(
            driver,
            diff,
            usage,
            unexpected_index_details,
        )
    finally:
        await driver.close()
    return {
        "mode": "read-only-schema-deployment-diagnostic",
        "external_writes_performed": 0,
        "attempted_deployment_id": args.deployment_id,
        "attempted_issued_at": args.issued_at,
        "canonical_source": {
            "path": str(schema_path),
            "ref": args.schema_definition_ref,
            "declared_digest": args.schema_definition_digest,
            "actual_digest": actual_schema_digest,
            "digest_matches": actual_schema_digest == args.schema_definition_digest,
        },
        "live_snapshot": snapshot,
        "compatibility_diff": diff,
        "unexpected_token_usage": usage,
        "unexpected_index_details": unexpected_index_details,
        "unexpected_label_traces": unexpected_label_traces,
        "likely_causes": _likely_causes(diff, usage),
        "proper_fix": (
            "Reconcile the live graph through the governed Neo4j schema/data migration "
            "authority so every observed non-operational token is represented by the "
            "canonical SDL or removed/migrated, and every declared fulltext/vector index "
            "exists under its exact indexName. Then capture a fresh snapshot and issue a "
            "new attestation identity/time; never weaken the comparison or attest this "
            "failed snapshot."
        ),
    }


def main() -> None:
    args = _parser().parse_args()
    output = _checked_output(args.output)
    payload = _jsonable(asyncio.run(_run(args)))
    encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(encoded)
    print(
        json.dumps(
            {
                "artifact_path": str(output),
                "artifact_digest": sha256_digest(encoded),
                "compatible": payload["compatibility_diff"]["compatible"],
                "external_writes_performed": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
