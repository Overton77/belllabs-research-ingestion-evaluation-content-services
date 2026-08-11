from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from neo4j import READ_ACCESS, WRITE_ACCESS, AsyncDriver
from pydantic import BaseModel

from app.application.schema.schema_artifact_cleanup import (
    TARGET_LABELS,
    SchemaArtifactCleanupPlan,
    TargetLabelUsage,
    plan_zero_count_schema_artifact_cleanup,
    verify_schema_artifact_cleanup_postcondition,
)
from app.config import get_settings
from app.integrations.neo4j import create_neo4j
from app.integrations.neo4j_schema_deployment import Neo4jLiveSchemaDeploymentReader


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Governed reconciliation of the exact zero-count legacy schema artifacts "
            "attached to OrganizationState, ProductState, ResearchPlanRef, and "
            "ResearchRunRef. Defaults to read-only dry-run."
        )
    )
    parser.add_argument("--database", default="neo4j")
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Execute only the exact precondition-verified allowlisted drops. "
            "Without this flag the command is read-only."
        ),
    )
    return parser


async def _read_label_usage(
    driver: AsyncDriver,
    *,
    database: str,
) -> dict[str, TargetLabelUsage]:
    usage: dict[str, TargetLabelUsage] = {}
    async with driver.session(
        database=database,
        default_access_mode=READ_ACCESS,
    ) as session:
        for label in sorted(TARGET_LABELS):
            node_cursor = await session.run(
                "MATCH (n) WHERE $label IN labels(n) RETURN count(n) AS count",
                {"label": label},
            )
            incoming_cursor = await session.run(
                "MATCH ()-[r]->(n) WHERE $label IN labels(n) "
                "RETURN count(r) AS count",
                {"label": label},
            )
            outgoing_cursor = await session.run(
                "MATCH (n)-[r]->() WHERE $label IN labels(n) "
                "RETURN count(r) AS count",
                {"label": label},
            )
            node_row = await node_cursor.single(strict=True)
            incoming_row = await incoming_cursor.single(strict=True)
            outgoing_row = await outgoing_cursor.single(strict=True)
            usage[label] = TargetLabelUsage(
                node_count=int(node_row["count"]),
                incoming_relationship_count=int(incoming_row["count"]),
                outgoing_relationship_count=int(outgoing_row["count"]),
            )
    return usage


async def _prepare(
    driver: AsyncDriver,
    *,
    database: str,
) -> SchemaArtifactCleanupPlan:
    reader = Neo4jLiveSchemaDeploymentReader(driver)
    snapshot, usage = await asyncio.gather(
        reader.read_schema_snapshot(database=database),
        _read_label_usage(driver, database=database),
    )
    return plan_zero_count_schema_artifact_cleanup(snapshot, usage)


async def _apply(
    driver: AsyncDriver,
    *,
    database: str,
    plan: SchemaArtifactCleanupPlan,
) -> tuple[int, str]:
    fresh_plan = await _prepare(driver, database=database)
    if fresh_plan.plan_digest != plan.plan_digest:
        raise RuntimeError(
            "schema cleanup plan changed between dry-run and the immediate apply precheck"
        )
    if not fresh_plan.present_indexes and not fresh_plan.present_constraints:
        return 0, "already_clean"
    if not fresh_plan.all_allowlisted_artifacts_present:
        raise RuntimeError(
            "schema cleanup found a partial allowlist; refusing an ambiguous apply"
        )
    writes = 0
    async with driver.session(
        database=database,
        default_access_mode=WRITE_ACCESS,
    ) as session:
        for command in plan.constraint_drop_commands:
            await (await session.run(command)).consume()
            writes += 1
        for command in plan.independent_index_drop_commands:
            await (await session.run(command)).consume()
            writes += 1
    reader = Neo4jLiveSchemaDeploymentReader(driver)
    snapshot, usage = await asyncio.gather(
        reader.read_schema_snapshot(database=database),
        _read_label_usage(driver, database=database),
    )
    verify_schema_artifact_cleanup_postcondition(snapshot, usage)
    return writes, "applied_and_verified"


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    settings = get_settings()
    driver = await create_neo4j(settings)
    try:
        plan = await _prepare(driver, database=args.database)
        result: dict[str, Any] = {
            "mode": "apply" if args.apply else "dry-run",
            "database": args.database,
            "target_labels": tuple(sorted(TARGET_LABELS)),
            "precondition_verified": True,
            "plan": plan,
            "external_writes_performed": 0,
        }
        if args.apply:
            writes, outcome = await _apply(
                driver,
                database=args.database,
                plan=plan,
            )
            result["external_writes_performed"] = writes
            result["apply_outcome"] = outcome
            result["postcondition_verified"] = True
        else:
            result["postcondition_verified"] = False
        return result
    finally:
        await driver.close()


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _jsonable(value.model_dump(mode="python"))
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return [_jsonable(item) for item in sorted(value)]
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def main() -> None:
    print(
        json.dumps(
            _jsonable(asyncio.run(_run(_parser().parse_args()))),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
