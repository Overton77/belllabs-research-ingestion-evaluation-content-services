from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from app.application.schema_catalog import DEFAULT_SEMANTIC_OVERLAY
from app.config import PROJECT_ROOT, Settings
from app.integrations.schema_grounding_payloads import (
    SCHEMA_GROUNDING_INPUT_FORMATS,
    SchemaGroundingInputKind,
    schema_grounding_input_store,
)

DEFAULT_SCHEMA = (
    PROJECT_ROOT.parent / "biotech-kg" / "src" / "schema" / "neo4jbiotechschema.graphql"
)
DEFAULT_REPORT = (
    PROJECT_ROOT.parent
    / "biotech-kg"
    / "research"
    / "trudiagnostic-20260330-203619-research-mission"
    / "reports"
    / "products-labtests-biomarkers.md"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Stage exact content-addressed Scenario A/C inputs with the same S3 adapter "
            "used by the live coordinator. This does not issue graph authority."
        )
    )
    parser.add_argument("--artifact-bucket", required=True)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--semantic-overlay", type=Path, default=DEFAULT_SEMANTIC_OVERLAY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    return parser


async def _run(args: argparse.Namespace) -> dict[str, object]:
    base = Settings()
    settings = base.model_copy(update={"s3_bucket": args.artifact_bucket})
    paths: dict[SchemaGroundingInputKind, Path] = {
        "schema": args.schema.resolve(strict=True),
        "semantic_overlay": args.semantic_overlay.resolve(strict=True),
        "report": args.report.resolve(strict=True),
    }
    stores = {
        name: schema_grounding_input_store(
            settings,
            args.artifact_bucket,
            name,
        )
        for name in paths
    }
    payloads = await asyncio.gather(
        *(asyncio.to_thread(path.read_bytes) for path in paths.values())
    )
    addresses = await asyncio.gather(
        *(
            stores[name].put(payload)
            for name, payload in zip(paths, payloads, strict=True)
        )
    )
    return {
        name: {
            "source_path": str(path),
            "uri": address.uri,
            "digest": address.digest,
            "size_bytes": address.size,
            "version_id": address.version_id,
            "media_type": SCHEMA_GROUNDING_INPUT_FORMATS[name].media_type,
        }
        for (name, path), address in zip(paths.items(), addresses, strict=True)
    }


def main() -> None:
    print(
        json.dumps(
            asyncio.run(_run(_parser().parse_args())),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
