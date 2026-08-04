from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from app.application.schema_authority_issuance import (
    SchemaDeploymentEvidenceProvisioningService,
)
from app.config import get_settings
from app.domain.schema_grounding.contracts import (
    SchemaDeploymentEvidenceProvisioningRequest,
)
from app.integrations.neo4j import create_neo4j
from app.integrations.neo4j_schema_deployment import (
    Neo4jLiveSchemaDeploymentReader,
    schema_authority_issuer_identities,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Operator-only Issue-12 provisioning of a new current-schema "
            "verification/attestation event. The deployment identity names this governed "
            "evidence issuance event, not an inferred historical deployment. "
            "Coordinators consume this evidence but must never invoke this command."
        )
    )
    parser.add_argument("--schema-definition", type=Path, required=True)
    parser.add_argument("--schema-definition-ref", required=True)
    parser.add_argument("--schema-definition-digest", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--database", default="neo4j")
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument(
        "--issued-at",
        required=True,
        help="Aware ISO-8601 deployment occurrence time from the deployment event.",
    )
    return parser


async def _run(args: argparse.Namespace) -> None:
    settings = get_settings()
    canonical_sdl = args.schema_definition.read_bytes().decode("utf-8")
    issued_at = datetime.fromisoformat(args.issued_at.replace("Z", "+00:00"))
    request = SchemaDeploymentEvidenceProvisioningRequest(
        environment=args.environment,
        database=args.database,
        deployment_id=args.deployment_id,
        schema_definition_ref=args.schema_definition_ref,
        schema_definition_digest=args.schema_definition_digest,
        canonical_sdl=canonical_sdl,
        issued_at=issued_at,
    )
    driver = await create_neo4j(settings)
    try:
        graph = Neo4jLiveSchemaDeploymentReader(driver)
        evidence = await SchemaDeploymentEvidenceProvisioningService(
            graph=graph,
            identities=schema_authority_issuer_identities(settings),
        ).provision(request)
    finally:
        await driver.close()
    print(json.dumps(evidence.model_dump(mode="json"), indent=2, sort_keys=True))


def main() -> None:
    asyncio.run(_run(_parser().parse_args()))


if __name__ == "__main__":
    main()
