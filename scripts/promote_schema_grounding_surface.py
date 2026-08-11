from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.application.control_plane.service import ControlPlaneService
from app.application.control_plane.control_plane_repository import BeanieDefinitionRepository
from app.config import Settings
from app.domain.control_plane.contracts import (
    Definition,
    ExactDefinitionRef,
    PublishedDefinition,
    PublishRequest,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.schema_grounding.definitions import (
    register_schema_grounding_extensions,
    schema_grounding_agent_definitions,
    schema_grounding_definitions,
)
from app.integrations.catalog_projection_admin import list_published_definition_refs
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from app.integrations.mongodb import create_mongodb


@dataclass(frozen=True)
class PromotionPlan:
    definitions: tuple[Definition, ...]
    expected_head_revisions: tuple[int, ...]
    reused: tuple[ExactDefinitionRef, ...]


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preflight or publish the exact Schema Context Selection and "
            "Supporting Graph Reconciliation control-plane definitions."
        )
    )
    parser.add_argument("--actor", default="schema-grounding-surface-promotion")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--agent-assets-only",
        action="store_true",
        help="Publish only the new schema Prompt, Skill, and Agent Profile assets.",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    definitions = (
        schema_grounding_agent_definitions()
        if args.agent_assets_only
        else schema_grounding_definitions()
    )
    settings = Settings()
    mongo_client, _database = await create_mongodb(settings)
    try:
        repository = BeanieDefinitionRepository()
        refs = await list_published_definition_refs()
        records = tuple([await repository.get(ref) for ref in refs])
        plan = _plan(definitions, records)
        if not args.apply:
            return {
                "mode": "preflight",
                "definition_count": len(definitions),
                "publish_count": len(plan.definitions),
                "reuse_count": len(plan.reused),
                "target_identities": [
                    {
                        "kind": definition.kind.value,
                        "logical_id": definition.logical_id,
                    }
                    for definition in definitions
                ],
            }
        extensions = ExtensionRegistry()
        register_schema_grounding_extensions(extensions)
        service = ControlPlaneService(
            repository,
            extensions,
            InMemoryPayloadStore(),
        )
        published: list[ExactDefinitionRef] = []
        published_at = datetime.now(UTC)
        for definition, expected_revision in zip(
            plan.definitions,
            plan.expected_head_revisions,
            strict=True,
        ):
            record = await service.publish(
                PublishRequest(
                    definition=definition,
                    actor_id=args.actor,
                    published_at=published_at,
                    expected_head_revision=expected_revision,
                )
            )
            published.append(record.ref)
        return {
            "mode": "applied",
            "published": [ref.model_dump(mode="json") for ref in published],
            "reused": [ref.model_dump(mode="json") for ref in plan.reused],
        }
    finally:
        await mongo_client.close()


def _plan(
    definitions: tuple[Definition, ...],
    records: tuple[PublishedDefinition, ...],
) -> PromotionPlan:
    by_identity_revision = {
        (record.ref.kind, record.ref.logical_id, record.ref.revision): record
        for record in records
    }
    current_heads: dict[tuple[object, str], int] = {}
    for record in records:
        identity = (record.ref.kind, record.ref.logical_id)
        current_heads[identity] = max(
            current_heads.get(identity, 0),
            record.ref.revision,
        )

    target_revisions: dict[tuple[object, str], int] = {}
    publish: list[Definition] = []
    expected: list[int] = []
    reused: list[ExactDefinitionRef] = []
    for definition in definitions:
        identity = (definition.kind, definition.logical_id)
        target_revision = target_revisions.get(identity, 0) + 1
        target_revisions[identity] = target_revision
        existing = by_identity_revision.get((*identity, target_revision))
        if existing is not None:
            if existing.definition != definition or existing.retired_at is not None:
                raise RuntimeError(
                    "live schema-grounding revision conflicts with the exact fixture: "
                    f"{definition.kind.value}:{definition.logical_id}@{target_revision}"
                )
            reused.append(existing.ref)
            continue
        current_head = current_heads.get(identity, 0)
        if current_head != target_revision - 1:
            raise RuntimeError(
                "schema-grounding promotion is not append-only at "
                f"{definition.kind.value}:{definition.logical_id}@{target_revision}"
            )
        publish.append(definition)
        expected.append(current_head)
        current_heads[identity] = target_revision
    return PromotionPlan(
        definitions=tuple(publish),
        expected_head_revisions=tuple(expected),
        reused=tuple(reused),
    )


def main() -> None:
    print(json.dumps(asyncio.run(_run(_arguments())), sort_keys=True))


if __name__ == "__main__":
    main()
