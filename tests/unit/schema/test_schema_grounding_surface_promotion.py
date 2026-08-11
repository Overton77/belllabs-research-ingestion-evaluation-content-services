from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.control_plane.service import ControlPlaneService
from app.application.control_plane.control_plane_repository import InMemoryDefinitionRepository
from app.domain.control_plane.contracts import PublishRequest
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.schema_grounding.definitions import (
    register_schema_grounding_extensions,
    schema_grounding_definitions,
)
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from scripts.promote_schema_grounding_surface import _plan

NOW = datetime(2026, 7, 26, 15, 30, tzinfo=UTC)


@pytest.mark.asyncio
async def test_schema_grounding_promotion_is_append_only_and_idempotent() -> None:
    definitions = schema_grounding_definitions()
    initial = _plan(definitions, ())

    reconciliation_expectations = [
        expected
        for definition, expected in zip(
            initial.definitions,
            initial.expected_head_revisions,
            strict=True,
        )
        if definition.logical_id == "supporting-graph-reconciliation.implementation"
    ]
    assert reconciliation_expectations == [0, 1]

    repository = InMemoryDefinitionRepository()
    extensions = ExtensionRegistry()
    register_schema_grounding_extensions(extensions)
    service = ControlPlaneService(
        repository,
        extensions,
        InMemoryPayloadStore(),
    )
    records = []
    for definition, expected_revision in zip(
        initial.definitions,
        initial.expected_head_revisions,
        strict=True,
    ):
        records.append(
            await service.publish(
                PublishRequest(
                    definition=definition,
                    actor_id="schema-grounding-promotion-test",
                    published_at=NOW,
                    expected_head_revision=expected_revision,
                )
            )
        )

    repeated = _plan(definitions, tuple(records))
    assert not repeated.definitions
    assert not repeated.expected_head_revisions
    assert len(repeated.reused) == len(definitions)
