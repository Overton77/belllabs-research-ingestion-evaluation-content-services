from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.bootstrap.coordinator_composition import load_coordinator_catalog_bindings
from mission_control.domain.authoring.contracts import AliasRef, CapabilityDefinition
from mission_control.domain.authoring.errors import (
    DefinitionConflict,
    DefinitionNotFound,
    ReferenceMismatch,
    RetiredDefinition,
)
from tests.integration.postgres.test_immutable_runtime_documents import (
    document_pool as document_pool,
)
from tests.unit.coordinator.test_coordinator_facade import coordinator_skill, propose_prompt

NOW = datetime(2026, 10, 3, tzinfo=UTC)


def definition():
    return CapabilityDefinition(
        logical_id="fixture.tool",
        title="Fixture",
        description="Catalog transaction test",
        kind="tool",
        capability_kind="tool",
        maturity="qualified",
        attachment_targets=frozenset({"agent.main"}),
    )


@pytest.mark.asyncio
async def test_publication_draft_alias_retirement_and_installation_isolation(document_pool):
    repository = PostgresDefinitionRepository(document_pool, catalog_scope=str(uuid4()))
    other = PostgresDefinitionRepository(document_pool, catalog_scope=str(uuid4()))
    value = definition()
    draft = await repository.save_draft(value, "author", NOW, 0)
    assert draft.draft_revision == 1
    results = await asyncio.gather(
        *[repository.publish(value, "publisher", NOW, 0, 1) for _ in range(2)],
        return_exceptions=True,
    )
    assert sum(isinstance(result, DefinitionConflict) for result in results) == 1
    published = next(result for result in results if not isinstance(result, BaseException))
    assert (await repository.get_draft(value.kind.value, value.logical_id)).published_revision == 1
    with pytest.raises(DefinitionNotFound):
        await other.get(published.ref)
    with pytest.raises(ReferenceMismatch):
        await repository.get(published.ref.model_copy(update={"digest": "sha256:" + "0" * 64}))
    alias = AliasRef(kind=value.kind, logical_id=value.logical_id, alias="stable")
    await repository.move_alias(alias, published.ref, "operator", NOW)
    assert (await repository.resolve(alias)).target == published.ref
    retired = await repository.retire(published.ref, "operator", NOW)
    assert retired.retired_at == NOW
    assert await repository.retire(published.ref, "operator", NOW + timedelta(hours=1)) == retired
    with pytest.raises(RetiredDefinition):
        await repository.resolve(alias)
    assert (await repository.resolve(alias, selectable=False)).target == published.ref
    assert len(await repository.list_projection_events()) == 2
    assert await other.list_projection_events() == ()


@pytest.mark.asyncio
async def test_erc_replay_compilation_conflict_rolls_back_new_digest(document_pool):
    repository = PostgresDefinitionRepository(document_pool, catalog_scope=str(uuid4()))
    record = {
        "digest": "sha256:" + "a" * 64,
        "compilation_id": "compilation-1",
        "compiled_at": NOW,
        "payload": {"value": 1},
    }
    await repository.save_erc_record(record)
    await repository.save_erc_record(record)
    assert (await repository.get_erc_record(record["digest"]))["payload"] == {"value": 1}
    with pytest.raises(DefinitionConflict):
        await repository.save_erc_record({**record, "payload": {"value": 2}})
    other_digest = "sha256:" + "b" * 64
    with pytest.raises(DefinitionConflict):
        await repository.save_erc_record({**record, "digest": other_digest})
    with pytest.raises(DefinitionNotFound):
        await repository.get_erc_record(other_digest)


@pytest.mark.asyncio
async def test_coordinator_bindings_use_scoped_postgres_publications(document_pool):
    repository = PostgresDefinitionRepository(document_pool, catalog_scope=str(uuid4()))
    skill = await repository.publish(coordinator_skill(), "publisher", NOW, 0)
    prompt = await repository.publish(propose_prompt(), "publisher", NOW, 0)
    selector, prompts = await load_coordinator_catalog_bindings(repository=repository)
    assert selector.exact == skill.ref
    assert prompts == {"propose_workflow": prompt.ref}
    other = PostgresDefinitionRepository(document_pool, catalog_scope=str(uuid4()))
    with pytest.raises(RuntimeError, match="unavailable"):
        await load_coordinator_catalog_bindings(repository=other)
    await repository.retire(skill.ref, "operator", NOW)
    with pytest.raises(RuntimeError, match="retired"):
        await load_coordinator_catalog_bindings(repository=repository)
