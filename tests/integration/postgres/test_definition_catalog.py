from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from mission_control.adapters.postgres.control_plane.catalog_assets import (
    PUBLISHED_DEFINITION_CONTRACT,
    definition_asset_id,
)
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.scope import apply_catalog_scope_string
from mission_control.bootstrap.coordinator_composition import load_coordinator_catalog_bindings
from mission_control.domain.authoring.contracts import AliasRef, CapabilityDefinition
from mission_control.domain.authoring.errors import (
    DefinitionConflict,
    DefinitionNotFound,
    ReferenceMismatch,
    RetiredDefinition,
)
from tests.fixtures.mission_control_common_db import (
    AI_ENGINEER_INSTALLATION_ID,
    CommonDatabase,
    catalog_scope,
    common_database,
)
from tests.integration.postgres.catalog_common import (
    AI_ENGINEER_CATALOG,
    BIOTECH_CATALOG,
    add_disabled_installation,
)
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool
from tests.unit.coordinator.test_coordinator_facade import coordinator_skill, propose_prompt

pytestmark = pytest.mark.common_db

NOW = datetime(2026, 10, 3, tzinfo=UTC)


def definition(description: str = "Catalog transaction test") -> CapabilityDefinition:
    return CapabilityDefinition(
        logical_id="fixture.tool",
        title="Fixture",
        description=description,
        kind="tool",
        capability_kind="tool",
        maturity="qualified",
        attachment_targets=frozenset({"agent.main"}),
    )


@pytest.mark.asyncio
async def test_publication_draft_alias_retirement_and_installation_isolation(runtime_pool):
    repository = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    other = PostgresDefinitionRepository(runtime_pool, catalog_scope=AI_ENGINEER_CATALOG)
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
async def test_erc_replay_compilation_conflict_rolls_back_new_digest(runtime_pool):
    repository = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
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
async def test_coordinator_bindings_use_scoped_postgres_publications(runtime_pool):
    repository = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    skill = await repository.publish(coordinator_skill(), "publisher", NOW, 0)
    prompt = await repository.publish(propose_prompt(), "publisher", NOW, 0)
    selector, prompts = await load_coordinator_catalog_bindings(repository=repository)
    assert selector.exact == skill.ref
    assert prompts == {"propose_workflow": prompt.ref}
    other = PostgresDefinitionRepository(runtime_pool, catalog_scope=AI_ENGINEER_CATALOG)
    with pytest.raises(RuntimeError, match="unavailable"):
        await load_coordinator_catalog_bindings(repository=other)
    await repository.retire(skill.ref, "operator", NOW)
    with pytest.raises(RuntimeError, match="retired"):
        await load_coordinator_catalog_bindings(repository=repository)


@pytest.mark.asyncio
async def test_publication_is_canonical_asset_with_append_only_decisions(
    catalog_db: CommonDatabase, runtime_pool
):
    repository = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    published = await repository.publish(definition(), "publisher", NOW, 0)
    await repository.retire(published.ref, "operator", NOW + timedelta(minutes=1))
    async with runtime_pool.acquire() as connection, connection.transaction():
        await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
        asset = await connection.fetchrow(
            "SELECT asset_version_id, kind, contract, status, version_no, manifest_digest "
            "FROM mission_control.asset_version WHERE asset_id=$1 AND version='1'",
            definition_asset_id(published.ref.kind, published.ref.logical_id),
        )
        assert (asset["kind"], asset["contract"], asset["status"], asset["version_no"]) == (
            "tool",
            PUBLISHED_DEFINITION_CONTRACT,
            "retired",
            2,
        )
        decisions = await connection.fetch(
            "SELECT decision FROM mission_control.asset_decision WHERE asset_version_id=$1 "
            "ORDER BY decided_at",
            asset["asset_version_id"],
        )
        assert [row["decision"] for row in decisions] == ["admit", "retire"]
        jobs = await connection.fetchval(
            "SELECT count(*) FROM mission_control.catalog_projection_job"
        )
        assert jobs == 2
    # Retirement stays retired: the lifecycle guard rejects revival and identity edits.
    for sql in (
        "UPDATE mission_control.asset_version SET status='admitted', version_no=version_no+1",
        "UPDATE mission_control.asset_version SET status='revoked', version_no=version_no+5",
    ):
        async with runtime_pool.acquire() as connection, connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            with pytest.raises(asyncpg.RestrictViolationError):
                await connection.execute(
                    sql + " WHERE asset_version_id=$1", asset["asset_version_id"]
                )
    for sql in (
        "UPDATE mission_control.asset_version SET manifest_digest=$1",
        "DELETE FROM mission_control.asset_version WHERE manifest_digest<>$1",
        "UPDATE mission_control.asset_decision SET disposition=$1",
        "DELETE FROM mission_control.asset_decision WHERE disposition<>$1",
    ):
        async with runtime_pool.acquire() as connection, connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(sql, "sha256:" + "f" * 64)
    # Revocation is a further forward transition and is also terminal.
    async with runtime_pool.acquire() as connection, connection.transaction():
        await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
        await connection.execute(
            "UPDATE mission_control.asset_version SET status='revoked', version_no=version_no+1, "
            "updated_at=now() WHERE asset_version_id=$1",
            asset["asset_version_id"],
        )
    async with runtime_pool.acquire() as connection, connection.transaction():
        await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
        with pytest.raises(asyncpg.RestrictViolationError):
            await connection.execute(
                "UPDATE mission_control.asset_version SET status='retired', "
                "version_no=version_no+1 WHERE asset_version_id=$1",
                asset["asset_version_id"],
            )
    assert (await repository.get(published.ref)).retired_at == NOW + timedelta(minutes=1)


@pytest.mark.asyncio
async def test_revision_pinning_and_alias_compare_and_swap(runtime_pool):
    repository = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    first = await repository.publish(definition("first"), "publisher", NOW, 0)
    second = await repository.publish(definition("second"), "publisher", NOW, 1)
    assert (first.ref.revision, second.ref.revision) == (1, 2)
    assert first.ref.digest != second.ref.digest
    # Exact revision pins never drift to the newer revision.
    assert (await repository.get(first.ref)).definition.description == "first"
    with pytest.raises(ReferenceMismatch):
        await repository.get(first.ref.model_copy(update={"digest": second.ref.digest}))
    with pytest.raises(DefinitionConflict):  # stale head expectation
        await repository.publish(definition("third"), "publisher", NOW, 1)
    alias = AliasRef(kind=first.ref.kind, logical_id=first.ref.logical_id, alias="stable")
    with pytest.raises(DefinitionConflict):  # alias must not exist yet
        await repository.move_alias(alias, first.ref, "op", NOW, expected_target=second.ref)
    await repository.move_alias(alias, first.ref, "op", NOW, expected_target=None)
    assert (await repository.resolve(alias)).target == first.ref
    # Two movers observed the same old target: exactly one wins, no last-write-wins.
    outcomes = await asyncio.gather(
        repository.move_alias(alias, second.ref, "op-a", NOW, expected_target=first.ref),
        repository.move_alias(alias, first.ref, "op-b", NOW, expected_target=first.ref),
        return_exceptions=True,
    )
    assert sum(isinstance(outcome, DefinitionConflict) for outcome in outcomes) == 1
    winner = next(outcome for outcome in outcomes if not isinstance(outcome, BaseException))
    assert (await repository.resolve(alias)).target == winner.target
    with pytest.raises(DefinitionConflict):
        await repository.move_alias(alias, first.ref, "op", NOW, expected_target=None)
    await repository.retire(first.ref, "op", NOW)
    with pytest.raises(RetiredDefinition):
        await repository.move_alias(alias, first.ref, "op", NOW)
    assert (await repository.get(second.ref)).retired_at is None


@pytest.mark.asyncio
async def test_cross_application_catalog_denial(catalog_db: CommonDatabase, runtime_pool):
    biotech = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    ai_engineer = PostgresDefinitionRepository(runtime_pool, catalog_scope=AI_ENGINEER_CATALOG)
    ours = await biotech.publish(definition("biotech"), "publisher", NOW, 0)
    theirs = await ai_engineer.publish(definition("ai-engineer"), "publisher", NOW, 0)
    # Same asset id and revision in both apps; each installation sees only its own bytes.
    assert ours.ref.logical_id == theirs.ref.logical_id and ours.ref.revision == theirs.ref.revision
    assert (await biotech.get(ours.ref)).definition.description == "biotech"
    with pytest.raises((DefinitionNotFound, ReferenceMismatch)):
        await biotech.get(theirs.ref)
    assert await biotech.list_published_definition_refs() == (ours.ref,)
    assert await ai_engineer.list_published_definition_refs() == (theirs.ref,)
    async with runtime_pool.acquire() as connection, connection.transaction():
        await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
        count = await connection.fetchval("SELECT count(*) FROM mission_control.asset_version")
        assert count == 1
        # A spoofed foreign installation row cannot be written under the biotech context.
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(
                "INSERT INTO mission_control.catalog_record (installation_id, application_id, "
                "catalog_record_id, contract, record_key, payload, payload_digest, created_at, "
                "created_by_actor_ref) VALUES ($1,'ai-engineer',gen_random_uuid(),"
                "'catalog-event/1','spoof','{}'::jsonb,$2,now(),'spoof')",
                AI_ENGINEER_INSTALLATION_ID,
                "sha256:" + "0" * 64,
            )
    async with runtime_pool.acquire() as connection, connection.transaction():
        # Missing catalog context fails closed (no rows, no writes).
        assert await connection.fetchval("SELECT count(*) FROM mission_control.asset_version") == 0
    with pytest.raises(ValueError):
        PostgresDefinitionRepository(runtime_pool, catalog_scope=catalog_db.scope("tenant-1"))


@pytest.mark.asyncio
async def test_two_application_databases_hold_independent_catalogs() -> None:
    async with (
        common_database() as biotech_db,
        common_database(application_id="ai-engineer") as ai_db,
    ):
        await add_disabled_installation(biotech_db)
        biotech_pool = await biotech_db.pool("mission_control_runtime")
        ai_pool = await ai_db.pool("mission_control_runtime")
        try:
            biotech = PostgresDefinitionRepository(biotech_pool, catalog_scope=catalog_scope())
            ai_engineer = PostgresDefinitionRepository(
                ai_pool, catalog_scope=catalog_scope("ai-engineer")
            )
            ours = await biotech.publish(definition("biotech"), "publisher", NOW, 0)
            await ai_engineer.publish(definition("ai-engineer"), "publisher", NOW, 0)
            # The ai-engineer catalog scope reaches nothing in the biotech project.
            wrong = PostgresDefinitionRepository(
                biotech_pool, catalog_scope=catalog_scope("ai-engineer")
            )
            with pytest.raises(DefinitionNotFound):
                await wrong.get(ours.ref)
            assert await wrong.list_published_definition_refs() == ()
            for pool, app in ((biotech_pool, "biotech"), (ai_pool, "ai-engineer")):
                async with pool.acquire() as connection, connection.transaction():
                    await apply_catalog_scope_string(connection, catalog_scope(app))
                    manifest = await connection.fetchval(
                        "SELECT manifest FROM mission_control.asset_version"
                    )
                    assert json.loads(manifest)["definition"]["description"] == app
        finally:
            await biotech_pool.close()
            await ai_pool.close()
