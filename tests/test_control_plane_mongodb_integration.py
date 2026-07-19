from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import BeanieDefinitionRepository
from app.domain.control_plane.contracts import (
    AliasRef,
    DefinitionKind,
    DefinitionSelector,
    GoalDirectedBlueprint,
    MoveAliasRequest,
    PublishDraftRequest,
    RetireRequest,
    SaveDraftRequest,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from app.integrations.mongodb import BEANIE_MODELS
from app.models.control_plane import PublishedDefinitionDocument
from tests.test_control_plane import configured_service, invocation


async def test_real_mongodb_published_revision_is_immutable_and_readable(
    test_mongodb_uri: str,
) -> None:
    # Atlas caps database names at 38 bytes.
    database_name = f"cp_test_{uuid4().hex[:20]}"
    client = AsyncMongoClient(
        test_mongodb_uri,
        serverSelectionTimeoutMS=5_000,
        tz_aware=True,
        tzinfo=UTC,
    )
    database = client[database_name]
    try:
        await database.command("ping")
        await init_beanie(database=database, document_models=BEANIE_MODELS)
        repository = BeanieDefinitionRepository()
        service = ControlPlaneService(repository, ExtensionRegistry(), InMemoryPayloadStore())
        now = datetime.now(UTC)
        definition = GoalDirectedBlueprint(
            logical_id="integration.generic-goal",
            title="Mongo integration fixture",
            description="Contract-only fixture",
            objective_contract="contract:objective@1",
            acceptance_contract="contract:acceptance@1",
            max_iterations=1,
        )
        head = await service.save_draft(
            SaveDraftRequest(
                definition=definition,
                actor_id="integration-author",
                updated_at=now,
                expected_draft_revision=0,
            )
        )
        published = await service.publish_draft(
            PublishDraftRequest(
                kind=DefinitionKind.BLUEPRINT,
                logical_id=definition.logical_id,
                actor_id="integration-publisher",
                published_at=now,
                expected_draft_revision=head.draft_revision,
                expected_published_revision=0,
            )
        )
        loaded = await repository.get(published.ref)
        assert loaded == published
        assert loaded.definition == definition
        retired = await service.retire(
            RetireRequest(
                ref=published.ref,
                actor_id="integration-operator",
                retired_at=now,
            )
        )
        assert retired.retired_at is not None
        immutable_document = await PublishedDefinitionDocument.find_one(
            PublishedDefinitionDocument.logical_id == definition.logical_id,
            PublishedDefinitionDocument.revision == 1,
        )
        assert immutable_document is not None
        assert "retired_at" not in immutable_document.model_dump()

        compiled_service, _, records = await configured_service(repository=repository)
        workflow = records["workflow"]
        alias = AliasRef(
            kind=DefinitionKind.WORKFLOW_TYPE,
            logical_id="generic.workflow",
            alias="integration-stable",
        )
        await compiled_service.move_alias(
            MoveAliasRequest(
                alias=alias,
                target=workflow.ref,  # type: ignore[attr-defined]
                actor_id="integration-operator",
                moved_at=now,
            )
        )
        compile_request = invocation(records).model_copy(
            update={"workflow_type": DefinitionSelector(alias=alias)}
        )
        compiled = await compiled_service.compile(compile_request)
        loaded_configuration = await compiled_service.retrieve(compiled.digest)
        assert loaded_configuration == compiled
        assert compiled.alias_evidence[0].target == workflow.ref  # type: ignore[attr-defined]
    finally:
        await client.drop_database(database_name)
        await client.close()
