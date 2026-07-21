from __future__ import annotations

from datetime import UTC
from uuid import uuid4

from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.mongo_operation_execution_repository import (
    MongoOperationBindingRepository,
)
from app.application.operation_execution import OperationExecutionService
from app.integrations.conformance_operation_runtime import (
    ConformanceAssetVerifier,
    ConformanceAuthority,
    ConformanceBudgetAuthority,
    ConformanceEventSink,
    ConformanceRuntime,
    ConformanceSandbox,
    ConformanceSecretResolver,
)
from app.integrations.mongodb import BEANIE_MODELS
from tests.test_operation_execution import (
    MCP_DIGEST,
    SECRET_VALUE,
    SKILL_DIGEST,
    operation_request,
)


async def test_mongodb_binding_claim_and_settlement_are_immutable_and_idempotent(
    test_mongodb_uri: str,
) -> None:
    database_name = f"op_test_{uuid4().hex[:20]}"
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
        request = operation_request()
        repository = MongoOperationBindingRepository()
        runtime = ConformanceRuntime()
        assets = ConformanceAssetVerifier(
            mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
            asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
        )
        service = OperationExecutionService(
            authority=ConformanceAuthority(
                accepted_run_id=request.identity.run_id,
                configuration_digest=request.effective_configuration_digest,
                control_revision=request.run_control_revision,
                reservation_id=request.budget_reservation_id,
            ),
            bindings=repository,
            runtime=runtime,
            sandbox=ConformanceSandbox(),
            assets=assets,
            mcp=assets,
            secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": SECRET_VALUE}),
            events=ConformanceEventSink(),
            budget=ConformanceBudgetAuthority(),
        )

        first = await service.execute(request)
        replayed = await service.execute(request)
        binding = await repository.get_binding(request.identity.semantic_key)
        settlement = await repository.get_settlement(first.binding_id)

        assert first == replayed
        assert binding is not None
        assert settlement is not None
        assert await repository.claim_execution(binding) is False
        assert runtime.effect_count == 1
        assert SECRET_VALUE not in repr((binding, settlement))
    finally:
        await client.drop_database(database_name)
        await client.close()
