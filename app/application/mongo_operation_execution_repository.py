from __future__ import annotations

from pymongo.errors import DuplicateKeyError

from app.application.mongo_operation_authority_migration import (
    MongoOperationBindingAuthorityMigrationRepository,
    VersionedMongoOperationBindingRepository,
)
from app.config import Settings
from app.domain.operation_execution.contracts import (
    OperationExecutionBinding,
    OperationSettlement,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.models.operation_execution import (
    OperationExecutionBindingDocument,
    OperationExecutionClaimDocument,
    OperationSettlementDocument,
)


class MongoOperationBindingRepository:
    """Beanie persistence for immutable operation intent and settlement records."""

    async def get_binding(
        self,
        semantic_attempt_key: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None:
        document = await OperationExecutionBindingDocument.find_one(
            OperationExecutionBindingDocument.semantic_attempt_key == semantic_attempt_key,
            OperationExecutionBindingDocument.request_scope == request_scope,
        )
        if document is None:
            document = await OperationExecutionBindingDocument.find_one(
                OperationExecutionBindingDocument.semantic_attempt_key == semantic_attempt_key
            )
        binding = (
            OperationExecutionBinding.model_validate(document.payload)
            if document is not None
            else None
        )
        if binding is not None and binding.request_scope != request_scope:
            return None
        return binding

    async def get_binding_by_id(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None:
        document = await OperationExecutionBindingDocument.find_one(
            OperationExecutionBindingDocument.binding_id == binding_id,
            OperationExecutionBindingDocument.request_scope == request_scope,
        )
        if document is None:
            document = await OperationExecutionBindingDocument.find_one(
                OperationExecutionBindingDocument.binding_id == binding_id
            )
        binding = (
            OperationExecutionBinding.model_validate(document.payload)
            if document is not None
            else None
        )
        if (
            binding is not None
            and binding.request_scope != request_scope
        ):
            return None
        return binding

    async def create_binding(
        self,
        binding: OperationExecutionBinding,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding:
        if binding.request_scope != request_scope:
            raise ValueError("operation binding write cannot cross request scope")
        document = OperationExecutionBindingDocument(
            request_scope=binding.request_scope,
            binding_id=binding.binding_id,
            semantic_attempt_key=binding.semantic_attempt_key,
            request_fingerprint=binding.request_fingerprint,
            run_id=binding.run_id,
            operation_id=binding.operation_id,
            operation_attempt=binding.operation_attempt,
            payload=binding.model_dump(mode="json"),
            bound_at=binding.bound_at,
        )
        try:
            await document.insert()
            return binding
        except DuplicateKeyError:
            prior = await self.get_binding(
                binding.semantic_attempt_key,
                request_scope=request_scope,
            )
            if prior is None or prior.request_fingerprint != binding.request_fingerprint:
                raise IdempotencyConflict(
                    "semantic operation binding has a conflicting fingerprint"
                ) from None
            return prior
    async def get_settlement(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationSettlement | None:
        document = await OperationSettlementDocument.find_one(
            OperationSettlementDocument.binding_id == binding_id,
            OperationSettlementDocument.request_scope == request_scope,
        )
        if document is None:
            legacy = await OperationSettlementDocument.find_one(
                OperationSettlementDocument.binding_id == binding_id
            )
            binding = await self.get_binding_by_id(
                binding_id,
                request_scope=request_scope,
            )
            if legacy is not None and binding is not None:
                document = legacy
        return (
            OperationSettlement.model_validate(document.payload) if document is not None else None
        )

    async def claim_execution(self, binding: OperationExecutionBinding) -> bool:
        claim = OperationExecutionClaimDocument(
            request_scope=binding.request_scope,
            side_effect_key=binding.side_effect_key,
            binding_id=binding.binding_id,
            claimed_at=binding.bound_at,
        )
        try:
            await claim.insert()
            return True
        except DuplicateKeyError:
            prior = await OperationExecutionClaimDocument.find_one(
                OperationExecutionClaimDocument.side_effect_key == binding.side_effect_key,
                OperationExecutionClaimDocument.request_scope == binding.request_scope,
            )
            if prior is None:
                legacy = await OperationExecutionClaimDocument.find_one(
                    OperationExecutionClaimDocument.side_effect_key == binding.side_effect_key
                )
                legacy_binding = (
                    await self.get_binding_by_id(
                        legacy.binding_id,
                        request_scope=binding.request_scope,
                    )
                    if legacy is not None
                    else None
                )
                if legacy_binding is not None:
                    prior = legacy
            if prior is None or prior.binding_id != binding.binding_id:
                raise IdempotencyConflict(
                    "operation side-effect key belongs to another binding"
                ) from None
            return False

    async def settle(
        self,
        settlement: OperationSettlement,
        *,
        request_scope: str,
    ) -> OperationSettlement:
        binding = await self.get_binding_by_id(
            settlement.binding_id,
            request_scope=request_scope,
        )
        if binding is None:
            raise ValueError("operation settlement binding is outside request scope")
        document = OperationSettlementDocument(
            request_scope=request_scope,
            settlement_id=settlement.settlement_id,
            binding_id=settlement.binding_id,
            payload=settlement.model_dump(mode="json"),
            settled_at=settlement.settled_at,
        )
        try:
            await document.insert()
            return settlement
        except DuplicateKeyError:
            prior = await self.get_settlement(
                settlement.binding_id,
                request_scope=request_scope,
            )
            if prior is None:
                raise IdempotencyConflict("operation settlement identity collision") from None
            comparable = prior.model_copy(update={"settled_at": settlement.settled_at})
            if comparable != settlement:
                raise IdempotencyConflict(
                    "operation settlement conflicts with its prior result"
                ) from None
            return prior


def create_semantic_operation_binding_repository(
    settings: Settings,
) -> VersionedMongoOperationBindingRepository:
    return VersionedMongoOperationBindingRepository(
        legacy=MongoOperationBindingRepository(),
        v2=MongoOperationBindingAuthorityMigrationRepository(),
        write_authority=settings.operation_binding_write_authority,
        allow_legacy_read_fallback=settings.operation_binding_legacy_read_fallback,
    )
