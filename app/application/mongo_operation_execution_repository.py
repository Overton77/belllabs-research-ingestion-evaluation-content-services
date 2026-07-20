from __future__ import annotations

from pymongo.errors import DuplicateKeyError

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
        self, semantic_attempt_key: str
    ) -> OperationExecutionBinding | None:
        document = await OperationExecutionBindingDocument.find_one(
            OperationExecutionBindingDocument.semantic_attempt_key == semantic_attempt_key
        )
        return (
            OperationExecutionBinding.model_validate(document.payload)
            if document is not None
            else None
        )

    async def create_binding(
        self, binding: OperationExecutionBinding
    ) -> OperationExecutionBinding:
        document = OperationExecutionBindingDocument(
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
            prior = await self.get_binding(binding.semantic_attempt_key)
            if prior is None or prior.request_fingerprint != binding.request_fingerprint:
                raise IdempotencyConflict(
                    "semantic operation binding has a conflicting fingerprint"
                ) from None
            return prior

    async def get_settlement(self, binding_id: str) -> OperationSettlement | None:
        document = await OperationSettlementDocument.find_one(
            OperationSettlementDocument.binding_id == binding_id
        )
        return (
            OperationSettlement.model_validate(document.payload)
            if document is not None
            else None
        )

    async def claim_execution(self, binding: OperationExecutionBinding) -> bool:
        claim = OperationExecutionClaimDocument(
            side_effect_key=binding.side_effect_key,
            binding_id=binding.binding_id,
            claimed_at=binding.bound_at,
        )
        try:
            await claim.insert()
            return True
        except DuplicateKeyError:
            prior = await OperationExecutionClaimDocument.find_one(
                OperationExecutionClaimDocument.side_effect_key
                == binding.side_effect_key
            )
            if prior is None or prior.binding_id != binding.binding_id:
                raise IdempotencyConflict(
                    "operation side-effect key belongs to another binding"
                ) from None
            return False

    async def settle(self, settlement: OperationSettlement) -> OperationSettlement:
        document = OperationSettlementDocument(
            settlement_id=settlement.settlement_id,
            binding_id=settlement.binding_id,
            payload=settlement.model_dump(mode="json"),
            settled_at=settlement.settled_at,
        )
        try:
            await document.insert()
            return settlement
        except DuplicateKeyError:
            prior = await self.get_settlement(settlement.binding_id)
            if prior is None:
                raise IdempotencyConflict("operation settlement identity collision") from None
            comparable = prior.model_copy(update={"settled_at": settlement.settled_at})
            if comparable != settlement:
                raise IdempotencyConflict(
                    "operation settlement conflicts with its prior result"
                ) from None
            return prior
