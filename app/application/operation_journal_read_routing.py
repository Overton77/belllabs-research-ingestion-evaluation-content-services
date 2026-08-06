from __future__ import annotations

from app.application.operation_journal import AtomicOperationJournalRepository
from app.application.operation_journal_backfill import (
    LegacyOperationJournalSource,
    transform_legacy_claim,
    transform_legacy_settlement,
)
from app.domain.operation_execution.contracts import OperationExecutionBinding
from app.domain.operation_execution.journal import (
    OperationEffectClaim,
    OperationJournalSettlement,
)


class OperationJournalReadRouter:
    """PostgreSQL-first reads with an explicit bounded legacy rollback window."""

    def __init__(
        self,
        *,
        postgres: AtomicOperationJournalRepository,
        legacy: LegacyOperationJournalSource,
        legacy_fallback_enabled: bool = False,
    ) -> None:
        self._postgres = postgres
        self._legacy = legacy
        self._legacy_fallback_enabled = legacy_fallback_enabled

    async def get_claim(
        self,
        binding: OperationExecutionBinding,
        *,
        effect_claim_id: str,
    ) -> OperationEffectClaim | None:
        claim = await self._postgres.get_claim(
            binding.request_scope,
            effect_claim_id,
        )
        if claim is not None or not self._legacy_fallback_enabled:
            return claim
        source = await self._legacy.get_claim_for_binding(
            request_scope=binding.request_scope,
            binding_id=binding.binding_id,
        )
        if source is None:
            return None
        fallback = transform_legacy_claim(source, binding).claim
        if fallback.effect_claim_id != effect_claim_id:
            raise ValueError("legacy fallback claim identity does not match requested claim")
        return fallback

    async def get_settlement(
        self,
        binding: OperationExecutionBinding,
        *,
        effect_claim_id: str,
    ) -> OperationJournalSettlement | None:
        settlement = await self._postgres.get_settlement(
            binding.request_scope,
            effect_claim_id,
        )
        if settlement is not None or not self._legacy_fallback_enabled:
            return settlement
        claim_source = await self._legacy.get_claim_for_binding(
            request_scope=binding.request_scope,
            binding_id=binding.binding_id,
        )
        if claim_source is None:
            return None
        claim = transform_legacy_claim(claim_source, binding).claim
        if claim.effect_claim_id != effect_claim_id:
            raise ValueError("legacy fallback claim identity does not match requested claim")
        source = await self._legacy.get_settlement_for_binding(
            request_scope=binding.request_scope,
            binding_id=binding.binding_id,
        )
        if source is None:
            return None
        return transform_legacy_settlement(source, binding, claim).settlement
