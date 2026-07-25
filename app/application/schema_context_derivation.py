from __future__ import annotations

from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from app.application.schema_catalog import SchemaCatalog
from app.application.schema_grounding_repository import (
    SchemaGroundingRecordRepository,
    schema_grounding_record,
)
from app.domain.schema_context.contracts import AcceptedSchemaContextSelection
from app.domain.schema_context.expansion import expand_selection
from app.domain.schema_context.projection import build_operation_projection
from app.domain.schema_grounding.contracts import SchemaContextDerivationResult


class SchemaContextDerivationService:
    """Expand structural closure and create a purpose-bound operation projection."""

    def __init__(
        self,
        records: SchemaGroundingRecordRepository | None = None,
    ) -> None:
        self._records = records

    async def derive(
        self,
        *,
        request_scope: str,
        run_id: str,
        accepted: AcceptedSchemaContextSelection,
        catalog: SchemaCatalog,
        purpose: str = "read_query_reconciliation",
        live_indexes: tuple[dict, ...] = (),
        allow_vector: bool = False,
        derived_at: datetime | None = None,
    ) -> SchemaContextDerivationResult:
        if purpose != "read_query_reconciliation":
            raise ValueError(
                "a projection admitted for read-query reconciliation cannot be reused "
                f"for {purpose}"
            )
        if accepted.selection.catalog_digest != catalog.catalog_digest:
            raise ValueError("accepted selection belongs to a different Schema Catalog Build")
        if accepted.selection.schema_definition_digest != catalog.source_digest:
            raise ValueError("accepted selection belongs to a different Schema Definition")
        expanded = expand_selection(accepted, catalog)
        projection = build_operation_projection(
            accepted,
            expanded,
            live_indexes=live_indexes,
            allow_vector=allow_vector,
        )
        timestamp = derived_at or datetime.now(UTC)
        derivation_id = str(
            uuid5(
                NAMESPACE_URL,
                "schema-context-derivation:"
                f"{run_id}:{accepted.accepted_selection_digest}:{purpose}",
            )
        )
        result = SchemaContextDerivationResult(
            derivation_id=derivation_id,
            request_scope=request_scope,
            run_id=run_id,
            purpose=purpose,
            accepted_selection_digest=accepted.accepted_selection_digest,
            expanded_slice=expanded,
            projection=projection,
            derived_at=timestamp,
        )
        if self._records is not None:
            await self._records.append(
                schema_grounding_record(
                    record_type="expanded_slice",
                    record_id=expanded.expanded_slice_digest,
                    request_scope=request_scope,
                    run_id=run_id,
                    payload=expanded.model_dump(mode="json"),
                    created_at=timestamp,
                )
            )
            await self._records.append(
                schema_grounding_record(
                    record_type="operation_projection",
                    record_id=projection.projection_id,
                    request_scope=request_scope,
                    run_id=run_id,
                    payload=projection.model_dump(mode="json"),
                    created_at=timestamp,
                )
            )
        return result
