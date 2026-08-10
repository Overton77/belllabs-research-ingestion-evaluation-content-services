from __future__ import annotations

from datetime import datetime
from uuid import NAMESPACE_URL, uuid5

from app.application.schema_grounding_repository import (
    SchemaGroundingRecordRepository,
    schema_grounding_record,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.schema_grounding.contracts import (
    GraphAdmissionDecision,
    GraphAdmissionRequest,
)


class SchemaGraphAdmissionService:
    """Consume Issue 12/13 authorities and fail closed before any graph client exists."""

    def __init__(
        self,
        records: SchemaGroundingRecordRepository | None = None,
    ) -> None:
        self._records = records

    async def decide(
        self,
        request: GraphAdmissionRequest,
        *,
        decided_at: datetime | None = None,
    ) -> GraphAdmissionDecision:
        timestamp = decided_at or request.requested_at
        failure_code, reason = self._evaluate(request)
        manifest = request.deployment_manifest
        binding = request.workspace_binding
        capability = request.graph_capability
        decision_id = str(
            uuid5(
                NAMESPACE_URL,
                "schema-graph-admission:"
                + sha256_digest(request.model_dump(mode="json", exclude={"requested_at"})),
            )
        )
        decision = GraphAdmissionDecision(
            decision_id=decision_id,
            admitted=failure_code is None,
            failure_code=failure_code,
            reason=reason,
            deployment_manifest_id=manifest.manifest_id if manifest else None,
            workspace_binding_id=binding.binding_id if binding else None,
            graph_capability_grant_id=capability.grant_id if capability else None,
            schema_definition_digest=request.schema_definition_digest,
            projection_digest=request.projection_digest,
            decided_at=timestamp,
        )
        if self._records is not None:
            await self._records.append(
                schema_grounding_record(
                    record_type="compatibility_decision",
                    record_id=decision.decision_id,
                    request_scope=request.request_scope,
                    run_id=request.run_id,
                    payload=decision.model_dump(mode="json"),
                    created_at=timestamp,
                )
            )
            if binding is not None:
                await self._records.append(
                    schema_grounding_record(
                        record_type="workspace_binding",
                        record_id=binding.binding_id,
                        request_scope=request.request_scope,
                        run_id=request.run_id,
                        payload=binding.model_dump(mode="json"),
                        created_at=binding.created_at,
                    )
                )
        return decision

    @staticmethod
    def _evaluate(
        request: GraphAdmissionRequest,
    ) -> tuple[str | None, str]:
        manifest = request.deployment_manifest
        if manifest is None:
            return "deployment_manifest_missing", "Issue 12 deployment manifest is required"
        if manifest.revoked or not manifest.active:
            return "deployment_manifest_revoked", "deployment manifest is revoked or inactive"
        if (
            not manifest.issuer_authority_ref.startswith("issue-12:")
            or manifest.environment != request.environment
            or manifest.database != request.database
            or manifest.deployment_id != request.deployment_id
            or manifest.schema_definition_ref != request.schema_definition_ref
            or manifest.deployed_sdl_digest != request.schema_definition_digest
        ):
            return (
                "schema_deployment_mismatch",
                "active deployment identity and deployed SDL must exactly match the catalog source",
            )

        binding = request.workspace_binding
        if binding is None:
            return "workspace_binding_missing", "Issue 13 Schema Workspace Binding is required"
        if (
            not binding.issuer_authority_ref.startswith("issue-13:")
            or binding.request_scope != request.request_scope
            or binding.run_id != request.run_id
            or binding.catalog_build_id != request.catalog_build_id
            or binding.catalog_digest != request.catalog_digest
            or binding.resource_manifest_digest != request.resource_manifest_digest
            or binding.slot_name != "graph_query_runtime"
            or binding.profile != "graph-query-runtime"
            or binding.purpose != request.purpose
            or not binding.read_only
        ):
            return (
                "workspace_profile_invalid",
                "workspace binding must be exact, purpose-bound, read-only, and run-scoped",
            )

        capability = request.graph_capability
        if capability is None or not capability.admitted:
            return "graph_capability_denied", "independent graph read capability was not admitted"
        if (
            not capability.decided_by_authority_ref.startswith("graph-authority:")
            or capability.request_scope != request.request_scope
            or capability.run_id != request.run_id
            or capability.environment != request.environment
            or capability.database != request.database
            or capability.purpose != request.purpose
            or not capability.secret_ref
            or not capability.budget_reservation_id
        ):
            return (
                "graph_capability_denied",
                "graph capability does not match the run, target, purpose, secret, and budget",
            )
        return None, "exact deployment, workspace, and independent graph authority accepted"
