from __future__ import annotations

import base64
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from app.application.schema_catalog import (
    CATALOG_GENERATOR_VERSION,
    parse_schema_catalog,
)
from app.application.schema_grounding_repository import (
    SchemaGroundingRecordRepository,
    schema_grounding_record,
)
from app.application.schema_workspace import materialize_schema_workspace
from app.domain.schema_catalog import (
    load_semantic_overlay,
    parse_physical_schema,
)
from app.domain.schema_context.canonicalization import (
    canonical_json_bytes,
    sha256_digest,
)
from app.domain.schema_grounding.contracts import (
    CatalogResourceRecord,
    DurableObjectRef,
    SchemaCatalogBuildRecord,
    SchemaCatalogBuildRequest,
)
from app.domain.schema_grounding.errors import (
    CatalogNondeterministic,
    CatalogPublicationConflict,
    SchemaGroundingRecordNotFound,
    SchemaSourceDigestMismatch,
)
from app.integrations.control_plane_payloads import (
    ContentAddress,
    ContentAddressedPayloadStore,
)


class SchemaCatalogBuildService:
    """Deterministically publish one exact SDL/overlay catalog by content digest."""

    def __init__(
        self,
        records: SchemaGroundingRecordRepository,
        payloads: ContentAddressedPayloadStore,
    ) -> None:
        self._records = records
        self._payloads = payloads

    async def build(
        self,
        request: SchemaCatalogBuildRequest,
        *,
        schema_definition: bytes,
        semantic_overlay: bytes,
        report_seed: bytes = b"",
    ) -> SchemaCatalogBuildRecord:
        request_fingerprint = _request_fingerprint(request)
        try:
            prior = await self.get(request.request_scope, request.build_id)
        except SchemaGroundingRecordNotFound:
            prior = None
        if prior is not None:
            if (
                prior.idempotency_key != request.idempotency_key
                or prior.request_fingerprint != request_fingerprint
            ):
                raise CatalogPublicationConflict(
                    "catalog build identity was reused with conflicting canonical inputs"
                )
            _verify_declared_inputs(
                request,
                schema_definition=schema_definition,
                semantic_overlay=semantic_overlay,
                report_seed=report_seed,
            )
            return prior
        try:
            _verify_declared_inputs(
                request,
                schema_definition=schema_definition,
                semantic_overlay=semantic_overlay,
                report_seed=report_seed,
            )
            return await self._build(
                request,
                schema_definition=schema_definition,
                semantic_overlay=semantic_overlay,
                report_seed=report_seed,
            )
        except Exception as error:
            if isinstance(error, CatalogPublicationConflict):
                raise
            await self._persist_rejection(request, error)
            raise

    async def get(
        self, request_scope: str, build_id: str
    ) -> SchemaCatalogBuildRecord:
        envelope = await self._records.get(request_scope, "catalog_build", build_id)
        return SchemaCatalogBuildRecord.model_validate(envelope.payload)

    async def _build(
        self,
        request: SchemaCatalogBuildRequest,
        *,
        schema_definition: bytes,
        semantic_overlay: bytes,
        report_seed: bytes,
    ) -> SchemaCatalogBuildRecord:
        actual_source_digest = sha256_digest(schema_definition)
        if actual_source_digest != request.schema_definition_digest:
            raise SchemaSourceDigestMismatch(
                "declared Schema Definition digest does not match the supplied bytes"
            )
        actual_overlay_digest = sha256_digest(semantic_overlay)
        if actual_overlay_digest != request.semantic_overlay_digest:
            raise SchemaSourceDigestMismatch(
                "declared semantic-overlay digest does not match the supplied bytes"
            )
        if report_seed:
            if request.candidate_seed_digest is None:
                raise SchemaSourceDigestMismatch(
                    "report-derived candidate seed bytes require an exact durable digest binding"
                )
            if sha256_digest(report_seed) != request.candidate_seed_digest:
                raise SchemaSourceDigestMismatch(
                    "declared candidate seed digest does not match the supplied bytes"
                )
        elif request.candidate_seed_digest is not None:
            raise SchemaSourceDigestMismatch(
                "candidate seed bytes are missing for the declared digest binding"
            )
        if request.generator_version != CATALOG_GENERATOR_VERSION:
            raise ValueError(
                f"unsupported catalog generator {request.generator_version}; "
                f"expected {CATALOG_GENERATOR_VERSION}"
            )

        overlay = load_semantic_overlay(semantic_overlay)
        physical = parse_physical_schema(
            schema_definition,
            request.schema_definition_ref,
        )
        catalog = parse_schema_catalog(
            schema_definition,
            request.schema_definition_ref,
            semantic_overlay=overlay,
        )
        rebuilt = parse_schema_catalog(
            schema_definition,
            request.schema_definition_ref,
            semantic_overlay=overlay,
        )
        if rebuilt.catalog_digest != catalog.catalog_digest:
            raise CatalogNondeterministic(
                "rebuilding identical canonical catalog inputs changed the logical digest"
            )

        with TemporaryDirectory(prefix="belllabs-schema-catalog-") as temporary:
            root = Path(temporary) / "schema"
            manifest = materialize_schema_workspace(
                catalog,
                schema_definition,
                root,
                report=report_seed,
            )
            bundle_payload = _bundle_payload(root, manifest)

        bundle_bytes = canonical_json_bytes(bundle_payload)
        bundle_address = await self._payloads.put(bundle_bytes)
        _verify_payload_address(bundle_bytes, bundle_address)
        profile_paths = {
            name: tuple(str(path) for path in paths)
            for name, paths in sorted(manifest["profiles"].items())
        }
        resources = tuple(
            CatalogResourceRecord(
                logical_path=str(resource["logical_path"]),
                content_digest=str(resource["content_digest"]),
                media_type=str(resource["media_type"]),
                size_bytes=int(resource["size_bytes"]),
                read_only=True,
                profiles=tuple(
                    sorted(
                        profile
                        for profile, paths in profile_paths.items()
                        if resource["logical_path"] in paths
                    )
                ),
            )
            for resource in manifest["resources"]
        )
        published_at = request.requested_at
        record = SchemaCatalogBuildRecord(
            build_id=request.build_id,
            idempotency_key=request.idempotency_key,
            request_fingerprint=_request_fingerprint(request),
            request_scope=request.request_scope,
            status="published",
            schema_definition_ref=request.schema_definition_ref,
            schema_definition_digest=request.schema_definition_digest,
            semantic_overlay_ref=request.semantic_overlay_ref,
            semantic_overlay_revision=request.semantic_overlay_revision,
            semantic_overlay_digest=request.semantic_overlay_digest,
            candidate_seed_ref=request.candidate_seed_ref,
            candidate_seed_digest=request.candidate_seed_digest,
            catalog_schema_version=request.catalog_schema_version,
            parser_generator_version=request.generator_version,
            normalization_policy_version=request.normalization_policy_version,
            physical_schema_digest=physical.catalog_digest,
            catalog_digest=catalog.catalog_digest,
            resource_manifest_digest=str(manifest["resources_digest"]),
            bundle=DurableObjectRef(
                uri=bundle_address.uri,
                digest=bundle_address.digest,
                size_bytes=bundle_address.size,
                media_type="application/vnd.belllabs.schema-catalog-bundle+json",
                version_id=bundle_address.version_id,
            ),
            resource_count=len(resources),
            total_bytes=sum(item.size_bytes for item in resources),
            tier0_size_bytes=int(manifest["tier0_size_bytes"]),
            profiles=profile_paths,
            resources=resources,
            validation_decision="accepted",
            diagnostics=(),
            publication_target=request.publication_target,
            published_at=published_at,
        )
        envelope = schema_grounding_record(
            record_type="catalog_build",
            record_id=record.build_id,
            request_scope=record.request_scope,
            payload=record.model_dump(mode="json"),
            created_at=published_at,
        )
        persisted = await self._records.append(envelope)
        return SchemaCatalogBuildRecord.model_validate(persisted.payload)

    async def _persist_rejection(
        self,
        request: SchemaCatalogBuildRequest,
        error: Exception,
    ) -> None:
        rejected_at = request.requested_at
        record = SchemaCatalogBuildRecord(
            build_id=request.build_id,
            idempotency_key=request.idempotency_key,
            request_fingerprint=_request_fingerprint(request),
            request_scope=request.request_scope,
            status="rejected",
            schema_definition_ref=request.schema_definition_ref,
            schema_definition_digest=request.schema_definition_digest,
            semantic_overlay_ref=request.semantic_overlay_ref,
            semantic_overlay_revision=request.semantic_overlay_revision,
            semantic_overlay_digest=request.semantic_overlay_digest,
            candidate_seed_ref=request.candidate_seed_ref,
            candidate_seed_digest=request.candidate_seed_digest,
            catalog_schema_version=request.catalog_schema_version,
            parser_generator_version=request.generator_version,
            normalization_policy_version=request.normalization_policy_version,
            resource_count=0,
            total_bytes=0,
            tier0_size_bytes=0,
            profiles={},
            resources=(),
            validation_decision="rejected",
            diagnostics=(type(error).__name__,),
            publication_target=request.publication_target,
            published_at=rejected_at,
        )
        envelope = schema_grounding_record(
            record_type="catalog_build",
            record_id=record.build_id,
            request_scope=record.request_scope,
            payload=record.model_dump(mode="json"),
            created_at=rejected_at,
        )
        try:
            await self._records.append(envelope)
        except CatalogPublicationConflict:
            # The original immutable record remains the authoritative outcome.
            pass


def _bundle_payload(root: Path, manifest: dict[str, Any]) -> dict[str, object]:
    files = {
        f"schema/{path.relative_to(root).as_posix()}": base64.b64encode(
            path.read_bytes()
        ).decode("ascii")
        for path in sorted(item for item in root.rglob("*") if item.is_file())
    }
    return {
        "bundle_schema_version": "1",
        "catalog_digest": manifest["catalog_digest"],
        "schema_definition_digest": manifest["schema_definition_digest"],
        "resource_manifest_digest": manifest["resources_digest"],
        "files_base64": files,
    }


def _verify_payload_address(payload: bytes, address: ContentAddress) -> None:
    if address.digest != sha256_digest(payload) or address.size != len(payload):
        raise CatalogPublicationConflict(
            "content-addressed payload store returned an inconsistent catalog bundle address"
        )


def _verify_declared_inputs(
    request: SchemaCatalogBuildRequest,
    *,
    schema_definition: bytes,
    semantic_overlay: bytes,
    report_seed: bytes,
) -> None:
    """Authenticate replay bytes before returning an idempotent publication."""
    if sha256_digest(schema_definition) != request.schema_definition_digest:
        raise SchemaSourceDigestMismatch(
            "declared Schema Definition digest does not match the supplied bytes"
        )
    if sha256_digest(semantic_overlay) != request.semantic_overlay_digest:
        raise SchemaSourceDigestMismatch(
            "declared semantic-overlay digest does not match the supplied bytes"
        )
    if report_seed:
        if request.candidate_seed_digest is None:
            raise SchemaSourceDigestMismatch(
                "report-derived candidate seed bytes require an exact durable digest binding"
            )
        if sha256_digest(report_seed) != request.candidate_seed_digest:
            raise SchemaSourceDigestMismatch(
                "declared candidate seed digest does not match the supplied bytes"
            )
    elif request.candidate_seed_digest is not None:
        raise SchemaSourceDigestMismatch(
            "candidate seed bytes are missing for the declared digest binding"
        )


def _request_fingerprint(request: SchemaCatalogBuildRequest) -> str:
    return sha256_digest(
        request.model_dump(mode="json", exclude={"requested_at"})
    )
