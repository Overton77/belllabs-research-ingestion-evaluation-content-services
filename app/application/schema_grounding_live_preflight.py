from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from app.application.capability_search import CapabilitySearchService
from app.application.control_plane_repository import BeanieDefinitionRepository
from app.application.postgres_capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from app.application.schema_catalog import parse_schema_catalog
from app.config import Settings
from app.domain.control_plane.contracts import DefinitionKind
from app.domain.schema_context.contracts import (
    QueryExecutionIntent,
    SchemaOperationProjection,
)
from app.integrations.capability_embeddings import OpenAICapabilityEmbeddingAdapter
from app.integrations.catalog_projection_admin import list_published_definition_refs
from app.integrations.mongodb import create_mongodb
from app.integrations.neo4j import create_neo4j
from app.integrations.neo4j_schema_deployment import Neo4jLiveSchemaDeploymentReader
from app.integrations.postgres import (
    create_application_postgres_pool,
    create_postgres_pool,
)
from app.integrations.s3 import s3_client
from app.integrations.schema_grounding_payloads import (
    SCHEMA_GROUNDING_INPUT_FORMATS,
    schema_grounding_input_uri,
)
from app.integrations.temporal import create_temporal_client

from .schema_grounding_coordinator_live import (
    SCENARIO_A_QUERY,
    SCENARIO_C_QUERY,
    _current_record,
    _implementation_for,
    _retrieve_workflow,
    _runtime_workspace,
    _verify_projection_inputs,
)


def _payload_digest(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _resolve_inputs(args: Any) -> tuple[Path, Path, Path, Path, tuple[Path, ...]]:
    return (
        Path(args.schema).resolve(strict=True),
        Path(args.semantic_overlay).resolve(strict=True),
        Path(args.report).resolve(strict=True),
        Path(args.projection).resolve(strict=True),
        tuple(Path(path).resolve(strict=True) for path in args.intent),
    )


async def _verify_read_contracts(
    pool: Any,
    relations: tuple[str, ...],
) -> tuple[str, ...]:
    async with pool.acquire() as connection:
        verified = []
        for relation in relations:
            row = await connection.fetchrow(
                """
                SELECT
                    to_regclass($1) IS NOT NULL AS relation_exists,
                    CASE
                        WHEN to_regclass($1) IS NULL THEN false
                        ELSE has_table_privilege(current_user, $1, 'SELECT')
                    END AS can_select
                """,
                relation,
            )
            if row is None or not row["relation_exists"] or not row["can_select"]:
                raise RuntimeError(
                    f"runtime read contract is unavailable for {relation}"
                )
            verified.append(relation)
    return tuple(verified)


def _object_status(
    expected: dict[str, Any],
    head: dict[str, Any] | None,
) -> dict[str, Any]:
    if head is None:
        return {
            **expected,
            "present": False,
            "version_id": None,
            "observed_media_type": None,
            "integrity_verified": False,
        }
    return {
        **expected,
        "present": True,
        "version_id": head.get("VersionId"),
        "observed_media_type": head.get("ContentType"),
        "integrity_verified": (
            head.get("ContentLength") == expected["size_bytes"]
            and head.get("ContentType") == expected["media_type"]
            and head.get("Metadata", {}).get("sha256")
            == str(expected["digest"]).removeprefix("sha256:")
        ),
    }


async def preflight_live_schema_grounding(args: Any) -> dict[str, Any]:
    """Exercise every read-only A/C launch dependency without creating run state."""

    base_settings = Settings()
    settings = base_settings.model_copy(update={"s3_bucket": args.artifact_bucket})
    assert settings.s3_bucket is not None
    (
        schema_path,
        overlay_path,
        report_path,
        projection_path,
        intent_paths,
    ) = _resolve_inputs(args)

    schema_bytes, overlay_bytes, report_bytes = await asyncio.gather(
        asyncio.to_thread(schema_path.read_bytes),
        asyncio.to_thread(overlay_path.read_bytes),
        asyncio.to_thread(report_path.read_bytes),
    )
    schema_digest = _payload_digest(schema_bytes)
    overlay_digest = _payload_digest(overlay_bytes)
    report_digest = _payload_digest(report_bytes)
    schema_uri = schema_grounding_input_uri(
        settings.s3_bucket,
        schema_digest,
        "schema",
    )
    overlay_uri = schema_grounding_input_uri(
        settings.s3_bucket,
        overlay_digest,
        "semantic_overlay",
    )
    report_uri = schema_grounding_input_uri(
        settings.s3_bucket,
        report_digest,
        "report",
    )
    catalog = parse_schema_catalog(
        schema_bytes,
        schema_uri,
        semantic_overlay=overlay_path,
    )
    projection = SchemaOperationProjection.model_validate_json(
        projection_path.read_text(encoding="utf-8")
    )
    intents = tuple(
        QueryExecutionIntent.model_validate_json(path.read_text(encoding="utf-8"))
        for path in intent_paths
    )
    _verify_projection_inputs(
        projection,
        intents,
        schema_digest=schema_digest,
        catalog_digest=catalog.catalog_digest,
    )

    mongo_client, mongo_database = await create_mongodb(settings)
    capability_pool = await create_postgres_pool(settings)
    application_pool = await create_application_postgres_pool(settings)
    neo4j = await create_neo4j(settings)
    try:
        definitions = BeanieDefinitionRepository()
        refs = await list_published_definition_refs()
        records = tuple([await definitions.get(ref) for ref in refs])
        search = CapabilitySearchService(
            search=PostgresCatalogSearchRepository(capability_pool),
            definitions=definitions,
            embeddings=OpenAICapabilityEmbeddingAdapter(settings),
            embedding_model_id=settings.capability_embedding_model,
            embedding_dimensions=settings.capability_embedding_dimensions,
        )
        workflow_a, search_a = await _retrieve_workflow(
            search,
            definitions=definitions,
            query=SCENARIO_A_QUERY,
            tenant_scope=args.tenant_scope,
            expected_logical_id="schema-context-selection",
        )
        workflow_c, search_c = await _retrieve_workflow(
            search,
            definitions=definitions,
            query=SCENARIO_C_QUERY,
            tenant_scope=args.tenant_scope,
            expected_logical_id="supporting-graph-reconciliation",
        )
        implementation_a = _implementation_for(
            records,
            workflow_a.ref,
            blueprint_logical_id="schema-context-selection-v1",
        )
        implementation_c = _implementation_for(
            records,
            workflow_c.ref,
            blueprint_logical_id="supporting-graph-reconciliation-goal-directed-v1",
        )
        runtime_a, workspace_a = _runtime_workspace(records, implementation_a)
        runtime_c, workspace_c = _runtime_workspace(records, implementation_c)
        profile_a = _current_record(
            records,
            DefinitionKind.AGENT_PROFILE,
            "agent-profile.schema-context-selection-v1",
        )
        profile_c = _current_record(
            records,
            DefinitionKind.AGENT_PROFILE,
            "agent-profile.supporting-graph-reconciliation-v1",
        )

        temporal = await create_temporal_client(settings)
        await temporal.service_client.check_health()
        graph_reader = Neo4jLiveSchemaDeploymentReader(neo4j)
        snapshot = await graph_reader.read_schema_snapshot(database=args.database)
        evidence = None
        if args.deployment_id:
            evidence = await graph_reader.read(
                environment="production",
                database=args.database,
                deployment_id=args.deployment_id,
            )

        expected_objects = {
            "schema": {
                "uri": schema_uri,
                "digest": schema_digest,
                "size_bytes": len(schema_bytes),
                "media_type": SCHEMA_GROUNDING_INPUT_FORMATS["schema"].media_type,
            },
            "semantic_overlay": {
                "uri": overlay_uri,
                "digest": overlay_digest,
                "size_bytes": len(overlay_bytes),
                "media_type": (
                    SCHEMA_GROUNDING_INPUT_FORMATS["semantic_overlay"].media_type
                ),
            },
            "report": {
                "uri": report_uri,
                "digest": report_digest,
                "size_bytes": len(report_bytes),
                "media_type": SCHEMA_GROUNDING_INPUT_FORMATS["report"].media_type,
            },
        }
        object_heads: dict[str, dict[str, Any] | None] = {}
        async with s3_client(settings) as client:
            await client.head_bucket(Bucket=settings.s3_bucket)
            versioning = await client.get_bucket_versioning(Bucket=settings.s3_bucket)
            encryption = await client.get_bucket_encryption(Bucket=settings.s3_bucket)
            for name, expected in expected_objects.items():
                key = str(expected["uri"]).removeprefix(
                    f"s3://{settings.s3_bucket}/"
                )
                try:
                    object_heads[name] = await client.head_object(
                        Bucket=settings.s3_bucket,
                        Key=key,
                    )
                except ClientError as error:
                    status = error.response.get("ResponseMetadata", {}).get(
                        "HTTPStatusCode"
                    )
                    if status != 404:
                        raise
                    object_heads[name] = None

        capability_contracts, application_contracts = await asyncio.gather(
            _verify_read_contracts(
                capability_pool,
                (
                    "capability_search.documents",
                    "capability_search.generations",
                    "capability_search.active_generations",
                ),
            ),
            _verify_read_contracts(
                application_pool,
                (
                    "belllabs_control.workflow_runs",
                    "belllabs_control.coordinator_launch_tickets",
                    "belllabs_control.workflow_semantic_input_bindings",
                    "belllabs_control.coordinator_audit_events",
                    "belllabs_control.coordinator_workflow_results",
                ),
            ),
        )
        object_status = {
            name: _object_status(expected, object_heads[name])
            for name, expected in expected_objects.items()
        }
        objects_ready = all(
            bool(status["integrity_verified"]) for status in object_status.values()
        )
        evidence_present = evidence is not None
        launch_ready = objects_ready and bool(args.deployment_id) and evidence_present
        return {
            "ok": True,
            "launch_ready": launch_ready,
            "write_operations_performed": 0,
            "local_inputs": {
                "schema_path": str(schema_path),
                "schema_definition_ref": schema_uri,
                "schema_definition_digest": schema_digest,
                "semantic_overlay_path": str(overlay_path),
                "semantic_overlay_digest": overlay_digest,
                "semantic_overlay_ref": overlay_uri,
                "report_path": str(report_path),
                "report_digest": report_digest,
                "report_ref": report_uri,
                "catalog_digest": catalog.catalog_digest,
                "projection_id": projection.projection_id,
                "projection_digest": projection.projection_digest,
                "intent_ids": [intent.intent_id for intent in intents],
            },
            "catalog": {
                "published_definition_count": len(records),
                "scenario_a": {
                    "query": search_a.query,
                    "workflow_ref": workflow_a.ref.model_dump(mode="json"),
                    "implementation_ref": implementation_a.ref.model_dump(mode="json"),
                    "runtime_ref": runtime_a.ref.model_dump(mode="json"),
                    "workspace_ref": workspace_a.ref.model_dump(mode="json"),
                    "agent_profile_ref": profile_a.ref.model_dump(mode="json"),
                },
                "scenario_c": {
                    "query": search_c.query,
                    "workflow_ref": workflow_c.ref.model_dump(mode="json"),
                    "implementation_ref": implementation_c.ref.model_dump(mode="json"),
                    "runtime_ref": runtime_c.ref.model_dump(mode="json"),
                    "workspace_ref": workspace_c.ref.model_dump(mode="json"),
                    "agent_profile_ref": profile_c.ref.model_dump(mode="json"),
                },
            },
            "authorities": {
                "mongodb": {
                    "database": mongo_database.name,
                    "exact_rehydration": True,
                },
                "capability_postgres": {
                    "connected": True,
                    "verified_read_contracts": list(capability_contracts),
                },
                "application_postgres": {
                    "connected": True,
                    "runtime_identity_verified": True,
                    "verified_read_contracts": list(application_contracts),
                },
                "neo4j": {
                    "database": snapshot.database,
                    "server_agent": snapshot.server_agent,
                    "live_schema_snapshot_digest": snapshot.snapshot_digest,
                    "token_catalog_node_label_count": len(
                        snapshot.token_catalog_node_labels
                    ),
                    "token_catalog_relationship_type_count": len(
                        snapshot.token_catalog_relationship_types
                    ),
                    "active_node_label_count": len(snapshot.active_node_labels),
                    "active_relationship_type_count": len(
                        snapshot.active_relationship_types
                    ),
                    "index_count": len(snapshot.indexes),
                    "constraint_count": len(snapshot.constraints),
                    "requested_deployment_id": args.deployment_id,
                    "deployment_evidence_present": evidence_present,
                    "deployment_evidence_ref": (
                        None if evidence is None else evidence.evidence_id
                    ),
                },
                "s3": {
                    "bucket": settings.s3_bucket,
                    "versioning_status": versioning.get("Status"),
                    "encryption_rules": len(
                        encryption.get("ServerSideEncryptionConfiguration", {}).get(
                            "Rules",
                            [],
                        )
                    ),
                    "expected_input_objects": object_status,
                },
                "temporal": {
                    "address": settings.temporal_address,
                    "namespace": settings.temporal_namespace,
                    "healthy": True,
                    "worker_mode": "live-runner-starts-both-families-in-process",
                },
            },
            "launch_blockers": [
                blocker
                for blocker, present in (
                    (
                        "one or more exact typed S3 input objects are absent or "
                        "failed metadata/integrity verification",
                        not objects_ready,
                    ),
                    (
                        "operator deployment evidence identity is not supplied",
                        not args.deployment_id,
                    ),
                    (
                        "operator deployment evidence is not visible by exact identity",
                        bool(args.deployment_id) and not evidence_present,
                    ),
                )
                if present
            ],
        }
    finally:
        await neo4j.close()
        await application_pool.close()
        await capability_pool.close()
        await mongo_client.close()
