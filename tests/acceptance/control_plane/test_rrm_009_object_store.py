"""RRM-009 ticket item 1: the S3 object artifact store in the production composition.

The deployment selects `S3ArtifactPayloadStore` when `S3_BUCKET` is set
(`app.temporal.deployment_composition.artifact_payload_store`): workspace candidates, the
journal's result manifests and output payloads, and promoted artifacts are then written to
and verified from the bucket through the S3 API, by the workers and the API alike. This
qualification runs the production stack against a local S3-compatible server (a disposable
MinIO container) instead of the filesystem content-addressed store.

Opt-in: `TEST_APPLICATION_POSTGRES_DSN`, `TEST_MONGODB_URI` and `RRM009_S3_ENDPOINT` with
`RRM009_S3_ACCESS_KEY` / `RRM009_S3_SECRET_KEY` (the container's root credentials). The AWS
configuration is a dedicated profile in temporary files: the operator's own `~/.aws` is never
read, and no request can reach AWS.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from app.application.workspaces.artifact_promotion import ArtifactPayloadAddress
from app.domain.operation_execution.errors import WorkspaceDigestMismatch
from app.integrations.artifact_payloads import S3ArtifactPayloadStore
from app.integrations.s3 import s3_client
from app.models import WorkspaceCandidateDocument
from app.temporal.deployment_composition import artifact_payload_store
from tests.fixtures.rrm009_production_harness import (
    mongo_database,  # noqa: F401 - the per-test Mongo database fixture
    open_production_stack,
    promote_generic_artifact,
)
from tests.fixtures.rrm009_production_stack import REPORT_PATH, technical_binding

PROFILE = "rrm009-minio"


def _s3_environment(root: Path) -> dict[str, str]:
    endpoint = os.getenv("RRM009_S3_ENDPOINT", "").strip()
    access = os.getenv("RRM009_S3_ACCESS_KEY", "").strip()
    secret = os.getenv("RRM009_S3_SECRET_KEY", "").strip()
    if not (endpoint and access and secret):
        pytest.skip(
            "RRM009_S3_ENDPOINT, RRM009_S3_ACCESS_KEY and RRM009_S3_SECRET_KEY are required"
        )
    aws = root / "aws"
    aws.mkdir()
    (aws / "config").write_text(
        f"[profile {PROFILE}]\nregion = us-east-1\ns3 =\n    addressing_style = path\n",
        encoding="utf-8",
    )
    (aws / "credentials").write_text(
        f"[{PROFILE}]\naws_access_key_id = {access}\naws_secret_access_key = {secret}\n",
        encoding="utf-8",
    )
    return {
        "S3_BUCKET": f"rrm009-artifacts-{uuid4().hex[:8]}",
        "AWS_PROFILE": PROFILE,
        "AWS_REGION": "us-east-1",
        "AWS_CONFIG_FILE": str(aws / "config"),
        "AWS_SHARED_CREDENTIALS_FILE": str(aws / "credentials"),
        "AWS_ENDPOINT_URL_S3": endpoint,
        "AWS_EC2_METADATA_DISABLED": "true",
    }


@pytest.mark.asyncio
async def test_s3_object_store_carries_candidates_results_and_promoted_artifacts(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
    mongo_database: str,  # noqa: F811
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = _s3_environment(tmp_path)
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    bucket = environment["S3_BUCKET"]
    technical = technical_binding()
    model_log: list[dict[str, Any]] = []
    async with open_production_stack(
        dsn=test_application_postgres_dsn,
        mongo_uri=test_mongodb_uri,
        mongo_database=mongo_database,
        root=tmp_path,
        monkeypatch=monkeypatch,
        technical=technical,
        components=technical.components(model_log),
        model_log=model_log,
        extra_environment=environment,
    ) as stack:
        assert stack.settings.s3_bucket == bucket
        store = artifact_payload_store(stack.settings)
        assert isinstance(store, S3ArtifactPayloadStore)
        async with s3_client(stack.settings) as client:
            await client.create_bucket(Bucket=bucket)
        run_id, result = await promote_generic_artifact(stack)
        artifact = result["artifact"]
        prefix = f"s3://{bucket}/artifacts/sha256/"

        # The promoted artifact lives in the bucket under its content address.
        assert artifact["object_ref"] == prefix + artifact["content_digest"].removeprefix("sha256:")
        key = artifact["object_ref"].removeprefix(f"s3://{bucket}/")
        async with s3_client(stack.settings) as client:
            head = await client.head_object(Bucket=bucket, Key=key)
            listed = await client.list_objects_v2(Bucket=bucket)
        assert head["Metadata"]["sha256"] == artifact["content_digest"].removeprefix("sha256:")
        address = ArtifactPayloadAddress(
            object_ref=artifact["object_ref"],
            content_digest=artifact["content_digest"],
            size_bytes=head["ContentLength"],
        )
        report = await store.retrieve(address)
        assert b"RRM-009 report" in report
        # Retrieval verifies the digest: a wrong address is refused, never served.
        with pytest.raises(WorkspaceDigestMismatch):
            await store.retrieve(
                address.model_copy(update={"content_digest": "sha256:" + "0" * 64})
            )

        # The captured workspace candidate is in the bucket, not on a worker-local disk.
        candidates = await WorkspaceCandidateDocument.find(
            WorkspaceCandidateDocument.logical_path == REPORT_PATH
        ).to_list()
        assert candidates and all(item.object_ref.startswith(prefix) for item in candidates)

        # The journal's result manifest and output payload were staged in the bucket too.
        async with stack.owner_pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT s.status, s.result_manifest_ref, s.result_manifest_digest,
                       s.result_manifest_size_bytes
                FROM belllabs_control.operation_settlements s
                JOIN belllabs_control.operation_effect_claims c
                  ON c.request_scope = s.request_scope AND c.effect_claim_id = s.effect_claim_id
                WHERE c.belllabs_run_id = $1
                """,
                run_id,
            )
            durable = await connection.fetchval(
                "SELECT count(*) FROM belllabs_control.durable_artifact_references "
                "WHERE run_id = $1",
                run_id,
            )
        assert durable == 1
        ((status, manifest_ref, manifest_digest, manifest_size),) = [tuple(row) for row in rows]
        assert status == "completed" and manifest_ref.startswith(prefix), manifest_ref
        manifest = json.loads(
            await store.retrieve(
                ArtifactPayloadAddress(
                    object_ref=manifest_ref,
                    content_digest=manifest_digest,
                    size_bytes=manifest_size,
                )
            )
        )
        assert manifest["output_payload_ref"].startswith(prefix)
        output = json.loads(
            await store.retrieve(
                ArtifactPayloadAddress(
                    object_ref=manifest["output_payload_ref"],
                    content_digest=manifest["output_payload_digest"],
                    size_bytes=manifest["output_payload_size_bytes"],
                )
            )
        )
        assert any("capability_lineage" in item for item in output["event_payloads"])
        # Nothing fell back to the filesystem content-addressed store.
        assert not stack.payload_root.exists() or not any(stack.payload_root.rglob("*"))
        keys = sorted(item["Key"] for item in listed.get("Contents", ()))
        print(
            "RRM-009 EVIDENCE s3:",
            json.dumps(
                {
                    "run": run_id,
                    "bucket": bucket,
                    "artifact": {
                        "object_ref": artifact["object_ref"],
                        "content_digest": artifact["content_digest"],
                        "durable_reference": artifact["durable_reference"],
                    },
                    "result_manifest_ref": manifest_ref,
                    "objects": len(keys),
                },
                sort_keys=True,
            ),
        )
