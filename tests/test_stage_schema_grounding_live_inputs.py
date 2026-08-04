from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Any

import pytest

from app.integrations.control_plane_payloads import ContentAddress
from app.integrations.schema_grounding_payloads import (
    SchemaGroundingInputKind,
    schema_grounding_input_uri,
)
from scripts import stage_schema_grounding_live_inputs


class _Store:
    def __init__(self, bucket: str, kind: SchemaGroundingInputKind) -> None:
        self._bucket = bucket
        self._kind = kind

    async def put(self, payload: bytes) -> ContentAddress:
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        return ContentAddress(
            uri=schema_grounding_input_uri(self._bucket, digest, self._kind),
            digest=digest,
            size=len(payload),
            version_id=f"version-{self._kind}",
        )


@pytest.mark.asyncio
async def test_staging_uses_exact_typed_store_for_each_raw_input(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    paths = {
        "schema": tmp_path / "schema.graphql",
        "semantic_overlay": tmp_path / "overlay.json",
        "report": tmp_path / "report.md",
    }
    paths["schema"].write_bytes(b"type Node { id: ID! }")
    paths["semantic_overlay"].write_bytes(b'{"version":1}')
    paths["report"].write_bytes(b"# Report\n")
    calls: list[tuple[str, SchemaGroundingInputKind]] = []

    def fake_store(
        settings: Any,
        bucket: str,
        kind: SchemaGroundingInputKind,
    ) -> _Store:
        del settings
        calls.append((bucket, kind))
        return _Store(bucket, kind)

    monkeypatch.setattr(
        stage_schema_grounding_live_inputs,
        "schema_grounding_input_store",
        fake_store,
    )
    result = await stage_schema_grounding_live_inputs._run(
        argparse.Namespace(
            artifact_bucket="private-versioned-bucket",
            schema=paths["schema"],
            semantic_overlay=paths["semantic_overlay"],
            report=paths["report"],
        )
    )

    assert calls == [
        ("private-versioned-bucket", "schema"),
        ("private-versioned-bucket", "semantic_overlay"),
        ("private-versioned-bucket", "report"),
    ]
    assert result["schema"]["uri"].endswith(".graphql")
    assert result["semantic_overlay"]["uri"].endswith(".json")
    assert result["report"]["uri"].endswith(".md")
    assert result["schema"]["media_type"] == "application/graphql"
    assert result["semantic_overlay"]["media_type"] == "application/json"
    assert result["report"]["media_type"] == "text/markdown; charset=utf-8"
