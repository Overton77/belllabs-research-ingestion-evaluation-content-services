from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from app.config import Settings
from app.integrations import control_plane_payloads
from app.integrations.control_plane_payloads import S3PayloadStore
from app.integrations.schema_grounding_payloads import (
    SchemaGroundingInputKind,
    schema_grounding_input_store,
)


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    async def __aenter__(self) -> _Body:
        return self

    async def __aexit__(self, *args: object) -> None:
        del args

    async def read(self) -> bytes:
        return self._payload


class _S3:
    def __init__(self) -> None:
        self.puts: list[dict[str, Any]] = []
        self.objects: dict[str, bytes] = {}

    async def put_object(self, **arguments: Any) -> dict[str, str]:
        self.puts.append(arguments)
        self.objects[str(arguments["Key"])] = bytes(arguments["Body"])
        return {"VersionId": f"version-{len(self.puts)}"}

    async def get_object(self, **arguments: Any) -> dict[str, _Body]:
        return {"Body": _Body(self.objects[str(arguments["Key"])])}


@pytest.mark.asyncio
async def test_s3_payload_store_preserves_json_defaults_and_raw_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _S3()

    @asynccontextmanager
    async def fake_s3_client(settings: Settings) -> AsyncIterator[_S3]:
        del settings
        yield client

    monkeypatch.setattr(control_plane_payloads, "s3_client", fake_s3_client)
    store = S3PayloadStore(Settings(), "private-bucket", prefix="payloads")

    address = await store.put(b'{"ok":true}')
    retrieved = await store.retrieve(address)

    assert retrieved == b'{"ok":true}'
    assert address.uri.endswith(".json")
    assert address.version_id == "version-1"
    assert client.puts[0]["ContentType"] == "application/json"
    assert client.puts[0]["Metadata"]["sha256"] == address.digest.removeprefix(
        "sha256:"
    )


@pytest.mark.asyncio
async def test_s3_payload_store_writes_explicit_media_type_and_suffix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _S3()

    @asynccontextmanager
    async def fake_s3_client(settings: Settings) -> AsyncIterator[_S3]:
        del settings
        yield client

    monkeypatch.setattr(control_plane_payloads, "s3_client", fake_s3_client)
    store = S3PayloadStore(
        Settings(),
        "private-bucket",
        prefix="schema-grounding/live-inputs",
        media_type="application/graphql",
        suffix=".graphql",
    )

    address = await store.put(b"type Node { id: ID! }")
    retrieved = await store.retrieve(address)

    assert retrieved == b"type Node { id: ID! }"
    assert address.uri.endswith(".graphql")
    assert client.puts[0]["ContentType"] == "application/graphql"
    assert client.puts[0]["Body"] == b"type Node { id: ID! }"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "media_type", "suffix"),
    [
        ("schema", "application/graphql", ".graphql"),
        ("semantic_overlay", "application/json", ".json"),
        ("report", "text/markdown; charset=utf-8", ".md"),
    ],
)
async def test_schema_grounding_store_uses_exact_typed_object_format(
    monkeypatch: pytest.MonkeyPatch,
    kind: SchemaGroundingInputKind,
    media_type: str,
    suffix: str,
) -> None:
    client = _S3()

    @asynccontextmanager
    async def fake_s3_client(settings: Settings) -> AsyncIterator[_S3]:
        del settings
        yield client

    monkeypatch.setattr(control_plane_payloads, "s3_client", fake_s3_client)
    store = schema_grounding_input_store(Settings(), "private-bucket", kind)

    address = await store.put(b"raw bytes")

    assert address.uri.endswith(suffix)
    assert client.puts[0]["ContentType"] == media_type
    assert client.puts[0]["Body"] == b"raw bytes"


@pytest.mark.parametrize(
    ("media_type", "suffix"),
    [
        ("", ".json"),
        ("application/json", "json"),
        ("application/json", "."),
        ("application/json", "../json"),
        ("application/json", ".nested/json"),
    ],
)
def test_s3_payload_store_rejects_invalid_object_format(
    media_type: str,
    suffix: str,
) -> None:
    with pytest.raises(ValueError):
        S3PayloadStore(
            Settings(),
            "private-bucket",
            media_type=media_type,
            suffix=suffix,
        )
