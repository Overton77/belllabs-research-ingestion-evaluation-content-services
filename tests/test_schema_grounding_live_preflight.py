from __future__ import annotations

from app.application.schema_grounding_live_preflight import _payload_digest
from app.integrations.schema_grounding_payloads import (
    SCHEMA_GROUNDING_INPUT_FORMATS,
    schema_grounding_input_uri,
)
from scripts.preflight_schema_grounding_coordinator_live import parse_args
from scripts.run_schema_grounding_coordinator_live import DEFAULT_SOURCE_RUN


def test_preflight_expands_exact_reviewed_intent_set() -> None:
    args = parse_args(["--artifact-bucket", "private-versioned-test-bucket"])

    assert args.deployment_id is None
    assert args.intent == tuple(
        DEFAULT_SOURCE_RUN / "queries" / f"{index:03d}-intent.json"
        for index in range(1, 6)
    )


def test_preflight_input_uris_are_content_addressed_and_typed() -> None:
    digest = _payload_digest(b"schema")

    assert digest == (
        "sha256:"
        "df0ad6e43880f09c90ebf95f19110178aba6890df0010ebda7485029e2b543b4"
    )
    assert schema_grounding_input_uri("private-bucket", digest, "schema") == (
        "s3://private-bucket/schema-grounding/live-inputs/"
        "df0ad6e43880f09c90ebf95f19110178aba6890df0010ebda7485029e2b543b4.graphql"
    )
    assert schema_grounding_input_uri(
        "private-bucket",
        digest,
        "semantic_overlay",
    ).endswith(".json")
    assert schema_grounding_input_uri("private-bucket", digest, "report").endswith(
        ".md"
    )
    assert SCHEMA_GROUNDING_INPUT_FORMATS["schema"].media_type == (
        "application/graphql"
    )
    assert SCHEMA_GROUNDING_INPUT_FORMATS["semantic_overlay"].media_type == (
        "application/json"
    )
    assert SCHEMA_GROUNDING_INPUT_FORMATS["report"].media_type == (
        "text/markdown; charset=utf-8"
    )
