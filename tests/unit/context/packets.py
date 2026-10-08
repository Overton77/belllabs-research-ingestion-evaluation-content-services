"""Fixture candidates and requests for Context Packer tests (deterministic, no I/O)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

from mission_control.domain.context.packet import (
    ContextBinding,
    ContextPurpose,
    ContextSourceKind,
    ContextTrust,
    ExpandMode,
    ItemProvenance,
    ModelBudgetProfile,
    PackCandidate,
    PacketScope,
    PacketTarget,
    PackRequest,
    RetrievalInstruction,
    RetrievalKind,
    ToolRetrieval,
)

SEALED_AT = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
PACKET_ID = "0199b8f0-0000-7000-8000-000000000001"
SELECTION_REF = "context_selection:0199b8f0-0000-7000-8000-0000000000aa"


def digest(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


class WordCounter:
    """An 'exact' deterministic tokenizer for tests: one token per whitespace word."""

    exact = True

    def count(self, text: str) -> int:
        return len(text.split())


SCOPE = PacketScope(installation_id="inst-1", application_id="biotech", tenant_id="tenant-1")


def target(purpose: ContextPurpose = ContextPurpose.STAGE_START) -> PacketTarget:
    return PacketTarget(
        mission_id="mission-1",
        run_id="run-1",
        revision_id="rev-1",
        node_key="synthesize",
        activation_id="act-synthesize-1",
        attempt_no=1,
        generation=0,
        purpose=purpose,
    )


def profile(**overrides: int | str) -> ModelBudgetProfile:
    values: dict[str, int | str] = {
        "model_profile_ref": "frontier.long_context",
        "tokenizer_ref": "o200k_base",
        "context_window": 400_000,
        "reserved_output": 16_000,
        "system_prompt_tokens": 3_200,
        "tool_schema_allowance": 7_100,
        "skills_metadata_tokens": 1_200,
        "control_reserve": 8_000,
        "safety_margin": 20_000,
    }
    values.update(overrides)
    return ModelBudgetProfile.model_validate(values)


def text_candidate(
    source_kind: ContextSourceKind,
    source_ref: str,
    text: str,
    *,
    binding_name: str | None = None,
    trust: ContextTrust = ContextTrust.ADMITTED_INPUT,
    mandatory: bool = False,
    expand: ExpandMode | None = None,
    media_type: str = "text/markdown",
    accepted: bool = True,
    **extra: object,
) -> PackCandidate:
    provenance = extra.pop("provenance", None) or ItemProvenance(
        producer_activation_id="act-collect-1",
        accepted_decision_ref="decision://act-collect-1/accepted" if accepted else None,
    )
    return PackCandidate.model_validate(
        {
            "source_kind": source_kind,
            "source_ref": source_ref,
            "binding_name": binding_name,
            "content_digest": digest(text),
            "bytes": len(text.encode("utf-8")),
            "media_type": media_type,
            "trust": trust,
            "mandatory": mandatory,
            "expand": expand,
            "text": text,
            "provenance": provenance,
            **extra,
        }
    )


def worked_example_request(**overrides: object) -> PackRequest:
    """SPEC-02 worked example: Mission 1 ``collect -> synthesize``."""

    sources_body = '{"records": [' + ",".join(['{"pmid": "1"}'] * 4) + "]}"
    candidates = (
        text_candidate(
            ContextSourceKind.OPERATING_CONTRACT,
            "state://run-1/synthesize/operating_contract",
            "Synthesize the collected sources into an evidence map. Cite every claim.",
            trust=ContextTrust.AUTHORITATIVE,
            mandatory=True,
        ),
        text_candidate(
            ContextSourceKind.PENDING_COMMITMENTS,
            "state://run-1/pending_commitments",
            "Human gate review runs after this stage.",
            trust=ContextTrust.AUTHORITATIVE,
            mandatory=True,
        ),
        text_candidate(
            ContextSourceKind.BUDGET_REMAINING,
            "state://run-1/budget_remaining",
            "usd 14.20 of 25; tokens 1.3M of 2M",
            trust=ContextTrust.AUTHORITATIVE,
            mandatory=True,
        ),
        text_candidate(
            ContextSourceKind.ACCEPTED_OUTPUT,
            "artifact://inst-1/run-1/coverage-review",
            "Coverage is adequate for NAD+ and muscle aging; gaps in human trials.",
            binding_name="coverage",
        ),
        PackCandidate(
            source_kind=ContextSourceKind.ACCEPTED_OUTPUT,
            source_ref="artifact://inst-1/run-1/source-manifest",
            binding_name="sources",
            content_digest=digest(sources_body),
            bytes=215_040,
            media_type="application/json",
            trust=ContextTrust.UNTRUSTED_CONTENT,
            file_name="source_manifest.json",
            durable_ref="payload://objects/source-manifest#" + digest(sources_body) + ":215040",
            summary="180 PubMed records with abstracts",
            provenance=ItemProvenance(
                producer_activation_id="act-collect-1",
                accepted_decision_ref="decision://act-collect-1/accepted",
            ),
        ),
        PackCandidate(
            source_kind=ContextSourceKind.JOURNAL_DIGEST,
            source_ref="journal://act-collect-1/seg-07",
            content_digest=digest("journal-seg-07"),
            bytes=4_096,
            media_type="application/json",
            trust=ContextTrust.AUTHORITATIVE,
            mandatory=True,
            expand=ExpandMode.REFERENCE,
            summary="sealed journal head of collect (7 segments)",
        ),
        PackCandidate(
            source_kind=ContextSourceKind.CATALOG_CONTEXT,
            source_ref="catalog://biotech.schema_context@1.3.0",
            binding_name="schema_context",
            content_digest=digest("schema-context"),
            bytes=480_000,
            media_type="text/markdown",
            trust=ContextTrust.ADMITTED_INPUT,
            summary="Biotech knowledge graph schema context for muscle aging NAD",
            retrieval=RetrievalInstruction(
                kind=RetrievalKind.TOOL_CALL,
                tool=ToolRetrieval(
                    name="biotech.schema_context.select",
                    args_digest=digest('{"search": "muscle aging NAD"}'),
                    args_excerpt='search="muscle aging NAD"',
                ),
            ),
        ),
        text_candidate(
            ContextSourceKind.WORKSPACE_MAP,
            "state://run-1/synthesize/workspace_map",
            "/inputs read-only; /outputs writable; .mission/context.md is this index.",
            trust=ContextTrust.AUTHORITATIVE,
            mandatory=True,
        ),
    )
    values: dict[str, object] = {
        "packet_id": PACKET_ID,
        "sealed_at": SEALED_AT,
        "context_selection_ref": SELECTION_REF,
        "scope": SCOPE,
        "target": target(),
        "producer_refs": ("act-collect-1",),
        "profile": profile(),
        "bindings": (
            ContextBinding(binding_name="sources", expand=ExpandMode.MATERIALIZE),
            ContextBinding(binding_name="coverage", expand=ExpandMode.INLINE),
            ContextBinding(binding_name="schema_context", expand=ExpandMode.REFERENCE),
        ),
        "candidates": candidates,
    }
    values.update(overrides)
    return PackRequest.model_validate(values)
