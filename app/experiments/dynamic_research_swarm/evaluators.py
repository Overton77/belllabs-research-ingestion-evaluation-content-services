from __future__ import annotations

import json
import math
import re
import unicodedata
from collections import Counter
from decimal import Decimal, InvalidOperation

from .contracts import (
    AgentClaim,
    ClaimEvaluation,
    GateResult,
    ResearchUnitResult,
    SourceSnapshot,
    sha256_text,
)

POLICY_VERSION = "swarm-fidelity-v1"
_NUMBER = re.compile(r"(?<![\w.])[-+]?\d[\d,]*(?:\.\d+)?(?:e[-+]?\d+)?%?", re.IGNORECASE)
_TOKENS = re.compile(r"[a-z0-9]+")
_NEGATIONS = {"no", "not", "never", "without", "neither", "nor"}
_MODALS = {"may", "might", "could", "suggests", "possible", "possibly"}


def _normalized_text(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).lower().split())


def _numbers(value: str) -> tuple[str, ...]:
    normalized: list[str] = []
    for match in _NUMBER.findall(unicodedata.normalize("NFKC", value)):
        percent = match.endswith("%")
        raw = match.removesuffix("%").replace(",", "")
        try:
            number = Decimal(raw).normalize()
            normalized.append(f"{number}{'%' if percent else ''}")
        except InvalidOperation:
            normalized.append(match.lower())
    return tuple(normalized)


def _token_counter(value: str) -> Counter[str]:
    return Counter(token for token in _TOKENS.findall(_normalized_text(value)) if len(token) > 2)


def lexical_cosine(left: str, right: str) -> float:
    a = _token_counter(left)
    b = _token_counter(right)
    if not a or not b:
        return 0.0
    numerator = sum(count * b[token] for token, count in a.items())
    denominator = math.sqrt(sum(v * v for v in a.values()) * sum(v * v for v in b.values()))
    return numerator / denominator if denominator else 0.0


def _gate(gate: str, passed: bool, detail: str) -> GateResult:
    return GateResult(gate=gate, passed=passed, detail=detail)


def evaluate_claim(
    *, run_id: str, unit_id: str, index: int, claim: AgentClaim, sources: tuple[SourceSnapshot, ...]
) -> ClaimEvaluation:
    claim_id = f"claim:{run_id}:{unit_id}:{index}"
    source = next((item for item in sources if item.source_id == claim.source_id), None)
    source_hash_ok = source is not None and sha256_text(source.text) == source.text_sha256
    quote_start = source.text.find(claim.source_quote) if source is not None else -1
    quote_exact = quote_start >= 0
    claim_numbers = _numbers(claim.claim_text)
    quote_numbers = _numbers(claim.source_quote)
    declared_numbers = tuple(
        number for mention in claim.numeric_mentions for number in _numbers(mention)
    )
    numeric_fidelity = set(claim_numbers).issubset(quote_numbers)
    declaration_complete = set(claim_numbers) == set(declared_numbers)
    score = lexical_cosine(claim.claim_text, claim.source_quote)
    claim_tokens = set(_TOKENS.findall(_normalized_text(claim.claim_text)))
    quote_tokens = set(_TOKENS.findall(_normalized_text(claim.source_quote)))
    negation_ok = not (claim_tokens & _NEGATIONS) or bool(quote_tokens & _NEGATIONS)
    modality_ok = not (quote_tokens & _MODALS) or bool(claim_tokens & _MODALS)
    gates = (
        _gate("source_attribution", source is not None, "source ID resolves in retrieval ledger"),
        _gate("source_hash", source_hash_ok, "stored source text matches its SHA-256"),
        _gate("exact_evidence_span", quote_exact, "quoted evidence is an exact source substring"),
        _gate(
            "numeric_fidelity",
            numeric_fidelity,
            f"claim={claim_numbers}; evidence={quote_numbers}",
        ),
        _gate(
            "numeric_declaration",
            declaration_complete,
            f"parsed={claim_numbers}; declared={declared_numbers}",
        ),
        _gate("negation_guard", negation_ok, "claim and evidence negation polarity agree"),
        _gate("modality_guard", modality_ok, "claim does not strengthen source modality"),
        _gate("lexical_support", score >= 0.20, f"deterministic token cosine={score:.4f}"),
    )
    disposition = "ACCEPT" if all(gate.passed for gate in gates) else "REJECT"
    canonical = json.dumps(
        {
            "claim_id": claim_id,
            "policy": POLICY_VERSION,
            "source_hash": source.text_sha256 if source else None,
            "gates": [gate.model_dump(mode="json") for gate in gates],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return ClaimEvaluation(
        evaluation_id=f"evaluation:{sha256_text(canonical)}",
        claim_id=claim_id,
        unit_id=unit_id,
        disposition=disposition,
        source_id=claim.source_id,
        source_url=source.url if source else None,
        source_start=quote_start if quote_exact else None,
        source_end=quote_start + len(claim.source_quote) if quote_exact else None,
        lexical_support_score=score,
        gates=gates,
        report_sha256=sha256_text(canonical),
    )


def evaluate_unit(run_id: str, result: ResearchUnitResult) -> tuple[ClaimEvaluation, ...]:
    return tuple(
        evaluate_claim(
            run_id=run_id,
            unit_id=result.unit.unit_id,
            index=index,
            claim=claim,
            sources=result.sources,
        )
        for index, claim in enumerate(result.analysis.claims, start=1)
    )
