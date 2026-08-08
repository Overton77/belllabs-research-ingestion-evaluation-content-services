from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class ResearchUnitSpec(BaseModel):
    unit_id: str = Field(pattern=r"^[a-z][a-z0-9_]{1,40}$")
    question: str = Field(min_length=10, max_length=500)
    search_query: str = Field(min_length=3, max_length=400)
    rationale: str = Field(min_length=3, max_length=500)
    mode: Literal["search", "decompose_search"] = "search"

    @field_validator("unit_id", mode="before")
    @classmethod
    def normalize_unit_id(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower()
        return value


class MissionPlan(BaseModel):
    objective: str = Field(min_length=10, max_length=1_000)
    plan_summary: str = Field(min_length=10, max_length=1_000)
    units: tuple[ResearchUnitSpec, ...] = Field(min_length=2, max_length=3)

    @field_validator("units")
    @classmethod
    def unique_units(cls, value: tuple[ResearchUnitSpec, ...]) -> tuple[ResearchUnitSpec, ...]:
        if len({unit.unit_id for unit in value}) != len(value):
            raise ValueError("mission unit IDs must be unique")
        return value


class SourceSnapshot(BaseModel):
    source_id: str
    title: str
    url: str
    retrieved_at: datetime
    text: str
    text_sha256: str


class SourceBundle(BaseModel):
    query: str
    sources: tuple[SourceSnapshot, ...]


class AgentClaim(BaseModel):
    claim_text: str = Field(min_length=5, max_length=700)
    claim_type: Literal["fact", "numeric", "direct_quote", "paraphrase", "inference"]
    source_id: str = Field(min_length=2, max_length=80)
    source_quote: str = Field(min_length=5, max_length=1_200)
    numeric_mentions: tuple[str, ...] = Field(default=(), max_length=12)


class UnitAnalysis(BaseModel):
    answer: str = Field(min_length=10, max_length=2_000)
    claims: tuple[AgentClaim, ...] = Field(min_length=1, max_length=8)


class ResearchUnitResult(BaseModel):
    unit: ResearchUnitSpec
    sources: tuple[SourceSnapshot, ...]
    analysis: UnitAnalysis


class GateResult(BaseModel):
    gate: str
    passed: bool
    detail: str


class ClaimEvaluation(BaseModel):
    evaluation_id: str
    claim_id: str
    unit_id: str
    disposition: Literal["ACCEPT", "REJECT", "REVIEW"]
    source_id: str
    source_url: str | None
    source_start: int | None
    source_end: int | None
    lexical_support_score: float
    gates: tuple[GateResult, ...]
    report_sha256: str


class AcceptedClaim(BaseModel):
    claim_id: str
    unit_id: str
    claim_text: str
    source_url: str
    source_quote: str


class FinalSynthesis(BaseModel):
    answer: str = Field(min_length=20, max_length=4_000)
    claim_ids_used: tuple[str, ...] = Field(min_length=1, max_length=30)
    limitations: tuple[str, ...] = Field(default=(), max_length=8)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
