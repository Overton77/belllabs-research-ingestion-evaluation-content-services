from __future__ import annotations

from enum import StrEnum
from typing import Literal, Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


class DiscoveryContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExternalDiscoverySource(StrEnum):
    MCP_REGISTRY = "mcp_registry"
    NPX_SKILLS = "npx_skills"


class ExternalDiscoveryEvidence(DiscoveryContract):
    source: ExternalDiscoverySource
    source_version: str = Field(min_length=1, max_length=128)
    query: str = Field(min_length=1, max_length=500)
    retrieved_at: AwareDatetime
    raw_response_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    raw_response_size_bytes: int = Field(ge=0)
    exit_code: int | None = None
    stderr_size_bytes: int = Field(default=0, ge=0)
    request_count: int = Field(default=1, ge=1)


class ExternalDiscoveryCandidate(DiscoveryContract):
    candidate_id: str = Field(pattern=r"^candidate:sha256:[0-9a-f]{64}$")
    source: ExternalDiscoverySource
    upstream_identity: str = Field(min_length=1, max_length=500)
    upstream_version: str | None = Field(default=None, max_length=256)
    locator: str = Field(min_length=1, max_length=2_048)
    publisher: str | None = Field(default=None, max_length=500)
    discovered_at: AwareDatetime
    query: str = Field(min_length=1, max_length=500)
    raw_response_ref: str | None = Field(default=None, max_length=2_048)
    raw_response_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    upstream_status: str = Field(default="active", min_length=1, max_length=128)
    trust_tier: Literal["untrusted"] = "untrusted"
    inspection_status: Literal["not_inspected"] = "not_inspected"
    inspection_findings: tuple[str, ...] = ()
    requested_capabilities: frozenset[str] = frozenset()
    license_evidence: tuple[str, ...] = ()
    promoted_ref: None = None

    @field_validator("locator", "raw_response_ref")
    @classmethod
    def locator_cannot_embed_credentials(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lowered = value.casefold()
        if "://" in value and "@" in value.split("://", maxsplit=1)[1].split("/", maxsplit=1)[0]:
            raise ValueError("external candidate references cannot embed URI credentials")
        if any(
            marker in lowered
            for marker in ("api_key=", "apikey=", "access_token=", "auth_token=", "secret=")
        ):
            raise ValueError("external candidate references cannot embed credential query values")
        return value


class ExternalDiscoveryBatch(DiscoveryContract):
    source: ExternalDiscoverySource
    candidates: tuple[ExternalDiscoveryCandidate, ...]
    evidence: tuple[ExternalDiscoveryEvidence, ...]


class MCPRegistryDiscoveryPort(Protocol):
    async def search(
        self,
        query: str,
        *,
        limit: int,
    ) -> ExternalDiscoveryBatch: ...


class SkillDiscoveryPort(Protocol):
    async def search(
        self,
        query: str,
        *,
        limit: int,
        owner: str | None = None,
    ) -> ExternalDiscoveryBatch: ...


class ExternalCandidateRepository(Protocol):
    async def record(
        self,
        batch: ExternalDiscoveryBatch,
    ) -> ExternalDiscoveryBatch: ...


class ExternalCapabilityDiscoveryDisabled(RuntimeError):
    pass


class ExternalCapabilityDiscoveryService:
    """Gate discovery and persist candidate-only records through application ports."""

    def __init__(
        self,
        *,
        enabled: bool,
        mcp_registry: MCPRegistryDiscoveryPort,
        skills: SkillDiscoveryPort,
        candidates: ExternalCandidateRepository,
        max_results: int,
    ) -> None:
        if max_results < 1:
            raise ValueError("external discovery max_results must be positive")
        self._enabled = enabled
        self._mcp_registry = mcp_registry
        self._skills = skills
        self._candidates = candidates
        self._max_results = max_results

    async def discover_mcp_servers(
        self,
        query: str,
        *,
        limit: int = 10,
    ) -> ExternalDiscoveryBatch:
        self._require_enabled()
        batch = await self._mcp_registry.search(
            _validated_query(query),
            limit=self._bounded_limit(limit),
        )
        return await self._candidates.record(batch)

    async def discover_agent_skills(
        self,
        query: str,
        *,
        limit: int = 10,
        owner: str | None = None,
    ) -> ExternalDiscoveryBatch:
        self._require_enabled()
        batch = await self._skills.search(
            _validated_query(query),
            limit=self._bounded_limit(limit),
            owner=owner,
        )
        return await self._candidates.record(batch)

    def _require_enabled(self) -> None:
        if not self._enabled:
            raise ExternalCapabilityDiscoveryDisabled("external capability discovery is disabled")

    def _bounded_limit(self, requested: int) -> int:
        if requested < 1:
            raise ValueError("external discovery limit must be positive")
        return min(requested, self._max_results)


def _validated_query(query: str) -> str:
    normalized = query.strip()
    if not normalized:
        raise ValueError("external discovery query cannot be blank")
    if len(normalized) > 500:
        raise ValueError("external discovery query is too long")
    return normalized
