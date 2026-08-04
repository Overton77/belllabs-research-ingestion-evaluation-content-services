from __future__ import annotations

import ipaddress
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"
WebResearchRecordKind = Literal[
    "admission",
    "firecrawl_evidence",
    "tavily_evidence",
    "cited_synthesis",
    "browser_verification",
    "verified_result",
]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class WebResearchGoal(Contract):
    question: str = Field(min_length=3, max_length=4_000)
    include_domains: tuple[str, ...] = Field(default=(), max_length=20)
    exclude_domains: tuple[str, ...] = Field(default=(), max_length=20)

    @model_validator(mode="after")
    def validate_domain_filters(self) -> WebResearchGoal:
        if self.include_domains and self.exclude_domains:
            raise ValueError("include_domains and exclude_domains are mutually exclusive")
        return self

    @field_validator("include_domains", "exclude_domains")
    @classmethod
    def normalize_domains(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized: list[str] = []
        for value in values:
            domain = value.strip().lower().rstrip(".")
            if (
                not domain
                or "/" in domain
                or ":" in domain
                or domain == "localhost"
                or domain.endswith(".local")
            ):
                raise ValueError("web research domain filters require public hostnames")
            normalized.append(domain)
        if len(normalized) != len(set(normalized)):
            raise ValueError("web research domain filters must be unique")
        return tuple(normalized)


class ReviewedRuntimeArtifactBinding(Contract):
    package_name: str = Field(min_length=1)
    package_version: str = Field(min_length=1)
    module_locator: str = Field(pattern=r"^workspace://\.tools/")
    module_digest: str = Field(pattern=DIGEST_PATTERN)
    tools_snapshot_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    commit_digest: str | None = Field(default=None, min_length=7)


class GovernedMCPServerBinding(Contract):
    server_ref: ExactDefinitionRef
    tool_ref: ExactDefinitionRef
    allowed_tools: frozenset[str] = Field(min_length=1)
    server_schema_digest: str = Field(pattern=DIGEST_PATTERN)


class ReviewedSkillMountBinding(Contract):
    skill_ref: ExactDefinitionRef
    bundle_ref: str = Field(min_length=1)
    bundle_digest: str = Field(pattern=DIGEST_PATTERN)
    manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    mount_path: str = Field(pattern=r"^/skills/[a-z0-9-]+/SKILL\.md$")


class BrowserExecutionGrantBinding(Contract):
    agent_profile_ref: ExactDefinitionRef
    runtime_profile_ref: ExactDefinitionRef
    workspace_template_ref: ExactDefinitionRef
    executable: str = Field(min_length=1)
    capabilities: frozenset[str] = Field(min_length=1)
    network_hosts: frozenset[str] = Field(min_length=1)
    workspace_paths: frozenset[str] = Field(min_length=1)
    grant_digest: str = Field(pattern=DIGEST_PATTERN)


class ExactOperationExecutionBinding(Contract):
    binding_id: str = Field(min_length=1)
    binding_digest: str = Field(pattern=DIGEST_PATTERN)


class OperationExecutionBindingAuthority(Contract):
    bindings: dict[
        Literal["search_firecrawl", "search_tavily", "browser_verify"],
        ExactOperationExecutionBinding,
    ]
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def all_effectful_stages_are_bound(self) -> OperationExecutionBindingAuthority:
        if set(self.bindings) != {
            "search_firecrawl",
            "search_tavily",
            "browser_verify",
        }:
            raise ValueError(
                "web research requires one OperationExecutionBinding per effectful stage"
            )
        if len({item.binding_id for item in self.bindings.values()}) != 3:
            raise ValueError("effectful web-research stages require distinct OEB identities")
        return self


class WebResearchStageInput(Contract):
    goal: WebResearchGoal
    firecrawl_tool_ref: ExactDefinitionRef
    tavily_tool_ref: ExactDefinitionRef
    browser_skill_ref: ExactDefinitionRef
    firecrawl_runtime: ReviewedRuntimeArtifactBinding
    tavily_runtime: ReviewedRuntimeArtifactBinding
    browser_runtime: ReviewedRuntimeArtifactBinding
    mcp_servers: tuple[GovernedMCPServerBinding, ...] = Field(min_length=2, max_length=2)
    skills: tuple[ReviewedSkillMountBinding, ...] = Field(min_length=3, max_length=3)
    browser_grant: BrowserExecutionGrantBinding
    operation_execution: OperationExecutionBindingAuthority
    maximum_results: int = Field(default=5, ge=1, le=10)
    browser_verification_limit: int = Field(default=3, ge=1, le=5)

    @model_validator(mode="after")
    def exact_capabilities_match_scenario(self) -> WebResearchStageInput:
        expected = (
            (
                self.firecrawl_tool_ref,
                DefinitionKind.MCP_TOOL,
                "mcp.firecrawl:firecrawl_search",
            ),
            (
                self.tavily_tool_ref,
                DefinitionKind.MCP_TOOL,
                "mcp.tavily:tavily_search",
            ),
            (
                self.browser_skill_ref,
                DefinitionKind.SKILL,
                "skill.agent-browser",
            ),
        )
        for ref, kind, logical_id in expected:
            if ref.kind != kind or ref.logical_id != logical_id:
                raise ValueError("web-research binding contains an unexpected governed capability")
        expected_runtimes = (
            (
                self.firecrawl_runtime,
                "firecrawl-mcp",
                "3.22.4",
                "sha256:69e305ec3cf14ddbfe62a7c509e218a9ec4b44c82604bffa023159130769498b",
                "sha256:b00747ddea6305fc08efcdd9fcaddcd69f62f0c3a59e2901d045475600c53bf2",
            ),
            (
                self.tavily_runtime,
                "tavily-mcp",
                "0.2.21",
                "sha256:60d2f3d0553f4879225990fd42e43265244ef5ac6d02799f6bafa5aef2d2d05e",
                "sha256:65d256e03f0e82bb425b089cecf372f91f4c33b0c32fd2a94421475f2a9c922d",
            ),
            (
                self.browser_runtime,
                "agent-browser",
                "0.33.0",
                "sha256:8e382f4a5ba22f45e1e0339abfe5a55ed95a19540b16a69ee3faf31c8dc8216a",
                None,
            ),
        )
        for runtime, name, version, module_digest, tools_digest in expected_runtimes:
            if (
                runtime.package_name != name
                or runtime.package_version != version
                or runtime.module_digest != module_digest
                or runtime.tools_snapshot_digest != tools_digest
            ):
                raise ValueError("web-research binding contains an unattested runtime artifact")
        servers = {item.server_ref.logical_id: item for item in self.mcp_servers}
        expected_servers = {
            "mcp.firecrawl": (
                self.firecrawl_tool_ref,
                frozenset({"firecrawl_search"}),
            ),
            "mcp.tavily": (
                self.tavily_tool_ref,
                frozenset({"tavily_search"}),
            ),
        }
        if set(servers) != set(expected_servers):
            raise ValueError("web-research binding requires exactly two selected MCP servers")
        for logical_id, (tool_ref, allowed_tools) in expected_servers.items():
            server = servers[logical_id]
            if (
                server.server_ref.kind != DefinitionKind.MCP_SERVER
                or server.tool_ref != tool_ref
                or server.allowed_tools != allowed_tools
            ):
                raise ValueError(
                    "web-research MCP selection exceeds its exact search-tool allowlist"
                )
        skills = {item.skill_ref.logical_id: item for item in self.skills}
        expected_skills = {
            "skill.firecrawl-search": "/skills/firecrawl-search/SKILL.md",
            "skill.tavily-search": "/skills/tavily-search/SKILL.md",
            "skill.agent-browser": "/skills/agent-browser/SKILL.md",
        }
        if set(skills) != set(expected_skills):
            raise ValueError("web-research binding requires exactly three reviewed skills")
        if any(
            skills[logical_id].skill_ref.kind != DefinitionKind.SKILL
            or skills[logical_id].mount_path != mount_path
            for logical_id, mount_path in expected_skills.items()
        ):
            raise ValueError("web-research skill bundle or mount selection is invalid")
        grant = self.browser_grant
        required_capabilities = {
            "browser.process",
            "browser.navigation",
            "browser.screenshot",
            "network.web",
            "workspace.browser.read",
            "workspace.browser.write",
            "artifact.browser-evidence.write",
        }
        if (
            grant.agent_profile_ref.kind != DefinitionKind.AGENT_PROFILE
            or grant.runtime_profile_ref.kind != DefinitionKind.RUNTIME_PROFILE
            or grant.workspace_template_ref.kind != DefinitionKind.WORKSPACE_TEMPLATE
            or grant.executable != "agent-browser"
            or not required_capabilities <= grant.capabilities
            or not grant.network_hosts
            or "*" in grant.network_hosts
            or not {"/workspace/browser", "/artifacts/browser-evidence"} <= grant.workspace_paths
        ):
            raise ValueError("agent-browser lacks its required execution authority")
        return self


class PublicGoalAdmission(Contract):
    admitted: Literal[True] = True
    goal_digest: str = Field(pattern=DIGEST_PATTERN)
    allowed_authority: frozenset[str] = frozenset(
        {
            "public-web-search",
            "public-browser-read",
            "durable-evidence-write",
        }
    )


class GovernedSearchRequest(Contract):
    query: str = Field(min_length=3, max_length=4_000)
    limit: int = Field(ge=1, le=10)
    include_domains: tuple[str, ...] = Field(default=(), max_length=20)
    exclude_domains: tuple[str, ...] = Field(default=(), max_length=20)
    idempotency_key: str = Field(min_length=1, max_length=500)
    exact_tool_ref: ExactDefinitionRef
    runtime_artifact: ReviewedRuntimeArtifactBinding


class NormalizedSearchResult(Contract):
    title: str = Field(min_length=1, max_length=500)
    url: str = Field(min_length=1, max_length=2_000)
    snippet: str = Field(default="", max_length=4_000)
    published_at: str | None = Field(default=None, max_length=100)

    @field_validator("url")
    @classmethod
    def public_url_only(cls, value: str) -> str:
        return normalized_public_url(value)


class GovernedSearchResponse(Contract):
    results: tuple[NormalizedSearchResult, ...] = Field(max_length=10)
    provider_request_id: str | None = Field(default=None, max_length=300)


class ProviderEvidence(Contract):
    provider: Literal["firecrawl", "tavily"]
    query_digest: str = Field(pattern=DIGEST_PATTERN)
    results: tuple[NormalizedSearchResult, ...] = Field(max_length=10)
    provider_request_id: str | None = Field(default=None, max_length=300)


class CitedFinding(Contract):
    finding: str = Field(min_length=1, max_length=4_000)
    citation_urls: tuple[str, ...] = Field(min_length=1, max_length=5)
    provider_names: frozenset[Literal["firecrawl", "tavily"]]

    @field_validator("citation_urls")
    @classmethod
    def citations_are_public(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(normalized_public_url(value) for value in values)


class CitedSynthesis(Contract):
    findings: tuple[CitedFinding, ...] = Field(min_length=1, max_length=20)
    source_urls: tuple[str, ...] = Field(min_length=1, max_length=20)
    provider_evidence_refs: tuple[str, str]

    @field_validator("source_urls")
    @classmethod
    def sources_are_public(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(normalized_public_url(value) for value in values)


class GovernedBrowserVerificationRequest(Contract):
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    urls: tuple[str, ...] = Field(min_length=1, max_length=5)
    objective: str = Field(min_length=3, max_length=4_000)
    idempotency_key: str = Field(min_length=1, max_length=500)
    exact_skill_ref: ExactDefinitionRef
    runtime_artifact: ReviewedRuntimeArtifactBinding

    @field_validator("urls")
    @classmethod
    def urls_are_public(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(normalized_public_url(value) for value in values)


class BrowserPageVerification(Contract):
    requested_url: str = Field(min_length=1, max_length=2_000)
    final_url: str = Field(min_length=1, max_length=2_000)
    status_code: int = Field(ge=100, le=599)
    title: str = Field(default="", max_length=500)
    text_excerpt: str = Field(default="", max_length=4_000)
    screenshot_ref: str = Field(min_length=1, max_length=2_000)
    verified: bool

    @field_validator("requested_url", "final_url")
    @classmethod
    def urls_are_public(cls, value: str) -> str:
        return normalized_public_url(value)

    @field_validator("screenshot_ref")
    @classmethod
    def screenshot_is_durable(cls, value: str) -> str:
        if not value.startswith(("belllabs://", "s3://", "gs://", "az://")):
            raise ValueError("browser screenshots require durable non-host-local references")
        return value


class GovernedBrowserVerificationResponse(Contract):
    pages: tuple[BrowserPageVerification, ...] = Field(min_length=1, max_length=5)


class BrowserVerificationEvidence(Contract):
    pages: tuple[BrowserPageVerification, ...] = Field(min_length=1, max_length=5)
    synthesis_ref: str = Field(min_length=1)


class VerifiedWebResearchResult(Contract):
    findings: tuple[CitedFinding, ...] = Field(min_length=1, max_length=20)
    source_urls: tuple[str, ...] = Field(min_length=1, max_length=20)
    provider_evidence_refs: tuple[str, str]
    browser_verification_ref: str = Field(min_length=1)
    verified_urls: tuple[str, ...] = Field(min_length=1, max_length=5)

    @field_validator("source_urls", "verified_urls")
    @classmethod
    def urls_are_public(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(normalized_public_url(value) for value in values)


class WebResearchRecordEnvelope(Contract):
    record_kind: WebResearchRecordKind
    record_id: str = Field(min_length=1)
    intent_key: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    payload: dict[str, Any]
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    created_at: AwareDatetime

    @model_validator(mode="after")
    def content_is_immutable(self) -> WebResearchRecordEnvelope:
        if sha256_digest(self.payload) != self.content_digest:
            raise ValueError("web-research record payload digest mismatch")
        return self


def normalized_public_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("web research only permits public HTTP(S) URLs")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("web research URLs cannot contain credentials")
    hostname = parsed.hostname.lower().rstrip(".")
    if hostname == "localhost" or hostname.endswith(".local"):
        raise ValueError("web research cannot access local hosts")
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        pass
    else:
        if not address.is_global:
            raise ValueError("web research cannot access non-public IP addresses")
    netloc = hostname
    if parsed.port is not None:
        netloc = f"{hostname}:{parsed.port}"
    return urlunsplit((parsed.scheme.lower(), netloc, parsed.path or "/", "", ""))
