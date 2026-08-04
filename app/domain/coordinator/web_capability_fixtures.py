"""Deterministic reviewed web-capability catalog fixtures.

The fixtures freeze synthetic inspection snapshots for tests and bootstrap. They
perform no discovery, filesystem reads, package installation, or network access.
"""

from __future__ import annotations

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    AuthorityCeiling,
    BudgetCeiling,
    CatalogPayloadRef,
    ControlProfileDefinition,
    Definition,
    EvaluationProfileDefinition,
    ExactDefinitionRef,
    MCPNetworkRequirement,
    MCPServerDefinition,
    MCPToolDefinition,
    ModelPolicy,
    ObligationRealization,
    OutputContractRealization,
    RuntimeProfileDefinition,
    SecretRef,
    SkillCompatibility,
    SkillDefinition,
    SkillFileManifestEntry,
    SourceProvenance,
    StageGraphBlueprint,
    StageNode,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
    WorkflowWorkspaceContract,
    WorkspaceSlot,
    WorkspaceTemplateDefinition,
)

FIRECRAWL_TOOL_NAMES = (
    "firecrawl_search",
    "firecrawl_scrape",
    "firecrawl_interact",
)
TAVILY_TOOL_NAMES = (
    "tavily_search",
    "tavily_extract",
    "tavily_map",
    "tavily_crawl",
)
REQUIRED_TOOL_NAMES = FIRECRAWL_TOOL_NAMES + TAVILY_TOOL_NAMES

FIRECRAWL_SKILL_NAMES = (
    "firecrawl",
    "firecrawl-agent",
    "firecrawl-crawl",
    "firecrawl-download",
    "firecrawl-interact",
    "firecrawl-map",
    "firecrawl-monitor",
    "firecrawl-parse",
    "firecrawl-scrape",
    "firecrawl-search",
)
TAVILY_SKILL_NAMES = (
    "tavily-best-practices",
    "tavily-cli",
    "tavily-crawl",
    "tavily-dynamic-search",
    "tavily-extract",
    "tavily-map",
    "tavily-research",
    "tavily-search",
)

SEARCH_TOOL_LOGICAL_IDS = frozenset(
    {
        "mcp.firecrawl:firecrawl_search",
        "mcp.tavily:tavily_search",
    }
)

BROWSER_CAPABILITIES = frozenset(
    {
        "browser.process",
        "browser.navigation",
        "browser.screenshot",
        "network.web",
        "workspace.browser.read",
        "workspace.browser.write",
        "artifact.browser-evidence.write",
    }
)
WEB_RESEARCH_CAPABILITIES = BROWSER_CAPABILITIES | frozenset(
    {
        "web.search.firecrawl",
        "web.search.tavily",
        "artifact.research-report.write",
        "operation.execute.agent",
    }
)

WEB_RESEARCH_BUDGETS = BudgetCeiling(
    dimensions={
        "tokens.input": 120_000,
        "tokens.output": 24_000,
        "tool.calls.total": 30,
        "operation.attempts": 12,
        "time.elapsed_ms": 900_000,
        "concurrency.slots": 2,
    }
)


def web_capability_definitions() -> tuple[Definition, ...]:
    """Return revision-one definitions in dependency-safe publication order."""
    firecrawl_server = _mcp_server(
        logical_id="mcp.firecrawl",
        title="Firecrawl MCP Server",
        description=(
            "Reviewed Firecrawl recipe for current web search, extraction, and "
            "interactive retrieval."
        ),
        transport="stdio",
        endpoint=None,
        launch_template=("catalog-runtime", "mcp.firecrawl"),
        credential_name="FIRECRAWL_API_KEY",
        network_host="api.firecrawl.dev",
        allowed_tools=FIRECRAWL_TOOL_NAMES,
        upstream_identity="firecrawl",
        upstream_version="fixture-inspection-2026-07-25",
        locator="codex:mcp/firecrawl",
    )
    tavily_server = _mcp_server(
        logical_id="mcp.tavily",
        title="Tavily MCP Server",
        description=(
            "Reviewed Tavily remote recipe for current search, extraction, mapping, "
            "and crawling."
        ),
        transport="streamable_http",
        endpoint="https://mcp.tavily.com/mcp/",
        launch_template=None,
        credential_name="TAVILY_API_KEY",
        network_host="mcp.tavily.com",
        allowed_tools=TAVILY_TOOL_NAMES,
        upstream_identity="tavily-remote-mcp",
        upstream_version="fixture-inspection-2026-07-25",
        locator="https://mcp.tavily.com/mcp/",
    )
    firecrawl_ref = _ref(firecrawl_server)
    tavily_ref = _ref(tavily_server)

    tools = (
        _mcp_tool(
            server_ref=firecrawl_ref,
            provider="Firecrawl",
            tool_name="firecrawl_search",
            purpose="Search the current public web and return ranked source results.",
            input_properties={"query": {"type": "string"}, "limit": {"type": "integer"}},
        ),
        _mcp_tool(
            server_ref=firecrawl_ref,
            provider="Firecrawl",
            tool_name="firecrawl_scrape",
            purpose="Extract reviewed page content from one public URL.",
            input_properties={"url": {"type": "string", "format": "uri"}},
        ),
        _mcp_tool(
            server_ref=firecrawl_ref,
            provider="Firecrawl",
            tool_name="firecrawl_interact",
            purpose="Perform bounded interactive navigation on one public web page.",
            input_properties={
                "url": {"type": "string", "format": "uri"},
                "actions": {"type": "array", "items": {"type": "object"}},
            },
            side_effect_class="consequential",
        ),
        _mcp_tool(
            server_ref=tavily_ref,
            provider="Tavily",
            tool_name="tavily_search",
            purpose="Search current public information and return source evidence.",
            input_properties={"query": {"type": "string"}, "max_results": {"type": "integer"}},
        ),
        _mcp_tool(
            server_ref=tavily_ref,
            provider="Tavily",
            tool_name="tavily_extract",
            purpose="Extract readable evidence from supplied public URLs.",
            input_properties={
                "urls": {"type": "array", "items": {"type": "string", "format": "uri"}}
            },
        ),
        _mcp_tool(
            server_ref=tavily_ref,
            provider="Tavily",
            tool_name="tavily_map",
            purpose="Discover the bounded public URL structure below one site root.",
            input_properties={"url": {"type": "string", "format": "uri"}},
        ),
        _mcp_tool(
            server_ref=tavily_ref,
            provider="Tavily",
            tool_name="tavily_crawl",
            purpose="Crawl a bounded set of pages under one public site root.",
            input_properties={
                "url": {"type": "string", "format": "uri"},
                "max_depth": {"type": "integer"},
            },
        ),
    )

    firecrawl_skills = tuple(
        _skill(
            name=name,
            provider="Firecrawl",
            summary=_firecrawl_skill_summary(name),
            required_capabilities=_firecrawl_skill_capabilities(name),
        )
        for name in FIRECRAWL_SKILL_NAMES
    )
    tavily_skills = tuple(
        _skill(
            name=name,
            provider="Tavily",
            summary=_tavily_skill_summary(name),
            required_capabilities=_tavily_skill_capabilities(name),
        )
        for name in TAVILY_SKILL_NAMES
    )
    agent_browser = _agent_browser_skill()
    skills = firecrawl_skills + tavily_skills + (agent_browser,)

    tool_refs_by_logical_id = {tool.logical_id: _ref(tool) for tool in tools}
    skill_refs_by_logical_id = {skill.logical_id: _ref(skill) for skill in skills}
    agent_profile = AgentProfileDefinition(
        logical_id="agent-profile.web-research-browser-verification",
        title="Two-provider web research and browser verification agent",
        description=(
            "Uses only the exact Firecrawl and Tavily search tools for retrieval, then "
            "the promoted agent-browser procedure for independent visual verification."
        ),
        skill_refs=frozenset(
            {
                skill_refs_by_logical_id["skill.firecrawl-search"],
                skill_refs_by_logical_id["skill.tavily-search"],
                skill_refs_by_logical_id["skill.agent-browser"],
            }
        ),
        mcp_server_refs=frozenset({firecrawl_ref, tavily_ref}),
        tool_refs=frozenset(
            tool_refs_by_logical_id[logical_id] for logical_id in SEARCH_TOOL_LOGICAL_IDS
        ),
        model_policy=ModelPolicy(
            provider="openai",
            model="gpt-5.4-nano",
            settings={"reasoning_effort": "medium", "max_turns": 12},
        ),
        guardrail_refs=frozenset(
            {
                "guardrail:untrusted-web-content:v1",
                "guardrail:public-network-only:v1",
            }
        ),
        output_schema_ref="schema:web-research-browser-verification-result:v1",
        maximum_capability_request=AuthorityCeiling(
            capabilities=WEB_RESEARCH_CAPABILITIES,
            budgets=WEB_RESEARCH_BUDGETS,
            max_concurrency=2,
        ),
    )

    blueprint = _web_research_blueprint()
    blueprint_ref = _ref(blueprint)
    control = ControlProfileDefinition(
        logical_id="web-research-browser-verification-control-v1",
        title="Web research and browser verification control profile",
        description="Bounds two-provider search, browser verification, artifacts, and usage.",
        blueprint_ref=blueprint_ref,
        authority_ceiling=AuthorityCeiling(
            capabilities=WEB_RESEARCH_CAPABILITIES,
            budgets=WEB_RESEARCH_BUDGETS,
            max_concurrency=2,
        ),
        overlayable_fields=frozenset({"budgets", "max_concurrency"}),
        strengthen_only_fields=frozenset({"budgets", "max_concurrency"}),
    )
    runtime = RuntimeProfileDefinition(
        logical_id="web-research-browser-verification-runtime-v1",
        title="Pinned browser-capable web research runtime",
        description=(
            "Runs governed operation execution with exact MCP search allowlists, a pinned "
            "browser process, controlled network access, and artifact capture."
        ),
        binding="temporal-stagegraph+operation-execution+browser-runtime",
        required_capabilities=WEB_RESEARCH_CAPABILITIES,
    )
    workspace = _web_research_workspace()
    evaluation = EvaluationProfileDefinition(
        logical_id="web-research-browser-verification-evaluation-v1",
        title="Web research and browser verification evaluation",
        description=(
            "Requires both provider evidence, cited synthesis, and independent browser "
            "screenshot verification."
        ),
        gate_contract_refs=frozenset(
            {
                "gate:firecrawl-provider-evidence:v1",
                "gate:tavily-provider-evidence:v1",
                "gate:cited-source-synthesis:v1",
                "gate:browser-verification-evidence:v1",
                "gate:exact-search-tool-allowlist:v1",
            }
        ),
        required_capabilities=frozenset(
            {
                "artifact.browser-evidence.write",
                "artifact.research-report.write",
            }
        ),
    )
    workflow = WorkflowTypeDefinition(
        logical_id="web-research-browser-verification",
        title="Web Research with Browser Verification",
        description=(
            "Researches current public information with Firecrawl and Tavily search, "
            "then verifies material claims through a governed browser."
        ),
        purpose=(
            "Produce a cited two-provider research result with independent browser "
            "verification evidence."
        ),
        non_goals=frozenset(
            {
                "authenticated browsing",
                "form submission",
                "private data access",
                "unbounded crawling",
            }
        ),
        input_admission_contract="admission:web-research-public-goal:v1",
        invariants=frozenset(
            {
                "invariant:two-provider-identity-preserved:v1",
                "invariant:untrusted-web-content-is-not-instruction:v1",
                "invariant:search-tools-only:v1",
                "invariant:browser-authority-explicit:v1",
            }
        ),
        obligations=frozenset(
            {
                "obligation:firecrawl-search-evidence:v1",
                "obligation:tavily-search-evidence:v1",
                "obligation:browser-verification:v1",
                "obligation:cited-synthesis:v1",
            }
        ),
        output_contracts=frozenset(
            {"schema:web-research-browser-verification-result:v1"}
        ),
        allowed_blueprints=frozenset({blueprint_ref}),
        allowed_control_profiles=frozenset({_ref(control)}),
        allowed_runtime_profiles=frozenset({_ref(runtime)}),
        allowed_workspace_templates=frozenset({_ref(workspace)}),
        allowed_evaluation_profiles=frozenset({_ref(evaluation)}),
        authority_ceiling=control.authority_ceiling,
        workspace_contract=WorkflowWorkspaceContract(slots=workspace.slots),
    )
    implementation = WorkflowImplementationBindingDefinition(
        logical_id="web-research-browser-verification.implementation",
        title="Default staged web research and browser verification implementation",
        description=(
            "Runs exact Firecrawl and Tavily search branches, synthesizes cited evidence, "
            "performs browser verification, and promotes a typed result."
        ),
        workflow_type_ref=_ref(workflow),
        blueprint_ref=blueprint_ref,
        control_profile_ref=_ref(control),
        runtime_profile_ref=_ref(runtime),
        workspace_template_ref=_ref(workspace),
        evaluation_profile_ref=_ref(evaluation),
        obligation_realizations=(
            ObligationRealization(
                obligation_ref="obligation:firecrawl-search-evidence:v1",
                realization_kind="stage",
                realization_ref="search_firecrawl",
            ),
            ObligationRealization(
                obligation_ref="obligation:tavily-search-evidence:v1",
                realization_kind="stage",
                realization_ref="search_tavily",
            ),
            ObligationRealization(
                obligation_ref="obligation:browser-verification:v1",
                realization_kind="stage",
                realization_ref="browser_verify",
            ),
            ObligationRealization(
                obligation_ref="obligation:cited-synthesis:v1",
                realization_kind="stage",
                realization_ref="synthesize_citations",
            ),
        ),
        output_contract_realizations=(
            OutputContractRealization(
                output_contract_ref="schema:web-research-browser-verification-result:v1",
                output_slot="verified_research_result",
            ),
        ),
        conformance_evidence_refs=frozenset(
            {
                "test:test_web_capability_catalog_seed:v1",
                "evaluation:web-research-browser-verification:v1",
            }
        ),
    )

    return (
        firecrawl_server,
        tavily_server,
        *tools,
        *skills,
        agent_profile,
        blueprint,
        control,
        runtime,
        workspace,
        evaluation,
        workflow,
        implementation,
    )


def _mcp_server(
    *,
    logical_id: str,
    title: str,
    description: str,
    transport: str,
    endpoint: str | None,
    launch_template: tuple[str, ...] | None,
    credential_name: str,
    network_host: str,
    allowed_tools: tuple[str, ...],
    upstream_identity: str,
    upstream_version: str,
    locator: str,
) -> MCPServerDefinition:
    snapshot_payload = {
        "fixture_format": "mcp-tools-list/1",
        "server": logical_id,
        "tools": list(allowed_tools),
    }
    snapshot_digest = sha256_digest(snapshot_payload)
    return MCPServerDefinition(
        logical_id=logical_id,
        title=title,
        description=description,
        transport=transport,
        endpoint=endpoint,
        launch_template=launch_template,
        credential_refs=(
            SecretRef(provider="environment", key=credential_name),
        ),
        allowed_tools=frozenset(allowed_tools),
        approval_policy={
            tool_name: ("always" if tool_name.endswith("_interact") else "never")
            for tool_name in allowed_tools
        },
        network_requirements=(
            MCPNetworkRequirement(host=network_host, port=443, protocol="https"),
        ),
        schema_snapshot_ref=CatalogPayloadRef(
            uri=f"fixture://catalog/mcp/{logical_id}/tools-list-v1.json",
            digest=snapshot_digest,
            media_type="application/json",
            size_bytes=len(str(snapshot_payload).encode()),
        ),
        schema_digest=snapshot_digest,
        source_provenance=SourceProvenance(
            source="local",
            locator=locator,
            upstream_identity=upstream_identity,
            upstream_version=upstream_version,
        ),
        review_status="approved",
    )


def _mcp_tool(
    *,
    server_ref: ExactDefinitionRef,
    provider: str,
    tool_name: str,
    purpose: str,
    input_properties: dict[str, object],
    side_effect_class: str = "read_only",
) -> MCPToolDefinition:
    input_schema: dict[str, object] = {
        "type": "object",
        "properties": input_properties,
        "additionalProperties": False,
    }
    output_schema: dict[str, object] = {
        "type": "object",
        "properties": {
            "results": {"type": "array", "items": {"type": "object"}},
            "provider": {"const": provider.lower()},
        },
        "required": ["results", "provider"],
        "additionalProperties": False,
    }
    annotations: dict[str, object] = {
        "fixtureSnapshot": True,
        "provider": provider,
        "readOnlyHint": side_effect_class == "read_only",
    }
    schema_payload = {
        "tool_name": tool_name,
        "input_schema": input_schema,
        "output_schema": output_schema,
        "annotations": annotations,
    }
    return MCPToolDefinition(
        logical_id=f"{server_ref.logical_id}:{tool_name}",
        title=f"{provider} {tool_name}",
        description=purpose,
        server_ref=server_ref,
        tool_name=tool_name,
        input_schema=input_schema,
        output_schema=output_schema,
        annotations=annotations,
        schema_digest=sha256_digest(schema_payload),
        side_effect_class=side_effect_class,
    )


def _skill(
    *,
    name: str,
    provider: str,
    summary: str,
    required_capabilities: frozenset[str],
) -> SkillDefinition:
    skill_file = SkillFileManifestEntry(
        path="SKILL.md",
        digest=sha256_digest(
            {
                "fixture_format": "agent-skill/1",
                "provider": provider,
                "name": name,
                "summary": summary,
            }
        ),
        size_bytes=len(summary.encode()),
    )
    manifest = (skill_file,)
    bundle_digest = sha256_digest(
        {
            "fixture_format": "agent-skill-bundle/1",
            "provider": provider,
            "name": name,
            "manifest": manifest,
        }
    )
    provider_slug = provider.lower()
    return SkillDefinition(
        logical_id=f"skill.{name}",
        title=f"{provider} {name} Agent Skill",
        description=summary,
        skill_name=name,
        frontmatter={
            "name": name,
            "description": summary,
            "fixture": True,
        },
        body_summary=summary,
        bundle_ref=CatalogPayloadRef(
            uri=f"fixture://catalog/skills/{provider_slug}/{name}/bundle-v1.tar",
            digest=bundle_digest,
            media_type="application/x-tar",
            size_bytes=skill_file.size_bytes,
        ),
        manifest_digest=sha256_digest(manifest),
        file_manifest=manifest,
        required_capabilities=required_capabilities,
        compatibility=SkillCompatibility(
            runtimes=frozenset({"python-3.12", "governed-agent-runtime"}),
            network_capabilities=frozenset({"network.web"}),
            workspace_capabilities=frozenset({"workspace.browser.read"}),
        ),
        source_provenance=SourceProvenance(
            source="local",
            locator=f".agents/skills/{name}/SKILL.md",
            upstream_identity=name,
            upstream_version="fixture-local-reviewed-2026-07-25",
        ),
        review_status="approved",
    )


def _agent_browser_skill() -> SkillDefinition:
    name = "agent-browser"
    summary = (
        "Operate a pinned browser executable for bounded navigation, clicking, "
        "screenshots, and visual verification."
    )
    skill_file = SkillFileManifestEntry(
        path="SKILL.md",
        digest=sha256_digest(
            {
                "fixture_format": "agent-skill/1",
                "repository": "vercel-labs/agent-browser",
                "path": "skills/agent-browser/SKILL.md",
                "summary": summary,
            }
        ),
        size_bytes=len(summary.encode()),
    )
    manifest = (skill_file,)
    bundle_digest = sha256_digest(
        {
            "fixture_format": "agent-skill-bundle/1",
            "repository": "vercel-labs/agent-browser",
            "commit": "fixture-pinned-commit-v1",
            "manifest": manifest,
        }
    )
    return SkillDefinition(
        logical_id="skill.agent-browser",
        title="Promoted agent-browser Agent Skill",
        description=summary,
        skill_name=name,
        frontmatter={"name": name, "description": summary, "fixture": True},
        body_summary=summary,
        bundle_ref=CatalogPayloadRef(
            uri="fixture://catalog/skills/vercel-labs/agent-browser/bundle-v1.tar",
            digest=bundle_digest,
            media_type="application/x-tar",
            size_bytes=skill_file.size_bytes,
        ),
        manifest_digest=sha256_digest(manifest),
        file_manifest=manifest,
        required_capabilities=BROWSER_CAPABILITIES,
        compatibility=SkillCompatibility(
            runtimes=frozenset({"browser-runtime"}),
            executables=frozenset({"agent-browser"}),
            network_capabilities=frozenset({"network.web"}),
            workspace_capabilities=frozenset(
                {
                    "workspace.browser.read",
                    "workspace.browser.write",
                    "artifact.browser-evidence.write",
                }
            ),
        ),
        source_provenance=SourceProvenance(
            source="git",
            locator=(
                "https://github.com/vercel-labs/agent-browser/"
                "tree/fixture-pinned-commit-v1/skills/agent-browser/SKILL.md"
            ),
            upstream_identity="vercel-labs/agent-browser",
            upstream_version="fixture-pinned-commit-v1",
            commit_digest="fixture-pinned-commit-v1",
            license="Apache-2.0",
        ),
        review_status="approved",
    )


def _web_research_blueprint() -> StageGraphBlueprint:
    return StageGraphBlueprint(
        logical_id="web-research-browser-verification-v1",
        title="Web Research and Browser Verification StageGraph",
        description=(
            "Searches with two exact providers, synthesizes citations, independently "
            "verifies claims in a browser, and promotes a typed result."
        ),
        stages=(
            StageNode(
                stage_id="admit_public_goal",
                output_slots=frozenset({"admission_decision"}),
            ),
            StageNode(
                stage_id="search_firecrawl",
                depends_on=frozenset({"admit_public_goal"}),
                reservation={"tool.calls.total": 5, "operation.attempts": 1},
                obligation_refs=frozenset(
                    {"obligation:firecrawl-search-evidence:v1"}
                ),
                output_slots=frozenset({"firecrawl_evidence"}),
            ),
            StageNode(
                stage_id="search_tavily",
                depends_on=frozenset({"admit_public_goal"}),
                reservation={"tool.calls.total": 5, "operation.attempts": 1},
                obligation_refs=frozenset({"obligation:tavily-search-evidence:v1"}),
                output_slots=frozenset({"tavily_evidence"}),
            ),
            StageNode(
                stage_id="synthesize_citations",
                depends_on=frozenset({"search_firecrawl", "search_tavily"}),
                reservation={"operation.attempts": 1},
                obligation_refs=frozenset({"obligation:cited-synthesis:v1"}),
                output_slots=frozenset({"cited_synthesis"}),
            ),
            StageNode(
                stage_id="browser_verify",
                depends_on=frozenset({"synthesize_citations"}),
                reservation={"tool.calls.total": 10, "operation.attempts": 1},
                obligation_refs=frozenset({"obligation:browser-verification:v1"}),
                output_slots=frozenset({"browser_verification_evidence"}),
            ),
            StageNode(
                stage_id="promote_verified_result",
                depends_on=frozenset({"browser_verify"}),
                reservation={"operation.attempts": 1},
                output_slots=frozenset({"verified_research_result"}),
            ),
        ),
        declared_output_slots=frozenset(
            {
                "admission_decision",
                "firecrawl_evidence",
                "tavily_evidence",
                "cited_synthesis",
                "browser_verification_evidence",
                "verified_research_result",
            }
        ),
        max_parallel_stages=2,
        workflow_evaluation_contract_ref=(
            "evaluation:web-research-browser-verification:v1"
        ),
    )


def _web_research_workspace() -> WorkspaceTemplateDefinition:
    slots = (
        WorkspaceSlot(
            name="research_input",
            path="/inputs/research",
            access="read_only",
            purpose="admitted public research goal and constraints",
        ),
        WorkspaceSlot(
            name="browser_workspace",
            path="/workspace/browser",
            access="exclusive_write",
            purpose="browser process state and downloaded public page material",
        ),
        WorkspaceSlot(
            name="browser_evidence",
            path="/artifacts/browser-evidence",
            access="exclusive_write",
            purpose="screenshots and visual verification evidence",
        ),
        WorkspaceSlot(
            name="research_output",
            path="/outputs/web-research",
            access="exclusive_write",
            purpose="cited synthesis and typed verified research result",
        ),
    )
    return WorkspaceTemplateDefinition(
        logical_id="web-research-browser-verification-workspace-v1",
        title="Browser-capable web research workspace",
        description=(
            "Separates admitted input, browser state, screenshot evidence, and final "
            "research artifacts."
        ),
        slots=slots,
        required_capabilities=frozenset(
            {
                "workspace.browser.read",
                "workspace.browser.write",
                "artifact.browser-evidence.write",
                "artifact.research-report.write",
            }
        ),
    )


def _firecrawl_skill_summary(name: str) -> str:
    summaries = {
        "firecrawl": "Choose and apply governed Firecrawl web-data procedures.",
        "firecrawl-agent": "Extract structured web data through a bounded Firecrawl agent.",
        "firecrawl-crawl": "Crawl a bounded public site section with Firecrawl.",
        "firecrawl-download": "Save an admitted public site as immutable local artifacts.",
        "firecrawl-interact": "Perform bounded interactions on a public web page.",
        "firecrawl-map": "Discover public URLs below an admitted site root.",
        "firecrawl-monitor": "Describe governed monitoring of public page changes.",
        "firecrawl-parse": "Parse an admitted local document into clean text.",
        "firecrawl-scrape": "Extract clean content from one admitted public URL.",
        "firecrawl-search": "Search current public information with Firecrawl evidence.",
    }
    return summaries[name]


def _tavily_skill_summary(name: str) -> str:
    summaries = {
        "tavily-best-practices": "Apply production-oriented Tavily integration guidance.",
        "tavily-cli": "Use governed Tavily CLI search and extraction procedures.",
        "tavily-crawl": "Crawl a bounded public site section with Tavily.",
        "tavily-dynamic-search": "Search programmatically with isolated Tavily context.",
        "tavily-extract": "Extract clean evidence from admitted public URLs.",
        "tavily-map": "Discover public URLs below an admitted site root.",
        "tavily-research": "Conduct structured current research with Tavily citations.",
        "tavily-search": "Search current public information with Tavily evidence.",
    }
    return summaries[name]


def _firecrawl_skill_capabilities(name: str) -> frozenset[str]:
    capability = {
        "firecrawl-search": "web.search.firecrawl",
        "firecrawl-scrape": "web.extract.firecrawl",
        "firecrawl-map": "web.map.firecrawl",
        "firecrawl-crawl": "web.crawl.firecrawl",
        "firecrawl-interact": "web.interact.firecrawl",
        "firecrawl-download": "artifact.web-content.write",
        "firecrawl-parse": "document.parse",
        "firecrawl-monitor": "web.monitor.firecrawl",
        "firecrawl-agent": "web.extract.structured.firecrawl",
        "firecrawl": "web.firecrawl",
    }[name]
    return frozenset({capability, "network.web"})


def _tavily_skill_capabilities(name: str) -> frozenset[str]:
    capability = {
        "tavily-search": "web.search.tavily",
        "tavily-extract": "web.extract.tavily",
        "tavily-map": "web.map.tavily",
        "tavily-crawl": "web.crawl.tavily",
        "tavily-research": "web.research.tavily",
        "tavily-dynamic-search": "web.search.tavily",
        "tavily-cli": "runtime.tavily-cli",
        "tavily-best-practices": "procedure.tavily",
    }[name]
    return frozenset({capability, "network.web"})


def _ref(definition: Definition) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=definition.kind,
        logical_id=definition.logical_id,
        revision=1,
        digest=sha256_digest(definition),
    )
