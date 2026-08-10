"""Build and promote reviewed web capabilities from immutable local evidence.

This module deliberately performs no discovery, package installation, or network
access. The MCP schemas and agent-browser skill are checked against reviewed
content digests, while workspace skills are content-addressed at promotion time.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    AliasBinding,
    AuthorityCeiling,
    CatalogPayloadRef,
    ControlProfileDefinition,
    Definition,
    DefinitionKind,
    ExactDefinitionRef,
    MCPNetworkRequirement,
    MCPServerDefinition,
    MCPToolDefinition,
    ModelPolicy,
    PublishedDefinition,
    PublishRequest,
    RetireRequest,
    SecretRef,
    SkillCompatibility,
    SkillDefinition,
    SkillFileManifestEntry,
    SourceProvenance,
    StageGraphBlueprint,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
)
from app.domain.coordinator.web_capability_fixtures import (
    BROWSER_CAPABILITIES,
    FIRECRAWL_SKILL_NAMES,
    REQUIRED_TOOL_NAMES,
    SEARCH_TOOL_LOGICAL_IDS,
    TAVILY_SKILL_NAMES,
    WEB_RESEARCH_BUDGETS,
    WEB_RESEARCH_CAPABILITIES,
)

TARGET_REVISION = 2
FIRECRAWL_COMMIT = "7232b6d1cdd80335107d53a33b80c902b515a334"
FIRECRAWL_VERSION = "3.22.4"
FIRECRAWL_SNAPSHOT = "firecrawl-mcp-tools-7232b6d1.json"
FIRECRAWL_SNAPSHOT_DIGEST = (
    "sha256:b00747ddea6305fc08efcdd9fcaddcd69f62f0c3a59e2901d045475600c53bf2"
)
TAVILY_COMMIT = "259bfd205de90d74a131e9d2b29cb69ebe11feb7"
TAVILY_VERSION = "0.2.21"
TAVILY_SNAPSHOT = "tavily-mcp-tools-259bfd20.json"
TAVILY_SNAPSHOT_DIGEST = "sha256:65d256e03f0e82bb425b089cecf372f91f4c33b0c32fd2a94421475f2a9c922d"
AGENT_BROWSER_COMMIT = "3cc7022271235694b5b5ce8aaea8bbfaa66e8cd5"
AGENT_BROWSER_VERSION = "0.33.0"
AGENT_BROWSER_SKILL_DIGEST = (
    "sha256:9f28168d0b4af6f6c8f5374e4620024e121f17c473480fdde979ec792fe95765"
)


class CapabilityPromotionService(Protocol):
    async def publish(self, request: PublishRequest) -> PublishedDefinition: ...

    async def retire(self, request: RetireRequest) -> PublishedDefinition: ...


class ReviewedCapabilityPromotionError(RuntimeError):
    """The reviewed evidence or catalog state cannot be promoted safely."""


@dataclass(frozen=True)
class ReviewedCapabilityBundle:
    """Dependency-ordered reviewed definitions.

    Existing synthetic fixture identities advance to revision two. Newly inspected
    MCP tool identities enter the catalog directly at revision one; publishing a
    dummy predecessor merely to manufacture revision two would be dishonest.
    """

    definitions: tuple[Definition, ...]

    @property
    def refs(self) -> tuple[ExactDefinitionRef, ...]:
        return tuple(_definition_ref(item) for item in self.definitions)


@dataclass(frozen=True)
class ReviewedCapabilityPromotionResult:
    published: tuple[ExactDefinitionRef, ...]
    reused: tuple[ExactDefinitionRef, ...]
    retired: tuple[ExactDefinitionRef, ...]
    retained: tuple[ExactDefinitionRef, ...]
    retention_reasons: dict[str, tuple[str, ...]]


@dataclass(frozen=True)
class ReviewedCapabilityPreflight:
    new: tuple[ExactDefinitionRef, ...]
    advance: tuple[ExactDefinitionRef, ...]
    reuse: tuple[ExactDefinitionRef, ...]


@dataclass(frozen=True)
class ScenarioDExecutionCorrectionBundle:
    """Revision-two executable workflow chain; immutable revision one is preserved."""

    definitions: tuple[
        StageGraphBlueprint,
        ControlProfileDefinition,
        WorkflowTypeDefinition,
        WorkflowImplementationBindingDefinition,
    ]

    @property
    def refs(self) -> tuple[ExactDefinitionRef, ...]:
        return tuple(_correction_ref(item) for item in self.definitions)


@dataclass(frozen=True)
class ScenarioDExecutionCorrectionResult:
    published: tuple[ExactDefinitionRef, ...]
    reused: tuple[ExactDefinitionRef, ...]


def build_scenario_d_execution_correction(
    *,
    catalog_records: tuple[PublishedDefinition, ...],
) -> ScenarioDExecutionCorrectionBundle:
    """Advance the invalid Scenario D workflow chain without changing revision one."""

    heads = _head_records(catalog_records)
    blueprint_record = _required_head(
        heads,
        DefinitionKind.BLUEPRINT,
        "web-research-browser-verification-v1",
    )
    control_record = _required_head(
        heads,
        DefinitionKind.CONTROL_PROFILE,
        "web-research-browser-verification-control-v1",
    )
    workflow_record = _required_head(
        heads,
        DefinitionKind.WORKFLOW_TYPE,
        "web-research-browser-verification",
    )
    implementation_record = _required_head(
        heads,
        DefinitionKind.WORKFLOW_IMPLEMENTATION,
        "web-research-browser-verification.implementation",
    )
    if not isinstance(blueprint_record.definition, StageGraphBlueprint):
        raise ReviewedCapabilityPromotionError("Scenario D blueprint head has wrong kind")
    if not isinstance(control_record.definition, ControlProfileDefinition):
        raise ReviewedCapabilityPromotionError("Scenario D control-profile head has wrong kind")
    if not isinstance(workflow_record.definition, WorkflowTypeDefinition):
        raise ReviewedCapabilityPromotionError("Scenario D workflow head has wrong kind")
    if not isinstance(
        implementation_record.definition,
        WorkflowImplementationBindingDefinition,
    ):
        raise ReviewedCapabilityPromotionError("Scenario D implementation head has wrong kind")

    records = (
        blueprint_record,
        control_record,
        workflow_record,
        implementation_record,
    )
    if any(
        record.ref.revision not in {1, 2} or record.retired_at is not None for record in records
    ):
        raise ReviewedCapabilityPromotionError(
            "Scenario D correction requires active revision-one heads or exact revision two"
        )

    corrected_blueprint = (
        blueprint_record.definition
        if blueprint_record.ref.revision == 2
        else blueprint_record.definition.model_copy(
            update={
                "stages": tuple(
                    stage.model_copy(update={"reservation": {"operation.attempts": 1}})
                    if stage.stage_id == "admit_public_goal"
                    else stage
                    for stage in blueprint_record.definition.stages
                )
            }
        )
    )
    blueprint_ref = _correction_ref(corrected_blueprint)
    corrected_control = (
        control_record.definition
        if control_record.ref.revision == 2
        else control_record.definition.model_copy(update={"blueprint_ref": blueprint_ref})
    )
    control_ref = _correction_ref(corrected_control)
    corrected_workflow = (
        workflow_record.definition
        if workflow_record.ref.revision == 2
        else workflow_record.definition.model_copy(
            update={
                "allowed_blueprints": frozenset({blueprint_ref}),
                "allowed_control_profiles": frozenset({control_ref}),
            }
        )
    )
    workflow_ref = _correction_ref(corrected_workflow)
    corrected_implementation = (
        implementation_record.definition
        if implementation_record.ref.revision == 2
        else implementation_record.definition.model_copy(
            update={
                "workflow_type_ref": workflow_ref,
                "blueprint_ref": blueprint_ref,
                "control_profile_ref": control_ref,
            }
        )
    )
    definitions = (
        corrected_blueprint,
        corrected_control,
        corrected_workflow,
        corrected_implementation,
    )
    _validate_scenario_d_correction(definitions)
    return ScenarioDExecutionCorrectionBundle(definitions=definitions)


async def publish_scenario_d_execution_correction(
    *,
    service: CapabilityPromotionService,
    bundle: ScenarioDExecutionCorrectionBundle,
    catalog_records: tuple[PublishedDefinition, ...],
    actor_id: str,
    changed_at: datetime,
) -> ScenarioDExecutionCorrectionResult:
    """Publish the executable revision-two chain in dependency order, resumably."""

    heads = _head_records(catalog_records)
    published: list[ExactDefinitionRef] = []
    reused: list[ExactDefinitionRef] = []
    for definition in bundle.definitions:
        identity = _identity(definition)
        target = _correction_ref(definition)
        head = heads.get(identity)
        if head is None:
            raise ReviewedCapabilityPromotionError(
                f"Scenario D revision-one prerequisite is missing for {identity}"
            )
        if head.ref.revision == 1:
            if head.retired_at is not None:
                raise ReviewedCapabilityPromotionError(
                    f"Scenario D revision-one prerequisite is retired for {identity}"
                )
            record = await service.publish(
                PublishRequest(
                    definition=definition,
                    actor_id=actor_id,
                    published_at=changed_at,
                    expected_head_revision=1,
                )
            )
            if record.ref != target:
                raise ReviewedCapabilityPromotionError(
                    f"Scenario D published target mismatch for {identity}"
                )
            published.append(record.ref)
            continue
        if (
            head.ref.revision == 2
            and head.retired_at is None
            and head.ref == target
            and head.definition == definition
        ):
            reused.append(head.ref)
            continue
        raise ReviewedCapabilityPromotionError(
            f"Scenario D head conflicts with executable correction for {identity}"
        )
    return ScenarioDExecutionCorrectionResult(
        published=tuple(published),
        reused=tuple(reused),
    )


def build_reviewed_capability_bundle(
    *,
    reviewed_payloads: Path,
    workspace_skills: Path,
) -> ReviewedCapabilityBundle:
    """Build exact revision-two definitions without mutating catalog state."""
    payload_root = _checked_directory(reviewed_payloads, "reviewed payload directory")
    skill_root = _checked_directory(workspace_skills, "workspace skill directory")

    firecrawl_snapshot = _load_reviewed_snapshot(
        payload_root / FIRECRAWL_SNAPSHOT,
        FIRECRAWL_SNAPSHOT_DIGEST,
    )
    tavily_snapshot = _load_reviewed_snapshot(
        payload_root / TAVILY_SNAPSHOT,
        TAVILY_SNAPSHOT_DIGEST,
    )
    agent_browser_root = _checked_directory(
        skill_root / "agent-browser",
        "workspace agent-browser skill",
    )
    firecrawl_tool_names = _snapshot_tool_names(firecrawl_snapshot)
    tavily_tool_names = _snapshot_tool_names(tavily_snapshot)

    firecrawl = _server_definition(
        logical_id="mcp.firecrawl",
        title="Firecrawl MCP Server",
        description=(
            "Reviewed, version-pinned Firecrawl MCP server for current web search, "
            "extraction, and bounded interaction."
        ),
        package=f"firecrawl-mcp@{FIRECRAWL_VERSION}",
        credential="FIRECRAWL_API_KEY",
        network_host="api.firecrawl.dev",
        allowed_tools=firecrawl_tool_names,
        snapshot_path=payload_root / FIRECRAWL_SNAPSHOT,
        snapshot_digest=FIRECRAWL_SNAPSHOT_DIGEST,
        upstream_identity="io.github.firecrawl/firecrawl-mcp-server",
        upstream_version=FIRECRAWL_VERSION,
        commit=FIRECRAWL_COMMIT,
        license_name="MIT",
        source="mcp_registry",
        source_locator=(
            f"mcp-registry:io.github.firecrawl/firecrawl-mcp-server@{FIRECRAWL_VERSION}"
        ),
    )
    tavily = _server_definition(
        logical_id="mcp.tavily",
        title="Tavily MCP Server",
        description=(
            "Reviewed, version-pinned Tavily MCP server for current web search, "
            "extraction, mapping, and crawling."
        ),
        package=f"tavily-mcp@{TAVILY_VERSION}",
        credential="TAVILY_API_KEY",
        network_host="api.tavily.com",
        allowed_tools=tavily_tool_names,
        snapshot_path=payload_root / TAVILY_SNAPSHOT,
        snapshot_digest=TAVILY_SNAPSHOT_DIGEST,
        upstream_identity="tavily-ai/tavily-mcp",
        upstream_version=TAVILY_VERSION,
        commit=TAVILY_COMMIT,
        license_name="MIT",
        source="git",
        source_locator=(f"https://github.com/tavily-ai/tavily-mcp/tree/{TAVILY_COMMIT}"),
    )
    servers = (firecrawl, tavily)
    server_refs = {item.logical_id: _definition_ref(item) for item in servers}

    tools = (
        *_tool_definitions(
            server_ref=server_refs["mcp.firecrawl"],
            snapshot=firecrawl_snapshot,
            selected_names=firecrawl_tool_names,
            provider="Firecrawl",
        ),
        *_tool_definitions(
            server_ref=server_refs["mcp.tavily"],
            snapshot=tavily_snapshot,
            selected_names=tavily_tool_names,
            provider="Tavily",
        ),
    )
    skills = tuple(
        _workspace_skill_definition(skill_root, name, provider="Firecrawl")
        for name in FIRECRAWL_SKILL_NAMES
    ) + tuple(
        _workspace_skill_definition(skill_root, name, provider="Tavily")
        for name in TAVILY_SKILL_NAMES
    )
    agent_browser = _agent_browser_definition(agent_browser_root)
    all_skills = (*skills, agent_browser)
    tool_refs = {item.logical_id: _definition_ref(item) for item in tools}
    skill_refs = {item.logical_id: _definition_ref(item) for item in all_skills}
    profile = AgentProfileDefinition(
        logical_id="agent-profile.web-research-browser-verification",
        title="Two-provider web research and browser verification agent",
        description=(
            "Uses the exact reviewed Firecrawl and Tavily search tools, their "
            "content-addressed procedures, and the reviewed agent-browser skill."
        ),
        skill_refs=frozenset(
            {
                skill_refs["skill.firecrawl-search"],
                skill_refs["skill.tavily-search"],
                skill_refs["skill.agent-browser"],
            }
        ),
        mcp_server_refs=frozenset(server_refs.values()),
        tool_refs=frozenset(tool_refs[item] for item in SEARCH_TOOL_LOGICAL_IDS),
        model_policy=ModelPolicy(
            provider="openai",
            model="gpt-5.4-nano",
            settings={"reasoning_effort": "medium", "max_turns": 12},
        ),
        maximum_capability_request=AuthorityCeiling(
            capabilities=WEB_RESEARCH_CAPABILITIES,
            budgets=WEB_RESEARCH_BUDGETS,
            max_concurrency=2,
        ),
    )
    definitions: tuple[Definition, ...] = (
        *servers,
        *all_skills,
        *tools,
        profile,
    )
    _validate_dependency_order(definitions)
    return ReviewedCapabilityBundle(definitions=definitions)


async def promote_reviewed_capabilities(
    *,
    service: CapabilityPromotionService,
    bundle: ReviewedCapabilityBundle,
    catalog_records: tuple[PublishedDefinition, ...],
    aliases: tuple[AliasBinding, ...],
    actor_id: str,
    changed_at: datetime,
    retire_superseded: bool,
) -> ReviewedCapabilityPromotionResult:
    """Publish/reuse all revision-two rows before optionally retiring safe revision-one rows."""
    heads = _head_records(catalog_records)
    preflight_reviewed_capabilities(bundle=bundle, catalog_records=catalog_records)
    target_by_identity = {_identity(item): item for item in bundle.definitions}
    if len(target_by_identity) != len(bundle.definitions):
        raise ReviewedCapabilityPromotionError("promotion bundle contains duplicate identities")

    published: list[ExactDefinitionRef] = []
    reused: list[ExactDefinitionRef] = []
    superseded: dict[tuple[str, str], PublishedDefinition] = {}
    for identity, definition in target_by_identity.items():
        head = heads.get(identity)
        target_revision = _target_revision(definition)
        if head is None and target_revision != 1:
            raise ReviewedCapabilityPromotionError(
                f"revision-one prerequisite is missing for {identity}"
            )
        if head is None:
            record = await service.publish(
                PublishRequest(
                    definition=definition,
                    actor_id=actor_id,
                    published_at=changed_at,
                    expected_head_revision=0,
                )
            )
            expected_ref = _definition_ref(definition)
            if record.ref != expected_ref:
                raise ReviewedCapabilityPromotionError(
                    f"published revision does not match reviewed target for {identity}"
                )
            published.append(record.ref)
        elif target_revision == 2 and head.ref.revision == 1:
            if head.retired_at is not None:
                raise ReviewedCapabilityPromotionError(
                    f"revision-one prerequisite is retired for {identity}"
                )
            superseded[identity] = head
            record = await service.publish(
                PublishRequest(
                    definition=definition,
                    actor_id=actor_id,
                    published_at=changed_at,
                    expected_head_revision=1,
                )
            )
            expected_ref = _definition_ref(definition)
            if record.ref != expected_ref:
                raise ReviewedCapabilityPromotionError(
                    f"published revision does not match reviewed target for {identity}"
                )
            published.append(record.ref)
        elif head.ref.revision == target_revision:
            expected_ref = _definition_ref(definition)
            if head.ref != expected_ref or head.definition != definition:
                raise ReviewedCapabilityPromotionError(
                    f"revision-two head conflicts with reviewed target for {identity}"
                )
            if head.retired_at is not None:
                raise ReviewedCapabilityPromotionError(
                    f"reviewed revision-two head is retired for {identity}"
                )
            if target_revision == 2:
                prior = next(
                    (
                        item
                        for item in catalog_records
                        if _record_identity(item) == identity and item.ref.revision == 1
                    ),
                    None,
                )
                if prior is None:
                    raise ReviewedCapabilityPromotionError(
                        f"revision-one predecessor is missing for {identity}"
                    )
                superseded[identity] = prior
            reused.append(head.ref)
        else:
            raise ReviewedCapabilityPromotionError(
                f"expected catalog head revision {target_revision - 1} or "
                f"{target_revision} for {identity}, found {head.ref.revision}"
            )

    retired: list[ExactDefinitionRef] = []
    retained: list[ExactDefinitionRef] = []
    reasons: dict[str, tuple[str, ...]] = {}
    if retire_superseded:
        safe, reasons = _safe_retirements(
            catalog_records=catalog_records,
            aliases=aliases,
            superseded=tuple(item.ref for item in superseded.values()),
        )
        retirement_order = sorted(
            (item.ref for item in superseded.values()),
            key=_retirement_rank,
        )
        for ref in retirement_order:
            if ref not in safe:
                retained.append(ref)
                continue
            await service.retire(RetireRequest(ref=ref, actor_id=actor_id, retired_at=changed_at))
            retired.append(ref)
    else:
        retained.extend(item.ref for item in superseded.values())
        reasons = {
            _ref_key(item.ref): ("retirement was not requested",) for item in superseded.values()
        }

    return ReviewedCapabilityPromotionResult(
        published=tuple(published),
        reused=tuple(reused),
        retired=tuple(retired),
        retained=tuple(retained),
        retention_reasons=reasons,
    )


def preflight_reviewed_capabilities(
    *,
    bundle: ReviewedCapabilityBundle,
    catalog_records: tuple[PublishedDefinition, ...],
) -> ReviewedCapabilityPreflight:
    """Fail closed on all known head conflicts before the first catalog mutation."""
    heads = _head_records(catalog_records)
    new: list[ExactDefinitionRef] = []
    advance: list[ExactDefinitionRef] = []
    reuse: list[ExactDefinitionRef] = []
    for definition in bundle.definitions:
        identity = _identity(definition)
        target = _definition_ref(definition)
        head = heads.get(identity)
        if head is None:
            if target.revision != 1:
                raise ReviewedCapabilityPromotionError(
                    f"revision-one prerequisite is missing for {identity}"
                )
            new.append(target)
            continue
        if head.ref.revision == target.revision:
            if head.ref != target or head.definition != definition:
                raise ReviewedCapabilityPromotionError(
                    f"revision-{target.revision} head conflicts with reviewed target for {identity}"
                )
            if head.retired_at is not None:
                raise ReviewedCapabilityPromotionError(
                    f"reviewed revision-{target.revision} head is retired for {identity}"
                )
            reuse.append(target)
            continue
        if target.revision == 2 and head.ref.revision == 1:
            if head.retired_at is not None:
                raise ReviewedCapabilityPromotionError(
                    f"revision-one prerequisite is retired for {identity}"
                )
            advance.append(target)
            continue
        raise ReviewedCapabilityPromotionError(
            f"expected catalog head revision {target.revision - 1} or "
            f"{target.revision} for {identity}, found {head.ref.revision}"
        )
    return ReviewedCapabilityPreflight(
        new=tuple(new),
        advance=tuple(advance),
        reuse=tuple(reuse),
    )


def _server_definition(
    *,
    logical_id: str,
    title: str,
    description: str,
    package: str,
    credential: str,
    network_host: str,
    allowed_tools: tuple[str, ...],
    snapshot_path: Path,
    snapshot_digest: str,
    upstream_identity: str,
    upstream_version: str,
    commit: str,
    license_name: str,
    source: Literal["git", "mcp_registry"],
    source_locator: str,
) -> MCPServerDefinition:
    return MCPServerDefinition(
        logical_id=logical_id,
        title=title,
        description=description,
        transport="stdio",
        launch_template=("npx", "-y", package),
        credential_refs=(SecretRef(provider="environment", key=credential),),
        allowed_tools=frozenset(allowed_tools),
        approval_policy={
            name: "always" if name.endswith("_interact") else "never" for name in allowed_tools
        },
        network_requirements=(
            MCPNetworkRequirement(host=network_host, port=443, protocol="https"),
        ),
        schema_snapshot_ref=CatalogPayloadRef(
            uri=f"belllabs://reviewed-capabilities/{snapshot_path.name}",
            digest=snapshot_digest,
            media_type="application/json",
            size_bytes=snapshot_path.stat().st_size,
        ),
        schema_digest=snapshot_digest,
        source_provenance=SourceProvenance(
            source=source,
            locator=source_locator,
            upstream_identity=upstream_identity,
            upstream_version=upstream_version,
            commit_digest=commit,
            license=license_name,
        ),
        review_status="approved",
    )


def _tool_definitions(
    *,
    server_ref: ExactDefinitionRef,
    snapshot: dict[str, object],
    selected_names: tuple[str, ...],
    provider: str,
) -> tuple[MCPToolDefinition, ...]:
    raw_tools = snapshot.get("tools")
    if not isinstance(raw_tools, list):
        raise ReviewedCapabilityPromotionError("reviewed MCP snapshot has no tools list")
    by_name: dict[str, dict[str, object]] = {}
    for raw in raw_tools:
        if not isinstance(raw, dict) or not isinstance(raw.get("name"), str):
            raise ReviewedCapabilityPromotionError("reviewed MCP tool row is malformed")
        name = str(raw["name"])
        if name in by_name:
            raise ReviewedCapabilityPromotionError(f"duplicate MCP tool in snapshot: {name}")
        by_name[name] = raw
    if missing := set(selected_names) - set(by_name):
        raise ReviewedCapabilityPromotionError(
            f"reviewed MCP snapshot is missing selected tools: {sorted(missing)}"
        )

    definitions: list[MCPToolDefinition] = []
    for name in selected_names:
        raw = by_name[name]
        input_schema = raw.get("inputSchema")
        output_schema = raw.get("outputSchema")
        annotations = raw.get("annotations", {})
        description = raw.get("description")
        if not isinstance(input_schema, dict):
            raise ReviewedCapabilityPromotionError(f"MCP tool {name} has no input schema")
        if output_schema is not None and not isinstance(output_schema, dict):
            raise ReviewedCapabilityPromotionError(f"MCP tool {name} output schema is malformed")
        if not isinstance(annotations, dict):
            raise ReviewedCapabilityPromotionError(f"MCP tool {name} annotations are malformed")
        if not isinstance(description, str) or not description.strip():
            raise ReviewedCapabilityPromotionError(f"MCP tool {name} has no description")
        schema_payload = {
            "tool_name": name,
            "input_schema": input_schema,
            "output_schema": output_schema,
            "annotations": annotations,
        }
        definitions.append(
            MCPToolDefinition(
                logical_id=f"{server_ref.logical_id}:{name}",
                title=f"{provider} {name}",
                description=description.strip(),
                server_ref=server_ref,
                tool_name=name,
                input_schema=input_schema,
                output_schema=output_schema,
                annotations=annotations,
                schema_digest=sha256_digest(schema_payload),
                side_effect_class=_side_effect_class(name, annotations),
            )
        )
    return tuple(definitions)


def _workspace_skill_definition(
    skill_root: Path,
    name: str,
    *,
    provider: str,
) -> SkillDefinition:
    root = _checked_directory(skill_root / name, f"workspace skill {name}")
    manifest = _file_manifest(root)
    frontmatter = _skill_frontmatter(root / "SKILL.md")
    if frontmatter.get("name") != name:
        raise ReviewedCapabilityPromotionError(f"workspace skill name mismatch: expected {name}")
    summary = frontmatter.get("description")
    if not isinstance(summary, str) or not summary.strip():
        raise ReviewedCapabilityPromotionError(f"workspace skill {name} has no description")
    manifest_digest = sha256_digest(manifest)
    return SkillDefinition(
        logical_id=f"skill.{name}",
        title=f"{provider} {name} Agent Skill",
        description=" ".join(summary.split()),
        skill_name=name,
        frontmatter=frontmatter,
        body_summary=" ".join(summary.split()),
        bundle_ref=CatalogPayloadRef(
            uri=f"workspace://.agents/skills/{name}/{manifest_digest.removeprefix('sha256:')}",
            digest=manifest_digest,
            media_type="application/vnd.belllabs.skill-manifest+json",
            size_bytes=sum(item.size_bytes for item in manifest),
        ),
        manifest_digest=manifest_digest,
        file_manifest=manifest,
        required_capabilities=_provider_skill_capabilities(name, provider),
        compatibility=SkillCompatibility(
            runtimes=frozenset({"governed-agent-runtime"}),
            network_capabilities=frozenset({"network.web"}),
            workspace_capabilities=frozenset({"workspace.browser.read"}),
        ),
        source_provenance=SourceProvenance(
            source="local",
            locator=f"workspace:.agents/skills/{name}",
            upstream_identity=f"workspace-skill/{name}",
            upstream_version=manifest_digest,
        ),
        review_status="approved",
    )


def _agent_browser_definition(root: Path) -> SkillDefinition:
    manifest = _file_manifest(root)
    skill_file = next((item for item in manifest if item.path == "SKILL.md"), None)
    if skill_file is None or skill_file.digest != AGENT_BROWSER_SKILL_DIGEST:
        actual = skill_file.digest if skill_file is not None else "missing"
        raise ReviewedCapabilityPromotionError(
            "workspace agent-browser skill does not match the pinned upstream commit: "
            f"expected {AGENT_BROWSER_SKILL_DIGEST}, got {actual}"
        )
    frontmatter = _skill_frontmatter(root / "SKILL.md")
    summary = frontmatter.get("description")
    if frontmatter.get("name") != "agent-browser" or not isinstance(summary, str):
        raise ReviewedCapabilityPromotionError("reviewed agent-browser frontmatter is malformed")
    manifest_digest = sha256_digest(manifest)
    return SkillDefinition(
        logical_id="skill.agent-browser",
        title="Reviewed vercel-labs agent-browser Agent Skill",
        description=" ".join(summary.split()),
        skill_name="agent-browser",
        frontmatter=frontmatter,
        body_summary=" ".join(summary.split()),
        bundle_ref=CatalogPayloadRef(
            uri=(
                "git+https://github.com/vercel-labs/agent-browser@"
                f"{AGENT_BROWSER_COMMIT}/skills/agent-browser/SKILL.md"
            ),
            digest=manifest_digest,
            media_type="application/vnd.belllabs.skill-manifest+json",
            size_bytes=sum(item.size_bytes for item in manifest),
        ),
        manifest_digest=manifest_digest,
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
            source="npx_skills",
            locator="https://skills.sh/vercel-labs/agent-browser/agent-browser",
            upstream_identity="vercel-labs/agent-browser",
            upstream_version=AGENT_BROWSER_VERSION,
            commit_digest=AGENT_BROWSER_COMMIT,
            license="Apache-2.0",
        ),
        review_status="approved",
    )


def _load_reviewed_snapshot(path: Path, expected_digest: str) -> dict[str, object]:
    checked = _checked_file(path, "reviewed MCP snapshot")
    raw = checked.read_bytes()
    actual = _bytes_digest(raw)
    if actual != expected_digest:
        raise ReviewedCapabilityPromotionError(
            f"reviewed MCP snapshot digest mismatch for {checked.name}: "
            f"expected {expected_digest}, got {actual}"
        )
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ReviewedCapabilityPromotionError(
            f"reviewed MCP snapshot is not JSON: {checked.name}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("snapshot_format") != "mcp-tools-list/1":
        raise ReviewedCapabilityPromotionError(
            f"reviewed MCP snapshot format is unsupported: {checked.name}"
        )
    return payload


def _snapshot_tool_names(snapshot: dict[str, object]) -> tuple[str, ...]:
    raw_tools = snapshot.get("tools")
    if not isinstance(raw_tools, list):
        raise ReviewedCapabilityPromotionError("reviewed MCP snapshot has no tools list")
    names: list[str] = []
    for tool in raw_tools:
        if not isinstance(tool, dict) or not isinstance(tool.get("name"), str):
            raise ReviewedCapabilityPromotionError("reviewed MCP tool row is malformed")
        names.append(str(tool["name"]))
    if len(names) != len(set(names)):
        raise ReviewedCapabilityPromotionError("reviewed MCP snapshot has duplicate tool names")
    return tuple(names)


def _file_manifest(root: Path) -> tuple[SkillFileManifestEntry, ...]:
    entries: list[SkillFileManifestEntry] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise ReviewedCapabilityPromotionError(
                f"skill bundles may not contain symlinks: {path}"
            )
        if not path.is_file():
            continue
        raw = path.read_bytes()
        entries.append(
            SkillFileManifestEntry(
                path=path.relative_to(root).as_posix(),
                digest=_bytes_digest(raw),
                size_bytes=len(raw),
            )
        )
    if not entries:
        raise ReviewedCapabilityPromotionError(f"skill bundle is empty: {root}")
    return tuple(entries)


def _skill_frontmatter(path: Path) -> dict[str, object]:
    text = _checked_file(path, "SKILL.md").read_text(encoding="utf-8")
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        raise ReviewedCapabilityPromotionError(f"SKILL.md has no frontmatter: {path}")
    try:
        end = next(index for index in range(1, len(lines)) if lines[index].strip() == "---")
    except StopIteration as exc:
        raise ReviewedCapabilityPromotionError(
            f"SKILL.md frontmatter is not terminated: {path}"
        ) from exc
    header = lines[1:end]
    parsed: dict[str, object] = {}
    index = 0
    while index < len(header):
        line = header[index]
        if not line or line[0].isspace() or ":" not in line:
            index += 1
            continue
        key, raw_value = line.split(":", 1)
        value = raw_value.strip()
        if value in {"|", ">"}:
            block: list[str] = []
            index += 1
            while index < len(header) and (not header[index] or header[index][0].isspace()):
                block.append(header[index].strip())
                index += 1
            parsed[key] = "\n".join(block).strip()
            continue
        if not value:
            items: list[str] = []
            index += 1
            while index < len(header) and (not header[index] or header[index][0].isspace()):
                candidate = header[index].strip()
                if candidate.startswith("- "):
                    items.append(candidate[2:].strip())
                index += 1
            parsed[key] = items
            continue
        if value.lower() in {"true", "false"}:
            parsed[key] = value.lower() == "true"
        else:
            parsed[key] = value
        index += 1
    return parsed


def _required_head(
    heads: dict[tuple[str, str], PublishedDefinition],
    kind: DefinitionKind,
    logical_id: str,
) -> PublishedDefinition:
    record = heads.get((kind.value, logical_id))
    if record is None:
        raise ReviewedCapabilityPromotionError(
            f"Scenario D catalog head is missing: {kind.value}:{logical_id}"
        )
    return record


def _correction_ref(definition: Definition) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=definition.kind,
        logical_id=definition.logical_id,
        revision=2,
        digest=sha256_digest(definition),
    )


def _validate_scenario_d_correction(
    definitions: tuple[
        StageGraphBlueprint,
        ControlProfileDefinition,
        WorkflowTypeDefinition,
        WorkflowImplementationBindingDefinition,
    ],
) -> None:
    blueprint, control, workflow, implementation = definitions
    admission = next(
        (stage for stage in blueprint.stages if stage.stage_id == "admit_public_goal"),
        None,
    )
    if admission is None or not admission.reservation:
        raise ReviewedCapabilityPromotionError(
            "Scenario D correction must reserve the admission operation before dispatch"
        )
    blueprint_ref = _correction_ref(blueprint)
    control_ref = _correction_ref(control)
    workflow_ref = _correction_ref(workflow)
    if control.blueprint_ref != blueprint_ref:
        raise ReviewedCapabilityPromotionError(
            "Scenario D corrected control profile does not bind the exact blueprint"
        )
    if workflow.allowed_blueprints != frozenset(
        {blueprint_ref}
    ) or workflow.allowed_control_profiles != frozenset({control_ref}):
        raise ReviewedCapabilityPromotionError(
            "Scenario D corrected workflow does not bind exact executable authorities"
        )
    if (
        implementation.workflow_type_ref != workflow_ref
        or implementation.blueprint_ref != blueprint_ref
        or implementation.control_profile_ref != control_ref
    ):
        raise ReviewedCapabilityPromotionError(
            "Scenario D corrected implementation has stale exact references"
        )


def _validate_dependency_order(definitions: tuple[Definition, ...]) -> None:
    positions = {_identity(item): index for index, item in enumerate(definitions)}
    expected_refs = {_definition_ref(item) for item in definitions}
    for index, definition in enumerate(definitions):
        for dependency in _definition_dependencies(definition):
            if dependency not in expected_refs:
                raise ReviewedCapabilityPromotionError(
                    f"promotion dependency is outside the reviewed bundle: {dependency}"
                )
            if positions[(dependency.kind.value, dependency.logical_id)] >= index:
                raise ReviewedCapabilityPromotionError(
                    f"promotion dependency is not publication ordered: {dependency}"
                )


def _safe_retirements(
    *,
    catalog_records: tuple[PublishedDefinition, ...],
    aliases: tuple[AliasBinding, ...],
    superseded: tuple[ExactDefinitionRef, ...],
) -> tuple[frozenset[ExactDefinitionRef], dict[str, tuple[str, ...]]]:
    candidates = set(superseded)
    reasons: dict[str, set[str]] = {}
    for alias in aliases:
        if alias.target in candidates:
            candidates.remove(alias.target)
            reasons.setdefault(_ref_key(alias.target), set()).add(
                f"active alias {alias.alias_ref.alias} targets revision one"
            )

    active_records = tuple(item for item in catalog_records if item.retired_at is None)
    changed = True
    while changed:
        changed = False
        for record in active_records:
            if record.ref in candidates:
                continue
            for dependency in _definition_dependencies(record.definition):
                if dependency not in candidates:
                    continue
                candidates.remove(dependency)
                reasons.setdefault(_ref_key(dependency), set()).add(
                    f"active consumer {_ref_key(record.ref)} still references revision one"
                )
                changed = True
    normalized = {key: tuple(sorted(value)) for key, value in reasons.items()}
    return frozenset(candidates), normalized


def _head_records(
    records: tuple[PublishedDefinition, ...],
) -> dict[tuple[str, str], PublishedDefinition]:
    heads: dict[tuple[str, str], PublishedDefinition] = {}
    for record in records:
        identity = _record_identity(record)
        current = heads.get(identity)
        if current is None or record.ref.revision > current.ref.revision:
            heads[identity] = record
    return heads


def _definition_dependencies(definition: Definition) -> frozenset[ExactDefinitionRef]:
    if isinstance(definition, MCPToolDefinition):
        return frozenset({definition.server_ref})
    if isinstance(definition, AgentProfileDefinition):
        return (
            definition.prompt_refs
            | definition.skill_refs
            | definition.mcp_server_refs
            | definition.tool_refs
        )
    return frozenset()


def _definition_ref(definition: Definition) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=definition.kind,
        logical_id=definition.logical_id,
        revision=_target_revision(definition),
        digest=sha256_digest(definition),
    )


def _target_revision(definition: Definition) -> int:
    if (
        isinstance(definition, MCPToolDefinition)
        and definition.tool_name not in REQUIRED_TOOL_NAMES
    ):
        return 1
    return TARGET_REVISION


def _identity(definition: Definition) -> tuple[str, str]:
    return definition.kind.value, definition.logical_id


def _record_identity(record: PublishedDefinition) -> tuple[str, str]:
    return record.ref.kind.value, record.ref.logical_id


def _retirement_rank(ref: ExactDefinitionRef) -> tuple[int, str]:
    rank = {
        "agent_profile": 0,
        "mcp_tool": 1,
        "skill": 2,
        "mcp_server": 3,
    }
    return rank.get(ref.kind.value, 4), ref.logical_id


def _ref_key(ref: ExactDefinitionRef) -> str:
    return f"{ref.kind.value}:{ref.logical_id}@{ref.revision}"


def _checked_directory(path: Path, label: str) -> Path:
    resolved = path.resolve(strict=True)
    if not resolved.is_dir():
        raise ReviewedCapabilityPromotionError(f"{label} is not a directory: {resolved}")
    return resolved


def _checked_file(path: Path, label: str) -> Path:
    resolved = path.resolve(strict=True)
    if resolved.is_symlink() or not resolved.is_file():
        raise ReviewedCapabilityPromotionError(f"{label} is not a regular file: {resolved}")
    return resolved


def _bytes_digest(raw: bytes) -> str:
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _side_effect_class(name: str, annotations: dict[str, object]) -> str:
    if annotations.get("readOnlyHint") is True:
        return "read_only"
    if annotations.get("destructiveHint") is True or name.endswith("_interact"):
        return "consequential"
    return "read_only"


def _provider_skill_capabilities(name: str, provider: str) -> frozenset[str]:
    firecrawl = {
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
    }
    tavily = {
        "tavily-search": "web.search.tavily",
        "tavily-extract": "web.extract.tavily",
        "tavily-map": "web.map.tavily",
        "tavily-crawl": "web.crawl.tavily",
        "tavily-research": "web.research.tavily",
        "tavily-dynamic-search": "web.search.tavily",
        "tavily-cli": "runtime.tavily-cli",
        "tavily-best-practices": "procedure.tavily",
    }
    selected = firecrawl if provider == "Firecrawl" else tavily
    try:
        capability = selected[name]
    except KeyError as exc:
        raise ReviewedCapabilityPromotionError(
            f"unsupported reviewed {provider} skill: {name}"
        ) from exc
    return frozenset({capability, "network.web"})
