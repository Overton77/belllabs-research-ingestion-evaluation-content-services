from __future__ import annotations

import ast
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.domain.control_plane.canonical import canonical_json, sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    AvailableCapability,
    CapabilityDefinition,
    CapabilityKind,
    CapabilityRequirement,
    DeepAgentPlacementDefinition,
    DefinitionKind,
    ExactDefinitionRef,
    ImmutablePayloadRef,
    ModelPolicy,
    OperationAssemblyDefinition,
    ProfileComponent,
    PromptDefinition,
)
from app.domain.control_plane.errors import CompilationRejected
from tests.test_agentic_asset_definitions import (
    server_definition,
    skill_definition,
    tool_definition,
)
from tests.test_control_plane import authority, configured_service, invocation, publish


async def _publish_capability(
    service,
    *,
    kind: DefinitionKind,
    capability_kind: CapabilityKind,
    logical_id: str,
    maturity: str = "qualified",
    compiler_versions: frozenset[str] = frozenset({"control-plane-definitions/1"}),
):
    return await publish(
        service,
        CapabilityDefinition(
            kind=kind,
            capability_kind=capability_kind,
            logical_id=logical_id,
            title=logical_id,
            description=f"Sanitized exact {capability_kind.value} fixture.",
            maturity=maturity,
            attachment_targets=frozenset({"agent.main"}),
            compatible_compiler_versions=compiler_versions,
        ),
    )


async def _deep_agent_records(service):
    prompt = await publish(
        service,
        PromptDefinition(
            logical_id="prompt.exact",
            title="Exact prompt",
            description="Sanitized prompt fixture.",
            format="markdown",
            template_engine="none",
            body="Use only exact bound capabilities.",
            trust_class="reviewed",
        ),
    )
    skill = await publish(service, skill_definition())
    server = await publish(service, server_definition())
    tool = await publish(service, tool_definition(server.ref))
    sandbox = await _publish_capability(
        service,
        kind=DefinitionKind.SANDBOX_PROFILE,
        capability_kind=CapabilityKind.SANDBOX,
        logical_id="sandbox.exact",
    )
    model = await _publish_capability(
        service,
        kind=DefinitionKind.MODEL,
        capability_kind=CapabilityKind.MODEL,
        logical_id="model.exact",
    )
    middleware = await _publish_capability(
        service,
        kind=DefinitionKind.MIDDLEWARE,
        capability_kind=CapabilityKind.MIDDLEWARE,
        logical_id="middleware.exact",
    )
    unavailable_tool = await _publish_capability(
        service,
        kind=DefinitionKind.TOOL,
        capability_kind=CapabilityKind.TOOL,
        logical_id="tool.unavailable-exact",
    )
    similarly_named_tool = await _publish_capability(
        service,
        kind=DefinitionKind.TOOL,
        capability_kind=CapabilityKind.TOOL,
        logical_id="tool.unavailable-exact-looking",
    )
    incompatible_tool = await _publish_capability(
        service,
        kind=DefinitionKind.TOOL,
        capability_kind=CapabilityKind.TOOL,
        logical_id="tool.incompatible",
        compiler_versions=frozenset({"another-compiler/1"}),
    )
    placement = await publish(
        service,
        DeepAgentPlacementDefinition(
            logical_id="placement.local",
            title="Local placement",
            description="Exact local Deep Agents placement.",
            deep_agents_version="0.7.5",
            placement="local_worker",
            runtime_binding="python-3.12",
            sandbox_ref=sandbox.ref,
        ),
    )
    requirements = (
        CapabilityRequirement(
            requirement_id="mcp",
            capability_kind="mcp",
            allowed_refs=frozenset({server.ref}),
            attachment_target="agent.main",
        ),
        CapabilityRequirement(
            requirement_id="skill",
            capability_kind="skill",
            allowed_refs=frozenset({skill.ref}),
            attachment_target="agent.main",
        ),
        CapabilityRequirement(
            requirement_id="sandbox",
            capability_kind="sandbox",
            allowed_refs=frozenset({sandbox.ref}),
            attachment_target="agent.main",
        ),
        CapabilityRequirement(
            requirement_id="model",
            capability_kind="model",
            allowed_refs=frozenset({model.ref}),
            attachment_target="agent.main",
        ),
        CapabilityRequirement(
            requirement_id="middleware",
            capability_kind="middleware",
            allowed_refs=frozenset({middleware.ref}),
            attachment_target="agent.main",
        ),
        CapabilityRequirement(
            requirement_id="tool",
            capability_kind="tool",
            allowed_refs=frozenset({tool.ref}),
            attachment_target="agent.main",
        ),
        CapabilityRequirement(
            requirement_id="optional-omit",
            capability_kind="tool",
            allowed_refs=frozenset({unavailable_tool.ref}),
            attachment_target="agent.main",
            required=False,
            when_unavailable="omit",
        ),
        CapabilityRequirement(
            requirement_id="optional-degrade",
            capability_kind="tool",
            allowed_refs=frozenset({unavailable_tool.ref}),
            attachment_target="agent.main",
            required=False,
            when_unavailable="degrade",
            degraded_ref=tool.ref,
        ),
        CapabilityRequirement(
            requirement_id="optional-incompatible",
            capability_kind="tool",
            allowed_refs=frozenset({incompatible_tool.ref}),
            attachment_target="agent.main",
            required=False,
            when_unavailable="omit",
        ),
    )
    parent = await publish(
        service,
        AgentProfileDefinition(
            logical_id="agent.parent",
            title="Parent agent",
            description="Exact parent composition fixture.",
            model_policy=ModelPolicy(provider="frozen", model="resolved-by-exact-ref"),
            maximum_capability_request=authority("sandbox", "evaluate", budget=60, concurrency=2),
            prompt_refs=frozenset({prompt.ref}),
            mcp_server_refs=frozenset({server.ref}),
            tool_refs=frozenset({tool.ref}),
            components=(ProfileComponent(slot="tool.search", ref=tool.ref),),
            model_ref=model.ref,
            middleware_refs=frozenset({middleware.ref}),
            sandbox_profile_ref=sandbox.ref,
            capability_requirements=requirements[:3],
        ),
    )
    child = await publish(
        service,
        AgentProfileDefinition(
            logical_id="agent.child",
            title="Child agent",
            description="Exact child composition fixture.",
            model_policy=ModelPolicy(provider="frozen", model="resolved-by-exact-ref"),
            maximum_capability_request=authority("sandbox", "evaluate", budget=60, concurrency=2),
            skill_refs=frozenset({skill.ref}),
            parent_profile_refs=(parent.ref,),
            components=(ProfileComponent(slot="skill.research", ref=skill.ref),),
            model_ref=model.ref,
            middleware_refs=frozenset({middleware.ref}),
            sandbox_profile_ref=sandbox.ref,
            capability_requirements=requirements[3:6],
        ),
    )
    refs = (
        server,
        skill,
        sandbox,
        model,
        middleware,
        tool,
        similarly_named_tool,
        incompatible_tool,
    )
    available = tuple(AvailableCapability(ref=record.ref) for record in refs)
    return parent, child, placement, requirements, available


@pytest.mark.asyncio
async def test_exact_deep_agent_composition_and_all_capability_families_compile() -> None:
    service, _, records = await configured_service()
    _, profile, placement, requirements, available = await _deep_agent_records(service)
    runtime = await publish(
        service,
        records["runtime"].definition.model_copy(
            update={
                "operation_assemblies": (
                    OperationAssemblyDefinition(
                        assembly_id="main",
                        deep_agent_profile_ref=profile.ref,
                        placement_ref=placement.ref,
                        capability_requirements=requirements,
                    ),
                )
            }
        ),
        expected=1,
    )
    workflow = await publish(
        service,
        records["workflow"].definition.model_copy(
            update={"allowed_runtime_profiles": frozenset({runtime.ref})}
        ),
        expected=1,
    )
    records.update(runtime=runtime, workflow=workflow)
    base = invocation(records)
    compiled = await service.compile(
        base.model_copy(
            update={
                "environment": base.environment.model_copy(update={"exact_capabilities": available})
            }
        )
    )
    decisions = {item.requirement_id: item for item in compiled.capability_attachment_plan}
    assert {item.capability_kind for item in compiled.capability_attachment_plan} == {
        "mcp",
        "middleware",
        "model",
        "sandbox",
        "skill",
        "tool",
    }
    assert decisions["optional-omit"].status == "omitted"
    assert decisions["optional-omit"].selected_ref is None
    assert decisions["optional-degrade"].status == "degraded"
    assert decisions["optional-degrade"].selected_ref == decisions["tool"].selected_ref
    assert decisions["optional-incompatible"].status == "omitted"
    binding = compiled.flattened_agent_bindings[0]
    assert [item.slot for item in binding.flattened_components] == ["skill.research", "tool.search"]
    assert binding.model_ref is not None and binding.sandbox_profile_ref is not None
    assert binding.prompt_refs and binding.skill_refs
    assert binding.mcp_server_refs and binding.tool_refs
    assert binding.model_policy.provider == "frozen"
    assert (
        binding.maximum_capability_request.capabilities <= compiled.effective_authority.capabilities
    )
    assert canonical_json(compiled) == canonical_json(await service.retrieve(compiled.digest))
    without_mcp = tuple(item for item in available if item.ref.kind != DefinitionKind.MCP_SERVER)
    with pytest.raises(CompilationRejected, match="exact capability unavailable"):
        await service.compile(
            base.model_copy(
                update={
                    "context": base.context.model_copy(update={"compilation_id": "missing-mcp"}),
                    "environment": base.environment.model_copy(
                        update={"exact_capabilities": without_mcp}
                    ),
                }
            )
        )
    unauthorized_profile = await publish(
        service,
        profile.definition.model_copy(
            update={
                "logical_id": "agent.unauthorized",
                "parent_profile_refs": (),
                "maximum_capability_request": authority(
                    "sandbox", "evaluate", budget=10_000, concurrency=10_000
                ),
            }
        ),
    )
    unauthorized_runtime = await publish(
        service,
        runtime.definition.model_copy(
            update={
                "operation_assemblies": (
                    runtime.definition.operation_assemblies[0].model_copy(
                        update={"deep_agent_profile_ref": unauthorized_profile.ref}
                    ),
                )
            }
        ),
        expected=2,
    )
    unauthorized_workflow = await publish(
        service,
        workflow.definition.model_copy(
            update={"allowed_runtime_profiles": frozenset({unauthorized_runtime.ref})}
        ),
        expected=2,
    )
    records.update(runtime=unauthorized_runtime, workflow=unauthorized_workflow)
    unauthorized = invocation(records).model_copy(
        update={
            "context": base.context.model_copy(update={"compilation_id": "unauthorized-profile"}),
            "environment": base.environment.model_copy(update={"exact_capabilities": available}),
        }
    )
    with pytest.raises(CompilationRejected, match="exceeds effective authority"):
        await service.compile(unauthorized)


@pytest.mark.asyncio
async def test_profile_composition_collisions_fail_deterministically() -> None:
    service, _, records = await configured_service()
    parent, child, placement, requirements, available = await _deep_agent_records(service)
    parent_definition = parent.definition
    conflicting_parent = await publish(
        service,
        parent_definition.model_copy(
            update={
                "logical_id": "agent.conflicting-parent",
                "components": (
                    ProfileComponent(
                        slot="tool.search",
                        ref=child.definition.components[0].ref,
                    ),
                ),
            }
        ),
    )
    conflicting_child = await publish(
        service,
        child.definition.model_copy(
            update={
                "logical_id": "agent.conflicting-child",
                "parent_profile_refs": (parent.ref, conflicting_parent.ref),
            }
        ),
    )
    runtime = await publish(
        service,
        records["runtime"].definition.model_copy(
            update={
                "operation_assemblies": (
                    OperationAssemblyDefinition(
                        assembly_id="main",
                        deep_agent_profile_ref=conflicting_child.ref,
                        placement_ref=placement.ref,
                        capability_requirements=requirements,
                    ),
                )
            }
        ),
        expected=1,
    )
    workflow = await publish(
        service,
        records["workflow"].definition.model_copy(
            update={"allowed_runtime_profiles": frozenset({runtime.ref})}
        ),
        expected=1,
    )
    records.update(runtime=runtime, workflow=workflow)
    base = invocation(records)
    with pytest.raises(CompilationRejected, match="component collision"):
        await service.compile(
            base.model_copy(
                update={
                    "environment": base.environment.model_copy(
                        update={"exact_capabilities": available}
                    )
                }
            )
        )


def test_definition_ref_contract_and_secret_value_rejection() -> None:
    ref = ExactDefinitionRef(
        kind="model",
        logical_id="model.sanitized",
        revision=1,
        digest="sha256:" + "a" * 64,
        payload_ref=ImmutablePayloadRef(
            schema_id="capability/1",
            digest="sha256:" + "b" * 64,
            media_type="application/json",
            size_bytes=12,
            uri="s3://immutable/sanitized",
        ),
    )
    assert ref.schema_version == "1" and ref.lifecycle_status == "published"
    with pytest.raises(ValidationError, match="secret values are forbidden"):
        CapabilityDefinition(
            kind="model",
            capability_kind="model",
            logical_id="model.bad",
            title="bad",
            description="bad",
            maturity="qualified",
            attachment_targets=frozenset({"agent.main"}),
            compatible_compiler_versions=frozenset(),
            api_token="not-a-reference",  # type: ignore[call-arg]
        )
    with pytest.raises(ValidationError, match="capability_kind must be 'model'"):
        CapabilityDefinition(
            kind="model",
            capability_kind="sandbox",
            logical_id="model.wrong-family",
            title="Wrong family",
            description="A capability family mismatch must fail closed.",
            maturity="qualified",
            attachment_targets=frozenset({"agent.main"}),
            compatible_compiler_versions=frozenset({"control-plane-definitions/1"}),
        )


@pytest.mark.asyncio
async def test_capability_family_mismatch_is_rejected_again_at_publication() -> None:
    service, _, _ = await configured_service()
    valid = CapabilityDefinition(
        kind="model",
        capability_kind="model",
        logical_id="model.publication-boundary",
        title="Publication boundary",
        description="A sanitized capability family publication fixture.",
        maturity="qualified",
        attachment_targets=frozenset({"agent.main"}),
        compatible_compiler_versions=frozenset({"control-plane-definitions/1"}),
    )
    bypassed = valid.model_copy(update={"capability_kind": CapabilityKind.SANDBOX})
    with pytest.raises(ValidationError, match="capability_kind must be 'model'"):
        await publish(service, bypassed)


def test_canonical_bytes_preserve_unicode_order_and_reject_nonfinite_numbers() -> None:
    assert canonical_json({"z": [2, 1], "á": "寿命"}) == (
        b'{"canonical_schema_version":"canonical-json/1","payload":{"z":[2,1],"\xc3\xa1":"\xe5\xaf\xbf\xe5\x91\xbd"}}'
    )
    assert sha256_digest({"z": [2, 1], "á": "寿命"}).startswith("sha256:")
    with pytest.raises(ValueError):
        canonical_json({"bad": float("nan")})


def test_compiler_has_database_network_clock_and_environment_drift_guards() -> None:
    source = Path("app/domain/control_plane/compiler.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    forbidden_imports = {
        "beanie",
        "pymongo",
        "requests",
        "httpx",
        "socket",
        "os",
        "time",
        "datetime",
    }
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        (node.module or "").split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert not imported & forbidden_imports
    assert "getenv" not in source and "datetime.now" not in source and "time.time" not in source
    assert "ExtensionRegistry" not in source and "validate_all" not in source


def test_optional_capability_policy_cannot_substitute_a_similar_name() -> None:
    exact = ExactDefinitionRef(
        kind="tool",
        logical_id="tool.exact",
        revision=1,
        digest="sha256:" + "1" * 64,
    )
    similarly_named = exact.model_copy(
        update={"logical_id": "tool.exact-looking", "digest": "sha256:" + "2" * 64}
    )
    requirement = CapabilityRequirement(
        requirement_id="optional-tool",
        capability_kind="tool",
        allowed_refs=frozenset({exact}),
        attachment_target="agent.main",
        required=False,
        when_unavailable="omit",
    )
    assert similarly_named not in requirement.allowed_refs
    assert requirement.when_unavailable == "omit"
