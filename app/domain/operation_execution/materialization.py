from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    CapabilityGrant,
    CognitiveChannelDefinition,
    CognitiveChannelPack,
    CognitiveChannelPackRef,
    CognitiveRuntimeContextPack,
    CognitiveRuntimeContextSchema,
    CognitiveRuntimeField,
    CognitiveStateSchema,
    DeepAgentAttachmentRecord,
    DeepAgentExecutionBinding,
    DeepAgentExecutionPlacementProfile,
    DeepAgentProfile,
    SubagentContextSlice,
    SubagentStateSlice,
    WorkspaceContract,
    WorkspaceMaterializationManifest,
)
from app.domain.operation_execution.errors import WorkspaceDigestMismatch


def compose_cognitive_state_schema(
    *,
    schema_id: str,
    packs: tuple[CognitiveChannelPack, ...],
    subagent_slices: tuple[SubagentStateSlice, ...] = (),
) -> CognitiveStateSchema:
    """Compile exact packs into one collision-checked, content-addressed state schema."""

    if not packs:
        raise ValueError("cognitive state composition requires at least one exact pack")
    channels: dict[str, CognitiveChannelDefinition] = {}
    for pack in packs:
        for channel in pack.channels:
            prior = channels.get(channel.name)
            if prior is not None and prior != channel:
                raise ValueError(f"cognitive state channel collision: {channel.name}")
            channels[channel.name] = channel
    ordered = tuple(channels[name] for name in sorted(channels))
    refs = tuple(
        CognitiveChannelPackRef(
            logical_id=pack.logical_id,
            revision=pack.revision,
            digest=pack.digest,
            contributor=pack.contributor,
        )
        for pack in packs
    )
    reducer_registry_digest = sha256_digest(
        {channel.name: channel.reducer.value for channel in ordered}
    )
    return CognitiveStateSchema.create(
        schema_id=schema_id,
        pack_refs=refs,
        channels=ordered,
        reducer_registry_digest=reducer_registry_digest,
        subagent_slices=subagent_slices,
    )


def compose_cognitive_context_schema(
    *,
    schema_id: str,
    packs: tuple[CognitiveRuntimeContextPack, ...],
    subagent_slices: tuple[SubagentContextSlice, ...] = (),
) -> CognitiveRuntimeContextSchema:
    """Compile immutable runtime-context packs without alias or convenience inheritance."""

    if not packs:
        raise ValueError("cognitive context composition requires at least one exact pack")
    fields: dict[str, CognitiveRuntimeField] = {}
    for pack in packs:
        for field in pack.fields:
            prior = fields.get(field.name)
            if prior is not None and prior != field:
                raise ValueError(f"cognitive runtime-context field collision: {field.name}")
            fields[field.name] = field
    refs = tuple(
        CognitiveChannelPackRef(
            logical_id=pack.logical_id,
            revision=pack.revision,
            digest=pack.digest,
            contributor=pack.contributor,
        )
        for pack in packs
    )
    return CognitiveRuntimeContextSchema.create(
        schema_id=schema_id,
        pack_refs=refs,
        fields=tuple(fields[name] for name in sorted(fields)),
        subagent_slices=subagent_slices,
    )


def compile_deep_agent_execution_binding(
    *,
    profile: DeepAgentProfile,
    placement: DeepAgentExecutionPlacementProfile,
    state_schema: CognitiveStateSchema,
    context_schema: CognitiveRuntimeContextSchema,
    context_values: dict[str, object],
    run_id: str,
    operation_id: str,
    operation_attempt: int,
    execution_generation: int,
    erc_digest: str,
    control_revision: int,
    workspace: WorkspaceContract,
    capability_grant: CapabilityGrant,
    reservation_id: str,
    authority_refs: tuple[str, ...],
    redaction_policy_ref: str,
    initial_context_manifest: dict[str, object],
    initial_artifact_index: dict[str, object] | None = None,
    initial_child_result_index: tuple[dict[str, object], ...] = (),
    applied_degradations: tuple[str, ...] = (),
) -> DeepAgentExecutionBinding:
    """Flatten one logical profile and placement into the exact operation binding."""

    if placement.logical_id not in profile.compatible_placement_ids:
        raise ValueError("Deep Agent profile is not compatible with the selected placement")
    if placement.placement != "local_in_worker":
        raise ValueError("WP-CP-040 supports only local-in-worker Deep Agent placement")
    if profile.async_subagent_policy_refs and not profile.async_subagents:
        raise ValueError("async subagent policy refs require exact compiled contracts")
    if profile.sandbox.backend not in placement.sandbox_backends:
        raise ValueError("Deep Agent sandbox is incompatible with the selected placement")
    if tuple(profile.cognitive_state_pack_refs) != tuple(state_schema.pack_refs):
        raise ValueError("profile state pack refs drift from the effective cognitive schema")
    if tuple(profile.cognitive_context_pack_refs) != tuple(context_schema.pack_refs):
        raise ValueError("profile context pack refs drift from the effective cognitive schema")
    middleware_orders = [item.order for item in profile.middleware]
    if middleware_orders != list(range(len(middleware_orders))):
        raise ValueError("middleware positions must be contiguous from zero")
    tool_names = [item.tool_name for item in profile.tools]
    mcp_tool_names = [tool.tool_name for server in profile.mcp_servers for tool in server.tools]
    if len(tool_names + mcp_tool_names) != len(set(tool_names + mcp_tool_names)):
        raise ValueError("Deep Agent tool/MCP namespace collision")
    parent_tool_refs = {item.ref for item in profile.tools}
    parent_skill_refs = {item.ref for item in profile.skills}
    state_slices = {item.slice_id for item in state_schema.subagent_slices}
    context_slices = {item.slice_id for item in context_schema.subagent_slices}
    for child in profile.sync_subagents:
        if not set(child.tool_refs) <= parent_tool_refs:
            raise ValueError("synchronous subagent tools exceed the parent profile ceiling")
        if not set(child.skill_refs) <= parent_skill_refs:
            raise ValueError("synchronous subagent Skills exceed the parent profile ceiling")
        if child.state_slice_id not in state_slices or child.context_slice_id not in context_slices:
            raise ValueError("synchronous subagent schema projection is not exact")
        if any(
            amount > profile.delegation_ceiling.budget_limits.get(dimension, -1)
            for dimension, amount in child.budget_limits.items()
        ):
            raise ValueError("synchronous subagent budget exceeds the delegation ceiling")
        if set(child.writable_paths) & set(workspace.exclusive_write_paths):
            raise ValueError("synchronous subagent cannot inherit parent writable slots")

    attachments = [
        DeepAgentAttachmentRecord(
            component_kind="model",
            component_digest=profile.model.ref.digest,
            attachment_target="agent.main",
        ),
        DeepAgentAttachmentRecord(
            component_kind="sandbox",
            component_digest=profile.sandbox.ref.digest,
            attachment_target="agent.main.backend",
        ),
    ]
    attachments.extend(
        DeepAgentAttachmentRecord(
            component_kind="middleware",
            component_digest=item.ref.digest,
            attachment_target=f"agent.main.middleware.{item.order}",
        )
        for item in profile.middleware
    )
    attachments.extend(
        DeepAgentAttachmentRecord(
            component_kind="tool",
            component_digest=item.ref.digest,
            attachment_target=item.attachment_target,
        )
        for item in profile.tools
    )
    attachments.extend(
        DeepAgentAttachmentRecord(
            component_kind="mcp",
            component_digest=item.ref.digest,
            attachment_target=item.attachment_target,
        )
        for item in profile.mcp_servers
    )
    attachments.extend(
        DeepAgentAttachmentRecord(
            component_kind="skill",
            component_digest=item.bundle_digest,
            attachment_target=item.attachment_target,
        )
        for item in profile.skills
    )
    binding_id = str(
        uuid5(
            NAMESPACE_URL,
            f"deep-agent-binding:{run_id}:{operation_id}:{operation_attempt}",
        )
    )
    return DeepAgentExecutionBinding.create(
        binding_id=binding_id,
        run_id=run_id,
        operation_id=operation_id,
        operation_attempt=operation_attempt,
        execution_generation=execution_generation,
        erc_digest=erc_digest,
        control_revision=control_revision,
        profile_id=profile.logical_id,
        profile_revision=profile.revision,
        profile_digest=profile.profile_digest,
        placement_id=placement.logical_id,
        placement_revision=placement.revision,
        placement_digest=placement.placement_digest,
        placement=placement.placement,
        task_queue=placement.task_queue,
        checkpoint_behavior=placement.checkpoint_behavior,
        cancellation_behavior=placement.cancellation_behavior,
        streaming_behavior=placement.streaming_behavior,
        message_injection_behavior=placement.message_injection_behavior,
        reconnect_behavior=placement.reconnect_behavior,
        model=profile.model,
        backend_ref=profile.backend_ref,
        store_ref=profile.store_ref,
        checkpointer_ref=profile.checkpointer_ref,
        middleware=profile.middleware,
        tools=profile.tools,
        mcp_servers=profile.mcp_servers,
        skills=profile.skills,
        sandbox=profile.sandbox,
        sync_subagents=profile.sync_subagents,
        async_subagents=profile.async_subagents,
        cognitive_state_schema=state_schema,
        cognitive_context_schema=context_schema,
        cognitive_context_values=context_values,
        initial_artifact_index=initial_artifact_index or {},
        initial_context_manifest=initial_context_manifest,
        initial_child_result_index=initial_child_result_index,
        workspace=workspace,
        capability_grant=capability_grant,
        reservation_id=reservation_id,
        authority_refs=authority_refs,
        redaction_policy_ref=redaction_policy_ref,
        package_versions=placement.package_versions,
        intended_attachments=tuple(attachments),
        applied_degradations=applied_degradations,
    )


def workspace_manifest_digest(
    manifest: WorkspaceMaterializationManifest,
) -> str:
    return sha256_digest(
        {
            "namespace_id": manifest.namespace_id,
            "workspace_id": manifest.workspace_id,
            "revision": manifest.revision,
            "template_ref": manifest.template_ref.model_dump(mode="json"),
            "workflow_contract_digest": manifest.workflow_contract_digest,
            "slots": [slot.model_dump(mode="json") for slot in manifest.slots],
            "entries": [entry.model_dump(mode="json") for entry in manifest.entries],
            "prior_manifest_digest": manifest.prior_manifest_digest,
        }
    )


def verify_workspace_manifest(
    manifest: WorkspaceMaterializationManifest,
) -> None:
    if workspace_manifest_digest(manifest) != manifest.manifest_digest:
        raise WorkspaceDigestMismatch("workspace materialization manifest digest is invalid")
    if (manifest.revision == 1) != (manifest.prior_manifest_digest is None):
        raise WorkspaceDigestMismatch("workspace materialization manifest lineage is invalid")
