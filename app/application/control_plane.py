from __future__ import annotations

import json
from dataclasses import asdict
from typing import TypeVar

from pydantic import TypeAdapter

from app.application.control_plane_repository import DefinitionRepository
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.compiler import compile_effective_run_configuration
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    AliasBinding,
    AliasRef,
    AuthoringHead,
    CapabilityDefinition,
    CompilationRequest,
    CompileInvocation,
    ControlProfileDefinition,
    DeepAgentPlacementDefinition,
    Definition,
    DefinitionKind,
    DefinitionSelector,
    EffectiveRunConfiguration,
    EvaluationProfileDefinition,
    ExactDefinitionRef,
    GoalDirectedBlueprint,
    MCPServerDefinition,
    MCPToolDefinition,
    MoveAliasRequest,
    PublishDraftRequest,
    PublishedDefinition,
    PublishRequest,
    ResolvedDefinitions,
    RetireRequest,
    RuntimeProfileDefinition,
    SaveDraftRequest,
    SkillDefinition,
    StageGraphBlueprint,
    WorkflowConfigurationDefinition,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
    WorkspaceTemplateDefinition,
)
from app.domain.control_plane.errors import (
    CompilationRejected,
    DefinitionConflict,
    PayloadIntegrityError,
    RetiredDefinition,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.integrations.control_plane_payloads import (
    ContentAddress,
    ContentAddressedPayloadStore,
)

ERC_ADAPTER = TypeAdapter(EffectiveRunConfiguration)
DefinitionT = TypeVar("DefinitionT")


class ControlPlaneService:
    def __init__(
        self,
        repository: DefinitionRepository,
        extension_registry: ExtensionRegistry,
        payload_store: ContentAddressedPayloadStore,
        *,
        externalize_above_bytes: int = 256_000,
    ) -> None:
        self._repository = repository
        self._extensions = extension_registry
        self._payload_store = payload_store
        self._externalize_above_bytes = externalize_above_bytes

    async def publish(self, request: PublishRequest) -> PublishedDefinition:
        await self._validate_publication(request.definition)
        return await self._repository.publish(
            request.definition,
            request.actor_id,
            request.published_at,
            request.expected_head_revision,
        )

    async def save_draft(self, request: SaveDraftRequest) -> AuthoringHead:
        await self._validate_definition_shape(request.definition)
        return await self._repository.save_draft(
            request.definition,
            request.actor_id,
            request.updated_at,
            request.expected_draft_revision,
        )

    async def get_draft(self, kind: str, logical_id: str) -> AuthoringHead:
        return await self._repository.get_draft(kind, logical_id)

    async def publish_draft(self, request: PublishDraftRequest) -> PublishedDefinition:
        head = await self._repository.get_draft(request.kind.value, request.logical_id)
        if head.draft_revision != request.expected_draft_revision:
            raise DefinitionConflict(
                f"expected draft revision {request.expected_draft_revision}, "
                f"current revision is {head.draft_revision}"
            )
        await self._validate_publication(head.definition)
        return await self._repository.publish(
            head.definition,
            request.actor_id,
            request.published_at,
            request.expected_published_revision,
            request.expected_draft_revision,
        )

    async def move_alias(self, request: MoveAliasRequest) -> AliasBinding:
        return await self._repository.move_alias(
            request.alias, request.target, request.actor_id, request.moved_at
        )

    async def resolve_alias(self, alias: AliasRef) -> AliasBinding:
        return await self._repository.resolve(alias)

    async def retire(self, request: RetireRequest) -> PublishedDefinition:
        return await self._repository.retire(request.ref, request.actor_id, request.retired_at)

    async def compile(self, invocation: CompileInvocation) -> EffectiveRunConfiguration:
        workflow_ref, workflow_alias = await self._resolve_selector(invocation.workflow_type)
        evidence: list[AliasBinding] = []
        if workflow_alias is not None:
            evidence.append(workflow_alias)
        workflow_record = await self._selectable(workflow_ref)
        workflow_type = self._expect(
            workflow_record.definition,
            WorkflowTypeDefinition,
        )

        component_selectors = (
            invocation.blueprint,
            invocation.control_profile,
            invocation.runtime_profile,
            invocation.workspace_template,
            invocation.evaluation_profile,
        )
        implementation_ref: ExactDefinitionRef | None = None
        implementation: WorkflowImplementationBindingDefinition | None = None
        if all(selector is None for selector in component_selectors):
            implementation_selector = invocation.implementation
            if implementation_selector is None:
                implementation_selector = DefinitionSelector(
                    alias=AliasRef(
                        kind=DefinitionKind.WORKFLOW_IMPLEMENTATION,
                        logical_id=f"{workflow_type.logical_id}.implementation",
                        alias="default",
                    )
                )
            implementation_ref, alias = await self._resolve_selector(implementation_selector)
            if alias is not None:
                evidence.append(alias)
            implementation_record = await self._selectable(implementation_ref)
            implementation = self._expect(
                implementation_record.definition,
                WorkflowImplementationBindingDefinition,
            )
            refs = [
                workflow_ref,
                implementation_ref,
                implementation.blueprint_ref,
                implementation.control_profile_ref,
                implementation.runtime_profile_ref,
                implementation.workspace_template_ref,
                implementation.evaluation_profile_ref,
            ]
            if implementation.workflow_configuration_ref is not None:
                refs.append(implementation.workflow_configuration_ref)
        else:
            selectors = tuple(selector for selector in component_selectors if selector is not None)
            refs = [workflow_ref]
            for selector in selectors:
                ref, alias = await self._resolve_selector(selector)
                refs.append(ref)
                if alias is not None:
                    evidence.append(alias)
            if invocation.workflow_configuration is not None:
                ref, alias = await self._resolve_selector(invocation.workflow_configuration)
                refs.append(ref)
                if alias is not None:
                    evidence.append(alias)

        published = [await self._selectable(ref) for ref in refs]
        offset = 2 if implementation is not None else 1
        blueprint = published[offset].definition
        if not isinstance(blueprint, StageGraphBlueprint | GoalDirectedBlueprint):
            raise CompilationRejected(
                f"expected workflow blueprint, got {type(blueprint).__name__}"
            )
        runtime_profile = self._expect(
            published[offset + 2].definition,
            RuntimeProfileDefinition,
        )
        core_count = len(published)
        runtime_dependencies = await self._resolve_runtime_dependencies(runtime_profile)
        known_refs = {record.ref for record in published}
        published.extend(record for record in runtime_dependencies if record.ref not in known_refs)
        definitions = ResolvedDefinitions(
            workflow_type=workflow_type,
            implementation_binding=implementation,
            blueprint=blueprint,
            control_profile=self._expect(
                published[offset + 1].definition,
                ControlProfileDefinition,
            ),
            runtime_profile=runtime_profile,
            workspace_template=self._expect(
                published[offset + 3].definition,
                WorkspaceTemplateDefinition,
            ),
            evaluation_profile=self._expect(
                published[offset + 4].definition,
                EvaluationProfileDefinition,
            ),
            workflow_configuration=(
                self._expect(
                    published[offset + 5].definition,
                    WorkflowConfigurationDefinition,
                )
                if core_count == offset + 6
                else None
            ),
            published_records=tuple(published),
            agent_profiles=tuple(
                record.definition
                for record in published
                if isinstance(record.definition, AgentProfileDefinition)
            ),
        )
        allowed_overlay_extensions = {
            (item.namespace, item.schema_version, item.discriminator)
            for item in workflow_type.allowed_overlay_extensions
        }
        for extension in invocation.overlay.extensions:
            identity = (
                extension.namespace,
                extension.schema_version,
                extension.discriminator,
            )
            if identity not in allowed_overlay_extensions:
                raise CompilationRejected(
                    f"overlay extension is not allowed by the Workflow Type: {identity}"
                )
        validated_overlay = invocation.overlay.model_copy(
            update={"extensions": self._extensions.validate_all(invocation.overlay.extensions)}
        )
        request = CompilationRequest(
            workflow_type_ref=workflow_ref,
            implementation_ref=implementation_ref,
            blueprint_ref=refs[offset],
            control_profile_ref=refs[offset + 1],
            runtime_profile_ref=refs[offset + 2],
            workspace_template_ref=refs[offset + 3],
            evaluation_profile_ref=refs[offset + 4],
            workflow_configuration_ref=(refs[offset + 5] if len(refs) == offset + 6 else None),
            input_manifest=invocation.input_manifest,
            overlay=validated_overlay,
            caller_authority=invocation.caller_authority,
            parent_authority=invocation.parent_authority,
            environment=invocation.environment,
            context=invocation.context,
            alias_evidence=tuple(evidence),
        )
        erc = compile_effective_run_configuration(request, definitions)
        await self._persist_erc(erc)
        return erc

    async def retrieve(self, digest: str) -> EffectiveRunConfiguration:
        record = await self._repository.get_erc_record(digest)
        if record.get("payload") is not None:
            erc = ERC_ADAPTER.validate_python(record["payload"])
        else:
            raw_ref = record.get("payload_ref")
            if not isinstance(raw_ref, dict):
                raise PayloadIntegrityError("ERC record has neither inline nor external payload")
            address = ContentAddress(**raw_ref)
            payload = await self._payload_store.retrieve(address)
            erc = ERC_ADAPTER.validate_json(payload)
        if erc.digest != digest:
            raise PayloadIntegrityError("ERC lookup digest does not match payload digest")
        digest_payload = {
            name: getattr(erc, name) for name in type(erc).model_fields if name != "digest"
        }
        actual = sha256_digest(digest_payload)
        if actual != erc.digest:
            raise PayloadIntegrityError(
                f"ERC payload digest mismatch: expected {erc.digest}, got {actual}"
            )
        return erc

    async def retrieve_for_admission(self, digest: str) -> EffectiveRunConfiguration:
        """Verify an immutable ERC and that every exact source remains admissible."""
        erc = await self.retrieve(digest)
        for ref in erc.source_refs:
            await self._selectable(ref)
        return erc

    async def _validate_publication(self, definition: Definition) -> None:
        await self._validate_definition_shape(definition)
        if isinstance(definition, CapabilityDefinition):
            # Revalidate at the publication boundary so model_copy/ORM-created instances
            # cannot bypass the definition-kind/capability-family invariant.
            CapabilityDefinition.model_validate(definition.model_dump(mode="python"))
        if isinstance(definition, WorkflowTypeDefinition):
            self._extensions.validate_all(definition.required_extensions)
            refs = (
                definition.allowed_blueprints
                | definition.allowed_control_profiles
                | definition.allowed_runtime_profiles
                | definition.allowed_workspace_templates
                | definition.allowed_evaluation_profiles
                | definition.allowed_workflow_configurations
            )
            refs |= frozenset(
                child_ref
                for slot in definition.linked_run_slots
                for child_ref in slot.allowed_child_workflow_types
            )
            for ref in refs:
                published = await self._selectable(ref)
                if ref in definition.allowed_workflow_configurations:
                    workflow_configuration = self._expect(
                        published.definition, WorkflowConfigurationDefinition
                    )
                    if workflow_configuration.workflow_type_logical_id != definition.logical_id:
                        raise CompilationRejected(
                            "allowed workflow-specific configuration targets "
                            "a different Workflow Type"
                        )
        elif isinstance(definition, ControlProfileDefinition):
            blueprint = await self._selectable(definition.blueprint_ref)
            if not isinstance(blueprint.definition, StageGraphBlueprint | GoalDirectedBlueprint):
                raise CompilationRejected("control profile target is not a blueprint")
        elif isinstance(definition, WorkflowImplementationBindingDefinition):
            await self._validate_implementation_publication(definition)
        elif isinstance(definition, WorkflowConfigurationDefinition):
            self._extensions.validate_all(definition.extensions)
        elif isinstance(definition, SkillDefinition):
            actual_manifest_digest = sha256_digest(definition.file_manifest)
            if definition.manifest_digest != actual_manifest_digest:
                raise CompilationRejected(
                    "Skill Definition manifest digest does not match its exact file manifest"
                )
        elif isinstance(definition, MCPServerDefinition):
            if definition.schema_digest != definition.schema_snapshot_ref.digest:
                raise CompilationRejected(
                    "MCP Server schema digest does not match its immutable schema snapshot"
                )
        elif isinstance(definition, MCPToolDefinition):
            server_record = await self._selectable(definition.server_ref)
            server = self._expect(server_record.definition, MCPServerDefinition)
            if definition.tool_name not in server.allowed_tools:
                raise CompilationRejected(
                    "MCP Tool is not present in the exact parent server allowlist"
                )
            schema_payload = {
                "tool_name": definition.tool_name,
                "input_schema": definition.input_schema,
                "output_schema": definition.output_schema,
                "annotations": definition.annotations,
            }
            if definition.schema_digest != sha256_digest(schema_payload):
                raise CompilationRejected(
                    "MCP Tool schema digest does not match its frozen schema payload"
                )
        elif isinstance(definition, AgentProfileDefinition):
            records = [
                await self._selectable(ref)
                for ref in (
                    *sorted(
                        definition.prompt_refs,
                        key=lambda ref: (ref.logical_id, ref.revision),
                    ),
                    *sorted(
                        definition.skill_refs,
                        key=lambda ref: (ref.logical_id, ref.revision),
                    ),
                    *sorted(
                        definition.mcp_server_refs,
                        key=lambda ref: (ref.logical_id, ref.revision),
                    ),
                    *sorted(
                        definition.tool_refs,
                        key=lambda ref: (ref.logical_id, ref.revision),
                    ),
                )
            ]
            selected_servers = set(definition.mcp_server_refs)
            for record in records:
                if isinstance(record.definition, MCPToolDefinition):
                    if record.definition.server_ref not in selected_servers:
                        raise CompilationRejected(
                            "Agent Profile tool selection requires its exact parent MCP Server"
                        )
            await self._resolve_profile_dependencies(definition)
        elif isinstance(definition, RuntimeProfileDefinition):
            await self._resolve_runtime_dependencies(definition)
        elif isinstance(definition, DeepAgentPlacementDefinition):
            await self._selectable(definition.sandbox_ref)

    async def _validate_definition_shape(self, definition: Definition) -> None:
        if isinstance(definition, WorkflowTypeDefinition):
            self._extensions.validate_all(definition.required_extensions)

    async def _validate_implementation_publication(
        self,
        binding: WorkflowImplementationBindingDefinition,
    ) -> None:
        records = [
            await self._selectable(ref)
            for ref in (
                binding.workflow_type_ref,
                binding.blueprint_ref,
                binding.control_profile_ref,
                binding.runtime_profile_ref,
                binding.workspace_template_ref,
                binding.evaluation_profile_ref,
            )
        ]
        workflow = self._expect(records[0].definition, WorkflowTypeDefinition)
        blueprint = records[1].definition
        if not isinstance(blueprint, StageGraphBlueprint | GoalDirectedBlueprint):
            raise CompilationRejected("Workflow Implementation target is not a blueprint")
        control = self._expect(records[2].definition, ControlProfileDefinition)
        runtime = self._expect(records[3].definition, RuntimeProfileDefinition)
        workspace = self._expect(records[4].definition, WorkspaceTemplateDefinition)
        evaluation = self._expect(records[5].definition, EvaluationProfileDefinition)
        if control.blueprint_ref != binding.blueprint_ref:
            raise CompilationRejected(
                "Workflow Implementation control profile selects a different blueprint"
            )
        if binding.workflow_configuration_ref is not None:
            configuration_record = await self._selectable(binding.workflow_configuration_ref)
            configuration = self._expect(
                configuration_record.definition,
                WorkflowConfigurationDefinition,
            )
            if configuration.workflow_type_logical_id != workflow.logical_id:
                raise CompilationRejected(
                    "Workflow Implementation configuration targets a different Workflow Type"
                )
        realized_obligations = {item.obligation_ref for item in binding.obligation_realizations}
        if not workflow.obligations <= realized_obligations:
            raise CompilationRejected(
                "Workflow Implementation does not realize every Workflow Type obligation"
            )
        realized_outputs = {
            item.output_contract_ref for item in binding.output_contract_realizations
        }
        if not workflow.output_contracts <= realized_outputs:
            raise CompilationRejected(
                "Workflow Implementation does not realize every Workflow Type output"
            )
        contract_slots = {slot.name: slot for slot in workflow.workspace_contract.slots}
        template_slots = {slot.name: slot for slot in workspace.slots}
        if any(
            template_slots.get(name) != contract_slot
            for name, contract_slot in contract_slots.items()
        ):
            raise CompilationRejected(
                "Workflow Implementation workspace does not satisfy the Workflow Type contract"
            )
        required_capabilities = (
            runtime.required_capabilities
            | workspace.required_capabilities
            | evaluation.required_capabilities
        )
        if not required_capabilities <= workflow.authority_ceiling.capabilities:
            raise CompilationRejected(
                "Workflow Implementation requirements exceed Workflow Type authority"
            )

    async def _resolve_selector(
        self, selector: DefinitionSelector
    ) -> tuple[ExactDefinitionRef, AliasBinding | None]:
        if selector.exact is not None:
            return selector.exact, None
        assert selector.alias is not None
        binding = await self._repository.resolve(selector.alias)
        return binding.target, binding

    async def _resolve_profile_dependencies(
        self,
        profile: AgentProfileDefinition,
        *,
        seen: frozenset[ExactDefinitionRef] = frozenset(),
    ) -> tuple[PublishedDefinition, ...]:
        refs = set(profile.parent_profile_refs)
        refs.update(profile.prompt_refs)
        refs.update(profile.skill_refs)
        refs.update(profile.mcp_server_refs)
        refs.update(profile.tool_refs)
        refs.update(component.ref for component in profile.components)
        refs.update(profile.middleware_refs)
        refs.update(
            requirement_ref
            for requirement in profile.capability_requirements
            for requirement_ref in requirement.allowed_refs
        )
        refs.update(
            requirement.degraded_ref
            for requirement in profile.capability_requirements
            if requirement.degraded_ref is not None
        )
        refs.update(
            ref for ref in (profile.model_ref, profile.sandbox_profile_ref) if ref is not None
        )
        records: list[PublishedDefinition] = []
        for ref in sorted(
            refs - seen, key=lambda item: (item.kind.value, item.logical_id, item.revision)
        ):
            record = await self._selectable(ref)
            records.append(record)
            if isinstance(record.definition, AgentProfileDefinition):
                records.extend(
                    await self._resolve_profile_dependencies(
                        record.definition,
                        seen=seen | frozenset({ref}),
                    )
                )
        unique = {record.ref: record for record in records}
        return tuple(
            unique[ref]
            for ref in sorted(
                unique, key=lambda item: (item.kind.value, item.logical_id, item.revision)
            )
        )

    async def _resolve_runtime_dependencies(
        self, runtime: RuntimeProfileDefinition
    ) -> tuple[PublishedDefinition, ...]:
        records: list[PublishedDefinition] = []
        for assembly in sorted(runtime.operation_assemblies, key=lambda item: item.assembly_id):
            profile_record = await self._selectable(assembly.deep_agent_profile_ref)
            profile = self._expect(profile_record.definition, AgentProfileDefinition)
            placement_record = await self._selectable(assembly.placement_ref)
            placement = self._expect(
                placement_record.definition,
                DeepAgentPlacementDefinition,
            )
            records.extend(
                (profile_record, placement_record, await self._selectable(placement.sandbox_ref))
            )
            records.extend(await self._resolve_profile_dependencies(profile))
            capability_refs = {
                ref
                for requirement in assembly.capability_requirements
                for ref in requirement.allowed_refs
            }
            capability_refs.update(
                requirement.degraded_ref
                for requirement in assembly.capability_requirements
                if requirement.degraded_ref is not None
            )
            records.extend(
                [
                    await self._selectable(ref)
                    for ref in sorted(
                        capability_refs,
                        key=lambda item: (item.kind.value, item.logical_id, item.revision),
                    )
                ]
            )
        unique = {record.ref: record for record in records}
        return tuple(
            unique[ref]
            for ref in sorted(
                unique, key=lambda item: (item.kind.value, item.logical_id, item.revision)
            )
        )

    async def _selectable(self, ref: ExactDefinitionRef) -> PublishedDefinition:
        definition = await self._repository.get(ref)
        if definition.retired_at is not None:
            raise RetiredDefinition(f"retired definition cannot be selected: {ref}")
        return definition

    @staticmethod
    def _expect(definition: Definition, expected: type[DefinitionT]) -> DefinitionT:
        if not isinstance(definition, expected):
            raise CompilationRejected(
                f"expected {expected.__name__}, got {type(definition).__name__}"
            )
        return definition

    async def _persist_erc(self, erc: EffectiveRunConfiguration) -> None:
        payload = json.dumps(
            erc.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
        record: dict[str, object] = {
            "contract_id": "CON-CP-ERC-V1",
            "schema_version": erc.schema_version,
            "digest": erc.digest,
            "compiler_version": erc.compiler_version,
            "compilation_id": erc.context.compilation_id,
            "compiled_at": erc.context.compiled_at,
            "payload": erc.model_dump(mode="json"),
            "payload_ref": None,
        }
        if len(payload) > self._externalize_above_bytes:
            address = await self._payload_store.put(payload)
            record["payload"] = None
            record["payload_ref"] = asdict(address)
        await self._repository.save_erc_record(record)
