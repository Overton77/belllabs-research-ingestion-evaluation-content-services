from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.control_plane.service import ControlPlaneService
from app.application.control_plane.control_plane_repository import InMemoryDefinitionRepository
from app.domain.control_plane.contracts import (
    AliasRef,
    CompilationContext,
    CompileInvocation,
    DefinitionKind,
    DefinitionSelector,
    EnvironmentAvailability,
    MoveAliasRequest,
    PublishRequest,
    RunInputManifestRef,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.schema_grounding.definitions import (
    register_schema_grounding_extensions,
    schema_grounding_definitions,
)
from app.integrations.control_plane_payloads import InMemoryPayloadStore

NOW = datetime(2026, 7, 24, 15, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_schema_grounding_definitions_publish_and_compile_exact_selection_erc() -> None:
    repository = InMemoryDefinitionRepository()
    extensions = ExtensionRegistry()
    register_schema_grounding_extensions(extensions)
    service = ControlPlaneService(repository, extensions, InMemoryPayloadStore())
    published = {}
    implementation_records = []
    revisions: dict[tuple[DefinitionKind, str], int] = {}
    for definition in schema_grounding_definitions():
        key = (definition.kind, definition.logical_id)
        record = await service.publish(
            PublishRequest(
                definition=definition,
                actor_id="schema-definition-publisher",
                published_at=NOW,
                expected_head_revision=revisions.get(key, 0),
            )
        )
        revisions[key] = record.ref.revision
        published[definition.logical_id] = record
        if isinstance(definition, WorkflowImplementationBindingDefinition):
            implementation_records.append(record)

    selection = published["schema-context-selection"].definition
    reconciliation = published["supporting-graph-reconciliation"].definition
    assert isinstance(selection, WorkflowTypeDefinition)
    assert isinstance(reconciliation, WorkflowTypeDefinition)
    assert reconciliation.linked_run_slots[0].allowed_child_workflow_types == frozenset(
        {published["schema-context-selection"].ref}
    )
    assert "Knowledge Preflight coverage" in reconciliation.non_goals

    erc = await service.compile(
        CompileInvocation(
            workflow_type=DefinitionSelector(
                exact=published["schema-context-selection"].ref
            ),
            blueprint=DefinitionSelector(
                exact=published["schema-context-selection-v1"].ref
            ),
            control_profile=DefinitionSelector(
                exact=published["schema-context-selection-control-v1"].ref
            ),
            runtime_profile=DefinitionSelector(
                exact=published["schema-context-selection-runtime-v1"].ref
            ),
            workspace_template=DefinitionSelector(
                exact=published["schema-context-selection-workspace-v1"].ref
            ),
            evaluation_profile=DefinitionSelector(
                exact=published["schema-context-selection-evaluation-v1"].ref
            ),
            workflow_configuration=DefinitionSelector(
                exact=published["schema-context-selection-official-v1"].ref
            ),
            input_manifest=RunInputManifestRef(
                manifest_id="schema-selection-input-1",
                revision=1,
                digest="sha256:" + "1" * 64,
            ),
            caller_authority=selection.authority_ceiling,
            environment=EnvironmentAvailability(
                capabilities=frozenset(
                    {"schema.catalog.read", "operation.execute.agent"}
                ),
                runtime_bindings=frozenset(
                    {"temporal-stagegraph+operation-execution"}
                ),
            ),
            context=CompilationContext(
                compilation_id="schema-selection-compilation-1",
                compiled_at=NOW,
                actor_id="operator-1",
                authority_subject_id="operator-1",
                authority_scope="tenant-1",
            ),
        )
    )

    assert erc.workflow_type.logical_id == "schema-context-selection"
    assert erc.selected_blueprint.logical_id == "schema-context-selection-v1"
    assert erc.workflow_specific_configuration is not None
    assert erc.workflow_specific_configuration.extensions[0].payload[
        "catalog_generator_version"
    ] == "typed-schema-catalog-v1"
    assert any(
        ref.kind == DefinitionKind.WORKFLOW_TYPE
        and ref.logical_id == "schema-context-selection"
        for ref in erc.source_refs
    )

    reconciliation_erc = await service.compile(
        CompileInvocation(
            workflow_type=DefinitionSelector(
                exact=published["supporting-graph-reconciliation"].ref
            ),
            blueprint=DefinitionSelector(
                exact=published["supporting-graph-reconciliation-v1"].ref
            ),
            control_profile=DefinitionSelector(
                exact=published["supporting-graph-reconciliation-control-v1"].ref
            ),
            runtime_profile=DefinitionSelector(
                exact=published["supporting-graph-reconciliation-runtime-v1"].ref
            ),
            workspace_template=DefinitionSelector(
                exact=published["supporting-graph-reconciliation-workspace-v1"].ref
            ),
            evaluation_profile=DefinitionSelector(
                exact=published["supporting-graph-reconciliation-evaluation-v1"].ref
            ),
            workflow_configuration=DefinitionSelector(
                exact=published[
                    "supporting-graph-reconciliation-official-v1"
                ].ref
            ),
            input_manifest=RunInputManifestRef(
                manifest_id="schema-reconciliation-input-1",
                revision=1,
                digest="sha256:" + "2" * 64,
            ),
            caller_authority=reconciliation.authority_ceiling,
            environment=EnvironmentAvailability(
                capabilities=frozenset(
                    {
                        "schema.catalog.read",
                        "schema.selection.read",
                        "schema.derivation.execute",
                        "schema.workspace.read",
                        "graph.read.bounded",
                        "operation.execute.agent",
                    }
                ),
                runtime_bindings=frozenset(
                    {
                        "temporal-stagegraph+operation-execution+neo4j-bounded-read"
                    }
                ),
            ),
            context=CompilationContext(
                compilation_id="schema-reconciliation-compilation-1",
                compiled_at=NOW,
                actor_id="operator-1",
                authority_subject_id="operator-1",
                authority_scope="tenant-1",
            ),
        )
    )

    assert (
        reconciliation_erc.workflow_type.logical_id
        == "supporting-graph-reconciliation"
    )
    assert reconciliation_erc.workflow_specific_configuration is not None
    assert (
        "schema:bounded-query-plan:v1"
        in reconciliation_erc.workflow_specific_configuration.extensions[
            0
        ].payload["output_schema_refs"]
    )

    selection_implementation = next(
        record
        for record in implementation_records
        if record.definition.workflow_type_ref
        == published["schema-context-selection"].ref
    )
    reconciliation_implementations = [
        record
        for record in implementation_records
        if record.definition.workflow_type_ref
        == published["supporting-graph-reconciliation"].ref
    ]
    staged_reconciliation = next(
        record
        for record in reconciliation_implementations
        if record.definition.blueprint_ref
        == published["supporting-graph-reconciliation-v1"].ref
    )
    goal_reconciliation = next(
        record
        for record in reconciliation_implementations
        if record.definition.blueprint_ref
        == published["supporting-graph-reconciliation-goal-directed-v1"].ref
    )
    for logical_id, alias, target in (
        ("schema-context-selection.implementation", "default", selection_implementation),
        (
            "supporting-graph-reconciliation.implementation",
            "default",
            staged_reconciliation,
        ),
        (
            "supporting-graph-reconciliation.implementation",
            "goal-directed",
            goal_reconciliation,
        ),
    ):
        await service.move_alias(
            MoveAliasRequest(
                alias=AliasRef(
                    kind=DefinitionKind.WORKFLOW_IMPLEMENTATION,
                    logical_id=logical_id,
                    alias=alias,
                ),
                target=target.ref,
                actor_id="schema-definition-publisher",
                moved_at=NOW,
            )
        )

    default_erc = await service.compile(
        CompileInvocation(
            workflow_type=DefinitionSelector(
                exact=published["supporting-graph-reconciliation"].ref
            ),
            input_manifest=RunInputManifestRef(
                manifest_id="schema-reconciliation-default-input",
                revision=1,
                digest="sha256:" + "3" * 64,
            ),
            caller_authority=reconciliation.authority_ceiling,
            environment=EnvironmentAvailability(
                capabilities=reconciliation.authority_ceiling.capabilities,
                runtime_bindings=frozenset(
                    {
                        "temporal-stagegraph+operation-execution+neo4j-bounded-read"
                    }
                ),
            ),
            context=CompilationContext(
                compilation_id="schema-reconciliation-default",
                compiled_at=NOW,
                actor_id="operator-1",
                authority_subject_id="operator-1",
                authority_scope="tenant-1",
            ),
        )
    )
    assert default_erc.selected_blueprint.family == "StageGraph"
    assert default_erc.source_refs[1] == staged_reconciliation.ref
    assert default_erc.alias_evidence[0].alias_ref.alias == "default"

    alternative_erc = await service.compile(
        CompileInvocation(
            workflow_type=DefinitionSelector(
                exact=published["supporting-graph-reconciliation"].ref
            ),
            implementation=DefinitionSelector(
                alias=AliasRef(
                    kind=DefinitionKind.WORKFLOW_IMPLEMENTATION,
                    logical_id="supporting-graph-reconciliation.implementation",
                    alias="goal-directed",
                )
            ),
            input_manifest=RunInputManifestRef(
                manifest_id="schema-reconciliation-goal-input",
                revision=1,
                digest="sha256:" + "4" * 64,
            ),
            caller_authority=reconciliation.authority_ceiling,
            environment=EnvironmentAvailability(
                capabilities=reconciliation.authority_ceiling.capabilities,
                runtime_bindings=frozenset(
                    {
                        "temporal-stagegraph+operation-execution+neo4j-bounded-read"
                    }
                ),
            ),
            context=CompilationContext(
                compilation_id="schema-reconciliation-goal",
                compiled_at=NOW,
                actor_id="operator-1",
                authority_subject_id="operator-1",
                authority_scope="tenant-1",
            ),
        )
    )
    assert alternative_erc.selected_blueprint.family == "GoalDirected"
    assert alternative_erc.source_refs[1] == goal_reconciliation.ref
    assert alternative_erc.alias_evidence[0].alias_ref.alias == "goal-directed"
