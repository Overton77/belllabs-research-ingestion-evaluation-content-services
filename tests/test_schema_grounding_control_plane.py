from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.domain.control_plane.contracts import (
    CompilationContext,
    CompileInvocation,
    DefinitionKind,
    DefinitionSelector,
    EnvironmentAvailability,
    PublishRequest,
    RunInputManifestRef,
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
    for definition in schema_grounding_definitions():
        record = await service.publish(
            PublishRequest(
                definition=definition,
                actor_id="schema-definition-publisher",
                published_at=NOW,
                expected_head_revision=0,
            )
        )
        published[definition.logical_id] = record

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
