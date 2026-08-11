from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.application.runtime.runtime_lineage import (
    InMemoryExecutionLineageRepository,
    PersistedExecutionLineage,
)
from app.domain.graph_runtime.definitions import ExecutionLineageEnvelope
from app.domain.graph_runtime.kernel import (
    LineageKind,
    LineageParentEdge,
    ProviderQualifiedLineageRecord,
)
from app.domain.run_control.errors import IdempotencyConflict

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def envelope(
    *,
    runtime_attempt_id: str,
    parent_lineage_id: str | None = None,
    result_manifest_ref: str | None = None,
) -> ExecutionLineageEnvelope:
    return ExecutionLineageEnvelope(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        execution_epoch=1,
        workflow_implementation_ref="workflow-implementation:1",
        graph_assembly_digest=DIGEST_A,
        workflow_cycle=0,
        stage_id="selection",
        stage_cycle=0,
        semantic_operation_attempt_id="semantic-attempt-1",
        runtime_attempt_id=runtime_attempt_id,
        operation_binding_id="operation-binding-1",
        operation_assembly_digest=DIGEST_A,
        parent_lineage_id=parent_lineage_id,
        result_manifest_ref=result_manifest_ref,
        evidence_refs=("evidence:1",),
        usage_settlement_refs=("settlement:1",),
    )


def identity(kind: LineageKind, provider_identity: str) -> ProviderQualifiedLineageRecord:
    return ProviderQualifiedLineageRecord(
        kind=kind,
        provider="belllabs" if kind != LineageKind.AGENT_RUN else "langgraph",
        provider_identity=provider_identity,
        request_scope="tenant-1",
        canonical_digest=DIGEST_A if kind != LineageKind.AGENT_RUN else DIGEST_B,
    )


def lineage(
    lineage_id: str,
    runtime_attempt_id: str,
    *,
    parent_lineage_id: str | None = None,
    result_manifest_ref: str | None = None,
    recorded_at: datetime = NOW,
    complete_identity_chain: bool = False,
) -> PersistedExecutionLineage:
    identities = (
        tuple(identity(kind, f"{kind.value}-1") for kind in LineageKind)
        if complete_identity_chain
        else (
            identity(LineageKind.BELL_LABS_RUN, "run-1"),
            identity(LineageKind.RUNTIME_ATTEMPT, runtime_attempt_id),
        )
    )
    edges = (
        tuple(
            LineageParentEdge(child=child, parent=parent, relationship="contains")
            for parent, child in zip(identities, identities[1:], strict=False)
        )
        if complete_identity_chain
        else ()
    )
    return PersistedExecutionLineage.create(
        lineage_id=lineage_id,
        envelope=envelope(
            runtime_attempt_id=runtime_attempt_id,
            parent_lineage_id=parent_lineage_id,
            result_manifest_ref=result_manifest_ref,
        ),
        qualified_identities=identities,
        parent_edges=edges,
        recorded_at=recorded_at,
        retain_until=recorded_at + timedelta(days=90),
    )


@pytest.mark.asyncio
async def test_final_result_provenance_reconstructs_semantic_and_runtime_retries() -> None:
    repository = InMemoryExecutionLineageRepository()
    root = lineage("lineage-1", "runtime-attempt-1")
    retry = lineage(
        "lineage-2",
        "runtime-attempt-2",
        parent_lineage_id=root.lineage_id,
        result_manifest_ref="result:accepted",
        recorded_at=NOW + timedelta(seconds=1),
        complete_identity_chain=True,
    )
    await repository.append(root)
    await repository.append(retry)

    provenance = await repository.provenance_for_result(
        "tenant-1",
        "result:accepted",
    )

    assert [item.lineage_id for item in provenance] == ["lineage-1", "lineage-2"]
    assert {item.envelope.semantic_operation_attempt_id for item in provenance} == {
        "semantic-attempt-1"
    }
    assert {item.envelope.runtime_attempt_id for item in provenance} == {
        "runtime-attempt-1",
        "runtime-attempt-2",
    }
    assert {item.kind for item in provenance[-1].qualified_identities} == set(LineageKind)
    assert len(provenance[-1].parent_edges) == len(LineageKind) - 1


@pytest.mark.asyncio
async def test_lineage_replay_is_idempotent_and_identity_collision_fails_closed() -> None:
    repository = InMemoryExecutionLineageRepository()
    original = lineage("lineage-1", "runtime-attempt-1")

    assert await repository.append(original) == await repository.append(original)
    conflicting = lineage("lineage-1", "runtime-attempt-other")
    with pytest.raises(IdempotencyConflict, match="conflicting facts"):
        await repository.append(conflicting)


@pytest.mark.asyncio
async def test_child_cannot_be_persisted_before_parent_or_cross_scope() -> None:
    repository = InMemoryExecutionLineageRepository()
    child = lineage(
        "lineage-2",
        "runtime-attempt-2",
        parent_lineage_id="missing-parent",
    )

    with pytest.raises(ValueError, match="parent must be persisted"):
        await repository.append(child)
    with pytest.raises(LookupError, match="no persisted lineage"):
        await repository.provenance_for_result("tenant-2", "result:accepted")
