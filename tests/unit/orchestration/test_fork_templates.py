"""RRM-009 (RRM-006): a fork's StageGraph templates derive from the source's with the patch."""

from __future__ import annotations

from datetime import datetime

import pytest

from app.application.orchestration.fork_templates import (
    ForkPatchNotApplicable,
    StageGraphForkTemplateDerivation,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import OperationExecutionRequest
from app.domain.run_control.forks import ForkPatchChange, RunForkPatch, stage_objective_path
from tests.unit.operations.test_operation_execution import operation_request


class MemoryTemplates:
    def __init__(self) -> None:
        self.stored: dict[tuple[str, str], dict[str, OperationExecutionRequest]] = {}

    async def list_templates(
        self, *, request_scope: str, semantic_input_binding_ref: str
    ) -> dict[str, OperationExecutionRequest]:
        return dict(self.stored.get((request_scope, semantic_input_binding_ref), {}))

    async def persist_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
        templates: dict[str, OperationExecutionRequest],
        recorded_at: datetime,
    ) -> None:
        del recorded_at
        self.stored[(request_scope, semantic_input_binding_ref)] = dict(templates)


def _patch(*changes: tuple[str, str]) -> RunForkPatch:
    snapshot = "run-snapshot:" + "a" * 64
    return RunForkPatch.create(
        source_snapshot_id=snapshot,
        source_snapshot_digest="sha256:" + "b" * 64,
        target_admission_request_ref="run-request:tenant-1:operator:fork",
        changes=tuple(ForkPatchChange(path=path, value=value) for path, value in changes),
        invalidation_frontier=("review",),
    )


@pytest.mark.asyncio
async def test_patched_stage_gains_an_admitted_objective_and_the_rest_is_copied() -> None:
    store = MemoryTemplates()
    source = {
        "draft/execute/default": operation_request(prompt="draft"),
        "review/execute/default": operation_request(prompt="review"),
    }
    store.stored[("tenant-1", "semantic-input:source")] = source
    derivation = StageGraphForkTemplateDerivation(store)

    patched = await derivation.derive_stagegraph(
        request_scope="tenant-1",
        source_semantic_input_binding_ref="semantic-input:source",
        derived_semantic_input_binding_ref="semantic-input:fork:f1",
        patch=_patch((stage_objective_path("review"), "Review more strictly.")),
    )
    assert patched == ("review/execute/default",)
    derived = store.stored[("tenant-1", "semantic-input:fork:f1")]
    assert derived["draft/execute/default"] == source["draft/execute/default"]
    segment = derived["review/execute/default"].prompt_segments[-1]
    assert segment.source_ref == "fork-objective:review"
    assert segment.trust_class.value == "admitted_input"
    assert segment.content == "Review more strictly."
    assert segment.rendered_digest == sha256_digest("Review more strictly.")
    assert derived["review/execute/default"].prompt_segments[:-1] == (
        source["review/execute/default"].prompt_segments
    )
    # The source's templates are untouched.
    assert store.stored[("tenant-1", "semantic-input:source")] == source


@pytest.mark.asyncio
async def test_patch_naming_a_stage_without_a_template_is_refused() -> None:
    store = MemoryTemplates()
    store.stored[("tenant-1", "semantic-input:source")] = {
        "draft/execute/default": operation_request(prompt="draft")
    }
    derivation = StageGraphForkTemplateDerivation(store)
    with pytest.raises(ForkPatchNotApplicable, match="review"):
        await derivation.derive_stagegraph(
            request_scope="tenant-1",
            source_semantic_input_binding_ref="semantic-input:source",
            derived_semantic_input_binding_ref="semantic-input:fork:f2",
            patch=_patch((stage_objective_path("review"), "x")),
        )
    with pytest.raises(ForkPatchNotApplicable, match="no StageGraph"):
        await derivation.derive_stagegraph(
            request_scope="tenant-1",
            source_semantic_input_binding_ref="semantic-input:missing",
            derived_semantic_input_binding_ref="semantic-input:fork:f3",
            patch=_patch(),
        )
    assert ("tenant-1", "semantic-input:fork:f2") not in store.stored
