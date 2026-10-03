"""Derive a fork's StageGraph operation templates from the source run's (RRM-009, RRM-006).

The fork admits the derived run with a typed patch; starting it is the governed launch
path's job, which includes applying the patch's semantic-input changes to the derived run's
templates. A `stage_objectives.<stage>` change becomes an `admitted_input` prompt segment of
that stage's template; every other template is copied unchanged. The derived templates live
under the fork's own semantic input binding reference, so the source's stay immutable.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import OperationExecutionRequest, PromptSegment
from app.domain.run_control.forks import RunForkPatch

STAGE_OBJECTIVE_PREFIX = "stage_objectives."


class StageGraphTemplateStore(Protocol):
    async def list_templates(
        self, *, request_scope: str, semantic_input_binding_ref: str
    ) -> dict[str, OperationExecutionRequest]: ...

    async def persist_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
        templates: dict[str, OperationExecutionRequest],
        recorded_at: datetime,
    ) -> None: ...


class ForkPatchNotApplicable(ValueError):
    """The patch changes a stage the source run has no operation template for."""


class StageGraphForkTemplateDerivation:
    def __init__(self, templates: StageGraphTemplateStore) -> None:
        self._templates = templates

    async def derive_stagegraph(
        self,
        *,
        request_scope: str,
        source_semantic_input_binding_ref: str,
        derived_semantic_input_binding_ref: str,
        patch: RunForkPatch,
    ) -> tuple[str, ...]:
        source = await self._templates.list_templates(
            request_scope=request_scope,
            semantic_input_binding_ref=source_semantic_input_binding_ref,
        )
        if not source:
            raise ForkPatchNotApplicable("the source run has no StageGraph operation templates")
        objectives = {
            change.path.removeprefix(STAGE_OBJECTIVE_PREFIX): str(change.value)
            for change in patch.changes
            if change.path.startswith(STAGE_OBJECTIVE_PREFIX)
        }
        stages = {key.split("/", 1)[0] for key in source}
        unknown = sorted(set(objectives) - stages)
        if unknown:
            raise ForkPatchNotApplicable(
                "the fork patch changes stages without templates: " + ", ".join(unknown)
            )
        derived: dict[str, OperationExecutionRequest] = {}
        patched: list[str] = []
        for key, template in sorted(source.items()):
            stage = key.split("/", 1)[0]
            objective = objectives.get(stage)
            if objective is None:
                derived[key] = template
                continue
            derived[key] = template.model_copy(
                update={
                    "prompt_segments": (
                        *template.prompt_segments,
                        PromptSegment(
                            source_ref=f"fork-objective:{stage}",
                            source_revision=1,
                            trust_class="admitted_input",
                            content=objective,
                            rendered_digest=sha256_digest(objective),
                        ),
                    )
                }
            )
            patched.append(key)
        await self._templates.persist_templates(
            request_scope=request_scope,
            semantic_input_binding_ref=derived_semantic_input_binding_ref,
            templates=derived,
            recorded_at=datetime.now(UTC),
        )
        return tuple(patched)


__all__ = ["ForkPatchNotApplicable", "StageGraphForkTemplateDerivation", "StageGraphTemplateStore"]
