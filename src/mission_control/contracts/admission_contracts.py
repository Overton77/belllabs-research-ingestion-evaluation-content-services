"""Typed parity admission/start contracts; GENERAL authored revisions remain separate."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import Field, model_validator

from mission_control.contracts.contracts import Contract
from mission_control.domain.authoring.contracts import ExactDefinitionRef, RunInputManifestRef
from mission_control.domain.policies.contracts import BudgetEnvelope
from mission_control.domain.programs.contracts import GoalDirectedRunInput, StageGraphRunInput


class MissionAdmissionRequest(Contract):
    schema_version: Literal["mc.runtime_admission.v1"] = "mc.runtime_admission.v1"
    request_id: UUID
    effective_configuration_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    workflow_type_ref: ExactDefinitionRef
    input_manifest: RunInputManifestRef
    budget_envelope: BudgetEnvelope
    parent_run_id: str | None = None
    sponsorship_ref: str = Field(min_length=1)
    approval_refs: tuple[str, ...] = ()
    delegation_authority_refs: frozenset[str] = frozenset()
    admission_evidence_refs: tuple[str, ...] = ()


class MissionLaunchRequest(Contract):
    schema_version: Literal["mc.runtime_launch.v1"] = "mc.runtime_launch.v1"
    family: Literal["StageGraph", "GoalDirected"]
    stagegraph: StageGraphRunInput | None = None
    goal_directed: GoalDirectedRunInput | None = None
    source_semantic_input_binding_ref: str | None = None

    @model_validator(mode="after")
    def exactly_one_family(self) -> MissionLaunchRequest:
        if self.family == "StageGraph":
            valid = self.stagegraph is not None and self.goal_directed is None
        else:
            valid = self.goal_directed is not None and self.stagegraph is None
        if not valid:
            raise ValueError("launch requires exactly the selected family input")
        return self
