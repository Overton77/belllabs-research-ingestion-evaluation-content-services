"""MP-10: example Mission 1's `review` Human Gate lowers onto its compiled Stage Graph."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from temporalio.exceptions import ApplicationError

from mission_control.adapters.temporal.workflows.stagegraph import _human_gates
from mission_control.application.programs.human_gates import (
    goal_human_review,
    stagegraph_human_gates,
)
from mission_control.domain.authoring.contracts import StageGraphBlueprint
from mission_control.domain.programs.contracts import StageGraphRunInput
from tests.unit.authoring.test_manifest_launch_inputs import compile_programs

ROOT = Path(__file__).resolve().parents[3]
MISSION_1 = ROOT / "docs/specs/fast-track-2026-10/missions/01-research-ingestion-deep-agents.yml"


@pytest.fixture(scope="module")
def mission_1():  # type: ignore[no-untyped-def]
    definitions, programs = asyncio.run(compile_programs(MISSION_1.read_text(encoding="utf-8")))
    return definitions[0], programs[0]


def _run_input(blueprint: StageGraphBlueprint, gates: tuple) -> StageGraphRunInput:  # type: ignore[type-arg]
    return StageGraphRunInput(
        run_id="run-1",
        request_scope="tenant-1",
        effective_configuration_digest="sha256:" + "a" * 64,
        workflow_type_digest="sha256:" + "a" * 64,
        blueprint_digest="sha256:" + "a" * 64,
        blueprint=blueprint.model_dump(mode="json"),
        human_gates=gates,
    )


def test_mission_1_review_gate_lowers_without_invented_remediation(mission_1) -> None:  # type: ignore[no-untyped-def]
    definition, program = mission_1
    (gate,) = stagegraph_human_gates(definition)
    assert gate.gate_key == "review"
    assert gate.reviewers == ("owner",)
    assert gate.packet_sources == ("synthesize.claim_table", "synthesize.source_manifest")
    assert gate.on_timeout == "keep_waiting"
    assert gate.remediation_target is None
    assert gate.permitted_decisions(1) == ("approve", "deny")
    blueprint = program.configuration.selected_blueprint
    assert isinstance(blueprint, StageGraphBlueprint)
    assert set(_human_gates(_run_input(blueprint, (gate,)), blueprint)) == {"review"}
    # The Stage Graph root is not a Goal Loop: no goal review is derived from its criteria.
    assert goal_human_review(definition, reviewers=("owner",)) is None


def test_a_declared_remediation_needs_a_stage_and_a_cycle_policy(mission_1) -> None:  # type: ignore[no-untyped-def]
    definition, program = mission_1
    (gate,) = stagegraph_human_gates(definition, remediation={"review": ("synthesize", 2)})
    assert gate.remediation_target == "synthesize" and gate.max_review_rounds == 2
    with pytest.raises(ValueError, match="not a stage"):
        stagegraph_human_gates(definition, remediation={"review": ("nowhere", 2)})
    blueprint = program.configuration.selected_blueprint
    # The frozen manifest lowering emits no workflow cycle policy, so a remediation route
    # is refused at the family start rather than silently dropped.
    assert blueprint.workflow_cycle_policy is None
    with pytest.raises(ApplicationError, match="workflow cycle policy"):
        _human_gates(_run_input(blueprint, (gate,)), blueprint)


def test_the_production_launch_author_lowers_the_manifest_gates(mission_1, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """The production `LaunchInputPort` (MP-02) carries MP-10 gates into the family input."""

    import asyncio as _asyncio
    from types import SimpleNamespace
    from typing import Any

    from mission_control.application.authoring import manifest_launch_inputs as module
    from mission_control.application.authoring.manifest_submit import ManifestStartUnavailable
    from mission_control.domain.programs.human_gate import HumanGateSpec

    definition, _program = mission_1

    class Definitions:
        async def run_subscriptions(self, _scope: str, _run_id: str) -> dict[str, Any]:
            return {"definition": definition.model_dump(mode="json")}

    author = module.ManifestLaunchInputAuthor(
        resolver=SimpleNamespace(),  # type: ignore[arg-type]
        run_control=SimpleNamespace(),  # type: ignore[arg-type]
        control_plane=SimpleNamespace(),  # type: ignore[arg-type]
        definitions=Definitions(),
        stage_templates=SimpleNamespace(),  # type: ignore[arg-type]
        goal_templates=SimpleNamespace(),  # type: ignore[arg-type]
    )
    gates, review = _asyncio.run(author._human_control("tenant-1", "run-1", "StageGraph"))
    assert [gate.gate_key for gate in gates] == ["review"] and review is None
    assert gates[0].reviewers == ("owner",)
    # Mission 1 is a Stage Graph: a GoalDirected start derives no review from it.
    assert _asyncio.run(author._human_control("tenant-1", "run-1", "GoalDirected")) == ((), None)

    # A Goal Loop that requires a human review borrows the manifest's declared reviewers;
    # the FIXTURE below stands in for such a loop (Mission 1 declares `owner` on its gate).
    required = HumanGateSpec(gate_key="goal-review", prompt="Review.", reviewers=("owner",))

    def goal_review(_definition: Any, *, reviewers: tuple[str, ...], **_: Any) -> HumanGateSpec:
        return required.model_copy(update={"reviewers": reviewers})

    monkeypatch.setattr(module, "goal_human_review", goal_review)
    gates, review = _asyncio.run(author._human_control("tenant-1", "run-1", "GoalDirected"))
    assert gates == () and review is not None and review.reviewers == ("owner",)
    # Without any declared reviewer the requirement is refused, never dropped.
    monkeypatch.setattr(module, "_human_gate_nodes", lambda _definition: [])
    with pytest.raises(ManifestStartUnavailable, match="names no reviewer"):
        _asyncio.run(author._human_control("tenant-1", "run-1", "GoalDirected"))
