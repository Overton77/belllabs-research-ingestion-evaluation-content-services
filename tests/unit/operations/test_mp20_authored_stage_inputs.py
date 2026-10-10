"""MP-20 (SPEC-02): a manifest node's authored `inputs[].name` and `expand` reach the consumer's
Context Packet; the frozen blueprint, its digests and every run without them are unchanged."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from mission_control.application.context.pack_service import goal_packet_root
from mission_control.application.programs.service import (
    StageGraphOperationPreparationService,
    StaticStageGraphOperationTemplateProvider,
    _check_authored_inputs,
)
from mission_control.application.programs.stage_inputs import stagegraph_authored_inputs
from mission_control.domain.authoring.manifest import Behavior
from mission_control.domain.authoring.manifest import ExpansionTier as AuthoredTier
from mission_control.domain.context.packet import ExpansionTier
from mission_control.domain.programs.contracts import StageAuthoredInput
from tests.integration.temporal.test_wp_bp_010_temporal import (
    RecordingOperationBindings,
    _blueprint,
)
from tests.unit.operations.test_ft_b2_stage_handoff import (
    FAST_REF,
    _compiled_template,
    _downstream_request,
    _service,
)

AUTHORED = StageAuthoredInput(
    consumer_stage_id="downstream",
    producer_stage_id="fast",
    producer_output_slot_id="result",
    name="sources",
    expand="materialize",
)


async def _packet(authored: tuple[StageAuthoredInput, ...]) -> tuple[Any, Any]:
    request = replace(_downstream_request([FAST_REF]), authored_inputs=authored)
    service, selections, _staging = _service()
    preparation = StageGraphOperationPreparationService(
        templates=StaticStageGraphOperationTemplateProvider(
            {request.proposal.operation_request_key: _compiled_template(request)}
        ),
        operation_bindings=RecordingOperationBindings(),  # type: ignore[arg-type]
        context_packs=service,
    )
    prepared = await preparation.materialize(request)
    (packet, _selection) = next(iter(selections.rows.values()))
    return packet, prepared


async def test_without_authored_inputs_the_slot_name_and_auto_tier_are_unchanged() -> None:
    packet, _prepared = await _packet(())
    item = next(i for i in packet.items if i.source_ref == FAST_REF)
    # The small accepted output is inlined under `auto`, bound by its dependency slot.
    assert (item.binding_name, item.tier) == ("fast-input", ExpansionTier.INLINE)


async def test_the_authored_name_and_materialize_tier_hold() -> None:
    packet, prepared = await _packet((AUTHORED,))
    item = next(i for i in packet.items if i.source_ref == FAST_REF)
    assert (item.binding_name, item.tier) == ("sources", ExpansionTier.MATERIALIZE)
    paths = {slot.logical_path for slot in prepared.operation.workspace.slot_bindings}
    assert "/inputs/sources/sources.json" in paths
    assert not any(path.startswith("/inputs/fast-input/") for path in paths)


@pytest.mark.parametrize(
    ("expand", "tier"),
    [("reference", ExpansionTier.REFERENCE), ("inline", ExpansionTier.INLINE)],
)
async def test_the_authored_tier_is_the_bindings(expand: str, tier: ExpansionTier) -> None:
    packet, _prepared = await _packet((replace(AUTHORED, expand=expand),))  # type: ignore[arg-type]
    item = next(i for i in packet.items if i.source_ref == FAST_REF)
    assert (item.binding_name, item.tier) == ("sources", tier)


async def test_an_input_authored_for_another_stage_or_producer_is_ignored() -> None:
    other = (
        replace(AUTHORED, consumer_stage_id="slow"),
        replace(AUTHORED, producer_stage_id="slow", name="other"),
    )
    packet, _prepared = await _packet(other)
    item = next(i for i in packet.items if i.source_ref == FAST_REF)
    assert (item.binding_name, item.tier) == ("fast-input", ExpansionTier.INLINE)


def test_launch_refuses_an_input_no_dependency_delivers() -> None:
    blueprint = _blueprint()
    _check_authored_inputs((AUTHORED,), blueprint)
    with pytest.raises(ValueError, match="not delivered by a dependency"):
        _check_authored_inputs((replace(AUTHORED, producer_stage_id="downstream"),), blueprint)
    with pytest.raises(ValueError, match="duplicated"):
        _check_authored_inputs((AUTHORED, replace(AUTHORED, expand="auto")), blueprint)


def _node(key: str, depends_on: tuple[str, ...] = (), inputs: tuple[Any, ...] = ()) -> Any:
    return SimpleNamespace(key=key, depends_on=depends_on, inputs=inputs)


def _input(name: str, ref: str | None, expand: str, source: str = "from") -> Any:
    return SimpleNamespace(name=name, ref=ref, source=source, expand=AuthoredTier(expand))


def test_authored_inputs_come_from_sibling_dependencies_only() -> None:
    definition = SimpleNamespace(
        program=SimpleNamespace(
            behavior=Behavior.STAGE_GRAPH,
            nodes=(
                _node("produce"),
                _node(
                    "consume",
                    ("produce",),
                    (
                        _input("draft", "produce.draft", "materialize"),
                        _input("notes", "elsewhere.notes", "inline"),  # not a sibling
                        _input("pinned", None, "auto", source="artifact"),
                    ),
                ),
                _node("late", (), (_input("draft", "produce.draft", "reference"),)),
            ),
        )
    )
    assert stagegraph_authored_inputs(definition) == (  # type: ignore[arg-type]
        StageAuthoredInput(
            consumer_stage_id="consume",
            producer_stage_id="produce",
            producer_output_slot_id="draft",
            name="draft",
            expand="materialize",
        ),
    )
    loop = SimpleNamespace(program=SimpleNamespace(behavior=Behavior.GOAL_LOOP, nodes=()))
    assert stagegraph_authored_inputs(loop) == ()  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("runtime", "root"),
    [
        ("deep_agent", "/goal/2/verifier"),
        ("native", "/goal/2/verifier"),
        ("cursor", ""),
        ("claude", ""),
        ("codex", ""),
    ],
)
def test_a_session_lane_goal_packet_is_mounted_at_the_lease_root(runtime: str, root: str) -> None:
    template = SimpleNamespace(execution_runtime=runtime)
    assert goal_packet_root(template, "/goal/2/verifier") == root  # type: ignore[arg-type]
