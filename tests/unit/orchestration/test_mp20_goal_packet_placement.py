"""MP-20: a GoalDirected packet keeps its slots under the unit's role root (the run's shared
workspace, RRM-020) and, on a Session Lane, names its files relative to the lease root the
lane maps that role root to (`.mission/`, `inputs/`), as the lane's operating contract does."""

from __future__ import annotations

from dataclasses import replace

import pytest

from mission_control.domain.context.render import is_context_input_slot
from tests.unit.orchestration.test_ft_b3_goal_iteration_packet import (
    _composition,
    _executor_result,
)
from tests.unit.orchestration.test_rrm_016_goal_directed_settlement import _claim, _preparation

ROLE_ROOT = "/goal/1/verifier"


@pytest.mark.parametrize(
    ("runtime", "pointer"),
    [
        ("deep_agent", f"This index: {ROLE_ROOT}/.mission/context.md"),
        ("claude", "This index: /.mission/context.md"),
        ("codex", "This index: /.mission/context.md"),
        ("cursor", "This index: /.mission/context.md"),
    ],
)
async def test_goal_packet_slots_stay_role_rooted_and_session_lanes_read_the_lease_root(
    runtime: str, pointer: str
) -> None:
    composition, run_id, _selections = await _composition()
    claim = await _claim(run_id)
    service = composition.family._operations._context_packs
    assert service is not None
    result = replace(_executor_result(claim, run_id), workspace_id=claim.workspace_namespace)
    request = _preparation(run_id, claim, "verifier", 3, 1, result)
    template = composition.templates["verifier"].model_copy(update={"execution_runtime": runtime})

    sealed = await service.pack_for_iteration(request, template, role_root=ROLE_ROOT)

    paths = {slot.logical_path for slot in sealed.slot_bindings}
    assert all(is_context_input_slot(slot) for slot in sealed.slot_bindings)
    assert all(path.startswith(f"{ROLE_ROOT}/") for path in paths), paths
    assert {f"{ROLE_ROOT}/.mission/context.md", f"{ROLE_ROOT}/.mission/inputs.json"} <= paths
    assert pointer in sealed.prompt_segment.content
    if runtime != "deep_agent":
        assert f"{ROLE_ROOT}/.mission" not in sealed.prompt_segment.content
