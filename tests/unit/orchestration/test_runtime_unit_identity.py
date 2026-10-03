"""REQ-CP-EXEC-013 / `CON-CP-RUNTIME-UNIT-V1` and the `CON-CP-CHECKPOINT-LINEAGE-V1` namespaces."""

from __future__ import annotations

import hashlib
from typing import Any

import pytest
from pydantic import ValidationError

from mission_control.domain.execution.checkpoint_lineage import (
    cognitive_session_namespace,
    namespace_owner,
    submission_invocation_id,
)
from mission_control.domain.graph_runtime.identities import (
    GoalDirectedUnitLocation,
    RuntimeUnitIdentity,
    StageGraphUnitLocation,
)

STAGE_CANONICAL = (
    '{"canonical_schema_version":"canonical-json/1","payload":{"belllabs_run_id":"run-1",'
    '"execution_epoch":1,"family":"stage_graph","location":{"mapped_instance_id":'
    '"NO_MAPPED_INSTANCE","operation_slot_id":"default","stage_cycle_ordinal":0,'
    '"stage_id":"draft","workflow_cycle_ordinal":0},"request_scope":"tenant-1",'
    '"schema_version":"belllabs.runtime-unit.v1","semantic_attempt":1,'
    '"semantic_operation_id":"execution-epoch:1:stage:draft:mapped:none:workflow-cycle:0:'
    'stage-cycle:0:slot:default","unit_kind":"stage_operation"}}'
)
STAGE_UNIT_KEY = "bl-unit-v1:60fcde010eb0dfe546cc558af31d1c1c8fd72cc1577fd34161462ec6cd24bd84"
GOAL_CANONICAL = (
    '{"canonical_schema_version":"canonical-json/1","payload":{"belllabs_run_id":"run-1",'
    '"execution_epoch":1,"family":"goal_directed","location":{"agent_run":1,'
    '"goal_iteration":1,"goal_revision_id":"goal-revision:1","operation_role":"executor",'
    '"session_generation":1},"request_scope":"tenant-1",'
    '"schema_version":"belllabs.runtime-unit.v1","semantic_attempt":1,'
    '"semantic_operation_id":"goal-iteration/1/executor","unit_kind":"goal_executor"}}'
)
GOAL_UNIT_KEY = "bl-unit-v1:0aaabe730d7fd2a9e571a8a04ad35bd0eb9e72dfd6524b6e59f5ab37cc325ece"


def stage_unit(**updates: Any) -> RuntimeUnitIdentity:
    location = {
        "stage_id": "draft",
        "mapped_instance_id": "NO_MAPPED_INSTANCE",
        "workflow_cycle_ordinal": 0,
        "stage_cycle_ordinal": 0,
        "operation_slot_id": "default",
        **updates.pop("location", {}),
    }
    values: dict[str, Any] = {
        "request_scope": "tenant-1",
        "belllabs_run_id": "run-1",
        "execution_epoch": 1,
        "family": "stage_graph",
        "unit_kind": "stage_operation",
        "semantic_operation_id": (
            "execution-epoch:1:stage:draft:mapped:none:workflow-cycle:0:stage-cycle:0:slot:default"
        ),
        "semantic_attempt": 1,
        "location": StageGraphUnitLocation(**location),
        **updates,
    }
    return RuntimeUnitIdentity(**values)


def goal_unit(**updates: Any) -> RuntimeUnitIdentity:
    location = {
        "goal_iteration": 1,
        "goal_revision_id": "goal-revision:1",
        "operation_role": "executor",
        "agent_run": 1,
        "session_generation": 1,
        **updates.pop("location", {}),
    }
    role = location["operation_role"]
    values: dict[str, Any] = {
        "request_scope": "tenant-1",
        "belllabs_run_id": "run-1",
        "execution_epoch": 1,
        "family": "goal_directed",
        "unit_kind": f"goal_{role}",
        "semantic_operation_id": f"goal-iteration/{location['goal_iteration']}/{role}",
        "semantic_attempt": 1,
        "location": GoalDirectedUnitLocation(**location),
        **updates,
    }
    return RuntimeUnitIdentity(**values)


@pytest.mark.parametrize(
    ("unit", "canonical", "golden_key"),
    [
        (stage_unit(), STAGE_CANONICAL, STAGE_UNIT_KEY),
        (goal_unit(), GOAL_CANONICAL, GOAL_UNIT_KEY),
    ],
)
def test_unit_key_is_the_prefixed_sha256_of_the_canonical_serialization(
    unit: RuntimeUnitIdentity, canonical: str, golden_key: str
) -> None:
    independent = "bl-unit-v1:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    assert unit.unit_key == independent == golden_key


@pytest.mark.parametrize(
    "changed",
    [
        stage_unit(request_scope="tenant-2"),
        stage_unit(belllabs_run_id="run-2"),
        stage_unit(execution_epoch=2),
        stage_unit(semantic_operation_id="other-operation"),
        stage_unit(semantic_attempt=2),
        stage_unit(location={"stage_id": "review"}),
        stage_unit(location={"mapped_instance_id": "item-7"}),
        stage_unit(location={"workflow_cycle_ordinal": 1}),
        stage_unit(location={"stage_cycle_ordinal": 1}),
        stage_unit(location={"operation_slot_id": "secondary"}),
    ],
)
def test_every_stagegraph_location_field_changes_the_key(changed: RuntimeUnitIdentity) -> None:
    assert changed.unit_key != STAGE_UNIT_KEY


@pytest.mark.parametrize(
    "changed",
    [
        goal_unit(location={"goal_iteration": 2}),
        goal_unit(location={"goal_revision_id": "goal-revision:2"}),
        goal_unit(location={"operation_role": "verifier"}),
        goal_unit(location={"agent_run": 2}),
        goal_unit(location={"session_generation": 2}),
        goal_unit(execution_epoch=2),
    ],
)
def test_every_goaldirected_location_field_changes_the_key(changed: RuntimeUnitIdentity) -> None:
    assert changed.unit_key != GOAL_UNIT_KEY


def test_payload_round_trip_across_retries_and_continue_as_new_keeps_the_key() -> None:
    unit = goal_unit()
    for _delivery in range(3):
        unit = RuntimeUnitIdentity.model_validate_json(unit.model_dump_json())

    assert unit.unit_key == GOAL_UNIT_KEY


@pytest.mark.parametrize(
    "technical_field",
    ["activity_attempt", "temporal_workflow_id", "worker_identity", "execution_generation"],
)
def test_technical_and_generation_fields_are_not_unit_identity(technical_field: str) -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        RuntimeUnitIdentity.model_validate(
            {**stage_unit().model_dump(mode="json"), technical_field: 1}
        )


def test_location_must_match_family_and_role() -> None:
    with pytest.raises(ValidationError, match="StageGraph location"):
        RuntimeUnitIdentity.model_validate(
            {**stage_unit().model_dump(mode="json"), "location": goal_unit().location}
        )
    with pytest.raises(ValidationError, match="operation role"):
        goal_unit(unit_kind="goal_verifier")


def test_stagegraph_namespace_is_per_unit_generation() -> None:
    unit = stage_unit()

    assert cognitive_session_namespace(unit, 1) == f"belllabs/stage/{STAGE_UNIT_KEY}/gen/1"
    assert cognitive_session_namespace(unit, 2) == f"belllabs/stage/{STAGE_UNIT_KEY}/gen/2"
    assert cognitive_session_namespace(stage_unit(semantic_attempt=2), 1) != (
        cognitive_session_namespace(unit, 1)
    )


def test_goaldirected_session_reuse_rollover_verifier_and_generation_namespaces() -> None:
    first = goal_unit()
    next_iteration = goal_unit(location={"goal_iteration": 2, "agent_run": 2})
    verifier = goal_unit(location={"operation_role": "verifier"})
    rollover = goal_unit(location={"goal_iteration": 2, "agent_run": 2, "session_generation": 2})

    shared = "belllabs/goal/run-1/epoch/1/session/1/role/executor"
    assert cognitive_session_namespace(first, 1) == shared
    assert cognitive_session_namespace(next_iteration, 1) == shared
    assert namespace_owner(first, 1) == namespace_owner(next_iteration, 1)
    assert cognitive_session_namespace(verifier, 1) == (
        "belllabs/goal/run-1/epoch/1/session/1/role/verifier"
    )
    assert cognitive_session_namespace(rollover, 1) == (
        "belllabs/goal/run-1/epoch/1/session/2/role/executor"
    )
    assert cognitive_session_namespace(first, 2) == (
        f"belllabs/goal/run-1/epoch/1/unit/{GOAL_UNIT_KEY}/gen/2"
    )
    assert namespace_owner(first, 2) != namespace_owner(next_iteration, 2)


def test_invocation_id_is_stable_per_unit_generation_and_distinct_across_generations() -> None:
    first = submission_invocation_id(STAGE_UNIT_KEY, 1)
    independent = (
        "sha256:"
        + hashlib.sha256(
            (
                '{"canonical_schema_version":"canonical-json/1","payload":["'
                + STAGE_UNIT_KEY
                + '",1,"submit"]}'
            ).encode()
        ).hexdigest()
    )

    assert first == independent
    assert submission_invocation_id(STAGE_UNIT_KEY, 1) == first
    assert submission_invocation_id(STAGE_UNIT_KEY, 2) != first
