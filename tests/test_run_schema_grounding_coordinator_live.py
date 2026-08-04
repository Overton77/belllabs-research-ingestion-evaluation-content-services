from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.orchestration import (
    ORCHESTRATION_AUTHORITY_REF,
    orchestration_lifecycle_actor,
)
from app.application.run_control import (
    ACTION_PERMISSIONS,
    REQUIRED_SHARED_BUDGET_DIMENSIONS,
    run_identity_for,
)
from app.application.schema_grounding_coordinator_live import _proposal
from app.config import PROJECT_ROOT
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import ExactDefinitionRef, PublishedDefinition
from app.domain.run_control.contracts import ActorContext
from app.domain.schema_grounding.definitions import schema_grounding_definitions
from scripts.run_schema_grounding_coordinator_live import (
    DEFAULT_SOURCE_RUN,
    _checked_artifact_root,
    parse_args,
)


def test_parser_requires_explicit_bucket_and_expands_reviewed_intents() -> None:
    args = parse_args(
        [
            "--artifact-dir",
            str(PROJECT_ROOT / ".artifacts" / "schema-live-test"),
            "--artifact-bucket",
            "private-versioned-test-bucket",
            "--deployment-id",
            "governed-attestation-test",
        ]
    )

    assert args.artifact_bucket == "private-versioned-test-bucket"
    assert args.intent == tuple(
        DEFAULT_SOURCE_RUN / "queries" / f"{index:03d}-intent.json"
        for index in range(1, 6)
    )


def test_artifact_root_must_stay_inside_project() -> None:
    with pytest.raises(ValueError, match="inside the project"):
        _checked_artifact_root(PROJECT_ROOT.parent / "outside-schema-live-artifacts")


def test_precomputed_live_run_identity_matches_admission_contract() -> None:
    first = run_identity_for(
        "global",
        "coordinator-schema-grounding-live",
        "scenario-c-live:fixed",
    )
    second = run_identity_for(
        "global",
        "coordinator-schema-grounding-live",
        "scenario-c-live:fixed",
    )

    assert first == second
    assert first != run_identity_for(
        "global",
        "coordinator-schema-grounding-live",
        "scenario-c-live:changed",
    )


def test_schema_live_lifecycle_actor_is_separate_and_least_privilege() -> None:
    coordinator = ActorContext(
        actor_id="coordinator-schema-grounding-live",
        authority_refs=frozenset({"authority:coordinator-schema-grounding-live"}),
        permissions=frozenset({"workflow_run.admit"}),
    )
    lifecycle = orchestration_lifecycle_actor()

    assert lifecycle.actor_id == ORCHESTRATION_AUTHORITY_REF
    assert lifecycle.authority_refs == frozenset({ORCHESTRATION_AUTHORITY_REF})
    assert lifecycle.permissions == frozenset(ACTION_PERMISSIONS.values())
    assert "workflow_run.admit" not in lifecycle.permissions
    assert ORCHESTRATION_AUTHORITY_REF not in coordinator.authority_refs


@pytest.mark.parametrize(
    ("workflow_id", "runtime_id", "initial_goal"),
    [
        ("schema-context-selection", "schema-context-selection-runtime-v1", None),
        (
            "supporting-graph-reconciliation",
            "supporting-graph-reconciliation-runtime-v1",
            "Reconcile the bounded supporting graph.",
        ),
    ],
)
def test_live_a_and_goal_directed_c_proposals_bound_concurrency_to_authority(
    workflow_id: str,
    runtime_id: str,
    initial_goal: str | None,
) -> None:
    definitions = schema_grounding_definitions()

    def _published(logical_id: str) -> PublishedDefinition:
        definition = next(
            item for item in definitions if item.logical_id == logical_id
        )
        return PublishedDefinition(
            ref=ExactDefinitionRef(
                kind=definition.kind,
                logical_id=definition.logical_id,
                revision=1,
                digest=sha256_digest(definition),
            ),
            definition=definition,
            published_at=datetime(2026, 7, 26, 23, 0, tzinfo=UTC),
            published_by="test",
        )

    workflow = _published(workflow_id)
    runtime = _published(runtime_id)
    implementation = _published(f"{workflow_id}.implementation")
    actor = ActorContext(
        actor_id="coordinator-schema-grounding-live",
        authority_refs=frozenset({"authority:coordinator"}),
        permissions=frozenset({"workflow_run.admit"}),
    )

    proposal, _context = _proposal(
        workflow,
        implementation,
        runtime,
        actor=actor,
        tenant_scope="global",
        request_scope="global",
        idempotency_key=f"test:{workflow_id}",
        now=datetime(2026, 7, 26, 23, 0, tzinfo=UTC),
        input_digest=sha256_digest({"workflow": workflow_id}),
        admission_evidence=("evidence:test",),
        initial_goal=initial_goal,
    )

    authority = workflow.definition.authority_ceiling  # type: ignore[union-attr]
    actual = {
        item.dimension: item.hard_cap
        for item in proposal.admission.budget_envelope.dimensions
    }
    assert set(actual) == REQUIRED_SHARED_BUDGET_DIMENSIONS
    assert actual["concurrency.slots"] == min(
        authority.budgets.dimensions["concurrency.slots"],
        authority.max_concurrency,
    )
    assert {
        dimension: hard_cap
        for dimension, hard_cap in actual.items()
        if dimension != "concurrency.slots"
    } == {
        dimension: authority.budgets.dimensions.get(dimension)
        for dimension in REQUIRED_SHARED_BUDGET_DIMENSIONS
        if dimension != "concurrency.slots"
    }
