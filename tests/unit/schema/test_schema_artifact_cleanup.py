from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest

from app.application.schema.schema_artifact_cleanup import (
    TARGET_CONSTRAINTS,
    TARGET_INDEXES,
    TARGET_LABELS,
    TargetLabelUsage,
    plan_zero_count_schema_artifact_cleanup,
    verify_schema_artifact_cleanup_postcondition,
)
from app.domain.schema_grounding.authority import live_neo4j_schema_snapshot_digest
from app.domain.schema_grounding.contracts import (
    LiveNeo4jSchemaSnapshot,
    Neo4jIndexDescriptor,
)
from app.domain.schema_grounding.errors import SchemaDeploymentMismatch
from scripts import reconcile_zero_count_schema_artifacts as cleanup_cli

NOW = datetime(2026, 7, 26, 22, 0, tzinfo=UTC)


def _snapshot(
    *,
    indexes: tuple[Any, ...] = TARGET_INDEXES,
    constraints: tuple[Any, ...] = TARGET_CONSTRAINTS,
    active_node_labels: frozenset[str] = frozenset(),
) -> LiveNeo4jSchemaSnapshot:
    token_labels = frozenset({"Organization", *TARGET_LABELS})
    digest = live_neo4j_schema_snapshot_digest(
        database="neo4j",
        server_agent="Neo4j/5.27",
        token_catalog_node_labels=token_labels,
        token_catalog_relationship_types=frozenset(),
        active_node_labels=active_node_labels,
        active_relationship_types=frozenset(),
        indexes=indexes,
        constraints=constraints,
    )
    return LiveNeo4jSchemaSnapshot(
        database="neo4j",
        server_agent="Neo4j/5.27",
        token_catalog_node_labels=token_labels,
        token_catalog_relationship_types=frozenset(),
        active_node_labels=active_node_labels,
        active_relationship_types=frozenset(),
        indexes=indexes,
        constraints=constraints,
        observed_at=NOW,
        snapshot_digest=digest,
    )


def _zero_usage() -> dict[str, TargetLabelUsage]:
    return {
        label: TargetLabelUsage(
            node_count=0,
            incoming_relationship_count=0,
            outgoing_relationship_count=0,
        )
        for label in TARGET_LABELS
    }


def test_exact_allowlist_plans_constraints_before_independent_indexes() -> None:
    plan = plan_zero_count_schema_artifact_cleanup(_snapshot(), _zero_usage())

    assert plan.all_allowlisted_artifacts_present is True
    assert len(plan.present_indexes) == 17
    assert len(plan.present_constraints) == 8
    assert len(plan.constraint_drop_commands) == 8
    assert len(plan.independent_index_drop_commands) == 13
    assert all(command.startswith("DROP CONSTRAINT") for command in plan.constraint_drop_commands)
    assert all(command.startswith("DROP INDEX") for command in plan.independent_index_drop_commands)


@pytest.mark.parametrize(
    "usage",
    [
        TargetLabelUsage(1, 0, 0),
        TargetLabelUsage(0, 1, 0),
        TargetLabelUsage(0, 0, 1),
    ],
)
def test_any_target_label_usage_fails_closed(usage: TargetLabelUsage) -> None:
    evidence = _zero_usage()
    evidence["OrganizationState"] = usage

    with pytest.raises(SchemaDeploymentMismatch, match="gained nodes or relationships"):
        plan_zero_count_schema_artifact_cleanup(_snapshot(), evidence)


def test_active_target_label_fails_even_if_count_evidence_claims_zero() -> None:
    with pytest.raises(SchemaDeploymentMismatch, match="active-label snapshot"):
        plan_zero_count_schema_artifact_cleanup(
            _snapshot(active_node_labels=frozenset({"OrganizationState"})),
            _zero_usage(),
        )


def test_descriptor_drift_and_extra_target_bound_artifact_fail_closed() -> None:
    drifted = TARGET_INDEXES[0].model_copy(update={"properties": ("changed",)})
    indexes = (drifted, *TARGET_INDEXES[1:])
    with pytest.raises(SchemaDeploymentMismatch, match="descriptor changed"):
        plan_zero_count_schema_artifact_cleanup(
            _snapshot(indexes=indexes),
            _zero_usage(),
        )

    extra = Neo4jIndexDescriptor(
        name="unapproved_target_index",
        index_type="RANGE",
        entity_type="NODE",
        labels_or_types=("OrganizationState",),
        properties=("name",),
        state="ONLINE",
    )
    with pytest.raises(SchemaDeploymentMismatch, match="escaped the exact allowlist"):
        plan_zero_count_schema_artifact_cleanup(
            _snapshot(indexes=(*TARGET_INDEXES, extra)),
            _zero_usage(),
        )


def test_postcondition_accepts_clean_state_and_rejects_any_remaining_artifact() -> None:
    verify_schema_artifact_cleanup_postcondition(
        _snapshot(indexes=(), constraints=()),
        _zero_usage(),
    )

    with pytest.raises(SchemaDeploymentMismatch, match="still contains"):
        verify_schema_artifact_cleanup_postcondition(
            _snapshot(indexes=(TARGET_INDEXES[0],), constraints=()),
            _zero_usage(),
        )


@pytest.mark.asyncio
async def test_apply_rechecks_plan_digest_before_opening_write_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = plan_zero_count_schema_artifact_cleanup(_snapshot(), _zero_usage())
    changed = replace(plan, plan_digest="sha256:" + "f" * 64)

    async def _changed_prepare(_driver: object, *, database: str) -> object:
        assert database == "neo4j"
        return changed

    monkeypatch.setattr(cleanup_cli, "_prepare", _changed_prepare)

    with pytest.raises(RuntimeError, match="plan changed"):
        await cleanup_cli._apply(object(), database="neo4j", plan=plan)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_apply_rejects_partial_allowlist_but_allows_idempotent_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    complete = plan_zero_count_schema_artifact_cleanup(_snapshot(), _zero_usage())
    partial = plan_zero_count_schema_artifact_cleanup(
        _snapshot(indexes=TARGET_INDEXES[:-1]),
        _zero_usage(),
    )
    clean = plan_zero_count_schema_artifact_cleanup(
        _snapshot(indexes=(), constraints=()),
        _zero_usage(),
    )

    async def _partial_prepare(_driver: object, *, database: str) -> object:
        return partial

    monkeypatch.setattr(cleanup_cli, "_prepare", _partial_prepare)
    with pytest.raises(RuntimeError, match="partial allowlist"):
        await cleanup_cli._apply(object(), database="neo4j", plan=partial)  # type: ignore[arg-type]

    async def _clean_prepare(_driver: object, *, database: str) -> object:
        return clean

    monkeypatch.setattr(cleanup_cli, "_prepare", _clean_prepare)
    assert await cleanup_cli._apply(  # type: ignore[arg-type]
        object(),
        database="neo4j",
        plan=clean,
    ) == (0, "already_clean")
    assert complete.all_allowlisted_artifacts_present is True
