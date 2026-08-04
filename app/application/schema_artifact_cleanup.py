from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from app.domain.control_plane.canonical import sha256_digest
from app.domain.schema_grounding.contracts import (
    LiveNeo4jSchemaSnapshot,
    Neo4jConstraintDescriptor,
    Neo4jIndexDescriptor,
)
from app.domain.schema_grounding.errors import SchemaDeploymentMismatch

TARGET_LABELS = frozenset(
    {
        "OrganizationState",
        "ProductState",
        "ResearchPlanRef",
        "ResearchRunRef",
    }
)


def _index(
    name: str,
    label: str,
    property_names: tuple[str, ...],
    *,
    index_type: str = "RANGE",
    owning_constraint: str | None = None,
) -> Neo4jIndexDescriptor:
    return Neo4jIndexDescriptor(
        name=name,
        index_type=index_type,
        entity_type="NODE",
        labels_or_types=(label,),
        properties=property_names,
        state="ONLINE",
        owning_constraint=owning_constraint,
    )


def _constraint(
    name: str,
    label: str,
    property_name: str,
    *,
    constraint_type: str,
    owned_index: str | None = None,
) -> Neo4jConstraintDescriptor:
    return Neo4jConstraintDescriptor(
        name=name,
        constraint_type=constraint_type,
        entity_type="NODE",
        labels_or_types=(label,),
        properties=(property_name,),
        owned_index=owned_index,
    )


TARGET_INDEXES = tuple(
    sorted(
        (
            _index(
                "OrganizationStateSearch",
                "OrganizationState",
                ("name", "description", "sector", "searchText"),
                index_type="FULLTEXT",
            ),
            _index(
                "OrganizationStateSearchEmbedding",
                "OrganizationState",
                ("searchEmbedding",),
                index_type="VECTOR",
            ),
            _index(
                "ProductStateSearch",
                "ProductState",
                ("name", "description", "searchText"),
                index_type="FULLTEXT",
            ),
            _index(
                "ProductStateSearchEmbedding",
                "ProductState",
                ("searchEmbedding",),
                index_type="VECTOR",
            ),
            _index("rpr_expiredAt", "ResearchPlanRef", ("expiredAt",)),
            _index("rpr_invalidAt", "ResearchPlanRef", ("invalidAt",)),
            _index(
                "rpr_mongoPlanId_unique",
                "ResearchPlanRef",
                ("mongoPlanId",),
                owning_constraint="rpr_mongoPlanId_unique",
            ),
            _index(
                "rpr_researchPlanRefId_unique",
                "ResearchPlanRef",
                ("researchPlanRefId",),
                owning_constraint="rpr_researchPlanRefId_unique",
            ),
            _index("rpr_validAt", "ResearchPlanRef", ("validAt",)),
            _index("rpr_version", "ResearchPlanRef", ("version",)),
            _index("rrr_endedAt", "ResearchRunRef", ("endedAt",)),
            _index("rrr_expiredAt", "ResearchRunRef", ("expiredAt",)),
            _index("rrr_invalidAt", "ResearchRunRef", ("invalidAt",)),
            _index(
                "rrr_mongoRunId_unique",
                "ResearchRunRef",
                ("mongoRunId",),
                owning_constraint="rrr_mongoRunId_unique",
            ),
            _index(
                "rrr_researchRunRefId_unique",
                "ResearchRunRef",
                ("researchRunRefId",),
                owning_constraint="rrr_researchRunRefId_unique",
            ),
            _index("rrr_startedAt", "ResearchRunRef", ("startedAt",)),
            _index("rrr_validAt", "ResearchRunRef", ("validAt",)),
        ),
        key=lambda item: item.name,
    )
)

TARGET_CONSTRAINTS = tuple(
    sorted(
        (
            _constraint(
                "rpr_mongoPlanId_not_null",
                "ResearchPlanRef",
                "mongoPlanId",
                constraint_type="NODE_PROPERTY_EXISTENCE",
            ),
            _constraint(
                "rpr_mongoPlanId_unique",
                "ResearchPlanRef",
                "mongoPlanId",
                constraint_type="UNIQUENESS",
                owned_index="rpr_mongoPlanId_unique",
            ),
            _constraint(
                "rpr_researchPlanRefId_not_null",
                "ResearchPlanRef",
                "researchPlanRefId",
                constraint_type="NODE_PROPERTY_EXISTENCE",
            ),
            _constraint(
                "rpr_researchPlanRefId_unique",
                "ResearchPlanRef",
                "researchPlanRefId",
                constraint_type="UNIQUENESS",
                owned_index="rpr_researchPlanRefId_unique",
            ),
            _constraint(
                "rrr_mongoRunId_not_null",
                "ResearchRunRef",
                "mongoRunId",
                constraint_type="NODE_PROPERTY_EXISTENCE",
            ),
            _constraint(
                "rrr_mongoRunId_unique",
                "ResearchRunRef",
                "mongoRunId",
                constraint_type="UNIQUENESS",
                owned_index="rrr_mongoRunId_unique",
            ),
            _constraint(
                "rrr_researchRunRefId_not_null",
                "ResearchRunRef",
                "researchRunRefId",
                constraint_type="NODE_PROPERTY_EXISTENCE",
            ),
            _constraint(
                "rrr_researchRunRefId_unique",
                "ResearchRunRef",
                "researchRunRefId",
                constraint_type="UNIQUENESS",
                owned_index="rrr_researchRunRefId_unique",
            ),
        ),
        key=lambda item: item.name,
    )
)


@dataclass(frozen=True, slots=True)
class TargetLabelUsage:
    node_count: int
    incoming_relationship_count: int
    outgoing_relationship_count: int


@dataclass(frozen=True, slots=True)
class SchemaArtifactCleanupPlan:
    snapshot_digest: str
    target_labels: tuple[str, ...]
    label_usage: dict[str, TargetLabelUsage]
    present_indexes: tuple[Neo4jIndexDescriptor, ...]
    present_constraints: tuple[Neo4jConstraintDescriptor, ...]
    missing_allowlisted_index_names: tuple[str, ...]
    missing_allowlisted_constraint_names: tuple[str, ...]
    constraint_drop_commands: tuple[str, ...]
    independent_index_drop_commands: tuple[str, ...]
    plan_digest: str

    @property
    def all_allowlisted_artifacts_present(self) -> bool:
        return (
            not self.missing_allowlisted_index_names
            and not self.missing_allowlisted_constraint_names
        )


def plan_zero_count_schema_artifact_cleanup(
    snapshot: LiveNeo4jSchemaSnapshot,
    label_usage: dict[str, TargetLabelUsage],
) -> SchemaArtifactCleanupPlan:
    if set(label_usage) != set(TARGET_LABELS):
        raise SchemaDeploymentMismatch(
            "schema cleanup usage evidence does not cover the exact target-label allowlist"
        )
    active_usage = {
        label: usage
        for label, usage in label_usage.items()
        if (
            usage.node_count != 0
            or usage.incoming_relationship_count != 0
            or usage.outgoing_relationship_count != 0
        )
    }
    if active_usage:
        raise SchemaDeploymentMismatch(
            "schema cleanup target label gained nodes or relationships"
        )
    if snapshot.active_node_labels & TARGET_LABELS:
        raise SchemaDeploymentMismatch(
            "schema cleanup target label is present in the active-label snapshot"
        )

    expected_indexes = {item.name: item for item in TARGET_INDEXES}
    expected_constraints = {item.name: item for item in TARGET_CONSTRAINTS}
    candidate_indexes = tuple(
        item
        for item in snapshot.indexes
        if item.name in expected_indexes
        or bool(set(item.labels_or_types) & TARGET_LABELS)
    )
    candidate_constraints = tuple(
        item
        for item in snapshot.constraints
        if item.name in expected_constraints
        or bool(set(item.labels_or_types) & TARGET_LABELS)
    )
    _verify_exact_descriptors(
        "index",
        candidate_indexes,
        expected_indexes,
    )
    _verify_exact_descriptors(
        "constraint",
        candidate_constraints,
        expected_constraints,
    )

    present_indexes = tuple(sorted(candidate_indexes, key=lambda item: item.name))
    present_constraints = tuple(
        sorted(candidate_constraints, key=lambda item: item.name)
    )
    present_index_names = {item.name for item in present_indexes}
    present_constraint_names = {item.name for item in present_constraints}
    missing_indexes = tuple(sorted(set(expected_indexes) - present_index_names))
    missing_constraints = tuple(
        sorted(set(expected_constraints) - present_constraint_names)
    )
    constraint_commands = tuple(
        f"DROP CONSTRAINT `{name}` IF EXISTS"
        for name in sorted(expected_constraints)
    )
    independent_index_commands = tuple(
        f"DROP INDEX `{item.name}` IF EXISTS"
        for item in TARGET_INDEXES
        if item.owning_constraint is None
    )
    payload = {
        "snapshot_digest": snapshot.snapshot_digest,
        "target_labels": TARGET_LABELS,
        "label_usage": {
            label: {
                "node_count": usage.node_count,
                "incoming_relationship_count": usage.incoming_relationship_count,
                "outgoing_relationship_count": usage.outgoing_relationship_count,
            }
            for label, usage in label_usage.items()
        },
        "present_indexes": present_indexes,
        "present_constraints": present_constraints,
        "missing_allowlisted_index_names": missing_indexes,
        "missing_allowlisted_constraint_names": missing_constraints,
        "constraint_drop_commands": constraint_commands,
        "independent_index_drop_commands": independent_index_commands,
    }
    return SchemaArtifactCleanupPlan(
        snapshot_digest=snapshot.snapshot_digest,
        target_labels=tuple(sorted(TARGET_LABELS)),
        label_usage=label_usage,
        present_indexes=present_indexes,
        present_constraints=present_constraints,
        missing_allowlisted_index_names=missing_indexes,
        missing_allowlisted_constraint_names=missing_constraints,
        constraint_drop_commands=constraint_commands,
        independent_index_drop_commands=independent_index_commands,
        plan_digest=sha256_digest(payload),
    )


def verify_schema_artifact_cleanup_postcondition(
    snapshot: LiveNeo4jSchemaSnapshot,
    label_usage: dict[str, TargetLabelUsage],
) -> None:
    plan = plan_zero_count_schema_artifact_cleanup(snapshot, label_usage)
    if plan.present_indexes or plan.present_constraints:
        raise SchemaDeploymentMismatch(
            "schema cleanup postcondition still contains allowlisted artifacts"
        )


def _verify_exact_descriptors(
    kind: str,
    candidates: tuple[Neo4jIndexDescriptor | Neo4jConstraintDescriptor, ...],
    expected_by_name: Mapping[
        str,
        Neo4jIndexDescriptor | Neo4jConstraintDescriptor,
    ],
) -> None:
    for candidate in candidates:
        name = candidate.name
        expected = expected_by_name.get(name)
        if expected is None or candidate != expected:
            raise SchemaDeploymentMismatch(
                f"schema cleanup {kind} descriptor changed or escaped the exact allowlist: "
                f"{name}"
            )


__all__ = [
    "SchemaArtifactCleanupPlan",
    "TARGET_CONSTRAINTS",
    "TARGET_INDEXES",
    "TARGET_LABELS",
    "TargetLabelUsage",
    "plan_zero_count_schema_artifact_cleanup",
    "verify_schema_artifact_cleanup_postcondition",
]
