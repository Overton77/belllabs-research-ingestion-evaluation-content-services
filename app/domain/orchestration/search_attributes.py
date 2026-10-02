"""BellLabs Temporal Search Attributes (REQ-CP-EXEC-015, `CON-CP-TEMPORAL-IDENTITY-V1`).

Search Attributes are the only join between PostgreSQL authority and Temporal executions.
They carry identifiers and hashes only: never checkpoint IDs, prompts, raw scopes, or
secrets. The policy travels in the root, family, and `OperationWorkflow` inputs (and
through Continue-As-New); workflow code never reads it from worker configuration. An
absent policy means `disabled`, which keeps every captured history replayable.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from types import MappingProxyType
from typing import Final, Literal

SearchAttributePolicy = Literal["required", "disabled"]
WorkflowKind = Literal["root", "family", "operation"]
SearchAttributeFamily = Literal["stage_graph", "goal_directed"]

SEARCH_ATTRIBUTES_REQUIRED: Final = "required"
SEARCH_ATTRIBUTES_DISABLED: Final = "disabled"

RUN_ID: Final = "BellLabsRunId"
SCOPE_HASH: Final = "BellLabsScopeHash"
WORKFLOW_KIND: Final = "BellLabsWorkflowKind"
FAMILY: Final = "BellLabsFamily"
EXECUTION_EPOCH: Final = "BellLabsExecutionEpoch"
PARENT_RUN_ID: Final = "BellLabsParentRunId"
UNIT_KEY: Final = "BellLabsUnitKey"
UNIT_KIND: Final = "BellLabsUnitKind"
EXECUTION_GENERATION: Final = "BellLabsExecutionGeneration"

# 7 Keyword and 2 Int custom attributes: within the postgres12 SQL visibility store's
# pre-allocated 10 Keyword / 3 Int columns per namespace. The semantic attempt is read
# from the PostgreSQL unit record, not from Visibility.
BELLLABS_SEARCH_ATTRIBUTES: Final = MappingProxyType(
    {
        RUN_ID: "keyword",
        SCOPE_HASH: "keyword",
        WORKFLOW_KIND: "keyword",
        FAMILY: "keyword",
        EXECUTION_EPOCH: "int",
        PARENT_RUN_ID: "keyword",
        UNIT_KEY: "keyword",
        UNIT_KIND: "keyword",
        EXECUTION_GENERATION: "int",
    }
)

_WORKFLOW_FAMILIES: Final = MappingProxyType(
    {
        "StageGraph": "stage_graph",
        "GoalDirected": "goal_directed",
        "stage_graph": "stage_graph",
        "goal_directed": "goal_directed",
    }
)


class SearchAttributePolicyError(ValueError):
    """A composition supplied a Search Attribute policy its environment forbids."""


def search_attribute_scope_hash(request_scope: str) -> str:
    """`BellLabsScopeHash`: SHA-256 of the request scope; the raw scope is never indexed."""

    if not request_scope:
        raise ValueError("request scope must be non-empty")
    return "sha256:" + sha256(request_scope.encode("utf-8")).hexdigest()


def search_attribute_family(family: str) -> SearchAttributeFamily:
    try:
        return _WORKFLOW_FAMILIES[family]  # type: ignore[return-value]
    except KeyError as error:
        raise ValueError(f"undeclared BellLabs workflow family: {family}") from error


def require_production_search_attribute_policy(policy: str) -> SearchAttributePolicy:
    """Production and persistent qualification compositions start executions `required`."""

    if policy != SEARCH_ATTRIBUTES_REQUIRED:
        raise SearchAttributePolicyError(
            "production composition requires the Search Attribute policy 'required'"
        )
    return SEARCH_ATTRIBUTES_REQUIRED


@dataclass(frozen=True)
class BellLabsSearchAttributeValues:
    """The exact attribute values one root, family, or operation execution carries."""

    run_id: str
    scope_hash: str
    workflow_kind: WorkflowKind
    family: SearchAttributeFamily | None = None
    execution_epoch: int | None = None
    parent_run_id: str | None = None
    unit_key: str | None = None
    unit_kind: str | None = None
    execution_generation: int | None = None

    def __post_init__(self) -> None:
        if not self.run_id or not self.scope_hash.startswith("sha256:"):
            raise ValueError("Search Attributes require a run id and a scope hash")
        if self.execution_epoch is not None and self.execution_epoch < 1:
            raise ValueError("execution epoch must be positive")
        if self.execution_generation is not None and self.execution_generation < 1:
            raise ValueError("execution generation must be positive")
        if self.workflow_kind != "operation" and (
            self.unit_key is not None
            or self.unit_kind is not None
            or self.execution_generation is not None
        ):
            raise ValueError("unit attributes are set on operation executions only")

    def as_mapping(self) -> dict[str, str | int]:
        values: dict[str, str | int | None] = {
            RUN_ID: self.run_id,
            SCOPE_HASH: self.scope_hash,
            WORKFLOW_KIND: self.workflow_kind,
            FAMILY: self.family,
            EXECUTION_EPOCH: self.execution_epoch,
            PARENT_RUN_ID: self.parent_run_id,
            UNIT_KEY: self.unit_key,
            UNIT_KIND: self.unit_kind,
            EXECUTION_GENERATION: self.execution_generation,
        }
        return {name: value for name, value in values.items() if value is not None}


def run_search_attributes(
    *,
    workflow_kind: Literal["root", "family"],
    run_id: str,
    request_scope: str,
    family: str,
    execution_epoch: int,
    parent_run_id: str | None = None,
) -> BellLabsSearchAttributeValues:
    return BellLabsSearchAttributeValues(
        run_id=run_id,
        scope_hash=search_attribute_scope_hash(request_scope),
        workflow_kind=workflow_kind,
        family=search_attribute_family(family),
        execution_epoch=execution_epoch,
        parent_run_id=parent_run_id,
    )


def operation_search_attributes(
    *,
    run_id: str,
    request_scope: str,
    execution_generation: int,
    family: str | None = None,
    execution_epoch: int | None = None,
    unit_key: str | None = None,
    unit_kind: str | None = None,
) -> BellLabsSearchAttributeValues:
    return BellLabsSearchAttributeValues(
        run_id=run_id,
        scope_hash=search_attribute_scope_hash(request_scope),
        workflow_kind="operation",
        family=search_attribute_family(family) if family is not None else None,
        execution_epoch=execution_epoch,
        unit_key=unit_key,
        unit_kind=unit_kind,
        execution_generation=execution_generation,
    )


def visibility_run_query(run_id: str, request_scope: str) -> str:
    """Visibility list filter for one run's executions, bound to the caller's scope."""

    return (
        f"{RUN_ID} = {_quote(run_id)} AND "
        f"{SCOPE_HASH} = {_quote(search_attribute_scope_hash(request_scope))}"
    )


def _quote(value: str) -> str:
    if "'" in value or "\\" in value or "\n" in value:
        raise ValueError("Visibility filter values must not contain quotes or escapes")
    return f"'{value}'"
