"""Temporal mapping of the BellLabs Search Attributes (REQ-CP-EXEC-015).

Three concerns live here, all keyed by `BELLLABS_SEARCH_ATTRIBUTES`:

* typed keys and values for starting executions (`start_workflow`, child starts);
* the in-workflow guarantee (`ensure_workflow_search_attributes`): under the `required`
  policy an execution that was not started with its attributes upserts exactly the
  missing ones, deterministically, from its own input; under `disabled` (the default,
  and every captured history) it emits no command at all;
* the administrative registration step and the read-only worker readiness check. Neither
  ever reads Temporal's persistence database; both use the Operator service API.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from temporalio import workflow
from temporalio.api.enums.v1 import IndexedValueType
from temporalio.api.operatorservice.v1 import (
    AddSearchAttributesRequest,
    ListSearchAttributesRequest,
)
from temporalio.client import Client
from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes

from mission_control.domain.execution.contracts import OperationWorkflowRequest
from mission_control.domain.programs.search_attributes import (
    BELLLABS_SEARCH_ATTRIBUTES,
    SEARCH_ATTRIBUTES_REQUIRED,
    BellLabsSearchAttributeValues,
    operation_search_attributes,
)

_INDEXED_TYPES = {
    "keyword": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
    "int": IndexedValueType.INDEXED_VALUE_TYPE_INT,
}


class SearchAttributeRegistrationError(RuntimeError):
    """The namespace has a conflicting attribute, no free slot, or misses a declared one."""


def _key(name: str) -> SearchAttributeKey[Any]:
    kind = BELLLABS_SEARCH_ATTRIBUTES[name]
    if kind == "int":
        return SearchAttributeKey.for_int(name)
    return SearchAttributeKey.for_keyword(name)


BELLLABS_SEARCH_ATTRIBUTE_KEYS: tuple[SearchAttributeKey[Any], ...] = tuple(
    _key(name) for name in BELLLABS_SEARCH_ATTRIBUTES
)


def typed_search_attributes(values: BellLabsSearchAttributeValues) -> TypedSearchAttributes:
    return TypedSearchAttributes(
        [SearchAttributePair(_key(name), value) for name, value in values.as_mapping().items()]
    )


def child_search_attributes(
    policy: str, values: BellLabsSearchAttributeValues
) -> TypedSearchAttributes | None:
    """Attributes a parent sets when it starts a child; `None` keeps the command unchanged."""

    if policy != SEARCH_ATTRIBUTES_REQUIRED:
        return None
    return typed_search_attributes(values)


def ensure_workflow_search_attributes(policy: str, values: BellLabsSearchAttributeValues) -> None:
    """Inside a workflow: upsert only the declared attributes it was not started with."""

    if policy != SEARCH_ATTRIBUTES_REQUIRED:
        return
    current = workflow.info().typed_search_attributes
    updates = [
        key.value_set(value)
        for name, value in values.as_mapping().items()
        if current.get(key := _key(name)) != value
    ]
    if updates:
        workflow.upsert_search_attributes(updates)


def operation_workflow_search_attributes(
    request: OperationWorkflowRequest,
    *,
    family: str | None = None,
    execution_epoch: int | None = None,
) -> BellLabsSearchAttributeValues:
    """Operation attributes from the request; its runtime unit is authoritative when bound."""

    operation = request.operation
    unit = operation.runtime_unit
    return operation_search_attributes(
        run_id=operation.identity.run_id,
        request_scope=operation.request_scope,
        execution_generation=request.execution_generation,
        family=unit.family if unit is not None else family,
        execution_epoch=unit.execution_epoch if unit is not None else execution_epoch,
        unit_key=unit.unit_key if unit is not None else None,
        unit_kind=unit.unit_kind if unit is not None else None,
    )


def visible_values(attributes: TypedSearchAttributes) -> dict[str, str | int]:
    """Decode the BellLabs attributes of a Visibility row (unknown attributes are ignored)."""

    values: dict[str, str | int] = {}
    for key in BELLLABS_SEARCH_ATTRIBUTE_KEYS:
        value = attributes.get(key)
        if value is not None:
            values[key.name] = value
    return values


async def _registered(client: Client, namespace: str) -> Mapping[str, int]:
    response = await client.operator_service.list_search_attributes(
        ListSearchAttributesRequest(namespace=namespace)
    )
    return dict(response.custom_attributes)


async def register_belllabs_search_attributes(client: Client, namespace: str) -> tuple[str, ...]:
    """Idempotent administrative registration; fails on a name/type conflict.

    Returns the names it added. The server refuses the request when the namespace has no
    free slot of a type, and that refusal surfaces as `SearchAttributeRegistrationError`.
    """

    registered = await _registered(client, namespace)
    missing: dict[str, int] = {}
    for name, kind in BELLLABS_SEARCH_ATTRIBUTES.items():
        expected = _INDEXED_TYPES[kind]
        actual = registered.get(name)
        if actual is None:
            missing[name] = expected
        elif actual != expected:
            raise SearchAttributeRegistrationError(
                f"Search Attribute {name} is registered with a conflicting type"
            )
    if missing:
        try:
            await client.operator_service.add_search_attributes(
                AddSearchAttributesRequest(namespace=namespace, search_attributes=missing)  # type: ignore[arg-type]
            )
        except Exception as error:  # noqa: BLE001 - surfaced as a typed registration failure
            raise SearchAttributeRegistrationError(
                f"Search Attribute registration failed: {error}"
            ) from error
    return tuple(sorted(missing))


async def verify_belllabs_search_attributes(client: Client, namespace: str) -> None:
    """Worker readiness: every declared attribute exists with its type. Never mutates."""

    registered = await _registered(client, namespace)
    for name, kind in BELLLABS_SEARCH_ATTRIBUTES.items():
        if registered.get(name) != _INDEXED_TYPES[kind]:
            raise SearchAttributeRegistrationError(
                f"Search Attribute {name} is not registered as {kind} in {namespace}"
            )
