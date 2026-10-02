"""Captured Temporal history inspection: patch IDs and scheduled activity inputs.

The Python SDK records `workflow.patched` as a `core_patch` marker whose `patch-data` detail
is the JSON `{"id": ..., "deprecated": ...}`; the marker name alone never names the patch.
"""

from __future__ import annotations

import json
from typing import Any

from temporalio.client import WorkflowHistory


def patch_ids(history: WorkflowHistory) -> set[str]:
    """The `workflow.patched` IDs recorded in a history."""

    ids: set[str] = set()
    for event in history.events:
        if not event.HasField("marker_recorded_event_attributes"):
            continue
        marker = event.marker_recorded_event_attributes
        if marker.marker_name != "core_patch":
            continue
        for payload in marker.details["patch-data"].payloads:
            ids.add(str(json.loads(payload.data)["id"]))
    return ids


def scheduled_activity_inputs(history: WorkflowHistory, activity_type: str) -> list[Any]:
    """The decoded JSON inputs of every scheduled activity of one type, in history order."""

    inputs: list[Any] = []
    for event in history.events:
        if not event.HasField("activity_task_scheduled_event_attributes"):
            continue
        attributes = event.activity_task_scheduled_event_attributes
        if attributes.activity_type.name == activity_type:
            inputs.extend(json.loads(item.data) for item in attributes.input.payloads)
    return inputs
