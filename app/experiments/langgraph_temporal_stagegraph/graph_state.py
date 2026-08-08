from __future__ import annotations

from typing import Annotated, TypedDict


class DispatchItem(TypedDict):
    attempt_id: str
    stage_id: str
    prompt: str
    delay_seconds: float


class LaunchReceipt(TypedDict):
    attempt_id: str
    temporal_workflow_id: str
    temporal_run_id: str | None


def merge_receipts(
    left: tuple[LaunchReceipt, ...], right: tuple[LaunchReceipt, ...]
) -> tuple[LaunchReceipt, ...]:
    merged = {item["attempt_id"]: item for item in (*left, *right)}
    return tuple(merged[key] for key in sorted(merged))


def merge_events(
    left: tuple[dict[str, object], ...], right: tuple[dict[str, object], ...]
) -> tuple[dict[str, object], ...]:
    combined = (*left, *right)
    seen: set[str] = set()
    result: list[dict[str, object]] = []
    for item in combined:
        key = str(item.get("event_id") or item)
        if key not in seen:
            seen.add(key)
            result.append(item)
    return tuple(result)


class ExperimentState(TypedDict, total=False):
    run_id: str
    thread_id: str
    dispatch_item: DispatchItem
    dispatch_batch: tuple[DispatchItem, ...]
    launch_receipts: Annotated[tuple[LaunchReceipt, ...], merge_receipts]
    admitted_outputs: dict[str, str]
    waiting_attempt_ids: tuple[str, ...]
    synthesized_output: str | None
    frozen_synthesis_stages: tuple[str, ...]
    event_log: Annotated[tuple[dict[str, object], ...], merge_events]
