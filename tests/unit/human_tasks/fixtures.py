"""FIXTURE builders for Human Gate tests (MP-10); no provider or service is contacted."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from mission_control.domain.programs.human_gate import (
    HumanGateActivation,
    HumanGateSpec,
    HumanResolutionRequest,
    HumanTaskView,
    ReviewPacketItem,
    open_activation,
    packet_item,
)

OPENED_AT = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
SCOPE = "tenant-1"
DRAFT_DIGEST = "sha256:" + "d" * 64


def spec(**updates: Any) -> HumanGateSpec:
    values: dict[str, Any] = {
        "gate_key": "review",
        "task_kind": "REVIEW",
        "prompt": "Accept the claim table?",
        "reviewers": ("owner",),
        "packet_sources": ("synthesize.claims",),
    }
    values.update(updates)
    return HumanGateSpec(**values)


def packet(digest: str = DRAFT_DIGEST) -> tuple[ReviewPacketItem, ...]:
    return (packet_item("synthesize.claims", f"artifact://claims.json@{digest}"),)


def activation(
    *,
    gate: HumanGateSpec | None = None,
    review_round: int = 1,
    digest: str = DRAFT_DIGEST,
    scope: str = SCOPE,
    run_id: str = "run-1",
    opened_at: datetime = OPENED_AT,
) -> HumanGateActivation:
    return open_activation(
        request_scope=scope,
        run_id=run_id,
        family="StageGraph",
        activation_key=f"stage:review:cycle:{review_round - 1}",
        execution_epoch=1,
        review_round=review_round,
        spec=gate or spec(),
        packet=packet(digest),
        opened_at=opened_at,
    )


def open_task(item: HumanGateActivation) -> HumanTaskView:
    return HumanTaskView(
        human_task_id=str(item.human_task_id),
        task_key=item.task_key,
        kind=item.kind,
        target_ref=item.target_ref,
        lifecycle="open",
        version=1,
        deadline_at=item.deadline_at,
        on_timeout=item.spec.on_timeout,
        activation=item,
        created_at=item.opened_at,
        updated_at=item.opened_at,
    )


def request(item: HumanGateActivation, **updates: Any) -> HumanResolutionRequest:
    values: dict[str, Any] = {
        "request_id": "req-1",
        "expected_task_version": 1,
        "decision": "approve",
        "reviewed_packet_digest": item.packet_digest,
    }
    values.update(updates)
    return HumanResolutionRequest(**values)


def later(seconds: int) -> datetime:
    return OPENED_AT + timedelta(seconds=seconds)
