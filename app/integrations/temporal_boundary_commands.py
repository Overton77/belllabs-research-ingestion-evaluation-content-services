"""Temporal transport for accepted boundary commands (RRM-007, REQ-CP-EXEC-007).

A command reaches Temporal only as a recorded delivery of an accepted run-control command.
The transport delivers through the stable root first (`deliver_message` Update: generation
and sequence are validated there and the root's receipt cache de-duplicates), then to the
exact family boundary (`deliver_boundary_command` Update). Only the family's acknowledgement
is evidence of `delivered`; application is a separate fact the family records itself.
"""

from __future__ import annotations

from dataclasses import replace

from temporalio.client import Client

from app.application.run_control.boundary_interventions import BoundaryDeliveryResult
from app.domain.control_plane.canonical import stable_json_dump
from app.domain.orchestration.contracts import (
    BoundaryCommandAck,
    BoundaryCommandDelivery,
    WorkflowMessage,
    WorkflowMessageReceipt,
)
from app.domain.run_control.contracts import BoundaryCommandStatus

ROOT_DELIVERY_UPDATE = "deliver_message"
FAMILY_DELIVERY_UPDATE = "deliver_boundary_command"


class BoundaryDeliveryGap(RuntimeError):
    """The root saw a sequence gap: an earlier command was not delivered yet."""


def family_delivery(status: BoundaryCommandStatus) -> BoundaryCommandDelivery:
    command = status.command
    if command.kind not in {"pause", "resume", "satisfy_wait"}:
        raise ValueError(f"{command.kind} is not delivered to a family boundary")
    return BoundaryCommandDelivery(
        command_id=command.command_id,
        kind=command.kind,  # type: ignore[arg-type]
        target_sequence=command.target_sequence,
        execution_epoch=command.target.execution_epoch,
        execution_generation=command.target.execution_generation,
        accepted_run_version=command.accepted_run_version,
        payload=stable_json_dump(command.action),
        payload_digest=command.payload_digest,
        idempotency_issuer=command.idempotency_issuer,
    )


class TemporalBoundaryCommandTransport:
    def __init__(self, client: Client) -> None:
        self._client = client

    async def deliver(self, status: BoundaryCommandStatus) -> BoundaryDeliveryResult:
        command = status.command
        target = command.target
        if target.family_workflow_id is None:
            raise ValueError("only a family-targeted command is delivered by this transport")
        family_ref = target.family_workflow_id
        if not await self._running(family_ref):
            # F1: the exact target execution is closed; the command can never apply there.
            return BoundaryDeliveryResult(
                "stale_target", family_ref, "the family execution is not running"
            )
        if target.root_workflow_id is not None:
            if not await self._running(target.root_workflow_id):
                return BoundaryDeliveryResult(
                    "stale_target", target.root_workflow_id, "the root execution is not running"
                )
            root_receipt: WorkflowMessageReceipt = await self._client.get_workflow_handle(
                target.root_workflow_id
            ).execute_update(
                ROOT_DELIVERY_UPDATE,
                WorkflowMessage(
                    message_id=command.command_id,
                    sequence=command.target_sequence,
                    kind="control",
                    payload_ref=f"boundary-command:{command.command_id}",
                    execution_generation=target.execution_generation,
                ),
                result_type=WorkflowMessageReceipt,
            )
            # F7: the root answers `duplicate` for a cached receipt and keeps the cached
            # status; a cached `accepted` is a delivery, anything else is decided again.
            if root_receipt.status == "duplicate":
                root_receipt = replace(root_receipt, status=root_receipt.cached_status)
            if root_receipt.status in {"gap", "duplicate"}:
                raise BoundaryDeliveryGap(
                    f"root {target.root_workflow_id} has not received the command before "
                    f"sequence {command.target_sequence}"
                )
            if root_receipt.status == "stale_generation":
                return BoundaryDeliveryResult(
                    "stale_generation",
                    f"{target.root_workflow_id}@segment:{root_receipt.technical_segment}",
                    "root execution generation moved past the command",
                )
        ack: BoundaryCommandAck = await self._client.get_workflow_handle(
            target.family_workflow_id
        ).execute_update(
            FAMILY_DELIVERY_UPDATE,
            family_delivery(status),
            result_type=BoundaryCommandAck,
        )
        if ack.status == "gap":
            raise BoundaryDeliveryGap(
                f"family {target.family_workflow_id} has not received the command before "
                f"sequence {command.target_sequence}"
            )
        return BoundaryDeliveryResult(
            ack.status,
            f"{target.family_workflow_id}@segment:{ack.technical_segment}",
            ack.detail,
        )

    async def _running(self, workflow_id: str) -> bool:
        description = await self._client.get_workflow_handle(workflow_id).describe()
        return description.status is not None and description.status.name == "RUNNING"
