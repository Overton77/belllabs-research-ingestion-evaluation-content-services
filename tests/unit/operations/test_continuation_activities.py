"""FT-B4: the continuation activities call the scoped service and type their failures."""

from __future__ import annotations

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from mission_control.adapters.temporal.activities.continuation import (
    CONTINUATION_REQUEST_ACTIVITY,
    CONTINUATION_SEAL_ACTIVITY,
    CONTINUATION_TRANSFER_ACTIVITY,
    ContinuationActivities,
    ContinuationRequestInput,
    ContinuationSealInput,
    ContinuationTransferInput,
)
from mission_control.application.context.continuation import (
    HydrationReceipt,
    HydrationRequest,
    TransferStatus,
)
from tests.fixtures.continuation import (
    CONT_SCOPE,
    RUN_KEY,
    build_service,
    facts,
    seal_target,
    trigger,
)


class Hydrator:
    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt:
        return HydrationReceipt(
            target_session_ref="thread-new", restored=dict(request.snapshot.manifest)
        )


@pytest.mark.asyncio
async def test_request_seal_transfer_through_activities() -> None:
    wired = build_service(session_files={"thread-collect-1": {"/inputs/a.json": "{}"}})
    activities = ContinuationActivities(
        lambda scope: wired["service"], lambda lane, scope: Hydrator()
    )
    names = {getattr(item, "__temporal_activity_definition").name for item in activities.all()}
    assert {CONTINUATION_REQUEST_ACTIVITY, CONTINUATION_SEAL_ACTIVITY} <= names
    assert CONTINUATION_TRANSFER_ACTIVITY in names
    env = ActivityEnvironment()
    transfer = await env.run(
        activities.request,
        ContinuationRequestInput(
            request_scope=CONT_SCOPE,
            run_key=RUN_KEY,
            activation_key="unit-collect-1",
            logical_execution_id="logical-collect-1",
            lane_profile="deep_agents",
            source_session_ref="thread-collect-1",
            trigger=trigger(),
        ),
    )
    sealed = await env.run(
        activities.seal,
        ContinuationSealInput(
            request_scope=CONT_SCOPE,
            transfer_id=transfer.transfer_id,
            facts=facts(),
            target=seal_target(),
        ),
    )
    assert sealed.checkpoint is not None and sealed.checkpoint.valid
    moved = await env.run(
        activities.transfer,
        ContinuationTransferInput(
            request_scope=CONT_SCOPE, transfer_id=transfer.transfer_id, lane_profile="deep_agents"
        ),
    )
    assert moved.transfer.status == TransferStatus.TRANSFERRED


@pytest.mark.asyncio
async def test_activity_failures_are_typed_and_non_retryable() -> None:
    wired = build_service()
    activities = ContinuationActivities(lambda scope: wired["service"])
    env = ActivityEnvironment()
    with pytest.raises(ApplicationError) as missing:
        await env.run(
            activities.transfer,
            ContinuationTransferInput(
                request_scope=CONT_SCOPE, transfer_id="nope", lane_profile="deep_agents"
            ),
        )
    assert missing.value.type == "unsupported_control" and missing.value.non_retryable
    with pytest.raises(ApplicationError) as unknown:
        await env.run(
            activities.release,
            ContinuationTransferInput(
                request_scope=CONT_SCOPE, transfer_id="nope", lane_profile="deep_agents"
            ),
        )
    assert unknown.value.type == "not_found" and unknown.value.non_retryable
