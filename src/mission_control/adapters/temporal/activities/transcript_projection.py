"""``transcript.project``: the transcript search projection job (SPEC-03, C4).

Rebuilds ``mission_control_search.transcript_document`` for the listed runs of one tenant
scope (forced RLS binds one tenant per transaction). It is idempotent: a second run of the
same activity upserts nothing and deletes nothing. Searches also refresh their run's
projection before ranking, so the job keeps idle runs' indexes warm rather than gating
correctness. Registration on a worker and its schedule are deployment wiring.
"""

from __future__ import annotations

from collections.abc import Callable

from pydantic import BaseModel, ConfigDict, Field
from temporalio import activity

from mission_control.application.frames.search import ProjectionReceipt, TranscriptProjector
from mission_control.application.frames.transcript import TranscriptRunNotFound

TRANSCRIPT_PROJECT_ACTIVITY = "transcript.project"


class TranscriptProjectInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1)
    run_ids: tuple[str, ...] = Field(min_length=1, max_length=500)


class TranscriptProjectResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    receipts: tuple[ProjectionReceipt, ...] = ()
    missing_runs: tuple[str, ...] = ()


class TranscriptProjectionActivities:
    def __init__(self, projectors: Callable[[str], TranscriptProjector]) -> None:
        self._projectors = projectors

    @activity.defn(name=TRANSCRIPT_PROJECT_ACTIVITY)
    async def project(self, request: TranscriptProjectInput) -> TranscriptProjectResult:
        projector = self._projectors(request.request_scope)
        receipts: list[ProjectionReceipt] = []
        missing: list[str] = []
        for run_id in request.run_ids:
            try:
                receipts.append(await projector.project(run_id))
            except TranscriptRunNotFound:
                missing.append(run_id)
            activity.heartbeat(run_id)
        return TranscriptProjectResult(receipts=tuple(receipts), missing_runs=tuple(missing))
