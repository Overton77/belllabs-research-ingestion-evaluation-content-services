"""Run Transcript over HTTP (SPEC-03, C3).

- `GET /v1/applications/{app}/runs/{run_id}/transcript` returns the materialized
  transcript: JSONL (`application/x-ndjson`, default), a JSON page (`format=json` or
  `Accept: application/json`), or Markdown (`format=md` or `Accept: text/markdown`). The
  next cursor travels in `X-Transcript-Next-Cursor`.
- `GET .../runs/{run_id}/frames/tail` is a non-canonical Server-Sent Events tail over the
  Native Event Store for dashboards and `--follow`; every event says `canonical: false`.

Authentication and the application/tenant scope are verified exactly as the Mission
Control router does; `workflow_run.read` is required. A malformed cursor is
`409 CURSOR_EXPIRED`.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response, StreamingResponse

from mission_control.application.frames.transcript import (
    FullBodyUnavailable,
    TranscriptDenied,
    TranscriptRunNotFound,
    TranscriptService,
)
from mission_control.domain.frames.render import to_jsonl, to_markdown
from mission_control.domain.frames.transcript import CursorExpired, TranscriptPage, TranscriptQuery
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    authorize_application,
    get_mission_principal,
    principal_request_scope,
)

router = APIRouter(prefix="/v1/applications/{application_id}", tags=["mission-transcript"])

NEXT_CURSOR_HEADER = "X-Transcript-Next-Cursor"
MAX_TAIL_SECONDS = 300.0


def get_transcript_service(
    application_id: str,
    request: Request,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
) -> TranscriptService:
    authorize_application(application_id, request, principal)
    registry = getattr(request.app.state, "mission_control_transcript_services", {})
    service = registry.get((principal.installation_id, application_id, principal.tenant_id))
    if not isinstance(service, TranscriptService):
        raise HTTPException(status_code=503, detail={"code": "transcript_unavailable"})
    if service.request_scope != principal_request_scope(principal):
        raise HTTPException(status_code=503, detail={"code": "service_scope_mismatch"})
    return service


Principal = Annotated[MissionPrincipal, Depends(get_mission_principal)]
Service = Annotated[TranscriptService, Depends(get_transcript_service)]


def _error(exc: Exception) -> HTTPException:
    if isinstance(exc, CursorExpired):
        return HTTPException(
            status_code=409, detail={"code": "CURSOR_EXPIRED", "message": str(exc)}
        )
    if isinstance(exc, TranscriptDenied):
        return HTTPException(status_code=403, detail={"code": "unauthorized"})
    if isinstance(exc, TranscriptRunNotFound):
        return HTTPException(status_code=404, detail={"code": "resource_not_found"})
    if isinstance(exc, FullBodyUnavailable):
        return HTTPException(status_code=501, detail={"code": "full_body_unavailable"})
    return HTTPException(status_code=422, detail={"code": "invalid_transcript_request"})


def _query(
    since: str | None,
    activation: str | None,
    node: str | None,
    kinds: str | None,
    canonical_only: bool,
    subordinate: str | None,
    limit: int,
) -> TranscriptQuery:
    try:
        return TranscriptQuery(
            since=since or None,
            activation=activation or None,
            node=node or None,
            kinds=frozenset(item.strip() for item in (kinds or "").split(",") if item.strip()),
            canonical_only=canonical_only,
            subordinate=subordinate or None,
            limit=limit,
        )
    except ValueError:
        raise HTTPException(
            status_code=422, detail={"code": "invalid_transcript_request"}
        ) from None


def _format(format_: str | None, accept: str | None) -> str:
    if format_:
        if format_ not in {"jsonl", "json", "md"}:
            raise HTTPException(status_code=422, detail={"code": "invalid_transcript_format"})
        return format_
    accepted = (accept or "").lower()
    if "text/markdown" in accepted:
        return "md"
    if "application/json" in accepted and "ndjson" not in accepted:
        return "json"
    return "jsonl"


def _page_response(page: TranscriptPage, kind: str, *, run_id: str) -> Response:
    headers = {
        NEXT_CURSOR_HEADER: page.next_cursor or "",
        "X-Transcript-Has-More": str(page.has_more).lower(),
    }
    if kind == "md":
        return PlainTextResponse(
            to_markdown(page.entries, run_id=run_id),
            media_type="text/markdown; charset=utf-8",
            headers=headers,
        )
    if kind == "json":
        return JSONResponse(page.model_dump(mode="json", exclude_none=True), headers=headers)
    body = to_jsonl(page.entries)

    async def stream() -> AsyncIterator[str]:
        for line in body.splitlines(keepends=True):
            yield line

    return StreamingResponse(stream(), media_type="application/x-ndjson", headers=headers)


@router.get("/runs/{run_id}/transcript")
async def run_transcript(
    run_id: str,
    principal: Principal,
    service: Service,
    format_: Annotated[str | None, Query(alias="format")] = None,
    since: str | None = None,
    activation: str | None = None,
    node: str | None = None,
    kinds: str | None = None,
    canonical_only: bool = False,
    subordinate: str | None = None,
    limit: Annotated[int, Query(ge=1, le=5_000)] = 500,
    full: bool = False,
    accept: Annotated[str | None, Header()] = None,
) -> Response:
    query = _query(since, activation, node, kinds, canonical_only, subordinate, limit)
    kind = _format(format_, accept)
    try:
        page = await service.materialize(run_id, actor=principal.actor, query=query, full=full)
    except (CursorExpired, TranscriptDenied, TranscriptRunNotFound, FullBodyUnavailable) as exc:
        raise _error(exc) from None
    return _page_response(page, kind, run_id=run_id)


@router.get("/runs/{run_id}/frames/tail")
async def frames_tail(
    run_id: str,
    principal: Principal,
    service: Service,
    after: str | None = None,
    seconds: Annotated[float, Query(alias="timeout", gt=0, le=MAX_TAIL_SECONDS)] = 30.0,
    poll: Annotated[float, Query(ge=0.05, le=10)] = 1.0,
) -> StreamingResponse:
    """Non-canonical live tail (SSE). Ends after `timeout` seconds with an `end` event."""

    try:
        first = await service.tail_frames(run_id, actor=principal.actor, after=after)
    except (CursorExpired, TranscriptDenied, TranscriptRunNotFound) as exc:
        raise _error(exc) from None

    def sse(event: str, data: dict[str, Any]) -> str:
        return f"event: {event}\ndata: {json.dumps(data, sort_keys=True, ensure_ascii=False)}\n\n"

    async def stream() -> AsyncIterator[str]:
        deadline = time.monotonic() + seconds
        page = first
        cursor = after
        while True:
            for entry in page.entries:
                cursor = entry.cursor
                yield sse(
                    "frame",
                    {**entry.model_dump(mode="json", exclude_none=True), "canonical": False},
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                yield sse("end", {"next_cursor": cursor, "reason": "timeout", "canonical": False})
                return
            yield ": keepalive\n\n"
            await asyncio.sleep(min(poll, remaining))
            page = await service.tail_frames(run_id, actor=principal.actor, after=cursor)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )
