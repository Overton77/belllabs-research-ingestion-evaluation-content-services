"""Read-only run Transcript on the coordinator MCP server (SPEC-03, C3).

- tool `mission_run_transcript(run_id, since?, format?, limit?, kinds?, canonical_only?)`
- resource `mc://applications/{application_id}/runs/{run_id}/transcript{?since}` (JSONL)

Both return exactly the entries the HTTP surface returns for the same principal and are
annotated read-only. The principal's request scope selects the tenant's transcript
service; a principal from another application is refused.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, Protocol

from fastmcp import Context, FastMCP

from mission_control.application.frames.transcript import TranscriptService
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from mission_control.domain.frames.render import to_jsonl, to_markdown
from mission_control.domain.frames.transcript import TranscriptQuery
from mission_control.domain.policies.contracts import ActorContext

TRANSCRIPT_RESOURCE = "mc://applications/{application_id}/runs/{run_id}/transcript{?since}"
TRANSCRIPT_TOOL = "mission_run_transcript"
READ_GRANTS = frozenset({"workflow_run.read", "mission.read", "workflow.result.read"})


class TranscriptPrincipal(Protocol):
    @property
    def actor_id(self) -> str: ...

    @property
    def permissions(self) -> frozenset[str]: ...

    @property
    def request_scope(self) -> str: ...


class TranscriptPrincipalResolver(Protocol):
    async def resolve(self, context: Context) -> Any: ...


class ScopedTranscripts:
    """Selects the transcript service of the principal's verified tenant scope."""

    def __init__(self, services: Mapping[str, TranscriptService]) -> None:
        self._services = dict(services)

    def _service(
        self, principal: TranscriptPrincipal, application_id: str | None
    ) -> TranscriptService:
        scope = principal.request_scope
        try:
            parsed = parse_request_scope(scope)
        except ValueError:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN,
                message="transcripts need a canonical tenant request scope",
            ) from None
        if application_id is not None and parsed.application_id != application_id:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message="application scope denied"
            )
        service = self._services.get(scope)
        if service is None:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message="no transcript service for scope"
            )
        return service

    @staticmethod
    def _actor(principal: TranscriptPrincipal) -> ActorContext:
        permissions = frozenset(principal.permissions)
        if not permissions & READ_GRANTS:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message="principal lacks mission read"
            )
        return ActorContext(
            actor_id=principal.actor_id,
            authority_refs=frozenset(),
            permissions=permissions | {"workflow_run.read"},
        )

    async def transcript(
        self,
        principal: TranscriptPrincipal,
        *,
        run_id: str,
        application_id: str | None = None,
        since: str | None = None,
        output: Literal["jsonl", "md", "json"] = "jsonl",
        limit: int = 500,
        kinds: list[str] | None = None,
        canonical_only: bool = False,
    ) -> dict[str, object]:
        service = self._service(principal, application_id)
        page = await service.materialize(
            run_id,
            actor=self._actor(principal),
            query=TranscriptQuery(
                since=since,
                limit=limit,
                kinds=frozenset(kinds or ()),
                canonical_only=canonical_only,
            ),
        )
        result: dict[str, object] = {
            "run_id": run_id,
            "format": output,
            "next_cursor": page.next_cursor,
            "has_more": page.has_more,
            "canonical": "mission events are canonical; provider frames are evidence",
        }
        if output == "md":
            result["content"] = to_markdown(page.entries, run_id=run_id)
        elif output == "json":
            result["entries"] = [
                entry.model_dump(mode="json", exclude_none=True) for entry in page.entries
            ]
        else:
            result["content"] = to_jsonl(page.entries)
        return result


def register_transcript_tools(
    server: FastMCP,
    transcripts: ScopedTranscripts,
    principals: TranscriptPrincipalResolver,
    *,
    call: Any,
) -> None:
    """Register the read-only tool and resource; `call` wraps results in the MCP envelope."""

    @server.tool(name=TRANSCRIPT_TOOL, annotations={"readOnlyHint": True})
    async def mission_run_transcript(
        run_id: str,
        context: Context,
        since: str | None = None,
        format: Literal["jsonl", "md", "json"] = "jsonl",  # noqa: A002 - public tool argument
        limit: int = 500,
        kinds: list[str] | None = None,
        canonical_only: bool = False,
    ) -> dict[str, object]:
        async def invoke(principal: Any) -> object:
            return await transcripts.transcript(
                principal,
                run_id=run_id,
                since=since,
                output=format,
                limit=limit,
                kinds=kinds,
                canonical_only=canonical_only,
            )

        return await call(context, principals, invoke)

    @server.resource(TRANSCRIPT_RESOURCE, mime_type="application/x-ndjson")
    async def run_transcript_resource(
        application_id: str, run_id: str, context: Context, since: str | None = None
    ) -> str:
        principal = await principals.resolve(context)
        result = await transcripts.transcript(
            principal, run_id=run_id, application_id=application_id, since=since
        )
        return str(result["content"])
