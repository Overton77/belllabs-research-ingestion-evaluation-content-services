from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict

from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.kernel import DecisionRequest, DecisionResponse
from app.domain.run_control.errors import IdempotencyConflict


class DurableDecisionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request: DecisionRequest
    status: Literal["pending", "answered", "expired", "cancelled"] = "pending"
    response: DecisionResponse | None = None


class DecisionResponseAuthorization(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    decision_id: str
    request_scope: str
    actor_ref: str
    approved: bool


class DecisionAuthority(Protocol):
    async def current_lifecycle_version(
        self,
        request_scope: str,
        binding_id: str,
    ) -> int: ...

    async def authorize_response(
        self,
        request: DecisionRequest,
        response: DecisionResponse,
    ) -> DecisionResponseAuthorization: ...


class DenyByDefaultDecisionAuthority:
    """Concrete production-safe default until an authenticated policy adapter is wired."""

    async def current_lifecycle_version(
        self,
        request_scope: str,
        binding_id: str,
    ) -> int:
        del request_scope, binding_id
        raise PermissionError("decision authority is not configured")

    async def authorize_response(
        self,
        request: DecisionRequest,
        response: DecisionResponse,
    ) -> DecisionResponseAuthorization:
        return DecisionResponseAuthorization(
            decision_id=request.decision_id,
            request_scope=request.request_scope,
            actor_ref=response.actor_ref,
            approved=False,
        )


class DecisionRepository(Protocol):
    async def create(self, request: DecisionRequest) -> DurableDecisionRecord: ...

    async def get(
        self,
        request_scope: str,
        decision_id: str,
    ) -> DurableDecisionRecord | None: ...

    async def answer(
        self,
        request: DecisionRequest,
        response: DecisionResponse,
    ) -> DurableDecisionRecord: ...


class InMemoryDecisionRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._records: dict[tuple[str, str], DurableDecisionRecord] = {}

    async def create(self, request: DecisionRequest) -> DurableDecisionRecord:
        key = (request.request_scope, request.decision_id)
        async with self._lock:
            prior = self._records.get(key)
            if prior is not None:
                if prior.request != request:
                    raise IdempotencyConflict("decision identity has conflicting intent")
                return deepcopy(prior)
            record = DurableDecisionRecord(request=request)
            self._records[key] = record
            return deepcopy(record)

    async def get(
        self,
        request_scope: str,
        decision_id: str,
    ) -> DurableDecisionRecord | None:
        return deepcopy(self._records.get((request_scope, decision_id)))

    async def answer(
        self,
        request: DecisionRequest,
        response: DecisionResponse,
    ) -> DurableDecisionRecord:
        key = (request.request_scope, request.decision_id)
        async with self._lock:
            prior = self._records.get(key)
            if prior is None or prior.request != request:
                raise LookupError("durable decision request not found")
            if prior.response is not None:
                if prior.response != response:
                    raise IdempotencyConflict("decision already has a different response")
                return deepcopy(prior)
            answered = prior.model_copy(update={"status": "answered", "response": response})
            self._records[key] = answered
            return deepcopy(answered)


class DurableDecisionService:
    """Persists a BellLabs decision before producing any Agent Server resume command."""

    def __init__(
        self,
        *,
        repository: DecisionRepository,
        authority: DecisionAuthority | None = None,
    ) -> None:
        self._repository = repository
        self._authority = authority or DenyByDefaultDecisionAuthority()

    async def create_request(self, request: DecisionRequest) -> DurableDecisionRecord:
        expected = sha256_digest(
            request.model_dump(mode="json", exclude={"request_digest"})
        )
        if expected != request.request_digest:
            raise ValueError("decision request digest mismatch")
        return await self._repository.create(request)

    async def respond(
        self,
        response: DecisionResponse,
        *,
        now: datetime,
    ) -> DurableDecisionRecord:
        record = await self._repository.get(response.request_scope, response.decision_id)
        if record is None:
            raise LookupError("durable decision request not found")
        request = record.request
        if request.expires_at is not None and now >= request.expires_at:
            raise ValueError("decision request is expired")
        if response.expected_lifecycle_version != request.expected_lifecycle_version:
            raise ValueError("decision response expected lifecycle version is stale")
        current_version = await self._authority.current_lifecycle_version(
            request.request_scope,
            request.binding_id,
        )
        if current_version != request.expected_lifecycle_version:
            raise ValueError("BellLabs lifecycle advanced before the decision response")
        if response.response_schema_ref != request.schema_ref:
            raise ValueError("decision response schema does not match the request")
        authorization = await self._authority.authorize_response(request, response)
        if (
            not authorization.approved
            or authorization.decision_id != request.decision_id
            or authorization.request_scope != request.request_scope
            or authorization.actor_ref != response.actor_ref
        ):
            raise PermissionError("decision response lacks matching scope and actor authority")
        return await self._repository.answer(request, response)

    async def resume_map(
        self,
        *,
        request_scope: str,
        runtime_interrupt_to_decision: dict[str, str],
    ) -> dict[str, dict[str, str]]:
        result: dict[str, dict[str, str]] = {}
        for interrupt_id, decision_id in runtime_interrupt_to_decision.items():
            record = await self._repository.get(request_scope, decision_id)
            if record is None or record.status != "answered" or record.response is None:
                raise ValueError("every runtime interrupt requires one persisted BellLabs decision")
            result[interrupt_id] = {
                "decision_id": decision_id,
                "response_digest": record.response.response_digest,
                "response_payload_ref": record.response.response_payload_ref,
            }
        return result
