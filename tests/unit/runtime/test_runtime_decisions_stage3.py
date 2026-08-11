from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.application.runtime.runtime_decisions import (
    DecisionResponseAuthorization,
    DurableDecisionService,
    InMemoryDecisionRepository,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.kernel import DecisionRequest, DecisionResponse
from app.domain.run_control.errors import IdempotencyConflict

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def request(
    decision_id: str = "decision-1",
    *,
    expires_at: datetime | None = None,
) -> DecisionRequest:
    values = {
        "decision_id": decision_id,
        "request_scope": "tenant-1",
        "binding_id": "binding-1",
        "decision_type": "approval",
        "schema_ref": "schema:approval:1",
        "choices_ref": "choices:approval:1",
        "evidence_refs": ("evidence:1",),
        "expected_lifecycle_version": 7,
        "policy_ref": "policy:approval:1",
        "requested_at": NOW,
        "expires_at": expires_at,
    }
    return DecisionRequest(**values, request_digest=sha256_digest(values))


def response(
    decision_id: str = "decision-1",
    *,
    actor_ref: str = "operator:1",
    expected_version: int = 7,
) -> DecisionResponse:
    return DecisionResponse(
        decision_id=decision_id,
        request_scope="tenant-1",
        response_id=f"response-{decision_id}",
        response_schema_ref="schema:approval:1",
        response_payload_ref=f"response-payload:{decision_id}",
        response_digest=DIGEST,
        expected_lifecycle_version=expected_version,
        actor_ref=actor_ref,
        decided_at=NOW + timedelta(seconds=1),
    )


class Authority:
    def __init__(self, *, version: int = 7, approved_actor: str = "operator:1") -> None:
        self.version = version
        self.approved_actor = approved_actor

    async def current_lifecycle_version(self, _scope, _binding):  # type: ignore[no-untyped-def]
        return self.version

    async def authorize_response(self, decision, answer):  # type: ignore[no-untyped-def]
        return DecisionResponseAuthorization(
            decision_id=decision.decision_id,
            request_scope=decision.request_scope,
            actor_ref=answer.actor_ref,
            approved=answer.actor_ref == self.approved_actor,
        )


@pytest.mark.asyncio
async def test_decision_is_persisted_before_resume_map_is_available() -> None:
    repository = InMemoryDecisionRepository()
    service = DurableDecisionService(repository=repository, authority=Authority())
    decision = request()
    await service.create_request(decision)

    with pytest.raises(ValueError, match="persisted BellLabs decision"):
        await service.resume_map(
            request_scope="tenant-1",
            runtime_interrupt_to_decision={"interrupt-1": decision.decision_id},
        )
    answered = await service.respond(response(), now=NOW + timedelta(seconds=1))
    resume = await service.resume_map(
        request_scope="tenant-1",
        runtime_interrupt_to_decision={"interrupt-1": decision.decision_id},
    )

    assert answered.status == "answered"
    assert resume["interrupt-1"]["decision_id"] == decision.decision_id
    assert resume["interrupt-1"]["response_digest"] == DIGEST


@pytest.mark.asyncio
async def test_duplicate_response_is_idempotent_but_conflicting_response_fails() -> None:
    repository = InMemoryDecisionRepository()
    service = DurableDecisionService(repository=repository, authority=Authority())
    await service.create_request(request())
    accepted = response()

    assert await service.respond(accepted, now=NOW) == await service.respond(accepted, now=NOW)
    with pytest.raises(IdempotencyConflict, match="different response"):
        await service.respond(
            accepted.model_copy(update={"response_digest": "sha256:" + "b" * 64}),
            now=NOW,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("decision", "answer", "authority", "message"),
    [
        (
            request(expires_at=NOW),
            response(),
            Authority(),
            "expired",
        ),
        (
            request(),
            response(expected_version=6),
            Authority(),
            "expected lifecycle version",
        ),
        (
            request(),
            response(),
            Authority(version=8),
            "lifecycle advanced",
        ),
        (
            request(),
            response(actor_ref="intruder"),
            Authority(),
            "scope and actor authority",
        ),
    ],
)
async def test_stale_expired_and_wrong_actor_responses_fail_closed(
    decision: DecisionRequest,
    answer: DecisionResponse,
    authority: Authority,
    message: str,
) -> None:
    service = DurableDecisionService(
        repository=InMemoryDecisionRepository(),
        authority=authority,
    )
    await service.create_request(decision)

    with pytest.raises((ValueError, PermissionError), match=message):
        await service.respond(answer, now=NOW + timedelta(seconds=1))


@pytest.mark.asyncio
async def test_parallel_interrupts_map_distinct_runtime_and_decision_ids() -> None:
    repository = InMemoryDecisionRepository()
    service = DurableDecisionService(repository=repository, authority=Authority())
    for decision_id in ("decision-a", "decision-b"):
        await service.create_request(request(decision_id))
        await service.respond(response(decision_id), now=NOW)

    resume = await service.resume_map(
        request_scope="tenant-1",
        runtime_interrupt_to_decision={
            "runtime-interrupt-a": "decision-a",
            "runtime-interrupt-b": "decision-b",
        },
    )

    assert set(resume) == {"runtime-interrupt-a", "runtime-interrupt-b"}
    assert resume["runtime-interrupt-a"]["decision_id"] == "decision-a"
    assert resume["runtime-interrupt-b"]["decision_id"] == "decision-b"


@pytest.mark.asyncio
async def test_uncomposed_decision_authority_denies_response() -> None:
    service = DurableDecisionService(repository=InMemoryDecisionRepository())
    await service.create_request(request())

    with pytest.raises(PermissionError, match="authority is not configured"):
        await service.respond(response(), now=NOW + timedelta(seconds=1))
