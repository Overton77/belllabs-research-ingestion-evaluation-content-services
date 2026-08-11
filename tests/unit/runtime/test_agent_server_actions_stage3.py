from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.runtime.agent_server_actions import BellLabsAgentServerActionResolver
from app.application.runtime.runtime_decisions import (
    DecisionResponseAuthorization,
    DurableDecisionService,
    InMemoryDecisionRepository,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.contracts import (
    ActorRef,
    AppendInputIntervention,
    Correlation,
    RespondToInterruptIntervention,
    RuntimeExecutionBinding,
)
from app.domain.graph_runtime.definitions import ContentAddressedRef, RuntimeDefinitionKind
from app.domain.graph_runtime.identities import ExecutionEpochKey
from app.domain.graph_runtime.kernel import DecisionRequest, DecisionResponse

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def binding() -> RuntimeExecutionBinding:
    return RuntimeExecutionBinding(
        binding_id="binding-1",
        epoch=ExecutionEpochKey(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            execution_epoch=1,
        ),
        submission_id="submission-1",
        submission_idempotency_key="submission-1",
        submission_digest=DIGEST,
        run_plan_digest=DIGEST,
        graph_assembly_digest=DIGEST,
        state_schema_digest=DIGEST,
        runtime_provider="legacy_temporal",
        created_at=NOW,
        updated_at=NOW,
    )


class Authority:
    async def current_lifecycle_version(self, _scope, _binding):  # type: ignore[no-untyped-def]
        return 3

    async def authorize_response(self, request, response):  # type: ignore[no-untyped-def]
        return DecisionResponseAuthorization(
            decision_id=request.decision_id,
            request_scope=request.request_scope,
            actor_ref=response.actor_ref,
            approved=True,
        )


class InterruptBindings:
    async def runtime_interrupt_map(self, _scope, decision_id):  # type: ignore[no-untyped-def]
        return {"runtime-interrupt-1": decision_id}


class Policy:
    def __init__(self, enqueue: bool) -> None:
        self.enqueue = enqueue

    async def enqueue_allowed(self, _binding):  # type: ignore[no-untyped-def]
        return self.enqueue


def append_input(current: RuntimeExecutionBinding) -> AppendInputIntervention:
    values = {
        "kind": "append_input",
        "command_id": "append-1",
        "idempotency_key": "append-1",
        "epoch": current.epoch,
        "expected_belllabs_version": 3,
        "expected_checkpoint": None,
        "actor": ActorRef(
            actor_id="operator-1",
            actor_type="operator",
            authority_ref="authority:operator@1",
        ),
        "reason": "append admitted input",
        "correlation": Correlation(correlation_id="correlation-1"),
        "requested_at": NOW,
        "input_manifest_ref": "input:manifest:2",
        "input_digest": DIGEST,
    }
    return AppendInputIntervention(**values, request_digest=sha256_digest(values))


@pytest.mark.asyncio
async def test_append_input_defaults_to_reject_and_enqueues_only_when_authored() -> None:
    current = binding()
    decisions = DurableDecisionService(
        repository=InMemoryDecisionRepository(),
        authority=Authority(),
    )
    rejected = BellLabsAgentServerActionResolver(
        decisions=decisions,
        interrupt_bindings=InterruptBindings(),
        policy=Policy(False),
    )
    enqueued = BellLabsAgentServerActionResolver(
        decisions=decisions,
        interrupt_bindings=InterruptBindings(),
        policy=Policy(True),
    )

    reject_action = await rejected.resolve(append_input(current), current)
    enqueue_action = await enqueued.resolve(append_input(current), current)

    assert reject_action.multitask_strategy == "reject"
    assert enqueue_action.multitask_strategy == "enqueue"
    assert enqueue_action.enqueue_authorized


@pytest.mark.asyncio
async def test_interrupt_resume_contains_only_persisted_decision_refs_and_digests() -> None:
    repository = InMemoryDecisionRepository()
    decisions = DurableDecisionService(repository=repository, authority=Authority())
    request_values = {
        "decision_id": "decision-1",
        "request_scope": "tenant-1",
        "binding_id": "binding-1",
        "decision_type": "approval",
        "schema_ref": "schema:approval:1",
        "choices_ref": None,
        "evidence_refs": ("evidence:1",),
        "expected_lifecycle_version": 3,
        "policy_ref": "policy:approval:1",
        "requested_at": NOW,
        "expires_at": None,
    }
    decision = DecisionRequest(
        **request_values,
        request_digest=sha256_digest(request_values),
    )
    await decisions.create_request(decision)
    await decisions.respond(
        DecisionResponse(
            decision_id=decision.decision_id,
            request_scope=decision.request_scope,
            response_id="response-1",
            response_schema_ref=decision.schema_ref,
            response_payload_ref="response:payload:1",
            response_digest=DIGEST,
            expected_lifecycle_version=3,
            actor_ref="operator:1",
            decided_at=NOW,
        ),
        now=NOW,
    )
    current = binding()
    intervention_values = {
        "kind": "respond_to_interrupt",
        "command_id": "respond-1",
        "idempotency_key": "respond-1",
        "epoch": current.epoch,
        "expected_belllabs_version": 3,
        "expected_checkpoint": None,
        "actor": ActorRef(
            actor_id="operator-1",
            actor_type="operator",
            authority_ref="authority:operator@1",
        ),
        "reason": "respond to durable decision",
        "correlation": Correlation(correlation_id="correlation-2"),
        "requested_at": NOW,
        "interrupt_request_id": decision.decision_id,
        "response_schema_ref": ContentAddressedRef(
            kind=RuntimeDefinitionKind.STATE_SCHEMA,
            logical_id="schema.approval",
            schema_version="1",
            digest=DIGEST,
        ),
        "response_payload_ref": "response:payload:1",
        "response_digest": DIGEST,
    }
    intervention = RespondToInterruptIntervention(
        **intervention_values,
        request_digest=sha256_digest(intervention_values),
    )
    resolver = BellLabsAgentServerActionResolver(
        decisions=decisions,
        interrupt_bindings=InterruptBindings(),
        policy=Policy(False),
    )

    action = await resolver.resolve(intervention, current)

    assert action.kind == "resume"
    assert action.command == {
        "resume": {
            "runtime-interrupt-1": {
                "decision_id": "decision-1",
                "response_digest": DIGEST,
                "response_payload_ref": "response:payload:1",
            }
        }
    }
