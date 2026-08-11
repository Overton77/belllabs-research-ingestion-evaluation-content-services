from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.runtime.runtime_reconciliation import (
    RuntimeIncidentDecision,
    RuntimeIncidentObservation,
    RuntimeIncidentReconciler,
    RuntimeIncidentType,
)

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


class Repository:
    def __init__(self) -> None:
        self.observations: dict[str, RuntimeIncidentObservation] = {}
        self.decisions: dict[str, RuntimeIncidentDecision] = {}

    async def reserve_incident(self, observation: RuntimeIncidentObservation) -> bool:
        prior = self.observations.get(observation.incident_id)
        if prior is not None:
            if prior != observation:
                raise ValueError("incident identity has conflicting observations")
            return False
        self.observations[observation.incident_id] = observation
        return True

    async def record_incident_decision(
        self,
        observation: RuntimeIncidentObservation,
        decision: RuntimeIncidentDecision,
    ) -> RuntimeIncidentDecision:
        prior = self.decisions.get(observation.incident_id)
        if prior is not None:
            if prior != decision:
                raise ValueError("incident replay changed its decision")
            return prior
        self.decisions[observation.incident_id] = decision
        return decision


def observation(
    incident_type: RuntimeIncidentType,
    proposed_action: str,
    **changes: object,
) -> RuntimeIncidentObservation:
    values = {
        "incident_id": f"incident-{incident_type.value}",
        "request_scope": "tenant-1",
        "binding_id": "binding-1",
        "incident_type": incident_type,
        "identity_digest": DIGEST,
        "observed_version": 3,
        "expected_version": 3,
        "evidence_refs": ("evidence:1",),
        "proposed_action": proposed_action,
        "observed_at": NOW,
    }
    values.update(changes)
    return RuntimeIncidentObservation.model_validate(values)


@pytest.mark.asyncio
async def test_safe_version_checked_repair_is_automatic_and_replay_idempotent() -> None:
    repository = Repository()
    reconciler = RuntimeIncidentReconciler(repository)
    item = observation(RuntimeIncidentType.EXPIRED_RESOURCE_LEASE, "expire_lease")

    first = await reconciler.reconcile(item)
    replay = await reconciler.reconcile(item)

    assert first == replay
    assert first.disposition == "automatic"
    assert first.before_version == 3
    assert first.after_version == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "item",
    [
        observation(
            RuntimeIncidentType.UNSETTLED_ACCEPTED_OPERATION,
            "settle_observed_usage",
            ambiguous_effect=True,
        ),
        observation(
            RuntimeIncidentType.INCOMPATIBLE_CHECKPOINT_ROUTE,
            "interrupt_runtime",
            compatible=False,
        ),
        observation(
            RuntimeIncidentType.LINEAGE_GAP_OR_COLLISION,
            "operator_required",
        ),
        observation(
            RuntimeIncidentType.OUTBOX_CURSOR_DRIFT,
            "repair_cursor",
            expected_version=2,
        ),
    ],
)
async def test_unsafe_incidents_require_operator_without_guessing(
    item: RuntimeIncidentObservation,
) -> None:
    decision = await RuntimeIncidentReconciler(Repository()).reconcile(item)

    assert decision.disposition == "operator_required"
    assert decision.action == "operator_required"
    assert decision.after_version is None


def test_incident_catalog_covers_every_stage3_inconsistency_class() -> None:
    assert {item.value for item in RuntimeIncidentType} == {
        "binding_without_thread",
        "thread_without_initial_run",
        "provider_active_while_belllabs_stopped",
        "belllabs_active_without_runtime",
        "unsettled_accepted_operation",
        "stale_decision",
        "orphan_runtime_resource",
        "terminal_without_typed_result",
        "incompatible_checkpoint_route",
        "outbox_cursor_drift",
        "expired_resource_lease",
        "lineage_gap_or_collision",
        "missing_assembly_or_context_digest",
    }
