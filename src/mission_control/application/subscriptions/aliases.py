"""Public event aliases: a versioned read projection over canonical mission events.

Manifests subscribe to the SPEC-06 vocabulary (`run.completed`, `activation.completed`,
`human_task.opened`); the reducer journal records canonical transitions. Each public name is
a *derived notification* of exactly one canonical event type (multi-provider SPEC-04 "Event
names and subordinate visibility"); no second authoritative transition is appended and
aliases never create or change state:

- `run.completed` <- `workflow_run.terminalize`: the reducer's only transition into the
  terminal phase, appended once per run (a terminal run is immutable);
- `activation.completed` <- `attempt.completed`: the activation's attempt reached an
  execution outcome (succeeded, failed or cancelled, in the canonical payload). Never from
  `activation.phase_changed{follow_up_turn | awaiting_human}` or
  `activation.lifecycle_changed{waiting}`: those leave the activation open. Frame derivation
  never emits `attempt.completed` for a provider subagent's result;
- `human_task.opened` <- `human_task.created` (persisted task creation). A writer that
  appends the public name `human_task.opened` itself is delivered as is. Never from
  `workflow_run.set_wait`: a wait is not a task.

A derived notification keeps the canonical `seq`, `payload_ref` and `payload_digest`, sets
`causation_ref = mission_event:<canonical event_id>`, and has a stable derived `event_id`
(uuid5 of alias version, public name and canonical id). Delivery receipts still reference
the canonical event id, which is what `subscription_delivery.event_id` keys to.

Subscription cursors advance by `seq`, so a subscription receives at most one envelope per
canonical event: the canonical event when its filters match it (keeps `*` and canonical
filters unchanged), otherwise the derived notification when the filters match that.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final
from uuid import UUID, uuid5

from mission_control.domain.subscriptions.contracts import (
    MissionEventEnvelope,
    SubscriptionFilters,
)

ALIAS_VERSION: Final = 1
_ALIAS_NAMESPACE: Final = UUID("6f0e3c55-9d2a-4c1b-8e47-3a5b2c1d0e9f")

PUBLIC_ALIASES: Final[dict[str, str]] = {
    "workflow_run.terminalize": "run.completed",
    "attempt.completed": "activation.completed",
    "human_task.created": "human_task.opened",
}
CANONICAL_SOURCES: Final[dict[str, str]] = {
    public: canonical for canonical, public in PUBLIC_ALIASES.items()
}


def derived_event_id(public_name: str, canonical_event_id: UUID) -> UUID:
    return uuid5(_ALIAS_NAMESPACE, f"v{ALIAS_VERSION}:{public_name}:{canonical_event_id}")


def derive_notification(event: MissionEventEnvelope) -> MissionEventEnvelope | None:
    """The public notification a canonical event yields, or None."""

    public = PUBLIC_ALIASES.get(event.event_type)
    if public is None:
        return None
    return event.model_copy(
        update={
            "event_id": derived_event_id(public, event.event_id),
            "event_type": public,
            "causation_ref": f"mission_event:{event.event_id}",
        }
    )


@dataclass(frozen=True)
class Selected:
    """What a subscription receives for one canonical event."""

    envelope: MissionEventEnvelope
    canonical_event_id: UUID

    @property
    def derived(self) -> bool:
        return self.envelope.event_id != self.canonical_event_id


def select(filters: SubscriptionFilters, event: MissionEventEnvelope) -> Selected | None:
    """At most one envelope per canonical event for one subscription's filters."""

    if filters.matches(event):
        return Selected(event, event.event_id)
    derived = derive_notification(event)
    if derived is not None and filters.matches(derived):
        return Selected(derived, event.event_id)
    return None


__all__ = [
    "ALIAS_VERSION",
    "CANONICAL_SOURCES",
    "PUBLIC_ALIASES",
    "Selected",
    "derive_notification",
    "derived_event_id",
    "select",
]
