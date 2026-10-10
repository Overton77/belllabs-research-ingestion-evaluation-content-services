"""MP-12: the per-lane hydrator registry refuses unknown lanes and serves the activities."""

from __future__ import annotations

import inspect

import pytest

from mission_control.adapters.cursor.controls import (
    CursorCloudSessionHydrator,
    CursorSessionHydrator,
)
from mission_control.adapters.deep_agents.compaction import DeepAgentsSessionHydrator
from mission_control.adapters.temporal.activities.continuation import ContinuationActivities
from mission_control.application.context.continuation import (
    ContinuationRejected,
    HydrationReceipt,
    HydrationRequest,
    SessionHydrator,
)
from mission_control.application.context.hydrators import (
    LaneContinuationRegistration,
    LaneHydratorRegistry,
)


class FakeHydrator:
    def __init__(self, scope: str) -> None:
        self.scope = scope

    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt:
        return HydrationReceipt(target_session_ref="fake", restored={})


def test_registry_resolves_registered_lanes_and_refuses_the_rest() -> None:
    registry = LaneHydratorRegistry()
    registry.register(LaneContinuationRegistration("cursor_local", FakeHydrator))
    assert registry.profiles() == ("cursor_local",)
    hydrator = registry("cursor_local", "tenant-1")
    assert isinstance(hydrator, FakeHydrator) and hydrator.scope == "tenant-1"
    assert registry.snapshots("cursor_local", "tenant-1") is None
    with pytest.raises(ContinuationRejected) as unknown:
        registry("codex", "tenant-1")
    assert unknown.value.code == "unsupported_control"
    with pytest.raises(ValueError, match="already has"):
        registry.register(LaneContinuationRegistration("cursor_local", FakeHydrator))
    assert not registry.registration("cursor_local").qualified, "registering never qualifies"


def test_the_existing_hydrators_satisfy_the_protocol_the_registry_serves() -> None:
    for hydrator in (CursorSessionHydrator, CursorCloudSessionHydrator, DeepAgentsSessionHydrator):
        assert hasattr(hydrator, "hydrate")
        signature = inspect.signature(hydrator.hydrate)
        assert list(signature.parameters) == ["self", "request"], hydrator
    # The activities take the registry as their `LaneHydrators` callable.
    registry = LaneHydratorRegistry(
        {"cursor_local": LaneContinuationRegistration("cursor_local", FakeHydrator)}
    )
    activities = ContinuationActivities(lambda scope: None, registry)  # type: ignore[arg-type, return-value]
    names = {getattr(item, "__temporal_activity_definition").name for item in activities.all()}
    assert {"continuation.pending", "continuation.advance", "continuation.transfer"} <= names
    resolved: SessionHydrator = registry("cursor_local", "scope")
    assert isinstance(resolved, FakeHydrator)
