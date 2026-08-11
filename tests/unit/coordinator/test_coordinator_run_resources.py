from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.application.coordinator.coordinator_run_resources import CoordinatorRunResourceService
from app.application.orchestration.orchestration_binding_repository import (
    SemanticInputBindingNotFound,
)


class Runs:
    async def get_run(self, request_scope: str, run_id: str):
        return SimpleNamespace(
            request_scope=request_scope,
            model_dump=lambda **_kwargs: {
                "run_id": run_id,
                "request_scope": request_scope,
                "phase": "active",
            },
        )


class Bindings:
    async def get_for_run(self, *, request_scope: str, run_id: str):
        if run_id == "missing":
            return None
        return SimpleNamespace(
            operation_execution_binding_refs=("oeb:search",),
            model_dump=lambda **_kwargs: {
                "binding_id": "semantic-binding:test",
                "request_scope": request_scope,
                "run_id": run_id,
            },
        )


@pytest.mark.asyncio
async def test_run_resources_return_canonical_authorized_links_and_bindings() -> None:
    resources = CoordinatorRunResourceService(
        runs=Runs(),
        bindings=Bindings(),
    )
    launch = await resources.launch("request-a", "run-1")
    assert launch["resource_uris"] == {
        "result": "belllabs://runs/run-1/result",
        "bindings": "belllabs://runs/run-1/bindings",
    }
    bindings = await resources.bindings("request-a", "run-1")
    assert bindings["operation_execution_binding_refs"] == ["oeb:search"]

    with pytest.raises(SemanticInputBindingNotFound):
        await resources.bindings("request-a", "missing")
