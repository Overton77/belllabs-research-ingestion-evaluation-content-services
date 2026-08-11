from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from app.application.coordinator.coordinator_results import (
    InMemoryWorkflowResultRepository,
    TerminalWorkflowCompletionService,
)
from app.domain.coordinator.launch import (
    BlueprintFamily,
    StageGraphResultDetails,
    TerminalWorkflowCompletion,
)
from app.domain.run_control.contracts import RunOutcome, RunPhase

NOW = datetime(2026, 7, 28, 20, 0, tzinfo=UTC)


class TerminalRuns:
    def __init__(
        self,
        *,
        phase: RunPhase = RunPhase.TERMINAL,
        outcome: RunOutcome | None = RunOutcome.COMPLETED,
    ) -> None:
        self.phase = phase
        self.outcome = outcome

    async def get_run(self, request_scope: str, run_id: str) -> object:
        return SimpleNamespace(
            request_scope=request_scope,
            run_id=run_id,
            phase=self.phase,
            terminal_outcome=self.outcome,
        )


class AmbiguousFirstSave:
    def __init__(self) -> None:
        self.inner = InMemoryWorkflowResultRepository()
        self.first = True

    async def save(self, result):
        persisted = await self.inner.save(result)
        if self.first:
            self.first = False
            raise TimeoutError("database response was ambiguous")
        return persisted

    async def get(self, tenant_scope: str, request_scope: str, run_id: str):
        return await self.inner.get(tenant_scope, request_scope, run_id)


def completion(
    *,
    outcome: RunOutcome = RunOutcome.COMPLETED,
    workflow_cycles: int = 0,
) -> TerminalWorkflowCompletion:
    return TerminalWorkflowCompletion(
        run_id="run-1",
        tenant_scope="tenant-a",
        request_scope="tenant-a",
        blueprint_family=BlueprintFamily.STAGE_GRAPH,
        terminal_outcome=outcome,
        output_contract_results={"final": ["artifact:result"]},
        evidence_refs=("evidence:source",),
        family_result=StageGraphResultDetails(
            execution_epoch=1,
            workflow_cycles=workflow_cycles,
            output_refs={"final": ("artifact:result",)},
        ),
        completed_at=NOW,
    )


@pytest.mark.asyncio
async def test_terminal_completion_is_idempotent_and_recoverable_after_ambiguous_save() -> None:
    repository = AmbiguousFirstSave()
    service = TerminalWorkflowCompletionService(
        runs=TerminalRuns(),
        results=repository,
    )

    with pytest.raises(TimeoutError, match="ambiguous"):
        await service.complete(completion())

    retried = await service.complete(completion())
    assert retried.run_id == "run-1"
    assert (
        await repository.get("tenant-a", "tenant-a", "run-1")
    ) == retried


@pytest.mark.asyncio
async def test_terminal_completion_rejects_conflicting_duplicate_payload() -> None:
    repository = InMemoryWorkflowResultRepository()
    service = TerminalWorkflowCompletionService(
        runs=TerminalRuns(),
        results=repository,
    )
    await service.complete(completion())

    with pytest.raises(ValueError, match="immutable"):
        await service.complete(completion(workflow_cycles=1))


@pytest.mark.asyncio
async def test_completion_requires_matching_authoritative_terminal_outcome() -> None:
    nonterminal = TerminalWorkflowCompletionService(
        runs=TerminalRuns(phase=RunPhase.ACTIVE, outcome=None),
        results=InMemoryWorkflowResultRepository(),
    )
    with pytest.raises(ValueError, match="terminality"):
        await nonterminal.complete(completion())

    conflicting = TerminalWorkflowCompletionService(
        runs=TerminalRuns(outcome=RunOutcome.FAILED),
        results=InMemoryWorkflowResultRepository(),
    )
    with pytest.raises(ValueError, match="terminal run outcome"):
        await conflicting.complete(completion())
