from __future__ import annotations

import json
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from typing import Any

import pytest

from app.application.coordinator_composition import (
    CoordinatorProductionDependencies,
    build_production_coordinator_facade,
)
from app.application.coordinator_facade import BlueprintRuntimeStatus
from app.application.postgres_workflow_result_repository import (
    PostgresWorkflowResultRepository,
)
from app.config import get_settings
from app.domain.control_plane.contracts import (
    DefinitionKind,
    DefinitionSelector,
    ExactDefinitionRef,
)
from app.domain.coordinator.launch import (
    BlueprintFamily,
    StageGraphResultDetails,
    WorkflowResultRecord,
)
from app.domain.run_control.contracts import RunOutcome, RunPhase

NOW = datetime(2026, 7, 26, 19, 0, tzinfo=UTC)
SENTINEL = "sk-proj-SENTINEL_OPENAI_KEY_1234567890"


def result(
    *,
    run_id: str = "run-result-1",
    warnings: tuple[str, ...] = (),
    output_contract_results: dict[str, Any] | None = None,
) -> WorkflowResultRecord:
    return WorkflowResultRecord(
        run_id=run_id,
        tenant_scope="global",
        request_scope="global",
        blueprint_family=BlueprintFamily.STAGE_GRAPH,
        terminal_outcome=RunOutcome.COMPLETED,
        output_contract_results=output_contract_results
        or {
            "verified_web_research": {"accepted": True},
            "final_result_ref": "belllabs://web-research/results/final",
        },
        artifact_refs=("belllabs://browser-evidence/screenshots/1",),
        evidence_refs=(
            "belllabs://web-research/admission/1",
            "belllabs://web-research/firecrawl/1",
            "belllabs://web-research/tavily/1",
            "belllabs://web-research/synthesis/1",
            "belllabs://web-research/browser/1",
            "belllabs://web-research/result/1",
        ),
        warnings=warnings,
        operation_binding_refs=(
            "operation-binding:search-firecrawl",
            "operation-binding:search-tavily",
            "operation-binding:browser-verify",
        ),
        usage_summary={"tool.calls.total": 3},
        family_result=StageGraphResultDetails(
            execution_epoch=1,
            workflow_cycles=1,
            stage_cycles={"browser_verify": 1},
            operation_attempts={"browser_verify": 1},
            output_refs={
                "verified_research_result": (
                    "belllabs://web-research/results/final",
                )
            },
            schedule_trace=("browser_verify",),
        ),
        completed_at=NOW,
    )


class _Transaction(AbstractAsyncContextManager[None]):
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        traceback: object,
    ) -> None:
        return None


class _Connection:
    def __init__(self) -> None:
        self.phase = "terminal"
        self.run_exists = True
        self.rows: dict[str, dict[str, object]] = {}
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def execute(self, query: str, *args: object) -> str:
        self.calls.append((query, args))
        return "SELECT 1"

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> dict[str, object] | None:
        self.calls.append((query, args))
        if "FROM belllabs_control.workflow_runs" in query:
            return {"phase": self.phase} if self.run_exists else None
        if "INSERT INTO belllabs_control.coordinator_workflow_results" in query:
            run_id = str(args[0])
            if run_id in self.rows:
                return None
            row = {
                "tenant_scope": args[1],
                "request_scope": args[2],
                "result_digest": args[6],
                "result_payload": args[7],
            }
            self.rows[run_id] = row
            return row
        if "FROM belllabs_control.coordinator_workflow_results" in query:
            run_id = str(args[0])
            row = self.rows.get(run_id)
            if row is None:
                return None
            if len(args) >= 3 and (
                row["tenant_scope"] != args[1]
                or row["request_scope"] != args[2]
            ):
                return None
            return row
        raise AssertionError(f"unexpected query: {query}")


class _Acquire(AbstractAsyncContextManager[_Connection]):
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _Connection:
        return self._connection

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        traceback: object,
    ) -> None:
        return None


class _Pool:
    def __init__(self) -> None:
        self.connection = _Connection()

    def acquire(self) -> _Acquire:
        return _Acquire(self.connection)


@pytest.mark.asyncio
async def test_save_get_and_repeated_save_are_immutable_and_idempotent() -> None:
    pool = _Pool()
    repository = PostgresWorkflowResultRepository(pool)  # type: ignore[arg-type]
    expected = result()

    assert await repository.save(expected) == expected
    assert await repository.save(expected) == expected
    assert await repository.get("global", "global", expected.run_id) == expected
    persisted = pool.connection.rows[expected.run_id]
    assert persisted["result_digest"].startswith("sha256:")
    assert SENTINEL not in json.dumps(persisted)
    set_configs = [
        args
        for query, args in pool.connection.calls
        if "set_config" in query
    ]
    assert ("global",) in set_configs


@pytest.mark.asyncio
async def test_changed_or_cross_scope_result_cannot_replace_existing_record() -> None:
    pool = _Pool()
    repository = PostgresWorkflowResultRepository(pool)  # type: ignore[arg-type]
    original = result()
    await repository.save(original)

    with pytest.raises(ValueError, match="immutable"):
        await repository.save(result(warnings=("changed",)))
    assert await repository.get("tenant-other", "global", original.run_id) is None


@pytest.mark.asyncio
async def test_nonterminal_run_and_secret_material_fail_before_persistence() -> None:
    pool = _Pool()
    repository = PostgresWorkflowResultRepository(pool)  # type: ignore[arg-type]
    pool.connection.phase = "active"
    with pytest.raises(ValueError, match="terminal Workflow Run"):
        await repository.save(result())
    assert not pool.connection.rows

    pool.connection.phase = "terminal"
    with pytest.raises(ValueError, match="secret material"):
        await repository.save(
            result(
                output_contract_results={
                    "verified_web_research": {
                        "summary": f"provider returned api_key={SENTINEL}"
                    }
                }
            )
        )
    assert not pool.connection.rows


class _Ready:
    async def snapshot(self) -> tuple[BlueprintRuntimeStatus, ...]:
        return ()


class _Runs:
    async def get_run(self, request_scope: str, run_id: str) -> object:
        return type(
            "Projection",
            (),
            {
                "request_scope": request_scope,
                "run_id": run_id,
                "phase": RunPhase.TERMINAL,
            },
        )()


def test_production_composition_uses_only_application_pool_for_results() -> None:
    capability_pool = _Pool()
    application_pool = _Pool()
    ref = ExactDefinitionRef(
        kind=DefinitionKind.SKILL,
        logical_id="skill.belllabs-workflow-coordinator",
        revision=1,
        digest="sha256:" + "a" * 64,
    )
    facade = build_production_coordinator_facade(
        settings=get_settings(),
        capability_postgres_pool=capability_pool,  # type: ignore[arg-type]
        application_postgres_pool=application_pool,  # type: ignore[arg-type]
        dependencies=CoordinatorProductionDependencies(
            readiness=_Ready(),
            coordinator_skill=DefinitionSelector(exact=ref),
            prompt_bindings={},
            run_projections=_Runs(),  # type: ignore[arg-type]
        ),
    )

    result_service = facade._results  # type: ignore[attr-defined]
    repository = result_service._results  # type: ignore[union-attr]
    assert isinstance(repository, PostgresWorkflowResultRepository)
    assert repository._pool is application_pool  # type: ignore[attr-defined]
    assert repository._pool is not capability_pool  # type: ignore[attr-defined]
