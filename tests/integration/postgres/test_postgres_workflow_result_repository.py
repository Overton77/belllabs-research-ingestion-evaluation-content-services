from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from typing import Any

import asyncpg
import pytest

from mission_control.adapters.postgres.coordinator.workflow_result_repository import (
    PostgresWorkflowResultRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.application.coordinator.coordinator_facade import BlueprintRuntimeStatus
from mission_control.bootstrap.coordinator_composition import (
    CoordinatorProductionDependencies,
    build_production_coordinator_facade,
)
from mission_control.bootstrap.settings import get_settings
from mission_control.domain.authoring.contracts import (
    DefinitionKind,
    DefinitionSelector,
    ExactDefinitionRef,
)
from mission_control.domain.coordinator.launch import (
    BlueprintFamily,
    StageGraphResultDetails,
    WorkflowResultRecord,
)
from mission_control.domain.policies.contracts import CancelAction, RunOutcome, RunPhase
from tests.fixtures.mission_control_common_db import CommonDatabase, catalog_scope
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import (
    owner_rows,
    scoped_command,
    scoped_request,
)
from tests.unit.run_control.test_run_control import service

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
            output_refs={"verified_research_result": ("belllabs://web-research/results/final",)},
            schedule_trace=("browser_verify",),
        ),
        completed_at=NOW,
    )


class _Acquire(AbstractAsyncContextManager[object]):
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None


class _Pool:
    """Composition-only stand-in: never acquired in the composition test."""

    def acquire(self) -> _Acquire:
        return _Acquire()


async def _terminal_run(db: CommonDatabase, pool: asyncpg.Pool, request_id: str) -> str:
    """An admitted run whose cancellation was accepted, then settled terminal by the
    owner-side fixture (terminal settlement itself is proven by the run-control tests)."""

    run_service, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    admitted = await run_service.admit(scoped_request(db, request_id=request_id))
    assert admitted.run_id is not None
    await run_service.execute(
        scoped_command(db, admitted.run_id, 1, f"{request_id}-cancel", CancelAction())
    )
    await owner_rows(
        db,
        """
        UPDATE mission_control.mission_run
        SET phase = 'terminal', lifecycle = 'completed', terminal_outcome = 'cancelled',
            projection = jsonb_set(jsonb_set(projection, '{phase}', '"terminal"'),
                                   '{terminal_outcome}', '"cancelled"')
        WHERE run_key = $1
        RETURNING 1
        """,
        admitted.run_id,
    )
    projection = await run_service.get_run(db.scope(), admitted.run_id)
    assert projection.phase == RunPhase.TERMINAL
    return admitted.run_id


def _scoped(record: WorkflowResultRecord, scope: str, run_id: str) -> WorkflowResultRecord:
    return record.model_copy(
        update={"tenant_scope": scope, "request_scope": scope, "run_id": run_id}
    )


@pytest.mark.asyncio
@pytest.mark.common_db
async def test_save_get_and_repeated_save_are_immutable_and_idempotent(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool()
    try:
        run_id = await _terminal_run(common_db, pool, "result-run")
        scope = common_db.scope()
        repository = PostgresWorkflowResultRepository(pool)
        expected = _scoped(result(), scope, run_id)

        assert await repository.save(expected) == expected
        assert await repository.save(expected) == expected
        assert await repository.get(scope, scope, run_id) == expected
        rows = await owner_rows(
            common_db,
            "SELECT result_digest, result_payload::text AS payload, run_key "
            "FROM mission_control.coordinator_workflow_result",
        )
        assert len(rows) == 1 and rows[0]["run_key"] == run_id
        assert rows[0]["result_digest"].startswith("sha256:")
        assert SENTINEL not in rows[0]["payload"]
        async with pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(
                    "UPDATE mission_control.coordinator_workflow_result SET result_digest = $1",
                    "sha256:" + "f" * 64,
                )
    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.common_db
async def test_changed_or_cross_scope_result_cannot_replace_existing_record(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool()
    try:
        run_id = await _terminal_run(common_db, pool, "result-immutable")
        scope = common_db.scope()
        repository = PostgresWorkflowResultRepository(pool)
        original = _scoped(result(), scope, run_id)
        await repository.save(original)

        with pytest.raises(ValueError, match="immutable"):
            await repository.save(_scoped(result(warnings=("changed",)), scope, run_id))
        other = common_db.scope("tenant-2")
        assert await repository.get(other, other, original.run_id) is None
        with pytest.raises(ValueError, match="tenant scope must match"):
            await repository.get(other, scope, original.run_id)
        with pytest.raises(ValueError):
            await repository.get("global", "global", original.run_id)
    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.common_db
async def test_nonterminal_run_and_secret_material_fail_before_persistence(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool()
    try:
        scope = common_db.scope()
        run_service, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        active = await run_service.admit(scoped_request(common_db, request_id="result-active"))
        assert active.run_id is not None
        repository = PostgresWorkflowResultRepository(pool)
        with pytest.raises(ValueError, match="terminal Workflow Run"):
            await repository.save(_scoped(result(), scope, active.run_id))

        run_id = await _terminal_run(common_db, pool, "result-secret")
        with pytest.raises(ValueError, match="secret material"):
            await repository.save(
                _scoped(
                    result(
                        output_contract_results={
                            "verified_web_research": {
                                "summary": f"provider returned api_key={SENTINEL}"
                            }
                        }
                    ),
                    scope,
                    run_id,
                )
            )
        assert not await owner_rows(
            common_db, "SELECT 1 FROM mission_control.coordinator_workflow_result"
        )
    finally:
        await pool.close()


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
        logical_id="skill.mission-control-coordinator",
        revision=1,
        digest="sha256:" + "a" * 64,
    )
    facade = build_production_coordinator_facade(
        settings=get_settings().model_copy(
            update={
                "mission_control_catalog_scope": catalog_scope("biotech"),
                "external_capability_discovery_enabled": False,
            }
        ),
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
