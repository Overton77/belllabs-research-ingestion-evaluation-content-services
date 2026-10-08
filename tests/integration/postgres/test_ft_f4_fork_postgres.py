"""FT-F4 on a disposable PostgreSQL 17: fork with an instruction under the runtime role.

The latest sealed Snapshot is read from `run_snapshot`; the fork saga admits the derived Run
and records the `mission_relationship` edge (kind `fork`) with its materialization; the
derived Run's own mailbox receives the instruction (first entry, queued) and the Snapshot
restore; the source Run's mailbox and Commands are untouched; lineage reads from both Runs;
a retry returns the same forked Run.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest

from mission_control.adapters.postgres.run_control.mailbox import PostgresCommandMailbox
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.runtime.run_forks import (
    PostgresForkMaterializationStore,
    PostgresRunSnapshotRepository,
)
from mission_control.adapters.postgres.runtime.stage3_kernel_repository import (
    PostgresForkRepository,
)
from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.missions.runtime import MissionControlRuntimeService
from mission_control.application.missions.service import MissionControlService
from mission_control.application.recovery.fork_seed import ForkSeedService
from mission_control.application.recovery.run_forks import (
    ForkPatchPolicyRegistry,
    RecordingForkMaterializer,
    RunControlForkAuthority,
    RunSnapshotService,
    SemanticForkService,
)
from mission_control.application.recovery.runtime_recovery import RuntimeForkService
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.contracts.runtime_contracts import MissionForkRequest
from mission_control.domain.policies.contracts import StartAction
from mission_control.domain.policies.mailbox import MailboxState
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.run_forks import stage_policy, technical_snapshot
from tests.integration.postgres.runtime_common import (
    common_db,  # noqa: F401
    owner_rows,
    scoped_command,
    scoped_request,
)
from tests.unit.run_control.test_run_control import WORKFLOW_DIGEST, actor
from tests.unit.run_control.test_run_control import service as run_control_service

pytestmark = pytest.mark.common_db

GRANTS = {
    "sponsorship_refs": frozenset({"sponsorship:test"}),
    "approval_refs": frozenset({"approval:test"}),
}


def caller() -> Any:
    return actor().model_copy(
        update={
            "permissions": actor().permissions
            | {"workflow_run.fork", "workflow_run.read", "workflow_run.control"}
        }
    )


async def test_fork_with_instruction_seeds_the_derived_run_and_records_lineage(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool(max_size=6)
    try:
        scope = common_db.scope()
        repository = PostgresRunControlRepository(pool)
        run_control, _ = run_control_service(repository)  # type: ignore[arg-type]
        admitted = await run_control.admit(scoped_request(common_db, request_id="ft-f4-source"))
        assert admitted.run_id is not None
        source = admitted.run_id
        await run_control.execute(
            scoped_command(common_db, source, 1, "ft-f4-source-start", StartAction())
        )
        mailbox = MailboxDeliveryService(PostgresCommandMailbox(pool), run_control)
        store = PostgresForkMaterializationStore(pool)
        lifecycle = MissionControlService(
            run_control,
            BoundaryInterventionService(run_control),
            request_scope=scope,
            mailbox=mailbox,
            forks=store,
        )
        # The source has its own queued instruction (never copied).
        version = (await run_control.get_run(scope, source)).version
        await lifecycle.command(
            source,
            MissionCommandRequest.model_validate(
                {
                    "request_id": str(uuid4()),
                    "expected_version": version,
                    "expected_generation": 1,
                    "target": {"kind": "run", "id": source},
                    "kind": "queue_instruction",
                    "payload": {"content": {"text": "source-only"}},
                    "reason": "note on the source",
                }
            ),
            caller(),
        )
        snapshots = PostgresRunSnapshotRepository(pool)
        older = technical_snapshot(source, request_scope=scope, projection_version=1)
        latest = technical_snapshot(source, request_scope=scope, projection_version=2)
        await snapshots.put(older)
        await snapshots.put(latest)
        assert await snapshots.latest(scope, source) == latest
        assert await snapshots.latest(common_db.scope("tenant-2"), source) is None
        policies = ForkPatchPolicyRegistry()
        policies.register(WORKFLOW_DIGEST, stage_policy())
        forks = SemanticForkService(
            snapshots=snapshots,
            saga=RuntimeForkService(
                repository=PostgresForkRepository(pool),
                authority=RunControlForkAuthority(run_control, repository),
                materializer=RecordingForkMaterializer(
                    store, retention=lambda at: at + timedelta(days=90)
                ),
            ),
            policies=policies,
        )
        runtime = MissionControlRuntimeService(
            RunSnapshotService(
                reads=None,  # type: ignore[arg-type]  # latest exists: nothing is taken
                sources=None,  # type: ignore[arg-type]
                snapshots=snapshots,
                async_children=None,  # type: ignore[arg-type]
                commands=None,  # type: ignore[arg-type]
            ),
            forks,
            request_scope=scope,
            seeds=ForkSeedService(run_control),
        )
        source_entries = await mailbox.list_entries(scope, source)
        source_commands = await run_control.list_boundary_commands(scope, source)
        request = MissionForkRequest.model_validate(
            {
                "request_id": str(uuid4()),
                "baseline_reservations": {"tokens.total": 20},
                "instruction": {"text": "retry with the integration tests"},
                "sponsorship_ref": "sponsorship:test",
                "approval_refs": ["approval:test"],
                "reason": "branch with tests",
            }
        )
        receipt = await runtime.fork(source, request, caller(), **GRANTS)
        assert receipt.receipt.snapshot_id == latest.snapshot_id
        assert await runtime.fork(source, request, caller(), **GRANTS) == receipt
        derived = receipt.receipt.target_run_id
        instruction, workspace = await mailbox.list_entries(scope, derived)
        assert (instruction.kind, instruction.admission_sequence, instruction.state) == (
            "queue_instruction",
            1,
            MailboxState.QUEUED,
        )
        assert instruction.content_inline == "retry with the integration tests"
        assert (workspace.expand, workspace.content_digest) == (
            "workspace",
            latest.snapshot_digest,
        )
        assert await mailbox.list_entries(scope, source) == source_entries
        assert await run_control.list_boundary_commands(scope, source) == source_commands

        rows = await owner_rows(
            common_db,
            """
            SELECT r.kind, s.run_key AS source_key, t.run_key AS target_key, r.detail
            FROM mission_control.mission_relationship r
            JOIN mission_control.mission_run s
              ON s.installation_id = r.installation_id AND s.application_id = r.application_id
             AND s.tenant_id = r.tenant_id AND s.run_id = r.source_run_id
            JOIN mission_control.mission_run t
              ON t.installation_id = r.installation_id AND t.application_id = r.application_id
             AND t.tenant_id = r.tenant_id AND t.run_id = r.target_run_id
            WHERE r.kind = 'fork'
            """,
        )
        assert [(row["kind"], row["source_key"], row["target_key"]) for row in rows] == [
            ("fork", source, derived)
        ]
        source_view = await lifecycle.inspect(source, caller())
        derived_view = await lifecycle.inspect(derived, caller())
        assert source_view.lineage is not None and derived_view.lineage is not None
        (edge,) = source_view.lineage.forks
        assert derived_view.lineage.forked_from == edge
        assert edge.snapshot_id == latest.snapshot_id
    finally:
        await pool.close()
