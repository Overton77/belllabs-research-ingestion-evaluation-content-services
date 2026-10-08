"""FT-F6 on a disposable PostgreSQL 17: inspection sections read under the runtime role.

Chain membership and the active Subscription count come from `mission_chain` / `chain_link`
and `mission_subscription`; sessions and the transcript cursor from `provider_frame` and the
mission events (no provider call); the mailbox and delivery reports from the ledger. Another
tenant reads none of it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.frames.transcript_reads import PostgresMissionEventReader
from mission_control.adapters.postgres.run_control.inspection_sections import (
    PostgresInspectionSections,
)
from mission_control.adapters.postgres.run_control.mailbox import PostgresCommandMailbox
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.frames.transcript import TranscriptService
from mission_control.application.missions.inspection import InspectionSources
from mission_control.application.missions.service import MissionControlService
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.manifest import load_manifest_yaml, parse_manifest
from mission_control.domain.composition.chain import (
    ChainScope,
    build_mission_chain,
    compile_chain,
)
from mission_control.domain.policies.contracts import StartAction
from mission_control.domain.subscriptions.contracts import SubscriptionRequest
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.integration.postgres.test_mission_chain_tables import insert_chain, insert_mission
from tests.unit.run_control.test_boundary_commands import TARGET
from tests.unit.run_control.test_run_control import actor, command, request, service

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "tests/fixtures/manifests/two-mission-chain.yml"
NOW = datetime(2026, 10, 8, 12, tzinfo=UTC)


def operator() -> Any:
    return actor().model_copy(
        update={"permissions": actor().permissions | {"workflow_run.read", "workflow_run.control"}}
    )


async def test_inspection_sections_on_postgres(common_db: CommonDatabase) -> None:  # noqa: F811
    pool = await common_db.pool(max_size=6)
    try:
        scope = common_db.scope()
        run_control, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        admitted = await run_control.admit(request(request_scope=scope, request_id=str(uuid4())))
        assert admitted.run_id is not None
        run_id = admitted.run_id
        await run_control.execute(
            command(run_id, 1, "start", StartAction(execution_target=TARGET)).model_copy(
                update={"request_scope": scope}
            )
        )
        sections = PostgresInspectionSections(pool)
        assert await sections.chain_membership(scope, run_id) is None
        assert await sections.active_subscriptions(scope, run_id) == 0

        # The run's mission is the first member of a two-mission chain.
        parsed = parse_request_scope(scope)
        chain_scope = ChainScope(
            installation_id=parsed.installation_id,
            application_id=parsed.application_id,
            tenant_id=parsed.tenant_id,
        )
        compilation = compile_chain(
            parse_manifest(load_manifest_yaml(FIXTURE.read_text(encoding="utf-8")))
        )
        assert compilation.resolution is not None
        order = compilation.resolution.order
        async with pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            mission_id = await connection.fetchval(
                "SELECT mission_id FROM mission_control.mission_run WHERE run_key = $1", run_id
            )
            other = await insert_mission(connection, chain_scope, f"{order[1]}-{uuid4().hex[:6]}")
            chain = build_mission_chain(
                resolution=compilation.resolution,
                chain_id=uuid4(),
                scope=chain_scope,
                chain_key="two-mission-chain",
                title="Research then ingestion",
                manifest_digest="sha256:" + "c" * 64,
                members={order[0]: (mission_id, uuid4()), order[1]: (other, uuid4())},
                created_at=NOW,
                created_by_actor_ref="actor:owner",
            )
            await insert_chain(connection, chain)
        subscriptions = SubscriptionService(PostgresSubscriptionStore(pool, scope))
        for _ in range(2):
            await subscriptions.subscribe(
                SubscriptionRequest.model_validate(
                    {
                        "target": "mission",
                        "target_id": str(mission_id),
                        "events": ["*"],
                        "channel": {
                            "kind": "webhook",
                            "url": "http://127.0.0.1:9/hook",
                            "secret_ref": "environment:MC_TEST_WEBHOOK_SECRET",
                        },
                    }
                ),
                operator(),
            )

        mailbox = MailboxDeliveryService(PostgresCommandMailbox(pool), run_control)
        facade = MissionControlService(
            run_control,
            BoundaryInterventionService(run_control),
            request_scope=scope,
            mailbox=mailbox,
            inspection=InspectionSources(
                transcripts=TranscriptService(
                    PostgresMissionEventReader(pool),
                    PostgresFrameRepository(pool),
                    request_scope=scope,
                ),
                sections=sections,
            ),
        )
        version = (await run_control.get_run(scope, run_id)).version
        await facade.command(
            run_id,
            MissionCommandRequest.model_validate(
                {
                    "request_id": str(uuid4()),
                    "expected_version": version,
                    "expected_generation": 1,
                    "target": {"kind": "run", "id": run_id},
                    "kind": "queue_instruction",
                    "payload": {"content": {"text": "never in inspection"}},
                    "reason": "steer",
                }
            ),
            operator(),
        )
        inspection = await facade.inspect(run_id, operator())
        assert inspection.chain is not None
        assert inspection.chain.chain_key == "two-mission-chain"
        assert inspection.chain.links and {link.state for link in inspection.chain.links} == {
            "armed"
        }
        assert inspection.subscriptions is not None and inspection.subscriptions.active == 2
        assert inspection.sessions == ()  # no lane has written frames for this run
        assert inspection.lane is None
        assert inspection.frames_cursor is not None
        assert inspection.frames_cursor.frame_count == 0
        assert inspection.frames_cursor.transcript_cursor is not None  # events only
        (entry,) = inspection.mailbox or ()
        assert (entry.kind, entry.state) == ("queue_instruction", "queued")
        (report,) = inspection.delivery_reports or ()
        assert report.lifecycle == "queued"
        assert "never in inspection" not in inspection.model_dump_json()
        # Another tenant reads nothing.
        other_scope = common_db.scope("tenant-2")
        assert await sections.chain_membership(other_scope, run_id) is None
        assert await sections.active_subscriptions(other_scope, run_id) == 0
    finally:
        await pool.close()
