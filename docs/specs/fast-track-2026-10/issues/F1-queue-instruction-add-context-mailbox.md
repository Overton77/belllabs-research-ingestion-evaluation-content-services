# [FT-F1] queue_instruction and add_context mailbox with boundary delivery

Linear: OVE-44

**Epic:** Control (SPEC-06)
**Team:** T5
**Blocked by:** FT-B1
**Status:** ready-for-agent

**What to build:** An operator or coordinator sends `queue_instruction` or `add_context` through `POST /runs/{id}/commands` (and `missionctl command queue`), the Reducer admits it into a durable `command_mailbox` entry for the Run's current Generation, and the family boundary (StageGraph admission boundary or GoalDirected iteration boundary) hands undelivered entries to the Context Packer so the next turn or iteration reads them exactly once. The Command passes through `accepted → queued → delivered → observed → completed` with a Delivery Report of `turn_boundary_guaranteed` on Deep Agents (`wait_then_send` declared for Cursor). An immediate cancel or a superseded Generation expires undelivered entries visibly.

**Spec sections:** SPEC-06 "Command vocabulary and lifecycle", "Mailbox and boundary delivery", "Contracts", "Persistence" (`command_mailbox`, receipt states); SPEC-02 (packer mandatory items).

**Writable regions:** `src/mission_control/domain/policies/mailbox.py` (new), `application/subscriptions/` untouched, `adapters/temporal/boundary_commands.py`, `interfaces/http/mission_control.py`, `packages/mission-control-db-contract/component/migrations/0029_command_mailbox_stop_fence_subscriptions.sql` (mailbox and receipt-state parts; coordinate the `stop_fence` and subscription tables with FT-F3 and FT-F5 in the same file through the integrator). Shared (integrator-only): `application/missions/service.py::_action`, `domain/policies/contracts.py`, `contracts/contracts.py`, family workflows' `deliver_boundary_command`.

**Acceptance criteria:**
- [ ] `POST /runs/{id}/commands` with `kind: queue_instruction` and `kind: add_context` returns 202 and a receipt in state `queued`; the old `unsupported_control` rejection no longer exists for these kinds.
- [ ] A mailbox entry is written with `boundary`, content ref or bounded inline text (cap enforced, 413-style typed rejection above it), digest, admission sequence in space `mailbox:<generation>`.
- [ ] At the next boundary the family delivers the entry into the packet, receipt moves to `delivered` then `observed` then `completed/applied`; `command.delivered` and `command.completed` mission events carry the Delivery Report.
- [ ] The entry is consumed exactly once across a worker restart between delivery and turn start (integration test with the time-skipping Temporal environment).
- [ ] An identical retry (same `request_id`) returns the same receipt; a stale `expected_version` returns 409 with the current frontier.
- [ ] A normal or immediate cancel admitted before delivery marks entries `superseded` with `superseded_by` and outcome `expired`.
- [ ] A Generation mismatch expires the entry with `expired_reason: stale_generation`.
- [ ] `missionctl command queue RUN --file FILE` and `command list` show the mailbox-bound receipt; OpenAPI exports the new payload unions.
- [ ] Unit tests for the Reducer actions and supersession; `make check` passes.

**Verification:** `make check`; `uv run --group biotech pytest -m common_db tests/integration/postgres/test_mission_control_lifecycle_postgres.py tests/unit/run_control -q`; new `tests/integration/temporal/test_command_mailbox.py`.

**Notes:** Temporal Update id equals `command_id`; validators reject duplicates without history cost; carry handled command ids across continue-as-new (research/temporal-lifecycle.md 1.2). Delivery into the packet depends on FT-B1's packer interface; until FT-C2 lands, `observed` is recorded at delivery time. No paid provider call is needed.
