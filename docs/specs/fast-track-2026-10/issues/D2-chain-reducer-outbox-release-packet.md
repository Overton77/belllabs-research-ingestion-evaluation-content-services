# [FT-D2] Chain reducer releases the next run through the outbox with a packet

Linear: OVE-39

**Epic:** Chains (SPEC-04)
**Team:** T3
**Blocked by:** FT-D1, FT-B1
**Status:** ready-for-agent

**What to build:** When the mission event that satisfies a Chain Link's release condition commits through `append_events`, the chain reducer, in the same transaction, marks the link released, builds a Context Packet for the consumer from the supplier's accepted outputs plus its final checkpoint and journal digest references, admits the consumer's Run, writes the outbox `start_run` intent and emits `chain_link.released` into both missions' event streams; the relay then starts `mission-run:<run_id>` idempotently. Blocking terminal outcomes mark links blocked and close the chain; cancellation applies `cancel_downstream` or `detach`; `missionctl chain inspect`, `GET /chains/{id}` and the `mission_chain_inspect` MCP tool show the projection.

**Spec sections:** SPEC-04 §Chain reducer, §Release through the outbox, §State transfer, §Cancellation cascade, §Interfaces; SPEC-02 §Packer (chain link source)

**Writable regions:** `src/mission_control/application/chains/reducer.py`, `src/mission_control/application/chains/service.py`, `src/mission_control/interfaces/http/chains.py`, `src/mission_control/interfaces/mcp/coordinator_server.py` (new tool and resource only), `tests/unit/chains/`, `tests/integration/postgres/test_chain_release.py`, `tests/integration/temporal/test_chain_start_idempotent.py`; integrator-owned lines: `adapters/postgres/run_control/canonical.py::append_events` post-write hook, CLI `chain` group in `interfaces/cli/main.py`

**Acceptance criteria:**
- [ ] `ChainReducer.on_events` is pure over rows and events and returns `ChainTransition`s; `append_events` applies them before commit through a registered hook.
- [ ] Release on `goal_accepted`, `mission_accepted` and `execution_complete`; a consumer with several incoming links is released only when all are released.
- [ ] The consumer Run is admitted and its `start_run` outbox intent written in the same transaction as the triggering event; an injected exception after the event write rolls everything back.
- [ ] The packet contains the supplied artifacts (tier per binding), the supplier's acceptance disposition inline, and checkpoint and journal digest references with a `missionctl run transcript` retrieval instruction; packet digest is recorded on the link.
- [ ] Relay start uses `id=mission-run:<run_id>`, `id_conflict_policy=USE_EXISTING`, `id_reuse_policy=REJECT_DUPLICATE`; delivering the same intent twice yields one workflow.
- [ ] Blocking outcomes produce `chain_link.blocked{reason}` and `chain.completed{not_accepted|execution_failed|cancelled}`; `cancel_downstream` admits a normal cancel for released consumers; `detach` leaves them running.
- [ ] `consumer_already_started` guard prevents a second admission when a consumer run already exists.
- [ ] `missionctl chain inspect CHAIN_ID --json` and `GET /v1/applications/{app}/chains/{chain_id}` return the `mc.chain.v1` projection; the MCP tool is read-only annotated.
- [ ] Chain events carry the same `event_id` in every member stream with one `seq` per mission.

**Verification:** `uv run pytest tests/unit/chains -q`; `uv run pytest tests/integration/postgres/test_chain_release.py tests/integration/temporal/test_chain_start_idempotent.py -q -m common_db` with the disposable DSN and `make temporal-up`; `make check`

**Notes:** Chains are the one case where a Run starts without a human `start`; record `actor_ref = chain:<chain_id>` on the launch and honour `controls.chain_autostart: false` by leaving the consumer `pending`. Do not call Temporal or providers from the reducer. The `append_events` hook line is integrator-owned; propose the diff in the handoff. Depends on the packer API from FT-B1 (`domain/context/packet.py`); if B1's signature differs from SPEC-02, adapt and note it.
