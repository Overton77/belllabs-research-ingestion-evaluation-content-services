# [FT-G7] temporalio 1.34 upgrade, worker versioning and search attribute registration

Linear: OVE-56

**Epic:** Lanes (SPEC-07)
**Team:** T4
**Blocked by:** None (can start immediately)
**Status:** ready-for-agent

**What to build:** Move the repository from `temporalio==1.30.0` to `1.34.x` deliberately: apply the 1.31 breaking change (payload limits move to `Client.connect(payload_limits=PayloadLimitsConfig(...))` with renamed fields), adopt `workflow.uuid7()` where the code hand-rolls UUIDv7 in workflows, record `original_execution_run_id`, adopt Worker Deployment versioning (`deployment_config`) in `create_production_workers` with a replay test over captured histories, and add the four Keyword search attributes `mc_mission_id`, `mc_run_id`, `mc_lane`, `mc_phase` plus `ForkedFromRunId` to the existing registry (`domain/programs/search_attributes.py`, `adapters/temporal/search_attributes.py`), registered by `make temporal-up` (`--search-attribute` flags) and by the operator-service registration path. `missionctl run list --query` is FT-C4; this ticket makes the attributes exist and be upserted at start and segment boundaries.

**Spec sections:** SPEC-07 §4.5 (visibility), §4.6 (upgrade); research/temporal-lifecycle.md §4 and §9.

**Writable regions:** `pyproject.toml` and `uv.lock` (temporalio pin), `src/mission_control/adapters/temporal/{client.py,worker.py,search_attributes.py,visibility.py}`, `src/mission_control/domain/programs/search_attributes.py`, `tests/integration/temporal/test_replay_histories.py`, `Makefile` (`temporal-up` flags, integrator-reviewed), `docs/DEVELOPMENT.md` (upgrade note).

**Acceptance criteria:**
- [ ] `uv lock` resolves `temporalio[opentelemetry]>=1.34,<2`; `make check` passes; no `DataConverter` payload-limit configuration remains.
- [ ] `Client.connect` sets `payload_limits` with the renamed fields; a unit test asserts the warn sizes.
- [ ] Temporal Cloud connection: when `TEMPORAL_CLOUD_API_KEY` is set, `adapters/temporal/client.py`, `bootstrap/api.py` and `bootstrap/worker.py` connect with `api_key=` and `tls=True` to `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE`; without it the local `make temporal-up` server stays the default; preflight reports which one is in use (the key is set in `.env`; local remains the development default).
- [ ] Captured histories under `tests/integration/temporal/histories/` (StageGraph, GoalDirected, operation, linked run) replay without nondeterminism on the upgraded SDK.
- [ ] `create_production_workers` accepts a `deployment_config` built from the release version; the local dev profile runs with versioning enabled and a replay test proves a rolled worker drains the old version.
- [ ] The five new search attributes are declared with types, registered idempotently (existing attribute with the same type is accepted, conflicting type raises `SearchAttributeRegistrationError`), and upserted at run start and at operation segment boundaries only.
- [ ] `make temporal-up` starts the dev server with the new attributes; the readiness check lists them.
- [ ] `docs/DEVELOPMENT.md` records the upgrade, the breaking change and the rollback (pin 1.30.0, revert the connect call).

**Verification:** `uv lock && make check`; `uv run --group biotech pytest tests/integration/temporal -q`; `make temporal-up && uv run python -m mission_control.bootstrap.preflight`.

**Notes:** Experimental surfaces stay out: `workflow.signal_with_start_workflow`, Workflow Streams, OpenTelemetryPlugin, Workflow Pause, the Temporal Deep Agents plugin. The 1.33 `deepagents.retire-result-cache` patch is irrelevant because Mission Control does not use the Temporal Deep Agents integration. Self-hosted Update limits matching Temporal Cloud is UNVERIFIED; document it. Both targets are available: local stack by default, Temporal Cloud through the configured key when a drill needs it. Budget: small real fixture runs are permitted by default (see TEAM-WORKSPACE.md, Environment); record spent units in the handoff; larger drills need an approved cap in Linear.
