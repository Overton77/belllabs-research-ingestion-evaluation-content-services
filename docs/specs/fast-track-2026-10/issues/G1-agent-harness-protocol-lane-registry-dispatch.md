# [FT-G1] AgentHarness protocol, lane registry and dispatch by lane

Linear: OVE-50

**Epic:** Lanes (SPEC-07)
**Team:** T4
**Blocked by:** None (can start immediately)
**Status:** ready-for-agent

**What to build:** The provider-neutral `AgentHarness` Protocol (describe, prepare, start, reattach, send_turn, cancel_turn, observe, snapshot, usage, end_session) with typed request and handle contracts, `mc.lane_describe.v1`, a `LaneRegistry` keyed by Lane Profile, and dispatch in `OperationExecutionService` by `execution_runtime` (`native | deep_agent | cursor`) and `lane_profile`. The existing `DeepAgentRuntimeAdapter` is wrapped in a `DeepAgentsHarness` that conforms without behavior change, so the Deep Agents acceptance tests still pass through the new dispatch. `missionctl lane list` and `lane describe deep_agents --json` print the registry's matrices. A registered Cursor stub profile (`describe().qualified=False`, every control `unqualified`) proves the registry refuses unqualified lanes at admission unless application policy allows local proof.

**Spec sections:** SPEC-07 §1 (protocol), §2 (`mc.lane_describe.v1`), §3 (registry and dispatch), Contracts, Insertion points; 00-ARCHITECTURE.md §6 (lane matrix).

**Writable regions:** `src/mission_control/application/execution/harness/` (new: `protocol.py`, `registry.py`, `describe.py`, `deep_agents_harness.py`), `src/mission_control/adapters/cursor/__init__.py` (stub describe only), `migrations/0030_lane_bindings.sql` (lane_profile table and execution_binding columns only). Shared, integrator-reviewed: `domain/execution/contracts.py` (`execution_runtime` literal, `lane_profile`, request and handle contracts), `application/execution/operations/operation_execution.py` (dispatch), `adapters/temporal/deployment_composition.py` (registry wiring), `interfaces/cli/main.py` (`lane` group).

**Acceptance criteria:**
- [ ] `AgentHarness` Protocol and all request, handle, observation and receipt contracts exist as strict Pydantic models with JSON Schema export; `HarnessUnsupported(operation, lane_profile, reason)` defined.
- [ ] `mc.lane_describe.v1` model validates the three profile matrices from 00-ARCHITECTURE.md §6 as fixtures; `qualified` defaults to false for Cursor profiles.
- [ ] `LaneRegistry.for_profile` resolves `deep_agents` to `DeepAgentsHarness` and raises a typed error for unknown profiles; registry built once in `deployment_composition.py`.
- [ ] `OperationExecutionRequest` accepts `lane_profile` and the extended `execution_runtime`; the pairing validator rejects a `cursor` runtime without a Cursor binding and a `deep_agent` runtime with one.
- [ ] `OperationExecutionService.execute` dispatches through the registry; `tests/acceptance/control_plane/test_wp_cp_040.py` and `tests/acceptance/mission_control/test_postgres_runtime_parity.py` pass unchanged.
- [ ] Conformance test: for every registered profile, each control reported `unsupported` raises `HarnessUnsupported` and each reported `native` or `emulated` is implemented (method present, not the default stub).
- [ ] Migration `0030` creates `lane_profile` (seeded with three rows) and adds `execution_binding.lane_profile` and `lane_binding`; `mission-db` plan, apply and replay are no-ops on a second run.
- [ ] `missionctl lane list` and `missionctl lane describe <profile> --json` work against the configured API (`GET /v1/applications/{app}/lanes[/{profile}]`).

**Verification:** `make check`; `uv run --group biotech pytest tests/unit/harness tests/unit/operations -q`; `uv run --group biotech pytest -m common_db tests/acceptance/control_plane/test_wp_cp_040.py tests/acceptance/mission_control/test_postgres_runtime_parity.py` with `MISSION_CONTROL_TEST_ADMIN_DSN` set; `uv run pytest packages/mission-control-db-contract/tests -q`.

**Notes:** Prefactor first: lift the `execution_runtime == "deep_agent"` lineage branch into `DeepAgentsHarness.prepare` before adding the registry so the diff to `operation_execution.py` is small. Keep `RuntimePort` and friends; they become the Deep Agents harness internals. Do not touch `operation.py` workflow or activities (FT-G2). Budget: small real fixture runs are permitted by default (see TEAM-WORKSPACE.md, Environment); record spent units in the handoff; larger drills need an approved cap in Linear.
