---
type: Playbook
title: Trusted startup and process roles
description: How operators bind API workers and Agent Server without fabricating readiness, including the multi-provider readiness gate at worker startup, the preflight CLI, the run's Temporal cluster binding and the cluster-outage procedure.
tags: [mission-control, implementation]
---

# Trusted startup and process roles

Start the API at `mission_control.bootstrap.api:create_app`, worker at
`mission_control.bootstrap.worker`, and read-only preflight at
`mission_control.bootstrap.preflight`. The missionctl client calls the authenticated
application-scoped API. Detailed configuration lives in the operator guide outside
this bundle.

Operator deployment configuration names secret environment references, accepted
issuers/audiences, public verification keys, tenant grants and immutable bindings.
Token permissions and user-editable metadata do not create authority.
Installing the common release, its runtime phase and seeds are explicit
`mission-db` administrative actions with reviewed plans; startup never performs them.

Workers select one application and binding digest, verify actual persisted
installation identity and role separation, and then connect to the configured
Temporal namespace and queues. Runtime option factories are installed trusted
code; requests and skill bundles cannot select them.

The fast-track packet adds process behavior, configured by environment names only (the list is
in the operator guide). The API process runs the subscription relay only when
`MISSION_CONTROL_SUBSCRIPTION_RELAY=1` ([events and commands](events-and-commands.md)). The
worker registers Cursor lane profiles only when `CURSOR_API_KEY` is bound, refuses unqualified
lanes unless `MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES=true`, serves the Kernel Hook callback on
`127.0.0.1` (`MISSION_CONTROL_HOOK_CALLBACK_PORT`), serves `mc.frames_expire.v1` on its
maintenance queue with a daily Schedule (`MISSION_CONTROL_FRAMES_EXPIRE_SCHEDULE`), and polls as
a Worker Deployment version (`TEMPORAL_WORKER_VERSIONING`, `TEMPORAL_DEPLOYMENT_NAME`,
`TEMPORAL_BUILD_ID`, `TEMPORAL_PROMOTE_ON_START`) on `temporalio` 1.34
([cursor lane](cursor-lane.md), `adapters/temporal/versioning.py`). The worker can launch only
capabilities in its pin file (`CAPABILITY_PINS_PATH`). A live mission start still needs the
operator's launch bindings ([mission manifest](mission-manifest.md)).

## Multi-provider readiness and cluster binding (2026-10)

**Socket entrypoint.** `mission_control.bootstrap.realtime:create_asgi_app` serves the API plus the
`/missions` namespace ([mission stream](mission-stream.md)); `create_app` stays valid alone.

**Worker readiness gate.** Before any Temporal poller or provider process exists, `run_worker`
calls `verify_startup_readiness` (`bootstrap/worker.py`): it verifies every capability pin under
`MISSION_CONTROL_PREFLIGHT_WORKSPACE_ROOT` (or the derived workspace root) and, with
`MISSION_CONTROL_LOCAL_RUN_PROFILE` set, the lane hosts of that `mc.local_run_profile.v1`.
Blocking issues (`PIN_DRIFT`, `LANE_UNSUPPORTED_OS`, ...) refuse startup with
`InstallationUnavailable` naming each code and pointer; a drifted skill is a typed refusal, not a
bare `CapabilityPinError` later. Without a profile, host mismatches of the lanes the settings
register are logged as advisory. `cursor_local`, `claude_agent_sdk` and `codex` need Linux, macOS
or WSL (the Windows worker runs a selector loop without subprocess support); `claude_cloud` and
`codex_cloud` are refused on every host.

**Preflight CLI** (`bootstrap/preflight.py`). `python -m mission_control.bootstrap.preflight
readiness --profile <file>` prints `mc.local_readiness.v1` with every unresolved
`<file>#<json pointer>` (profile, lane OS, launch bindings, auth routes by presence only,
capability pins, release lock and, with `--db-dsn-env`, the installed release fingerprint) and
exits 2 when blocked, with no composition, login or provider call; `compose-bindings` re-seals an
owner's `mc.manifest_launch_bindings.v1` from the committed example plus a selections file;
`guard-launch` keeps a write-once file ledger for operators. The owner-workspace report
([readiness](../qualification/local-profiles/readiness-owner-workspace-2026-10-08.json)) is not
ready, with 24 unresolved pointers.

**Cluster binding.** `RunLaunchService` (`application/execution/run_launch.py`) binds a run to the
Temporal cluster (address and namespace) of its first admitted launch in
`mission_control.run_cluster_binding` (migration 0032) before it submits; a later launch from a
service bound to another cluster is refused `run_bound_to_other_cluster`. It is composed for the
API, the chain relay pump and the technical API (`bootstrap/composition.py`, `manifests.py`,
`worker.py`, `runtime_control.py`).

**Temporal fallback.** The same workflow code runs on local Temporal and Temporal Cloud
(`TEMPORAL_TARGET`). A cloud outage never moves an active run: new runs may target the local
binding, and an active run waits for its original cluster or an explicit reconciled successor.
The drill on two real namespaces (`tests/integration/temporal/test_mp22_outage_drill.py`) proves
the guard refuses `RUN_BOUND_TO_OTHER_CLUSTER`, `ACTIVE_IN_OTHER_CLUSTER` and `RUN_NOT_PENDING`
and starts no second copy; Temporal Cloud itself was not exercised. Procedure: owner runbook
sections 2.8 and 2.9.

Agent Server's canonical configuration is `agent_server/langgraph.json`. Its
graphs execute bounded subordinate cognition and qualification surfaces. Serving
a graph does not make it a mission scheduler or certify deployed production state.
The runtime, qualification and N1-only profiles share one authentication entrypoint;
profile checks also guard native thread run creation and exact assistant UUIDs.
Disallowed registered graphs return 403.

# Citations

- [API bootstrap](../../src/mission_control/bootstrap/api.py).
- [Worker bootstrap](../../src/mission_control/bootstrap/worker.py).
- [Common installation readiness](../../src/mission_control/bootstrap/common_installation.py).
- [Settings](../../src/mission_control/bootstrap/settings.py),
  [subscription composition](../../src/mission_control/bootstrap/subscriptions.py),
  [Worker Deployment versioning](../../src/mission_control/adapters/temporal/versioning.py),
  [worker maintenance tests](../../tests/unit/mission_control/test_worker_maintenance.py),
  [versioning tests](../../tests/integration/temporal/test_worker_versioning.py).
- [Canonical Agent Server profile proofs](../../tests/integration/agent_server/test_canonical_server_local.py).
- [Realtime entrypoint](../../src/mission_control/bootstrap/realtime.py),
  [preflight and readiness](../../src/mission_control/bootstrap/preflight.py),
  [run launch cluster binding](../../src/mission_control/application/execution/run_launch.py),
  [cluster binding store](../../src/mission_control/adapters/postgres/run_control/cluster_bindings.py).
- Tests: [local readiness](../../tests/unit/runtime/test_mp22_local_readiness.py),
  [cluster refusal](../../tests/unit/run_control/test_run_launch.py),
  [outage drill](../../tests/integration/temporal/test_mp22_outage_drill.py),
  [release preflight](../../tests/integration/postgres/test_mp22_db_release_preflight.py),
  [local profile start](../../tests/integration/temporal/test_mp22_local_profile_start.py),
  [0032 cluster binding](../../tests/integration/postgres/test_release_0032_cluster_binding_and_stream_hints.py),
  [worker startup](../../tests/integration/postgres/test_mission_worker_startup.py).
- Spec: [VALIDATION](../specs/multi-provider-2026-10/VALIDATION.md) (local Temporal fallback);
  [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md) (2.8, 2.9);
  [local profile readiness](../qualification/local-profiles/README.md).
- Operator setup: `docs/MISSION_CONTROL_LOCAL_API.md`.
