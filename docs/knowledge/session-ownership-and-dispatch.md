---
type: Concept
title: Session ownership, the dispatch journal and capacity waits
description: How lane.turn fences a native session to one worker owner, journals every native create and send so an ambiguous dispatch is reconciled or parked in doubt instead of resent, admits each dispatch against the Stop Fence, records the attempt and serves dedicated lane queues, which lanes implement the optional reconcile and steer protocols, and how a provider capacity limit becomes a bounded Temporal wait under the deployment's policy. Auth routes are summarized with their gaps.
tags: [mission-control, harness, lanes, sessions, stop-fence, capacity, implementation]
---

# Session ownership, the dispatch journal and capacity waits

These rules sit inside `LaneTurnService` (`application/execution/harness/lane_turns.py`) and
apply to every lane that runs through `lane.turn` ([lanes and harness](lanes-and-harness.md)).
Built by MP-06 and composed in production by the integrator on 2026-10-09
(`adapters/temporal/deployment_composition.py`). The rules are proven with labelled fixture
lanes (`tests/fixtures/mp06_lanes.py`) on real PostgreSQL 17 and Temporal; no live provider
proves them.

## Ownership

`application/execution/harness/dispatch.py` holds the pure contracts. `SessionOwner` is
`owner_ref`, epoch, generation, lease expiry and the previous owner. `claim_ownership` refuses a
live foreign lease (`SessionOwnedElsewhere`), takes over an expired one with epoch + 1, and
refuses a lower generation (`StaleSessionOwner`). `WorkerSessionManager`
(`application/execution/harness/sessions.py`) owns sessions for one worker process: its owner ref
is the worker identity (default `hostname:pid`) plus a per-process instance, so a restarted
process is a new owner; the lease is `max(MISSION_CONTROL_SESSION_LEASE_MIN_S,
MISSION_CONTROL_SESSION_LEASE_HEARTBEATS × heartbeat timeout)` (defaults 10 s and 2), renewed on
the heartbeat ticker. A takeover fences the old owner out: it cannot end the session or settle.
Control of a `worker_hosted` session held by another live owner is refused and retried
(`session_owned_elsewhere`, retry delay = the remaining lease); a hosted placement is reachable
from any worker. Routing a control to the owning worker's queue is not built.

## The dispatch journal

Every native create and send (first turn, cancel-and-replace replacement, busy retry,
continuation handover) goes through one path: journal `intended`, admit the dispatch against the
run's [Stop Fence](../../GLOSSARY.md) as a governed effect (`dispatch:<kind>:<key>`), then issue
it, with the issue and its receipt shielded from activity cancellation for
`MISSION_CONTROL_DISPATCH_RECEIPT_GRACE_S` (default 10 s). Phases are `intended`,
`acknowledged`, `declined`, `not_received` and `in_doubt`; an acknowledgement is write-once and
only an operator clears `in_doubt`. A key that may have reached the provider is never sent
blindly: `acknowledged` reuses the recorded identity; `intended` asks the lane to reconcile
(found → acknowledge; authoritative `not_received` → exactly one more send under the same key;
unknown or no reconciler → `in_doubt`). The owner and journal live in
`harness_execution.native_identity` under `mc_session_owner` and `mc_dispatch`
(`adapters/postgres/lanes/execution_state.py`), written under the frame-append row lock; no
column was added. `lane.cancel` with an ambiguous dispatch and no recorded turn returns
`in_doubt` instead of "never sent", and `provider_acknowledged` is recorded when the provider
acknowledges ([interventions](interventions.md)).

## Attempt recording and lane queues

`admit_lane_session(attempt=)` records the attempt row through `lineage.observe_attempt` when a
Session Lane unit is admitted; before the 2026-10-09 recovery only the governed Deep Agents path did,
so Session Lane frames had no attempt row to hang off. Session Lane units run `lane.*` on their
binding's task queue (`provider_binding.task_queue`, or the Cursor binding's); production used to
poll only `<base>.agent-cognitive`, so a dedicated queue was never served. Each queue listed in
`MISSION_CONTROL_LANE_TASK_QUEUES` now gets a lane worker (`ProductionWorkerSet.lanes`,
`adapters/temporal/worker.py`).

## Optional lane protocols

`DispatchReconcilingLane` (look a dispatch up by idempotency key) and `SteeringLane` (steer the
exact native turn, answering `applied` or `stale_target`) are runtime-checkable protocols in
`dispatch.py`. `reconcile_dispatch` is implemented by Claude (state-root dispatch record), Codex
(live journal, then `thread/read` by `clientId`), Cursor local (this process's memory only, else
`unknown`) and Cursor cloud (client `agentId`, `latestRunId`); `unknown` still parks `in_doubt`.
Only Codex implements `SteeringLane` (`turn/steer` with `expectedTurnId`).
`resolve_interrupt_mode` (`application/execution/harness/inject.py`)
runs `interrupt_and_inject` in exactly one mode: the frozen per-profile `LANE_COMMAND_SEMANTICS`
and the describe must agree, and `cooperative_inject` needs a `SteeringLane`; otherwise
`unsupported`, with no fallback. No profile declares `cooperative_inject` today.

## Capacity waits

A lane raises `ProviderCapacityLimited` with an MP-05 `ProviderLimitSignal`; the activity reports
non-retryable `provider_capacity_limit`. Behind patch `mp05-capacity-wait`
(`adapters/temporal/workflows/operation.py`) the operation workflow calls `plan_limit_response`
and waits on a Temporal timer that a cancel wakes, bounded by `max_segments × start_to_close_s`
from the workflow start, then runs the segment again or settles `failed(capacity)`. The policy is
the deployment's: `OperationWorkflowRequest.capacity_wait` carries the `MISSION_CONTROL_CAPACITY_*`
settings (`operation_heartbeat_policy`, `adapters/temporal/worker.py`) into both family request
builders; histories without it replay to the defaults. The pure
rules are in `domain/execution/usage_admission.py`: no automatic switch to another auth route, a
reset after the deadline is refused, and unobservable cost stays `unknown`
([budgets and usage](budgets-and-usage.md)).

## Auth routes (MP-05)

`application/execution/auth_admission.py` admits an auth profile per lane and route
(`owner_cli_login`, `oauth_token_env`, `api_key`, `gateway_token`, `cloud_provider_credentials`,
`hosted_product_signin`) with pointed `AuthIssue`s, including `AUTH_ROUTE_SHADOWED` when an API
key would silently turn a subscription route into API billing. `bootstrap/provider_auth.py` loads `MISSION_CONTROL_AUTH_PROFILES_PATH`, builds child
environments that drop shadowing keys (`provider_child_environment`) and maps
`MISSION_CONTROL_CAPACITY_*` to a `LimitWaitPolicy`. The Claude and Codex lane compositions run
auth admission and build their child environments this way ([provider lanes](provider-lanes.md)).
No route is account-enabled: the owner has not supplied a profiles document.

## Gaps, reported not resolved

- `mc.auth_admission.v1` is not persisted (a 0034+ migration once a consumer exists); the Cursor
  lanes do not run auth admission.
- The wait ledger resets on Continue-As-New (proposed `limit_wait_ledger` contract delta).
- A steer is fence-admitted but not journaled; typed owner and journal columns are deferred; a real
  OS-process kill drill was not run.

# Citations

- Spec: [SPEC-01](../specs/multi-provider-2026-10/SPEC-01-runtime.md),
  [SPEC-02](../specs/multi-provider-2026-10/SPEC-02-environments.md) (auth and limits);
  [MP-05](../specs/multi-provider-2026-10/issues/MP-05-qualify-auth-routes-subscription-usage-and-account-capabilities.md),
  [MP-06](../specs/multi-provider-2026-10/issues/MP-06-harden-shared-session-ownership-dispatch-recovery-and-interventions.md).
- ADRs: [0008](../adr/0008-ordered-commands-with-urgent-stop-fence.md),
  [0035](../adr/0035-seven-lane-profiles-versioned-provider-contracts-and-mission-v2.md) (proposed).
- Code: [dispatch rules](../../src/mission_control/application/execution/harness/dispatch.py),
  [session manager](../../src/mission_control/application/execution/harness/sessions.py),
  [lane turns](../../src/mission_control/application/execution/harness/lane_turns.py),
  [interrupt mode](../../src/mission_control/application/execution/harness/inject.py),
  [lane state store](../../src/mission_control/adapters/postgres/lanes/execution_state.py),
  [operation workflow](../../src/mission_control/adapters/temporal/workflows/operation.py),
  [usage admission](../../src/mission_control/domain/execution/usage_admission.py),
  [auth admission](../../src/mission_control/application/execution/auth_admission.py),
  [auth composition](../../src/mission_control/bootstrap/provider_auth.py),
  [worker composition](../../src/mission_control/adapters/temporal/deployment_composition.py),
  [lane workers and capacity policy](../../src/mission_control/adapters/temporal/worker.py).
- Tests: [dispatch recovery](../../tests/unit/harness/test_mp06_dispatch_recovery.py),
  [journal in PostgreSQL](../../tests/integration/postgres/test_mp06_dispatch_journal_postgres.py),
  [takeover and fence race on Temporal](../../tests/integration/temporal/test_mp06_dispatch_recovery.py),
  [limit wait on Temporal](../../tests/integration/temporal/test_mp05_limit_wait.py),
  [auth admission](../../tests/unit/provider_auth/test_auth_admission.py),
  [usage admission](../../tests/unit/provider_auth/test_usage_admission.py),
  [auth composition](../../tests/unit/mission_control/test_provider_auth_composition.py),
  [fixture lanes](../../tests/fixtures/mp06_lanes.py),
  [Session Lane routing](../../tests/unit/operations/test_session_lane_routing.py),
  [lane task queue workers](../../tests/unit/runtime/test_lane_task_queue_workers.py).
