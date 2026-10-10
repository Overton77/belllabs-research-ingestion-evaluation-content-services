---
type: Verification Reference
title: Multi-provider release statement (2026-10, pass 2)
description: Per-profile release statement for the multi-provider packet after the 2026-10-09 recovery integration - implemented, offline-tested, DB-tested, Temporal-tested, live-qualified and account-enabled kept as separate facts for all seven lane profiles, the declared control, approval, continuation and compaction support, the blocked requirements with the exact owner actions, and an explicit verdict that the all-provider objective is not complete.
tags: [mission-control, qualification, release, providers]
---

# Multi-provider release statement — pass 2 (2026-10-10)

MP-23 / OVE-86, pass 2. Written from the team ledger
(`.scratch/multi-provider-2026-10-08/team/LEDGER.md`, entries from "2026-10-09 ~17:00" on), the
[recovery audit](../../specs/multi-provider-2026-10/RECOVERY-HANDOFF-2026-10-09.md) it started
from, the [MP-20 parity record](../parity/multi-provider-2026-10-09.md), the per-lane
qualification READMEs and the declared describe matrices in
[`describe.py`](../../../src/mission_control/application/execution/harness/describe.py). It
replaces pass 1 (2026-10-09), whose wave-3 "in flight" statements are now historical. The
session handoff is [HANDOFF-2026-10-10](../../specs/multi-provider-2026-10/HANDOFF-2026-10-10.md).

**Working state.** Everything below was integrated on `mp/integration-recovery-2026-10-09` (cut
from `efa55f9`), committed as `ca67a2d` and merged into `main` on 2026-10-10 with owner
authorization. Nothing is deployed, locked or applied to a live database; no provider login, paid or
account-bound call was made. This document grants no deployment, migration or paid-provider
authorization.

## Verdict

**The sprint's all-provider objective is not complete.** Two facts block it, and either alone
would: no lane profile is live-qualified by this packet (every G4 drill is owner-run and has not
run), and the provider-hosted products `claude_cloud` and `codex_cloud` remain Outcome 3 after a
fresh revalidation on 2026-10-09, so MP-18, MP-19 and MP-21 are evidence-blocked. This must not be
described as "all providers complete".

What is complete is the **local baseline implementation** with offline, real-PostgreSQL and
real-Temporal evidence: Deep Agents, Claude Agent SDK, Codex, Cursor local and Cursor cloud are
composed through the production launch path and run Stage Graph, GoalDirected (two iterations,
cross-provider verifier) and a two-member Mission Chain on disposable PostgreSQL 17 and local
Temporal with FIXTURE provider clients. Human Gates, native approvals through the durable broker,
governed effects, Session Lane continuation, the dispatch journal, the mission stream and
coordinator subscriptions are wired into the production composition. Release 1.2.0 (migrations
0001-0033) is built and proven on disposable clusters only.

## Statement rules

From [VALIDATION.md](../../specs/multi-provider-2026-10/VALIDATION.md) "Release statement": report
per profile implemented, offline-tested, DB/Temporal-tested, live-qualified, account/environment
status and blocked requirements. Here DB-tested and Temporal-tested are separate columns.
"FIXTURE" means a labelled synthetic or recorded provider client, never a live provider.
"DB" means the disposable PostgreSQL 17 at `127.0.0.1:55433` (or the `mcdb-a`/`mcdb-b`
package clusters); "Temporal" means the local server at `127.0.0.1:7233` or the production-stack
fixture's dev server. A `qualified` flag flips only through a reviewed release citing a live-drill
record under [`docs/qualification/lanes/`](../lanes/README.md); no flag was flipped. Parallel
Swarm and Evaluator Optimizer remain deferred (REQUIREMENTS.md). OVE-55 (Cursor drill) and
OVE-59 to OVE-61 (owner missions) close only on their own evidence.

## Evidence levels per profile

| Profile | Implemented | Offline-tested | DB-tested | Temporal-tested | Live-qualified | Account-enabled | Blocked |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `deep_agents` | yes: lane, production launch, chains, Human Gates, dispatch journal | yes | yes (MP-20 production path, FIXTURE chat model) | yes: MP-20 V02/V09/V12/V14/V15 (V06 by the FT-F3 suite); chain tests 7 passed [1] | flag `qualified=True` from the earlier WP-CP-040 suite; **no live run in this packet** | **no** (`OPENAI_API_KEY` not bound; readiness report not ready) | a budgeted real run; owner bindings file |
| `cursor_local` | yes (MP-09 integrated: reconcile, rehydration, host gate, Cursor launch author) | yes: `tests/unit/cursor` 37 passed [2] | yes (MP-20, FIXTURE bridge) | yes: MP-20 V02/V12/V14/V15 [1] | **no** (OVE-55 drill not run) | **no** (nothing observed) | live drill; Linux/WSL 2 worker |
| `cursor_cloud` | yes (allocator-backed lease, `branch:<branch>@<sha>` snapshots, run-branch publisher) | yes [2] | yes (MP-20, FIXTURE Cloud API) | yes: MP-20 rows; `test_mp09_cloud_segment_loop_real_services.py` [2] | **no** (OVE-55) | **no** | live drill; throwaway push repository |
| `claude_agent_sdk` | yes: `adapters/claude` over `claude-agent-sdk==0.2.165` (bundled Claude Code 2.1.294), composed when `MISSION_CONTROL_CLAUDE_LANE` and an auth profiles path are set on a non-Windows worker | yes: `tests/unit/claude` 79 passed [3] | yes: `test_claude_broker_postgres.py` on released 0033 [3] | yes: production `OperationWorkflow` with replay; continuation; MP-20 [3] | **no** (drill runner exists, skipped) | **unknown** (no login, no probe) | live drill; Linux/WSL 2/macOS worker; auth profile (and owner attestation for `owner_cli_login`) |
| `codex` | yes: `adapters/codex` over `codex-cli 0.162.0` app-server protocol v2 (schema `sha256:0bf5254b…`), composed when `MISSION_CONTROL_CODEX_LANE` and an auth profiles path are set on a non-Windows worker | yes: `tests/unit/codex` 107 passed [4] | yes: `test_codex_approvals_postgres.py` [4] | yes: `test_mp08_codex_lane_turns.py` 3/3 on four runs; continuation drill with worker loss; MP-20 [4] | **no** (drill runner exists, skipped) | **unknown** | live drill; Linux/WSL worker with `codex-cli 0.162.0` on `PATH`; auth profile |
| `claude_cloud` | stub only: v2 describe, every control `unqualified`, refusals everywhere [5] | refusals unit-tested [5] | not applicable | not applicable | **no**: Outcome 3, revalidated 2026-10-09 [6] | **no** | MP-18 and MP-21 evidence-blocked: 13 vendor operations missing |
| `codex_cloud` | stub only, as above [5] | refusals unit-tested [5] | not applicable | not applicable | **no**: Outcome 3, revalidated 2026-10-09 [6] | **no** | MP-19 and MP-21 evidence-blocked: 14 vendor operations missing |

Provider-hosted means the vendor's own hosted coding product only. A local SDK or CLI on our own
worker (`claude_agent_sdk`, `codex`) and any self-hosted environment do not count toward
`claude_cloud`, `codex_cloud` or MP-21.

## Declared support per profile

From `DECLARED_LANE_MATRICES`. Deep Agents keeps its digest-stable `mc.lane_describe.v1` matrix;
the Cursor, Claude and Codex profiles have implemented `mc.lane_describe.v2` matrices; the hosted
profiles are v2 stubs. Convention adopted in the recovery session: an implemented cell states how
it is supported (`native` or `emulated`) with `qualified=False`; `unqualified` is used only where
nothing is implemented. Release 1.2.0's migration 0033 regenerates the four implemented `lane_profile`
rows from these matrices, still unqualified.

| Profile | Controls not `native` | Queue / interrupt / resume | Approval modes | Continuation | Compaction control | Hooks fail closed |
| --- | --- | --- | --- | --- | --- | --- |
| `deep_agents` (v1) | `snapshot`, `fork` emulated | `turn_boundary_guaranteed` / `cancel_and_replace` / `turn_boundary_guaranteed`; `pause_at_tool_gate` | `workflow_gate` only (the v2 admission refuses the provider and governed modes) | `request_continuation` declared `emulated`, but ADR-0041: on the governed Deep Agents path a request is recorded only; rotation stays the GoalDirected session policy | observed (`before_compaction`/`after_compaction` frames); no control | yes (middleware) |
| `cursor_local` | `reattach`, `snapshot`, `fork` emulated; `pause` unsupported | `wait_then_send` / `cancel_and_replace` / `wait_then_send` | `workflow_gate` (`provider_permission` unsupported on `cursor-sdk 1.0.37`; `governed_effect` unqualified, rejected) | `emulated`: hydrated handover, registered with MP-12 | `unsupported` (`preCompact` observe-only) | yes |
| `cursor_cloud` | `snapshot`, `fork` emulated; `pause` unsupported | as `cursor_local` | `workflow_gate`; `approval_suspension` unsupported | `emulated` (files committed to the run branch, fresh agent) | `unsupported` | **no** (the VM cannot reach the loopback callback; MCP enforcement unsupported) |
| `claude_agent_sdk` | `reattach`, `snapshot` emulated; `pause`, `fork` unqualified | `turn_boundary_guaranteed` / `cancel_and_replace` / `turn_boundary_guaranteed`; `hard_pause` unsupported | `workflow_gate`, `provider_permission` (`can_use_tool` through the MP-11 broker), `governed_effect`; **not** `provider_question` or `mcp_elicitation` (the Python SDK forwards neither) | `emulated`: sealed checkpoint into a fresh session, no conversation fork | `unqualified` (`PreCompact` observed only) | yes (SDK callbacks plus command hooks) |
| `codex` | `reattach`, `snapshot`, `pause` emulated; `fork` unqualified | `wait_then_send` / `cancel_and_replace` / `wait_then_send` (steer exists as `SteeringLane`; no `cooperative_inject` declared) | all five: `workflow_gate`, `provider_permission`, `provider_question`, `mcp_elicitation`, `governed_effect` | `emulated`: fresh app-server and fresh thread, no `thread/fork` | **`native`** (`thread/compact/start`) | yes (command hooks) |
| `claude_cloud` | every control `unqualified` | design intent `wait_then_send`; `cancel`, `pause`, `fork` unsupported | `workflow_gate` declared; nothing runs | not implemented | `unqualified` | no |
| `codex_cloud` | every control `unqualified` | every delivery mode unsupported | `workflow_gate` declared; nothing runs | `unsupported` | `unsupported` | no hooks |

Usage dispositions: Deep Agents per model call; Claude and Codex tokens `settled_per_turn`, cost
`estimated`; Cursor tokens `settled_per_turn`, cost `estimated_then_settled`; hosted stubs
`unavailable`. Since the recovery session a Session Lane settlement charges its closing token
usage once to the run's reserved or declared token dimensions; unknown usage charges nothing and is
never recorded as zero ([budgets and usage](../../knowledge/budgets-and-usage.md)).

## Workflow forms

MP-20 ([parity record](../parity/multi-provider-2026-10-09.md)) runs Stage Graph cross-provider
handoffs (V14), GoalDirected with two iterations and a mixed Claude executor / Codex verifier
(V12), two linked Goal Loops with a replayed release (V15), V01 refusals at submit, V04 queued
instructions, V06 cancel and V09 gate approval through the public submit/start router, the
production launch author and the deployment's own operation activities on real PostgreSQL and
Temporal, with only the provider seam a FIXTURE. Several gaps listed in that record were closed
after it was written (ledger entries for the Claude continuation fix, denied gate, authored inputs,
Goal Loop packets, Cursor launch author, the run-branch publisher and stand-in removal): a denied
manifest Human Gate now fails the run, the Claude continuation turn is sent once, and every
test-local stand-in is removed. The last recorded MP-20 result is 32 passed and 1 xfailed, taken
before the integrator's run-branch publisher fix and stand-in removal; the rerun after them is not
yet recorded (see "Final validation run").

## Hosted parity (MP-21): blocked

The 2026-10-09 revalidations ([claude_cloud](../lanes/claude_cloud/REVALIDATION-2026-10-09.md),
[codex_cloud](../lanes/codex_cloud/REVALIDATION-2026-10-09.md)) checked current vendor
documentation and the local CLI or pinned open-source code without a login or paid call and kept
Outcome 3. Anthropic-hosted Claude Code sessions still have no public status, cancel/interrupt,
event replay, idempotent create or pending-approval answer; Codex Cloud still exposes only the
experimental `codex cloud exec` and `list`, with no follow-up, cancel, observation, approval
resume, usage or lineage operation. MP-18 and MP-19 cannot start; MP-21 stays open until they do.
The requirement is retained, not removed or substituted.

## Blocked requirements and exact owner actions

1. **Live drills (G4), one per profile, each with an owner-approved finite budget recorded in
   Linear.** From the repository root on the right host, after the offline suites pass:
   - `cursor_local` (Linux/WSL 2) and `cursor_cloud` (any host): approve on OVE-55; export
     `CURSOR_API_KEY`, `MC_PAID_BUDGET_USD=<amount>`, for cloud `MC_CURSOR_CLOUD_REPO=<throwaway
     repository>`, optionally `MC_LANE_DRILL_APPROVAL_URL`; run
     `make lane-qualify PROFILE=cursor_local LIVE=1`, then `PROFILE=cursor_cloud LIVE=1`
     ([cursor_local](../lanes/cursor_local/README.md), [cursor_cloud](../lanes/cursor_cloud/README.md)).
   - `claude_agent_sdk` (Linux/WSL 2/macOS): approve on OVE-70; export
     `MC_LIVE_CLAUDE_QUALIFICATION=1`, `MC_PAID_BUDGET_USD`, `MC_CLAUDE_AUTH_ROUTE=api_key|owner_cli_login`,
     `MISSION_CONTROL_AUTH_PROFILES_PATH`, `MC_CLAUDE_AUTH_PROFILE` (and
     `MC_CLAUDE_OWNER_ATTESTATION_REF` for the `policy_restricted` `owner_cli_login` route); run
     `make lane-qualify PROFILE=claude_agent_sdk LIVE=1` ([README](../lanes/claude_agent_sdk/README.md)).
   - `codex` (Linux/WSL with `codex-cli 0.162.0` on `PATH`): approve on OVE-71; export
     `MC_LIVE_CODEX_QUALIFICATION=1`, `MC_PAID_BUDGET_USD`, `MC_CODEX_AUTH_ROUTE`,
     `MC_CODEX_OWNER_HOME` (owner login route), `MISSION_CONTROL_AUTH_PROFILES_PATH`,
     `MC_CODEX_AUTH_PROFILE`; run `make lane-qualify PROFILE=codex LIVE=1` ([README](../lanes/codex/README.md)).
   - `deep_agents` has no `lane-qualify` profile: a real run needs `OPENAI_API_KEY` in the worker
     environment and a finite budget.
   Each drill writes `docs/qualification/lanes/<profile>-<date>.md`; only a reviewed change citing
   that record may set `qualified=True` together with a release migration row.
2. **Linux or WSL 2 worker** for `cursor_local`, `claude_agent_sdk` and `codex`. The Windows worker
   runs a `SelectorEventLoop` without subprocess support and every one of these lanes refuses it
   (`LANE_UNSUPPORTED_OS` / `HostUnsupported`); the event-loop policy is not changed globally. The
   WSL `Ubuntu` distro on this host has no `codex` on `PATH`.
3. **Auth profiles.** Author the `MISSION_CONTROL_AUTH_PROFILES_PATH` document (`{"profiles": [...]}`)
   for each lane and route; no route is account-enabled today. The lanes are opt-in
   (`MISSION_CONTROL_CLAUDE_LANE`, `MISSION_CONTROL_CODEX_LANE`), and bindings routed to dedicated
   lane queues need `MISSION_CONTROL_LANE_TASK_QUEUES` on the worker that serves them.
4. **`agent-browser` pin.** Remove the stray nested `agent-browser/` folder (added 2026-10-08 12:48)
   from the workspace bundle `Biotech/.agents/skills/agent-browser`; without it the bundle
   recomputes to the pinned digest (no re-pin needed). Until then the worker readiness gate refuses
   startup with `PIN_DRIFT` (`test_mission_worker_startup[True]`).
5. **DB release 1.2.0.** Locks still pin 1.1.0 (0001-0030); the live projects hold 1.0.0. Procedure
   ([db-contract README](../../../packages/mission-control-db-contract/README.md),
   [persistence](../../knowledge/persistence.md), `docs/qualification/two-project/LIVE_PLAN.md`):
   create `pg_trgm` in schema `extensions` on each project; `mission-db inspect` the installed
   receipts; `mission-db lock --app <app>`; `mission-db plan`; `mission-db apply` with
   `--confirm-target` and `--expected-plan-digest`; `mission-db verify`; then the seed phases
   (including `mc.catalog.workflow-parity@1.0.1`). Also open: the security review of the
   family-writer INSERT widening from 0028.
6. **Production launch bindings and local-run profile.** Author the production
   `mc.manifest_launch_bindings` file (v2 with `providers` for Claude, Codex and Cursor) and resolve
   the `OWNER-SELECT:` pointers and the Temporal Cloud cluster (or remove it) in the local-run profile.
7. **CRLF versus LF** for `skills-lock.json` and the seeds (ledger 2026-10-08; not resolved since).
8. **Linear.** Post evidence comments and states for OVE-63 to OVE-86 from the ledger and this
   statement; no Claude Code session updated Linear.
9. **Decisions.** ADR-0035 to ADR-0041 remain `proposed`.

Code follow-ups that do not need the owner are listed in the
[handoff](../../specs/multi-provider-2026-10/HANDOFF-2026-10-10.md#remaining-work).

## Evidence

1. MP-20: [parity record](../parity/multi-provider-2026-10-09.md),
   [`test_mp20_workflow_parity.py`](../../../tests/integration/temporal/test_mp20_workflow_parity.py);
   chain and launch: [`test_manifest_launch_production.py`](../../../tests/integration/temporal/test_manifest_launch_production.py),
   [`test_mp22_local_profile_start.py`](../../../tests/integration/temporal/test_mp22_local_profile_start.py),
   [`test_manifest_submit.py`](../../../tests/integration/postgres/test_manifest_submit.py) (7 passed after blocker A).
2. Cursor: [cursor_local](../lanes/cursor_local/README.md), [cursor_cloud](../lanes/cursor_cloud/README.md),
   [lane qualification](../lanes/README.md),
   [`test_mp09_cloud_segment_loop_real_services.py`](../../../tests/integration/cursor/test_mp09_cloud_segment_loop_real_services.py).
3. Claude: [claude_agent_sdk](../lanes/claude_agent_sdk/README.md),
   [`test_mp07_claude_lane_turns.py`](../../../tests/integration/temporal/test_mp07_claude_lane_turns.py),
   [`test_claude_broker_postgres.py`](../../../tests/integration/claude/test_claude_broker_postgres.py),
   [`test_claude_continuation_temporal.py`](../../../tests/integration/claude/test_claude_continuation_temporal.py).
4. Codex: [codex](../lanes/codex/README.md),
   [`test_mp08_codex_lane_turns.py`](../../../tests/integration/temporal/test_mp08_codex_lane_turns.py),
   [`test_codex_approvals_postgres.py`](../../../tests/integration/codex/test_codex_approvals_postgres.py),
   [`test_codex_continuation_temporal.py`](../../../tests/integration/codex/test_codex_continuation_temporal.py).
5. Hosted refusals: [`test_multi_provider_contracts.py`](../../../tests/unit/harness/test_multi_provider_contracts.py),
   [`test_environments.py`](../../../tests/unit/agentic_components/test_environments.py),
   [`test_mp22_local_readiness.py`](../../../tests/unit/runtime/test_mp22_local_readiness.py).
6. Feasibility: [claude_cloud](../lanes/claude_cloud/FEASIBILITY.md) and
   [codex_cloud](../lanes/codex_cloud/FEASIBILITY.md) (2026-10-08) with their 2026-10-09 revalidations.

Last recorded checks (ledger, 2026-10-09, before the final edits of that session): mypy clean
(601 source files); full `tests/integration/postgres` 219 passed and 1 failed (the `agent-browser`
`PIN_DRIFT` refusal, owner action 4); db-contract package suite 65 passed; release 1.2.0 built,
fingerprint `sha256:0113df03978540d777c5be712d8b17e0f3d2a3f98245cd8fd7661cef7f6b5d23`; saved-history
replay 47 passed; realtime 342 unit and 28 real-service passed. The final tree's full run is
recorded below.

## Final validation run (integrator)

Run on 2026-10-10 against the final working tree of `mp/integration-recovery-2026-10-09`
(committed afterwards as `ca67a2d`), on Windows with Docker Desktop: disposable PostgreSQL 17 on 55433 and `mcdb-a`/`mcdb-b`,
Temporal on 7233, Redis. The owner database on 55432 was not used. Logs are under
`.scratch/multi-provider-2026-10-08/recovery-logs/` (not committed).

| Check | Command | Result |
| --- | --- | --- |
| Lock, lint, format, types, dependencies, architecture | `make ci` | `uv lock --check` ok; `ruff check` ok; `ruff format --check` ok (1310 files); `mypy` ok (606 source files); `deptry` ok; `tests/architecture` 10 passed |
| Full unit suite | `make ci` (`pytest tests/unit`) | 2816 passed, 2 failed, 19 skipped, 2 xfailed. `test_publishing_is_idempotent_per_branch` was then updated to the corrected publisher contract; `tests/unit/harness/test_cursor_cloud.py` reruns 15 passed. The remaining failure is `test_workspace_artifacts_verify_against_their_pins_when_present`: the `agent-browser` `PIN_DRIFT` refusal (owner action 4), which fails closed as intended |
| Links and skills | `make links`, `make skills-check` | 0 broken links; all skills ok |
| PostgreSQL integration | `with-test-db.sh uv run --no-sync --group biotech --group dbcontract pytest -p no:cacheprovider tests/integration/postgres` | 219 passed, 1 failed: `test_mission_worker_startup` `[True]`, the same `agent-browser` `PIN_DRIFT` refusal |
| Temporal and lane integration | same wrapper, `tests/integration/{temporal,claude,codex,cursor}` | 213 passed, 3 failed, 7 skipped (the skips are owner-run paid live drills). `test_frames_expire_workflow` and `test_codex_approvals_postgres::test_a_relaunch_marks_the_dead_connections_correlation_lost` pass when run alone (load flakes). `test_pre_stage3_temporal_contracts::test_temporal_contracts_are_versioned_without_mutating_published_v2` fails identically at HEAD `efa55f9` and at the pre-sprint baseline `7c9b755` (`run_plan_v3` digest `sha256:7da9e136…` against the pinned `b97ee980…`); it predates this sprint and the pin was not changed |
| MP-20 workflow parity | `MP20_TEMPORAL_PORT=7397` + wrapper, `tests/integration/temporal/test_mp20_workflow_parity.py` | 32 passed, 0 xfailed, with no test stand-ins (production launch author, activities and routing) |
| Saved-history replay | replay suites under `tests/integration/temporal` | 47 passed (Session Lanes); 62 passed with linked runs |
| Database contract package | `with-mcdb.sh` db-contract package suite | 65 passed |
| Release | `mission-db release-build` (1.2.0, migrations 0001..0033) | fingerprint `sha256:0113df03978540d777c5be712d8b17e0f3d2a3f98245cd8fd7661cef7f6b5d23`, unchanged after the 0033 describe regeneration. Not applied to any live database |

Not run: paid live drills for any profile (`make lane-qualify PROFILE=... LIVE=1`), hosted lanes
(blocked on evidence), and the release lock and apply procedure. These are all owner actions. No
profile is live-qualified, and every profile still declares `qualified=False`.
