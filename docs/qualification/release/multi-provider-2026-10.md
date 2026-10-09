---
type: Verification Reference
title: Multi-provider release statement (2026-10, pass 1)
description: Per-profile release statement for the multi-provider packet after waves 0 to 2 and the integrator's 2026-10-09 wiring - implemented, offline-tested, DB/Temporal-tested, live-qualified and account-qualified status with evidence links, the blocked hosted parity statement (MP-21) and the owner decisions still open. Wave 3 is in flight and not integrated.
tags: [mission-control, qualification, release, providers]
---

# Multi-provider release statement — pass 1 (2026-10-09)

Written for MP-23 / OVE-86 from the packet ledger
(`.scratch/multi-provider-2026-10-08/team/LEDGER.md`), the ticket handoffs in
`.scratch/multi-provider-2026-10-08/team/handoffs/` and the integrated code. The integrated base is
`7c9b755` plus the uncommitted owner working state; nothing in the packet is committed, pushed,
deployed or applied to a live database. **Wave 3 (MP-07, MP-08, MP-09, MP-11, MP-12, MP-15) is in
flight in its own worktrees and is not integrated**; pass 2 of MP-23 updates this file after it
lands. This document grants no deployment, migration or paid-provider authorization.

## Statement rules

From [VALIDATION.md](../../specs/multi-provider-2026-10/VALIDATION.md) "Release statement":
report per profile implemented, offline-tested, DB/Temporal-tested, live-qualified,
account/environment-qualified and blocked requirements. A local baseline can ship while hosted work
remains open, but it must not be called "all providers complete". Parallel Swarm and Evaluator
Optimizer remain deferred. OVE-55 (Cursor drill) and OVE-59 to OVE-61 (owner missions) complete
only on their own evidence. A `qualified` flag flips only through a reviewed release that cites a
live-drill record ([lane qualification](../lanes/README.md)); no flag was flipped by this packet.

## Per-profile status

"Fixture" means a labelled synthetic or recorded fixture, never a live provider. "DB/Temporal"
means the disposable PostgreSQL 17 at `127.0.0.1:55433` and the local Temporal at `127.0.0.1:7233`.

| Profile | Implemented | Offline-tested | DB/Temporal-tested | Live-qualified | Account/environment-qualified | Blocked requirements |
| --- | --- | --- | --- | --- | --- | --- |
| `deep_agents` | yes: harness, production launch author, chain relay pump, Human Gates, dispatch journal | yes | yes, deterministic cognition only [1] | lane flag `qualified=True` from the earlier WP-CP-040 parity suite; no live run in this packet | **no** | owner decisions 1, 3, 4 and 5; manifest gates not lowered by the production launch [6] |
| `cursor_local` | yes (FT-G harness on the MP-04 allocator and the MP-06 journal) | yes [2] | yes, synthetic fixtures [2] | **no**: the paid drill has not run (OVE-55) | **no** | Linux, macOS or WSL worker; no `DispatchReconcilingLane`, so an ambiguous send parks `in_doubt`; MP-09 in flight |
| `cursor_cloud` | yes (Cloud Agents API harness) | yes [2] | yes, synthetic fixtures [2] | **no** (OVE-55) | **no** | harness not rewired onto the workspace allocator, so no `branch:<branch>@<sha>` snapshot ref; no fail-closed Kernel Hook; MP-09 in flight |
| `claude_agent_sdk` | partial: v2 describe stub, projection with kernel hook callbacks, frame mapping, auth routes; **no harness** (MP-07 in flight) | projection goldens; local CLI discovery recording; frame fixtures [3] | frame lineage and usage on real PostgreSQL with fixture frames [3] | **no** | **no**; the subscription route is `policy_restricted` and needs an owner attestation | MP-07; Linux, macOS or WSL worker |
| `codex` | partial: v2 describe stub, TOML agent projection, app-server and `exec --json` frame mapping, auth routes; **no harness** (MP-08 in flight) | projection goldens; frame fixtures; discovery **blocked** (Codex CLI absent) [3] | frame lineage and usage on real PostgreSQL with fixture frames [3] | **no** | **no** | MP-08; Codex CLI; Linux, macOS or WSL worker |
| `claude_cloud` | stub only: v2 describe with `unsupported` cells; every path refuses it [4] | refusals unit-tested [4] | not applicable | **no**: Outcome 3 [5] | **no** | missing vendor lifecycle operations [5]; MP-18 blocked |
| `codex_cloud` | stub only, as above [4] | refusals unit-tested [4] | not applicable | **no**: Outcome 3 [5] | **no** | missing vendor lifecycle operations [5]; MP-19 blocked |

Evidence:

1. `deep_agents`: production start and chain through `ManifestLaunchInputAuthor`
   ([`test_manifest_launch_production.py`](../../../tests/integration/temporal/test_manifest_launch_production.py),
   [`test_mp22_local_profile_start.py`](../../../tests/integration/temporal/test_mp22_local_profile_start.py));
   Human Gate restart, deadlines and lost wakes
   ([`test_mp10_human_gate_restart.py`](../../../tests/integration/temporal/test_mp10_human_gate_restart.py));
   dispatch recovery with fixture lanes
   ([`test_mp06_dispatch_recovery.py`](../../../tests/integration/temporal/test_mp06_dispatch_recovery.py));
   account readiness: [owner-workspace report](../local-profiles/readiness-owner-workspace-2026-10-08.json)
   (not ready, 24 unresolved pointers) and [local profiles](../local-profiles/README.md).
2. Cursor: [lane qualification](../lanes/README.md) (`make lane-qualify PROFILE=...` offline
   suites), [`test_lane_qualification_fixtures.py`](../../../tests/unit/harness/test_lane_qualification_fixtures.py),
   [`test_describe_honesty.py`](../../../tests/unit/harness/test_describe_honesty.py),
   [`test_lane_replay_histories.py`](../../../tests/integration/temporal/test_lane_replay_histories.py),
   [`test_workspace_lease_races.py`](../../../tests/integration/postgres/test_workspace_lease_races.py).
3. Claude Agent SDK and Codex: [`test_projection_materialization.py`](../../../tests/unit/agentic_components/test_projection_materialization.py),
   [`test_adapter_discovery.py`](../../../tests/unit/agentic_components/test_adapter_discovery.py)
   (Claude CLI 2.1.295 recording against a closed loopback port, zero provider calls),
   [`test_provider_lanes.py`](../../../tests/unit/frames/test_provider_lanes.py),
   [`test_provider_lineage_postgres.py`](../../../tests/integration/postgres/test_provider_lineage_postgres.py),
   [`test_auth_admission.py`](../../../tests/unit/provider_auth/test_auth_admission.py).
4. Hosted refusals: [`test_environments.py`](../../../tests/unit/agentic_components/test_environments.py)
   (`HOSTED_BOOTSTRAP_UNQUALIFIED` even with `allow_unqualified`),
   [`test_workspace_allocation.py`](../../../tests/unit/workspaces/test_workspace_allocation.py)
   (`WORKSPACE_POLICY_UNSUPPORTED`), [`test_mp22_local_readiness.py`](../../../tests/unit/runtime/test_mp22_local_readiness.py)
   (`LANE_UNQUALIFIED` on every host), [`test_multi_provider_contracts.py`](../../../tests/unit/harness/test_multi_provider_contracts.py).
5. Feasibility, checked 2026-10-08 from documentation and local `--help` only:
   [claude_cloud](../lanes/claude_cloud/FEASIBILITY.md), [codex_cloud](../lanes/codex_cloud/FEASIBILITY.md).
6. `prepare_bound` in `src/mission_control/application/programs/service.py` passes no
   `human_gates` or `human_review`; see [Human Gates](../../knowledge/human-gates.md).

## Workflow forms

Stage Graph, GoalDirected (Goal Loop) and Mission Chains run on `deep_agents` with deterministic
cognition on real local services (evidence 1). No form has run on any other profile: MP-20 (local
and Cursor parity across the three forms) has not started because its wave-3 blockers are not
integrated.

## Hosted parity (MP-21): blocked

MP-21 is blocked. The provider-hosted products (`claude_cloud`, `codex_cloud`) are Outcome 3: the
feasibility studies found no documented, automatable lifecycle covering launch, observe, follow-up,
cancel, approvals and usage, so MP-18 and MP-19 cannot start and no hosted lane exists. Cloud means
the provider-hosted product only: **no local SDK or CLI run (`claude_agent_sdk`, `codex`) and no
self-hosted worker counts toward hosted parity.** All-provider completion stays open until MP-21
passes.

## Owner decisions still open

1. **agent-browser pin.** The workspace bundle `.agents/skills/agent-browser` holds a nested copy
   that changes its bundle digest; move it out (no re-pin) or authorize a re-pin. Until then the
   worker readiness gate refuses startup with `PIN_DRIFT` and five unit tests fail.
2. **CRLF lock.** `skills-lock.json` and the seeds were hashed over CRLF bytes; regenerate from LF
   with `.agents/skills/** eol=lf` or keep the Windows-only lock.
3. **Release locks.** `deployments/{biotech,ai-engineer}/release.lock.json` pin the committed 1.1.0
   manifest (0001-0030); re-lock only after accepting release 1.1.0 with 0031 and 0032 (fingerprint
   `sha256:672549cd...`). No 1.1.0 build is applied live.
4. **Production bindings and local-run profile.** 18 `OWNER-SELECT:` pointers in the
   `mc.manifest_launch_bindings.v1` example and the Temporal Cloud address and namespace (or
   removing the `cloud` cluster).
5. **`OPENAI_API_KEY`** in the worker environment, with an explicit finite budget, for any real
   `deep_agents` run; readiness checks presence only.
6. **WSL or Linux worker** for `cursor_local`, `claude_agent_sdk` and `codex`.

ADR-0035 to ADR-0040 remain `proposed`; acceptance is the owner's call.

## Checks behind this statement

Recorded in the ledger on the integrated owner checkout (2026-10-09): ruff check and format
clean; mypy clean (544 files); targeted unit selections 617 and 93 passed; real PostgreSQL 17,
Temporal and Redis modules 25 passed and 1 failed (`test_mission_worker_startup[True]`, owner
decision 1); db-contract package suite 62 passed and 2 failed (pre-existing version
expectations); `mission-db release-build` built. The full unit suite on the integrated base and
the documentation checks of this pass are recorded in the implementation status
([multi-provider section](../../MISSION_CONTROL_IMPLEMENTATION_STATUS.md#multi-provider-packet-2026-10)).
