---
type: Implementation Evidence
title: Mission Control sprint recovery handoff (2026-10-09)
description: Verified checkpoint, unintegrated provider worktrees, fresh offline checks, recovery blockers and ordered continuation steps after the Cursor and Claude Code multi-provider sprint.
tags: [mission-control, providers, recovery, handoff]
---

# Mission Control sprint recovery handoff — 2026-10-09

The sprint's integrated work is preserved on GitHub. Substantial later work remains locally in five dirty worktrees. Mission Control has a multi-provider foundation, but the Claude and Codex implementations have not reached the production composition on main. The sprint is unfinished and the current checkout does not pass its full offline gate.

This is a recovery audit, not an implementation or qualification release. It supersedes the checkout/commit/in-flight claims in [CLAUDE-CODE-HANDOFF.md](CLAUDE-CODE-HANDOFF.md), the packet README's old execution status, the multi-provider section of [implementation status](../../MISSION_CONTROL_IMPLEMENTATION_STATUS.md), and [release statement pass 1](../../qualification/release/multi-provider-2026-10.md). Their design and historical evidence remain useful. No provider calls, login, deployment, live database changes, integration ports, commits or pushes were made during this audit.

## Start here — owner intent and complete sprint scope

Owner clarification after this audit: hand this file to Claude Code to continue the entire sprint, using subagents and workflows as needed, without asking the owner to reconstruct the specification or earlier discussions. The intended outcome is completion of the preserved multi-provider packet. Recovering the five worktrees is an early step; it is not the whole assignment.

### Load the specification and backlog from disk

Read the repository and scoped AGENTS.md files, then these documents in this same directory:

1. [README.md](README.md), [REQUIREMENTS.md](REQUIREMENTS.md) and [ARCHITECTURE.md](ARCHITECTURE.md): complete owner scope, completion boundaries and architecture. The requirements-to-issues-to-proof table is the scope checklist.
2. [SPEC-01-runtime.md](SPEC-01-runtime.md), [SPEC-02-environments.md](SPEC-02-environments.md), [SPEC-03-human-control.md](SPEC-03-human-control.md), [SPEC-04-realtime.md](SPEC-04-realtime.md): detailed implementation requirements.
3. [ISSUES.md](ISSUES.md) and **all 23 linked MP-01 through MP-23 issue definitions** under `issues/`: each ticket's work items, dependencies and acceptance criteria. Issue-state prose here is a planning snapshot, not current implementation truth; reconcile it with this audit, the code and the tracker when available.
4. [VALIDATION.md](VALIDATION.md), [RESEARCH.md](RESEARCH.md), [TEAM-WORKSPACE.md](TEAM-WORKSPACE.md) and [DELIVERY.md](DELIVERY.md): acceptance scenarios, provider evidence, ownership rules and decisions. The newer owner permission to continue in Claude Code supersedes the historical Cursor-only kickoff restriction.
5. The final team ledger entries, each retained child's handoff and code, and the accepted general Mission Control specification referenced by the root AGENTS.md. The recovery audit changes implementation-state claims; it does not replace the design specifications.

### Carry every workstream through to evidence

Track requirements and all 23 tickets against implementation, integration, executable acceptance evidence and remaining blockers. The full scope includes:

- Claude, Codex and Cursor execution lanes with honest local versus provider-hosted distinctions, and Stage Graph / GoalDirected / Mission Chain parity.
- Provider-neutral bindings, YAML authoring, launch inputs, materialized skills/plugins/MCP servers/subagents/executable hooks, explicit repositories/branches, workspace leases and snapshots.
- Auth-route/subscription-aware admission, account capabilities, usage/capacity limits, session ownership, dispatch recovery, queue/cancel/intervention semantics.
- Separate continuation, context compaction, turn/goal limits and Temporal history rollover; durable context/artifact/workspace transfer.
- Workflow Human Gates, approval/rejection/review with feedback, native tool approvals, MCP human input and governed fallback effects.
- **Scoped Socket.IO routes and shared handlers, durable replay, PostgreSQL/Redis hints, provider-native frames, common projections, subordinate lineage, bounded coordinator subscriptions and callbacks.** Verify existing integrated handlers against SPEC-04 and V16–19; do not assume that their presence completes the realtime work.
- Database release/contract reconciliation, real local profiles and Temporal fallback, parity/qualification evidence, operator guidance and final release documentation.

Parallel Swarm and Evaluator Optimizer remain deferred by REQUIREMENTS.md. Permission to use coding subagents does not add those Mission Control program behaviors to this sprint.

### Execute and preserve progress

Act as integrator. Use isolated subagents for independent ownership regions, following TEAM-WORKSPACE's shared-contract ownership and dependency gates. Reconcile the retained work before duplicating it. Own the shared contracts, migration numbering, composition, conflict resolution and final integration checks centrally. Existing independent mission concurrency is allowed under the admitted governors; Temporal remains Mission Control's scheduler.

Continue through implementation, integration, local disposable service tests, saved-history replay, acceptance/parity and documentation. Do not stop after a plan, a worktree port, a green unit suite or the first externally blocked ticket. Continue independent work while recording the precise blocked acceptance obligations. Keep the ledger and a resumable handoff current if the session is interrupted. Preserve dirty/index/untracked work; obtain new local recovery snapshots before broad integration or cleanup. Follow the repository's separate authorization rules for commits/pushes, deployment, live database changes and paid provider drills; no finite provider budget is supplied by this clarification.

Full completion still includes the provider-hosted requirement. Revalidate recorded hosted feasibility when approaching those tickets. If a necessary vendor operation, account entitlement or authorized live drill remains unavailable, document the exact evidence and unresolved tickets; do not silently remove the requirement, substitute our own hosted worker, or claim the whole sprint complete. Local implementation and parity can still be completed independently.

## 1. Canonical repository and preservation

| Item | Verified state |
| --- | --- |
| Canonical checkout | `C:\Users\Pinda\Proyectos\BellLabs\platform\mission-control` |
| Branch | `main` |
| Origin | `git@github-legacy:Overton77/mission-control.git` |
| Remote repository | `Overton77/mission-control` |
| Current checkpoint | `efa55f91c7693fe89a94c279aad8a93fbfbbe5e0` |
| Remote main | `git ls-remote origin refs/heads/main` matched that exact checkpoint during this audit |
| Integrated sprint checkpoint | `d7b09db9a16faa1d08f198bf720e3dc104f7427d`, Oct 9 15:21:15 -04:00, `feat: checkpoint partial multi-provider Mission Control work` |
| Follow-up checkpoint | `efa55f9`, Oct 9 15:29:03 -04:00, workspace-tool path comparison test correction |
| Checkout before this handoff | Clean; the audit adds documentation only to the repository |
| Registered worktrees | 19: main, 16 MP worktrees and two baseline checkouts |

`d7b09db` changes 420 files. This includes 126 source files (36,368 added / 8,814 removed lines), 96 test files, 80 documents and 98 imported specification files. The overall 82,023 added lines therefore include specifications, fixtures and formatting; they are not a measure of completed runtime functionality.

All 16 MP worktrees still have HEAD `7c9b755d75c4417ae364f33c340b9891446a391a`. Their deliverables are uncommitted. Looking only at branch commits, or cherry-picking those branch tips, will miss the work. Their indexes contain staged copies of the integrator's evolving base; unstaged changes and additional untracked files contain child work. A dirty-file count mixes inherited base with ticket changes.

The worktrees are physically under the canonical checkout's `.scratch/multi-provider-2026-10-08/worktrees/`. Git registrations still name the old `C:/Users/Pinda/Proyectos/Biotech/mission-control/...` alias. This is one retained set of worktrees, not a second implementation. Keep the alias and workspace skill/tool links until the separately documented relocation cleanup is completed. Closing an editor or chat did not commit these changes or remove these worktrees.

### Additional recovery copy made by this audit

`C:\Users\Pinda\Proyectos\BellLabs\.workspace-setup\mission-control-recovery-20261009\`

For MP-07/08/09/11/12 this contains the base HEAD, staged owner-base patch, unstaged child patch, compared untracked source/fixture/doc files, and direct handoff documents/logs. Its `manifest.json` records SHA-256 hashes; all **181 files / 19,696,313 bytes** were read back and verified. Environments, installed tools, node_modules and scratch tool binaries are excluded. This is an additional local copy on the same machine, not an off-machine backup or a replacement for the retained worktrees. It includes patches and logs that should remain local until reviewed.

The audit script and complete file comparison are at:

- `C:\Users\Pinda\Proyectos\BellLabs\.workspace-setup\mission-control-sprint-audit.py`
- `C:\Users\Pinda\Proyectos\BellLabs\.workspace-setup\mission-control-sprint-audit.json`
- `C:\Users\Pinda\Proyectos\BellLabs\.workspace-setup\mc-recovery-worktrees.txt`

Comparison normalizes CRLF to LF and compares the child unstaged/untracked files against main. A difference does not automatically mean the child is newer: main also contains subsequent integrator fixes. The comparison excludes scratch/tool/environment content.

## 2. What happened during the sprint

The retained team ledger records Cursor integrating the early waves, then an owner-directed continuation in Claude Code. Claude Code added wiring, Human Gates, database release 0032, coordinator subscription work and documentation, while isolated agents worked on the remaining lanes and policies.

The final ledger explicitly records a **Claude Code usage-limit cutoff around 12:20 on Oct 9**. MP-07 and MP-11 returned afterward; MP-09 and MP-12 also now have final handoffs in their worktrees. MP-08 has implementation and test logs, but **no final MP-08.md handoff was found**. Its recorded real-Temporal run failed all three cases.

Later that afternoon the integrated owner state was checkpointed and pushed as `d7b09db`, followed by `efa55f9`. The earlier handoff's statement that nothing was committed or pushed is consequently stale. The five late worktrees were not included in that checkpoint.

Primary historical record: `.scratch/multi-provider-2026-10-08/team/LEDGER.md`; early handoffs are in `team/handoffs/`, late ones in each worktree's `.scratch-handoff/`.

The available records establish interruption and unfinished integration. They do not establish the operating-system/process crash's root cause or verify the reported $200 Cursor spend. Spending on the coding session is separate from Mission Control executing a qualified provider lane. The child handoffs describe fixture/local-service proof and report no paid provider calls for those tasks.

## 3. Implementation inventory

### Preserved in main

| Work | State at checkpoint |
| --- | --- |
| MP-01 / OVE-64 | Seven lane-profile contracts, versioned provider bindings, mission/v2 and migration 0031; new provider describe entries remain unqualified stubs |
| MP-02 / OVE-65 | Production manifest launch author and chain launch inputs; the Human Gate follow-up introduces the chain-consumer refusal described below |
| MP-03 / OVE-66 | Provider-specific capability projections and materialization contracts |
| MP-04 / OVE-67 | Workspace allocation, leases, snapshots and artifact custody foundation; remaining Cursor wiring is in MP-09 |
| MP-05 / OVE-68 | Auth-route admission and typed usage/capacity policy foundation; production composition is still incomplete |
| MP-06 / OVE-69 | Worker session ownership, fenced dispatch journal, crash reconciliation and capacity-wait mechanics, with integrator wiring |
| MP-10 / OVE-73 | Human Task service, HTTP/Socket.IO resolution, Temporal HumanGateWorkflow, manifest gate lowering and development MCP tools; technical MCP composition remains incomplete |
| MP-13 / OVE-76 | Normalized provider frames, transcript/lineage and subscription contracts |
| MP-14 / OVE-77 | Mission Socket.IO service, PostgreSQL replay/hints and optional Redis fanout, with bootstrap wiring |
| MP-15 / OVE-78 | Coordinator inbox/subscription/gateway code and bootstrap part A; proposed inbox tables are not yet a released migration; API/technical MCP part B is outstanding |
| MP-16/17 / OVE-79/80 | Hosted-product feasibility reports, recorded Oct 8 as Outcome 3; no new hosted Claude/Codex runtime |
| MP-22 / OVE-85 | Local profile/preflight examples, startup readiness checks, run-to-Temporal-cluster binding and output-schema registration |
| MP-23 / OVE-86 | Pass 1 documentation; its execution-state text now needs the corrections in this handoff |

Main's `deployment_composition.compose_lane_registry` accepts Deep Agents and Cursor local/cloud harnesses. It has no Claude or Codex harness inputs. `src/mission_control/adapters/claude/` and `adapters/codex/` are absent on main. Declaring seven profile names does not register seven executable lanes.

### Important work still outside main

All paths below are relative to `.scratch/multi-provider-2026-10-08/worktrees/`.

| Ticket / directory | Recoverable work | Compared paths absent / different on main | Fresh targeted unit result |
| --- | --- | --- | --- |
| MP-07 / OVE-70 | `adapters/claude/`: pinned Python SDK harness, stream/session ownership, transport, hooks, permissions, workspace custody and fixtures | 25 / 1 | **42 passed** |
| MP-08 / OVE-71 | `adapters/codex/`: app-server JSON-RPC transport, protocol schemas, launcher, harness, approvals, compaction and fixtures | 48 / 1 | **45 passed** |
| MP-09 / OVE-72 | Cursor reconciliation, rate-limit handling, stream-expiry recovery, workspace reattachment/custody, host gates and relocated shared Git backend | 17 / 8 | **36 passed, 1 failed** |
| MP-11 / OVE-74 | Approval correlation/broker, governed effect intent and gateway, PostgreSQL repositories, extended Human Task resolution and proposed DDL | 22 / 1 | **41 passed** |
| MP-12 / OVE-75 | Durable continuation phases, pressure policy, hydrator registry, mailbox holds, lane/workflow changes and replay tests | 15 / 7 | **39 passed**; saved-history replay **47 passed** |

These counts include tests, fixtures and documents. Across the five there are **127 absent paths and 18 different paths**, including **41 new Python source files totaling 14,024 lines** absent from main. The two copies of the same provider-binding forwarding edit occur in MP-07 and MP-08; they should be integrated once.

At the audit baseline, before these documentation edits, MP-15's 16 compared deliverable paths and MP-23's 24 were identical to main after newline normalization. Their remaining dirty status largely reflects their staged historical base. Earlier MP worktrees also contain older versions of files subsequently changed on main; preserve them until reconciliation, but do not overwrite main with their full snapshots.

## 4. Provider readiness

| Profile | Current reality |
| --- | --- |
| `deep_agents` | Existing implementation and existing qualified describe entry; this audit does not renew a live/account qualification |
| `cursor_local` / `cursor_cloud` | Existing harnesses are composed when credentials are bound; both remain `qualified=False`; MP-09 improvements are unintegrated |
| `claude_agent_sdk` | Main has an unqualified describe stub. MP-07 has the actual SDK implementation against `claude-agent-sdk==0.2.165` / bundled Claude Code 2.1.294, proven with fixtures and historical local-service tests; integration and live drill remain |
| `codex` | Main has an unqualified describe stub. MP-08 has the app-server implementation and generated protocol-v2 schemas pinned to `codex-cli 0.162.0`; unit proof passes, recorded real-Temporal proof fails, production wiring and live drill remain |
| `claude_cloud` / `codex_cloud` | Unqualified stubs and recorded Outcome 3 feasibility decisions. MP-18/19 hosted implementation and MP-21 hosted parity remain blocked in the packet |

The hosted decisions describe the sprint's Oct 8 evidence, not a new verification of today's vendor APIs. Running an SDK on our own remote worker does not satisfy the packet's provider-hosted requirement.

The declared worker-host profiles require Linux/WSL for the local Claude/Codex/Cursor subprocess lanes. The production Windows SelectorEventLoop constraint has not been removed. Offline fixture tests on Windows do not prove a Windows production worker can launch those providers. Keep fixtures, real local services, live-provider qualification and account availability as separate claims.

## 5. Concrete blockers and missing wiring

### A. Mission 2's chain consumer is refused by Human Gate lowering

The final ledger recorded 5 passed / 2 failed in its real-Temporal rerun:

- `tests/integration/temporal/test_mp22_local_profile_start.py::test_public_start_and_chain_run_from_the_example_bindings`
- `tests/integration/temporal/test_manifest_launch_production.py::test_a_chain_starts_over_http_and_its_consumer_starts_once_under_redelivery`

This audit reproduced a specific failure path without services: compile the same Deep Agents-transformed Mission 2, then call `ManifestLaunchInputAuthor._human_control` with each compiled definition. Research returns no review; ingestion raises:

```text
ManifestStartUnavailable: the Goal Loop acceptance requires a human review but the manifest names no reviewer (declare a human_gate task with reviewers)
```

Mission 2's ingestion criterion contains `acceptance: { human: approved }` but declares no human_gate reviewer. The new lowering checks that criterion and refuses the consumer. The existing lowering unit tests use Mission 1 and a monkeypatched Goal Loop review; they do not cover this example's reviewer gap.

Source: `application/authoring/manifest_launch_inputs.py::_human_control`, `application/programs/human_gates.py::goal_human_review`; example `docs/specs/fast-track-2026-10/missions/02-research-ingestion-cursor-cloud-chain.yml`. Diagnostic script/log: BellLabs `.workspace-setup/mc-human-control-reproducer.py` / `.log`.

Resolve the manifest/reviewer contract and update the fixture expectation deliberately. Do not bypass the required review to make the chain start. This establishes a current refusal; without rerunning the service tests, it does not establish that it is their only failure.

### B. Shared provider contracts and visibility are unfinished

- Main `_binding_for` in `application/execution/operations/operation_execution.py` does not forward `provider_binding`. Both MP-07 and MP-08 carry the required additive edit.
- `domain/execution/contracts.py::OperationWorkflowRequest.activity_task_queue` reads Deep Agents, Cursor or native placement, but not `provider_binding.task_queue`. Claude/Codex cannot be routed correctly through that property yet.
- `domain/programs/search_attributes.py` defines `MissionLane` / `MC_LANES` with only `deep_agents`, `cursor_local`, `cursor_cloud`. MP-08's recorded three-case integration run fails in `lane_for_runtime` with **`ValueError: undeclared lane profile: codex`**. Extend the shared visibility contract and tests alongside provider wiring; also cover Claude.
- Replace describe stubs with the child matrices while retaining honest unqualified status; add provider frame kinds and `LaneFrame.subordinate_ref` where required by the handoffs.
- Compose auth admission, the selected capacity policy, actual harnesses, settings and worker queues. `bootstrap/provider_auth.py` has helper definitions, but main has no production caller for `compose_auth_admission` / `capacity_policy`.

MP-08's qualification README describes the intended integration proof, but its retained integration log is failing and it has no final handoff. Recover its missing wiring/contract notes from code and tests before treating it as delivered.

### C. Approvals, continuation and coordinator inbox need composition

- MP-07 defaults to `DenyWithoutGateway`; MP-08 defaults to `DenyingDecider`. Adapt their native permission ports to MP-11's durable broker with generation/policy/request correlation. Preserve the documented Claude SDK elicitation limitation.
- MP-11 needs the proposed wiring diff, stop-path cancellation, admission coverage checks and released SQL for `approval_correlation` / `governed_effect_intent`.
- MP-12 needs served continuation activities, hydrators, durable hold composition, binding-carried policy and the proposed contract deltas. Its handoff also identifies unresolved Deep Agents boundary handling and a `human_review` transfer resolution path. Replay passing does not complete this wiring.
- MP-15 needs migration 0033 for `coordinator_inbox`, `coordinator_notification`, `coordinator_causation`, then API lifecycle/mailbox composition and technical MCP mount wiring. The proposed inbox SQL already exists as a string in main, not as an applied migration.
- Some MP-12 proposed SQL still calls its slot `0032+`; that is historical. **0032 is already allocated.** Its optional typed phase/activation constraints need an explicit new-slot decision; do not overwrite 0032 or assume they are already in 0033.

### D. Cursor recovery test needs reconciliation with ownership rules

Fresh MP-09 units fail `test_cloud_workspace_and_resume.py::test_a_fresh_worker_reattaches_rehydrates_resumes_the_cursor_and_ends_the_session` with `SessionOwnedElsewhere`: the new service has a different worker owner while the previous owner's lease is still live. This is consistent with the ownership fence's refusal rule. Investigate the fixture's lease-expiry/new-owner setup and intended takeover semantics before changing runtime fencing. It is an observed test failure, not evidence that bypassing ownership is safe.

## 6. Database release and environment state

The common component manifest includes migrations **0031** (`multi_provider_lanes`) and **0032** (`run_cluster_binding_stream_hints`), component version **1.1.0**, schema fingerprint:

`sha256:672549cd78eda02e30f5f153ffdbf6882ec9643af0864ebf94232a1a041a84d6`

Both `deployments/biotech/release.lock.json` and `deployments/ai-engineer/release.lock.json` still omit 0031/0032. The ledger deliberately retained old locks and records a disposable release build; that does not establish a deployment install. No 0033 migration is released in main. Before preparing a new release, inspect actual installed receipts and preserve immutable applied migration/seed bytes. Do not infer installation from the shared version number 1.1.0.

The existing virtual environments already support the current offline checks. Tests used `uv run --no-sync`, preserving optional dependency groups. Editable environment metadata still uses the compatibility alias in places; this is why old paths can appear in tracebacks even when the shell is in BellLabs. Package/path cleanup remains a separate unfinished task.

Docker Desktop's Linux engine pipe was unavailable during this audit. No containers were started and no DB/Temporal services were changed. Historical disposable proof used PostgreSQL 17 at **55433**, Temporal **7233**, Redis **16379**. The owner application PostgreSQL port **55432** is not a replacement test target.

## 7. Validation performed during this audit

Logs below are under `C:\Users\Pinda\Proyectos\BellLabs\.workspace-setup\`.

| Check | Fresh result / log |
| --- | --- |
| Remote main identity | Exact match to efa55f9 |
| `make ci`: lock, ruff lint/format, mypy, deptry | Passed; mypy checked 549 source files; `mc-recovery-ci.log` |
| Architecture suite in CI | 10 passed |
| Full main unit suite in CI | **2,324 passed, 12 failed, 19 skipped, 2 xfailed** in 318.76s; CI failed at unit target, so its link target was not reached |
| Retained MP-07 / MP-08 units | 42 / 45 passed; `mc-recovery-MP-07-unit.log`, `mc-recovery-MP-08-unit.log` |
| Retained MP-09 units | 36 passed / 1 failed; `mc-recovery-MP-09-unit.log` |
| Retained MP-11 / MP-12 units | 41 / 39 passed; corresponding `mc-recovery-MP-11-unit.log`, `mc-recovery-MP-12-unit.log` |
| MP-12 saved-history replay, four suites | 47 passed; `mc-recovery-MP-12-replay.log` |
| Human-control refusal diagnostic | Reproduced offline; `mc-human-control-reproducer.log` |
| Documentation index / Markdown links | Index regenerated; 213 files checked, 0 broken links; `mc-recovery-doc-index.log`, `mc-recovery-links.log` |
| Repository skill manifests | `make skills-check` passed; `mc-recovery-skills.log`; this is distinct from the failing workspace skill pins/seeds above |
| Real PostgreSQL/Temporal, DB-contract integration | Not rerun: Docker engine unavailable; child/ledger results are historical evidence only |
| Paid/live provider drills, hosted APIs, account capability | Not run |

The 12 main unit failures group into:

1. **Four seed/source consistency failures:** three in `test_agent_skill_seeds.py`, one in `test_catalog_seed_bundles.py`; agent-browser pins/source bundles and generated catalog content disagree. Review source/pins and publish a new seed version when needed; do not blindly rewrite immutable applied seeds.
2. **Two workspace locator failures:** `test_workspace_artifacts_verify_against_their_pins_when_present` and `test_workspace_locators_cannot_escape_the_workspace`. The move places `.agents` / `.tools` behind links resolving outside `platform`, and `capability_pins.workspace_path` rejects that as an escape. Fix the explicit workspace/resource-root contract while retaining traversal protection.
3. **Six Biotech schema failures:** source/tests still expect `platform/biotech-kg/typedefs.graphql`, which does not exist. Reconcile the canonical Biotech schema source and path resolution; installing another Python package alone will not supply that file.

Some similarly named failures appear in earlier sprint logs, but this audit does not classify every failure as pre-existing. The complete current gate is red. The historical child logs' full-suite pass counts do not replace these fresh results, and one MP-08 log footer says `unit exit: 0` despite a pytest summary with 25 failures: use the pytest summary and actual process result, not that footer.

## 8. Ordered continuation instructions

1. Read this handoff, the final ledger entries, the five child handoffs/code and `VALIDATION.md`. Keep all dirty worktrees and the recovery snapshot. Work from the canonical checkout. Preserve the staged base separately from each child's actual delta; do not reset, clean or blindly merge a child snapshot.
2. Resolve the Mission 2 reviewer/acceptance contract. Once disposable services are available, rerun the two named chain tests plus `test_manifest_submit.py` with full failure output. The offline reproducer gives a concrete starting point.
3. Reconcile the shared provider forwarding, queue and visibility contracts. Recover MP-08's missing final handoff. Review/port MP-07/08/09/11/12 incrementally onto the current checkpoint in an isolated recovery branch/worktree, carrying current main fixes forward. A merge/cherry-pick of their old HEADs is insufficient.
4. Review all proposed integration deltas together: describe matrices, registry/composition, auth/capacity, queues, native approvals, frame identities, continuation policies/hydrators and control paths. Resolve MP-09's lease test. Keep provider profiles unqualified.
5. Allocate one reviewed 0033 proposal for MP-11 + MP-15's required tables. Decide separately whether MP-12's optional typed activation constraints belong in that release or a later one. Build/regenerate the component against disposable PostgreSQL 17; verify release identity/locks and immutable-history compatibility before changing any deployment lock. Finish MP-15 part B and the technical Human Task/MCP wiring.
6. Repair the relocation-related resource-root/schema paths and seed/pin drift. Run focused checks, then `make ci`, `make skills-check`, the DB-contract suite, and the relevant real PostgreSQL/Temporal suites. Rerun saved histories against the final composed workflow code. Record pass/fail/skip counts and exact commits.
7. Complete MP-20 / OVE-83 local parity and MP-23 pass 2 documentation against the final implementation. Hosted MP-18/19/21 require a fresh feasibility decision; do not relabel worker-hosted SDK execution as provider-hosted support.
8. Request a finite budget/account authorization only when ready for the concrete live qualification drills. Check that the documented runner actually exists: MP-07 explicitly says its live runner is a next-ticket deliverable. Only recorded provider evidence can justify qualification; fixture tests cannot.
9. Reconcile Linear with reviewed implementation evidence. Read-only issue access failed during this audit because the connector requires reauthentication. The retained initial tracker snapshot says 23 Backlog children / 58 blocking relations under OVE-63, but that is historical, not today's tracker state. No Linear issue was updated here.
10. After reviewed integration and recovery evidence are checkpointed/backed up, retire proven-redundant worktrees using their proper Git/app lifecycle and finish alias cleanup. Keep the five unintegrated worktrees until their deliverables and unresolved proposals are accounted for.

The old `C:\Users\Pinda\AppData\Local\Temp\mc-integrate.py` still exists. Its historical base-match assumption predates d7b09db, and the old handoff records a rename parsing defect. Inspect it before any use; do not run it wholesale against today's main. Prefer a reviewed three-way reconciliation using the preserved base/index/child patches.

## 9. Definition of finished

The sprint is complete only when the intended local providers are composed through the production launch path; Human Gates, approvals, continuation, recovery and chains pass the agreed persistence/replay/parity checks; database artifacts and installed releases are explicitly reconciled; and per-profile live/account qualification is honestly recorded. Hosted support needs its own resolved product/API feasibility. A pushed partial checkpoint and passing adapter units are useful recovery milestones, but do not meet that completion bar.
