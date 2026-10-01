# Special handoff: real company research missions and runtime lifecycle completion

Date: 2026-10-01 (America/New_York)
Repository: `biotech-research-ingestion-evaluation-system`
Status: user-directed mission and implementation handoff; accepted specifications remain authoritative
Session boundary: prerequisite implementation first; company fixtures only in a separate user-started session
Target integration branch: `integration/research-runtime-mission`
Target delivery branch: `main`

## 1. User goal and finish line

Complete a useful, bounded research mission through each accepted workflow family:

1. **StageGraph / Qualia Life:** investigate the company and one representative supplement offering; produce a cited company/product evidence brief.
2. **GoalDirected / GenerationLab:** investigate the company and one representative diagnostic/testing offering; produce a cited company/test evidence brief, with independent verification and a bounded repair loop.

Use real search and browser-control Agent Skills, exact capability bindings, sandboxes, and persisted report artifacts. Run through the governed BellLabs API, real application persistence, Temporal root/family/operation hierarchy, and production Deep Agent adapter. Inspect lifecycle while work is active and after settlement. Inspect an earlier cognitive checkpoint, and create a governed semantic fork that produces a revised report while preserving the original lineage.

Demonstrate a running-workflow intervention at a declared semantic boundary. Mid-invocation Deep Agent steering is conditional on the additional contracts and qualification described below; it must not be silently represented as complete by accepting a message at the root.

This is one delivery mission within the full system plan. Finish CP-050 and explicit lifecycle follow-on work; do not replace the architecture, rebuild the accepted family interpreters, or extract a generic framework during this mission. Keep reusable runtime behavior separate from company objectives and integrations so aiengineer can later consume it.

**User sequencing requirement (2026-10-01): the agent/session completing prerequisite issues must not create or run the company fixtures afterward.** Complete prerequisite code, technical qualifications, review, integration merge and readiness handoff, then stop. The user will initiate a separate fixture session. Do not auto-start it through an automation, another agent, task message, or continuation. Technical tests/provider qualification for prerequisite acceptance are allowed; the Qualia Life and GenerationLab research/report/fork missions remain held until that separate session.

## 2. Required reading and current baseline

Read in this order:

1. Repository `AGENTS.md` and applicable `.cursor/rules/`.
2. Canonical [ADR-0003](../../biotech-meta/docs/adr/0003-temporal-deepagents-control-plane-runtime.md), [control-plane specs](../../biotech-meta/docs/specs/control-plane-foundations/README.md), and [workflow-blueprint specs](../../biotech-meta/docs/specs/workflow-blueprints/README.md).
3. [V2 package index](migrations_instructions/implementation_work_packages_v2/README.md), [implementation readiness](migrations_instructions/implementation_work_packages_v2/IMPLEMENTATION_READINESS.md), and [CP-050](migrations_instructions/implementation_work_packages_v2/WP-CP-050-foundation-capability-materialization-vertical.md).
4. Accepted [StageGraph evidence](migrations_instructions/evidence_v2/WP-BP-010/README.md), [GoalDirected evidence](migrations_instructions/evidence_v2/WP-BP-020/README.md), and CP-020/030/040/045 evidence in the same directory.
5. [Runtime lifecycle implementation brief](RUNTIME_LIFECYCLE_INSPECTION_AND_CONTROL_IMPLEMENTATION_BRIEF.md), especially sections 11–15 and 20–28.
6. Current code and tests; use historical plans and old handoffs as provenance only.

At preparation time, `main` HEAD was `ca66c09` (2026-08-11). CP-001/010/020/030/040/045 and BP-010/020 are recorded accepted. CP-050 is ready, has unchecked acceptance criteria, and has no evidence directory. Prior package results are historical qualification, not a fresh green baseline.

Both family runtimes have recorded real-model API-to-Temporal qualification. StageGraph proves early downstream release, cycles, liabilities, replay, and worker loss. GoalDirected proves isolated executor/verifier sessions and workspaces, token rollover, typed handoff, persistent sandbox artifacts, and convergence.

The registered runtime hierarchy is:

```text
BellLabsRunWorkflow
  -> StageGraphWorkflow | GoalDirectedWorkflow
    -> OperationWorkflow
      -> operation.execute
        -> OperationExecutionService -> DeepAgentRuntimeAdapter
```

Application services/PostgreSQL own admission, lifecycle, budgets, effects, settlement, and terminality. Pure interpreters own semantic proposals. Temporal owns durable execution; LangGraph checkpoints and LangSmith traces are subordinate execution evidence.

## 3. Protect ongoing work before branching

The checkout was dirty. Pre-existing changes included:

- Modified `app/api/control_plane.py`, `app/application/control_plane/service.py`, and `app/application/runtime/postgres_runtime_execution_repository.py`.
- Untracked agentic-component packages under `app/application/`, `app/domain/`, `app/integrations/`, plus `scripts/query_agentic_components.py` and `tests/unit/agentic_components/`.
- Untracked harness/walkthrough documents and the runtime lifecycle brief/source note.

Inventory again at kickoff. Never discard, broadly stage, or silently absorb these changes. Some may be useful inputs, but their ownership and reviewed commit must be established before other branches depend on them. The lifecycle brief existed locally as an untracked file when this handoff was written: ensure a reviewed copy is reachable in the implementation base or available to every worktree before assigning work.

The mission may be completed by one agent or an agent team. If a team is used, the integrator owns shared contracts, migrations, registries, lifecycle authority, and the Deep Agent adapter; fixture owners do not independently edit these seams.

## 4. Scope and research fixture contracts

Two base runs are sufficient. Do not run a four-way company/family matrix by default. Use the real company names as discovery seeds; resolve official domains, company/product identity, and the current offering at execution time. Do not hard-code unverified URLs or assume every name denotes the same corporate or product entity.

### Shared bounds and report acceptance

- Public-source research only; no patient records, purchased tests/products, accounts, outreach, or submission of external forms.
- Initial limits per base run: 10 search calls, 8 browser page visits, at most 12 retained source documents, 25 minutes wall time, and USD 15 observed provider spend. Fork limit: USD 5 and 10 additional minutes. Freeze these in the admitted configuration; document provider accounting and any non-USD/token ceilings. A cap stops or degrades with an explicit unmet-obligation disposition.
- Operator-triggered wait time must have a bounded deadline and a declared timeout policy. Record it separately from active research time.
- Reports: approximately 1,000–1,500 words each, plus a structured claim/evidence table and source manifest. Markdown is the required report artifact; PDF is optional and not a completion dependency.
- Every material factual claim has a source URL, retrieval time, source classification, and evidence reference. Distinguish company statements from independently supported findings; preserve disagreement and uncertainty.
- Evaluate evidence and public claims, not personalized medical suitability. Missing evidence is a legitimate finding; never fill gaps with model knowledge.
- Retain canonical report, claim table, source manifest, verifier findings, and lineage manifest as promoted immutable artifacts. Use existing storage/artifact services and content digests, not a worker-local file as the sole durable output.
- No automatic canonical knowledge-graph ingestion in this mission. Produce ingestion-ready evidence references; approved ingestion remains a separate governed effect and delivery scope.

### Fixture A: StageGraph / Qualia Life

Objective: answer what the company offers, what one representative supplement contains or claims, what evidence supports its central claims, and what remains unverified.

Suggested stages:

1. Resolve company and offering; freeze the selected product and source scope.
2. Parallel branches: official product/ingredient/claim collection; independent evidence check for at most three central claims; small company/transparency profile.
3. Draft a provisional synthesis after an authored `minimum(2)` join. Classify the remaining branch under an explicit late-result policy and preserve its producer liability. A controlled result gate may prove release ordering without relying on model latency; it must not fabricate research output.
4. Final evidence reconciliation includes accepted remaining findings or an explicit allowed degradation. Verify citations and produce the final report only from the admitted evidence frontier.

The early draft is provisional. It cannot hide missing required evidence or become the accepted report before liabilities and mandatory obligations settle.

### Fixture B: GoalDirected / GenerationLab

Objective: explain one representative test's stated purpose, sample/measurement method, reported outputs, validation evidence, and limitations using public sources.

- Limit investigation to one offering and at most three central performance/utility claims.
- Allow at most three executor iterations and two repair/revision decisions inside the frozen objective envelope.
- Independent verifier checks entity/offer identity, material claim citations, distinction between measurement validity and claimed utility, explicit limitations, and report completeness.
- The verifier has a separate binding, session, and writable workspace; accepted executor artifacts are provided through immutable references or governed read-only mounts.
- Exercise one fresh-session handoff where feasible through a declared token/session threshold. Do not manufacture extra research merely to force rollover; deterministic qualification may prove a threshold case separately.
- Report verified completion, or a governed partial/failure with the exact remaining obligations. Never lower the rubric until a run passes.

## 5. Skills, MCP, CLI, and sandbox configuration

Audit existing catalog/materialization and agentic-component work first. Reuse accepted contracts and adapters; CP-050 permits immutable digest-verified capability fixtures as temporary catalog inputs. Full governed capability catalogs are outside CP-050.

For each fixture, freeze exact search and browser-control Skill bundle revisions/digests, entry points, tool/MCP allowlists, credentials by reference, sandbox placement, network policy, and runtime versions. The runtime agent must actually execute the supplied skills; desktop-agent browsing outside the workflow is not acceptance evidence.

Skills alone do not supply executable tools or credentials. Prove the CLI/browser binary or MCP endpoint is available in the selected placement and that the admitted agent can use it. Research requires controlled outbound access: the existing network-disabled Docker qualification sandbox cannot simply be assumed to support search/browser research. Select a supported mediated browser/search MCP service or an explicitly qualified egress placement; do not remove sandbox isolation wholesale.

Keep report workspace access scoped. Trace skill disclosure, search/browser tool calls, visited URLs, capability binding digests, artifacts, and observed usage. Include at least one bounded synchronous subagent and one governed asynchronous subordinate task across the two fixtures, as CP-050 requires. Async spawning is feature-gated off by default; enable only in the qualification configuration and prove reservation/link-before-provider-start, cancellation, and parent result admission.

The aiengineer knowledge-services MCP/skills/CLI may be useful later. They are not a hard dependency until exact resources, compatibility, permissions, and deployment are available and qualified. Never invent a connector or silently substitute tools.

## 6. Inspection, time travel, and fork semantics

Deliver scoped inspection joining run lifecycle, family position, semantic operation/unit, Activity attempts, exact binding, thread/namespace/checkpoint lineage, artifacts, budgets/effects, and reconciliation status. Expose projection freshness and `in_doubt` honestly.

Three distinct demonstrations are required:

1. **Historical inspection:** select an earlier qualified LangGraph checkpoint and obtain a redacted state summary with ancestry and binding/schema checks.
2. **Replay:** replay captured Temporal history to demonstrate deterministic scheduling without re-running provider/tool effects. Report replay separately from cognitive execution.
3. **Semantic fork:** create a new independently admitted BellLabs run at epoch 1 from an immutable macro snapshot plus an exact cognitive checkpoint reference and a validated patch. Run the derived branch and produce a distinct report artifact with parent/source lineage.

For a bounded fork patch, change report emphasis inside the original authority ceiling: Qualia Life can focus more on ingredient-evidence gaps; GenerationLab can focus more on measurement versus claimed utility. If the patch exceeds the envelope or invalidates binding compatibility, reject it or compile a separately authorized derived configuration; never silently coerce an incompatible checkpoint.

Fork each family once. Prefer a declared settled semantic boundary for the first proof. Historical checkpoints deep within an agent must be inspectable, but successful arbitrary node-level executable forks must not be claimed without graph resume/patch compatibility and a validated macro reuse frontier. If only settled-boundary forks qualify, report that limitation and retain an explicit follow-up ticket.

Snapshot only at a safe semantic boundary or through a quiescence protocol classifying active operations and external effects. Preserve active parent-child ownership, copy no pending message queue implicitly, and reuse only settled compatible results. Temporal Reset is incident recovery, not this product fork.

The production adapter currently selects deterministic thread IDs but does not capture exact before/after checkpoint IDs, explicit namespaces, or a checkpoint-completion fence. Therefore build and qualify checkpoint fencing/reconciliation **before** exposing checkpoint mutation/fork endpoints:

```text
attempt observation + expected source checkpoint
  -> check settlement / existing terminal result
  -> reconcile ambiguity
  -> invoke or resume
  -> record exact checkpoint via CAS
  -> authoritative settlement
  -> outbox / inspection projection
```

Prove crashes after checkpoint commit but before observation/settlement do not append the user prompt or repeat provider effects. Multiple valid descendants or unclear ancestry create an incident; they never trigger speculative reinvocation.

Reuse/version the existing runtime binding, checkpoint observation, incident, lineage, and fork machinery after contract audit. Do not create a second lifecycle store or fork saga. Proposed `/v2/graph-runtime/units/...` routes in the lifecycle brief are design targets, not currently operational endpoints.

## 7. Running intervention: decision and gates

**Decision: include boundary-level intervention now; defer arbitrary edits inside an executing Deep Agent until checkpoint-safe steering is qualified.** Core specs already cover typed messages, cancellation, waits, pause, and resume. The missing issue is complete delivery/application wiring and recovery, not whether intervention belongs in the architecture.

Current code observations:

| Surface | Existing behavior | Delivery requirement |
|---|---|---|
| Run-control API | Governed commands and lifecycle authority | Prove command acceptance reaches the correct runtime target and settles through the ledger/outbox |
| Root `deliver_message` / `signal_message` | Ordered in-memory workflow receipts; accepted `cancel` marks a flag | Receipt alone does not prove family/agent application; root message path does not itself forward the message into active cognition |
| Root/family cancellation | Root `request_cancel` cancels family handle; families have cancellation hooks | Prove running activity/provider cancellation, heartbeat delivery, quiescence, effect/budget settlement, late-output handling; do not assume a signal stopped the provider |
| StageGraph | `satisfy_wait` and `resume_pause` signals; durable wait logic | Add/prove authorized application-command delivery and versioned receipt; raw direct Temporal signal is not the public governance path |
| GoalDirected | Cancellation signal; paused interpreter state raises non-retryable `goal_paused` | This is not a durable pause/resume workflow loop; inspect and implement the accepted pause/resume semantics under a dedicated ticket |
| OperationWorkflow | Cancel flag checked before activity execution | That flag alone is not an interruption protocol for an already running Deep Agent |

Primary live demonstration: hold the Qualia StageGraph at a declared review wait while the workflow remains running. Inspect the state; submit an authorized, idempotent wait-release/resume command through the application facade; prove receipt, application to the exact family boundary, and subsequent report completion. A review approval can be the initial intervention without changing agent state or objectives.

Separately qualify cancellation on a disposable bounded run so the two useful base reports can still finish. Exercise GoalDirected pause/resume after its transport and durable waiting behavior are proven; do not hide the present failure behavior behind a successful lifecycle command response.

Optional subsequent steering: admit an additional public-source reference or bounded clarification at the next operation/iteration boundary through a typed contract, context projection, and new exact binding where required. Never mutate a frozen binding, accepted evidence, objective, budget, or capability grant in place.

Mid-invocation steering requires immutable command identity/sequence, expected generation/checkpoint/version, durable inbox/claim/outbox, exact application at a safe cognitive checkpoint, a checkpoint-committed receipt, cancellation/retry reconciliation, and stale/superseded rejection. Accept, provider-deliver, and checkpoint-apply are separate states. It is deferred unless this complete vertical is accepted; record its pending gate in the final delivery.

## 8. Work sequence and local ticket plan

Concrete requirement-linked local tickets have now been created; see the [durable ticket index](migrations_instructions/implementation_work_packages_v2/RESEARCH_RUNTIME_MISSION_TICKETS.md) and [implementation audit](RESEARCH_RUNTIME_IMPLEMENTATION_AUDIT_2026-10-01.md). GitHub publication is not requested; these are local files only. The M labels below describe mission slices, not new accepted canonical WP identifiers; RRM ticket bodies/dependencies now govern execution of those slices.

| Slice | Work | Dependencies / acceptance |
|---|---|---|
| M0 | Current implementation audit and local ticket creation complete; tracked audit records fresh checks, diagnosed gaps and dirty ownership | Baseline repair and spec acceptance remain open under RRM-002 and RRM-001; provider/service deployment readiness not yet established |
| M1 | CP-050 production capability composition prerequisite: real Skills/MCP/tool availability, sandbox/egress, shared persistence, sync/async subagents | Accepted CP packages; qualify composition with small technical inputs before company missions; full CP-050 acceptance follows M6 |
| M2 | Runtime-unit identity, checkpoint fence, exact observations, crash reconciliation, compatibility gates | Accepted canonical spec amendments and local ticket; existing contract/persistence audit |
| M3 | Visibility/projections, scoped run/unit/history inspection and historical state reads | M2; no raw checkpoint-body public API |
| M4 | Safe macro snapshots, validated patches, fork admission/start and reuse frontier | M2 + M3; parent isolation and technical fork qualification for each family before company missions |
| M5 | Governed boundary intervention, cancellation settlement, GoalDirected durable pause/resume | Core command specs + wiring audit; coordinate shared adapter/contracts with M2 owner |
| M6 | Separate user-started session creates/runs company fixtures, reports and forks; live acceptance and CP-050 aggregate evidence | RRM-010 readiness accepted AND a new user-started fixture session; held for all prerequisite agents |

The lifecycle brief is a design brief pending canonical specification/ticket acceptance. M2–M5 must first identify which requirements are already accepted and publish/accept any necessary canonical amendments in `biotech-meta`, then ticket the implementation. This handoff does not override that authority order. Complete amendments as the first delivery phase; do not stop at identifying the missing specs.

### Prerequisites first: next issue and ticket locations

User sequencing clarification: **complete the prerequisite issues before creating/running the Qualia Life and GenerationLab fixtures.** Small technical integration/provider qualifications needed to prove an issue remain allowed; they are not the company missions. Full CP-050 acceptance requires M6, so distinguish its completed capability prerequisite from its later aggregate acceptance.

M0 audit/ticket publication is complete. The next code issue is **RRM-002: repair the verification baseline**; **RRM-001: canonical contract/spec reconciliation** can be authored independently. New checkpoint implementation begins with RRM-003 only after both are accepted. Follow the concrete ticket index rather than rerunning M0 or jumping straight into company fixtures.

Existing accepted packages and CP-050 live in `docs/migrations_instructions/implementation_work_packages_v2/`; their qualification evidence lives in `docs/migrations_instructions/evidence_v2/`. The lifecycle source brief lives at `docs/RUNTIME_LIFECYCLE_INSPECTION_AND_CONTROL_IMPLEMENTATION_BRIEF.md`. Canonical requirements live in `../biotech-meta/docs/specs/control-plane-foundations/` and `../biotech-meta/docs/specs/workflow-blueprints/`.

**RRM-001–011 are now actual local issue files.** Their committed source lives under `docs/migrations_instructions/implementation_work_packages_v2/research-runtime-mission/issues/`, with matching ignored mirrors under `.scratch/research-runtime-mission/issues/`. The durable index links complete bodies, blockers, branches, authority and evidence locations. Regenerate scratch mirrors from tracked bodies in a fresh worktree. No GitHub issues were published. RRM-011 is held for the later user-started fixture session.

M2 can be split into identity/contract audit and checkpoint fencing implementation if reviewable scope requires it. Do not start M3/M4 operational endpoint work before the fencing qualification passes. Each issue must finish implementation, tests, review, evidence, and integration merge before its dependent issue begins.

## 9. Branch, review, and merge workflow

Use a reviewed committed common base; work in branches and isolated worktrees where concurrent. Preserve existing uncommitted work. Suggested topology:

```text
main
  -> integration/research-runtime-mission
       <- wp/cp-050-research-capabilities
       <- wp/research-company-fixtures
       <- wp/runtime-checkpoint-fencing
       <- wp/runtime-inspection
       <- wp/runtime-snapshot-forks
       <- wp/runtime-boundary-intervention
```

Names are suggested for the next session, not claims that these branches already exist. Start dependent branches from the reviewed prerequisite integration commit; do not assign work against invisible dirty changes. Record base/head, worktree path, dirty inventory, shared-file ownership, and tested integration revision in each evidence record. Preserve the existing blueprint parallel protocol if either accepted BP package is actually reopened; otherwise this is a new mission integration sequence.

Shared contract/adapter/schema changes merge first; rebase or merge dependent fixture/inspection branches before qualification. The integrator resolves shared-file conflicts and reviews semantic ownership. Use PRs where remote review is configured; otherwise review local diffs and merge commits. Push/remote PR publication follows the user's existing repository authorization, not an assumption that local ticket creation publishes issues.

Merge each slice to the integration branch only after focused checks and review pass. Run the combined deterministic, real-service, real-provider, and captured-history gates on the actual integration revision. Merge integration into `main` only when mandatory mission gates are accepted. Do not mark incomplete CP-050/lifecycle work accepted merely to merge; retain explicit follow-on tickets for conditional deep steering or unsupported arbitrary checkpoint forks.

## 10. Verification and evidence requirements

Use current test paths; older evidence names predate the application/test reorganization. Run narrow owning suites while developing, and integration/full checks at merge gates. Repair failures introduced by the mission; classify unrelated baseline failures explicitly and resolve those that block the shared foundation or final replacement gate.

Required evidence:

- Real API admission and compiler/ERC records; application PostgreSQL/Mongo/object-storage use; production worker composition and exact capability/binding identity.
- Two company reports with claim/source tables, verifier findings, artifact digests, observed spend, and unmet obligations if any.
- StageGraph early-release ordering in Temporal history and complete producer-liability settlement.
- GoalDirected independent verification, bounded repair/convergence, and qualified fresh-session handoff.
- Sync/async child contract, reservation, result-admission, cancellation, and recovery observations.
- Scoped run/unit inspection while active and terminal; exact checkpoint history, redacted historical reads, and projection freshness.
- Failure injection before checkpoint, after intermediate/terminal checkpoint, before observation, and before settlement; prompt/provider invocation counts and ancestry assertions.
- Two semantic fork receipts/new run IDs/epoch-1 lineage, safe snapshot manifests, validated patch/reuse frontier, derived reports, and immutable parents.
- Boundary intervention command/receipt/application evidence; duplicate, stale-version/generation, unauthorized, and replay cases. Cancellation must settle without late outputs mutating the terminal run.
- Captured Temporal histories and replay; worker loss and forced Continue-As-New where required by CP-050; no uncontrolled repeat of consequential effects.
- Ruff, mypy, applicable pytest, real-service/live opt-in gates, `git diff --check`, and replacement drift checks with sanitized outputs and exact revisions. A skipped live gate does not support acceptance.

Put CP-050 evidence in its assigned `docs/migrations_instructions/evidence_v2/WP-CP-050/` directory only after executable evidence exists. Assign lifecycle evidence locations in the accepted follow-on tickets. Separate report artifacts from engineering qualification evidence. Do not retain secrets, full sensitive prompts, or unredacted connection strings.

## 11. Completion checklist and next-session instruction

- [ ] Reviewed spec/ticket coverage exists for every new lifecycle seam.
- [ ] Prerequisite capability composition and lifecycle gate accepted as `ready_for_separate_fixture_session`; agent stops and hands readiness to the user.
- [ ] Separate fixture session initiated by the user; only then complete company mission work and CP-050 aggregate acceptance.
- [ ] Qualia Life StageGraph and GenerationLab GoalDirected reports are useful, cited, bounded, and durable.
- [ ] Lifecycle inspection works during execution and after settlement.
- [ ] Earlier cognitive checkpoints are inspectable with exact lineage and compatibility checks.
- [ ] Captured Temporal histories replay without provider effects.
- [ ] Each family has one safely admitted semantic fork producing a derived report; limitations on deeper executable forks are documented.
- [ ] A governed boundary intervention affects a running workflow with application evidence; cancellation settles correctly.
- [ ] GoalDirected pause/resume is proven or explicitly reported incomplete; do not claim full intervention completion without it.
- [ ] Mid-invocation cognitive steering is proven or explicitly deferred with its prerequisite ticket.
- [ ] Integration gates and review pass, evidence identifies the tested revision, and branches merge into `main`.

Start with RRM-002 baseline repair and RRM-001 specification/contract acceptance, then implement prerequisite tickets in dependency order through technical qualification, review and integration merge. Deliver the readiness manifest and stop; do not produce company reports in that session. The separate user-started fixture session later delivers the reports, CP-050 tracer evidence and a concise matrix of proven behavior, limitations, commits and follow-on tickets. Use the [next-session prompt](RESEARCH_RUNTIME_NEXT_SESSION_PROMPT.md).
