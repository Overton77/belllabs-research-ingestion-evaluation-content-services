---
type: Concept
title: Release gates and proof status
description: The four proof statuses, the G0 to G7 release gates, the C0 to C4 compute profiles and ProofBudget, what is live today against the still-not-done list, the 2026-10-08 fast-track position - release 1.1.0 and the Cursor lane qualification, both proven only on disposable infrastructure, with no live mission run - and the 2026-10-10 multi-provider position per lane profile (no profile live-qualified, hosted parity blocked).
tags: [mission-control, release, qualification, gates, compute, evidence]
---

# Release gates and proof status

Release is gated by evidence and issue dependencies, not dates. This concept gives the
vocabulary the expansion packet uses for proofs, gates and compute, then states what the
implementation status records as live. What the local test layers prove is in
[qualification](qualification.md); installation mechanics are in
[persistence](persistence.md).

## Proof statuses

expansion/README.md fixes four statuses for any proof result: `existing_recorded` (a
recorded Biotech qualification), `source_supported` (pinned source shows the facility),
`proposed` (designed, unproved) and `qualified_new` (a new proof passed). Only
`qualified_new` can enable a newly generalized capability, and documentation tasks add
no runtime proof results. The implementation status document does not use these labels;
its evidence is reported as test selections, fingerprints and receipts, so mapping it to
a status is a reading, not a record.

## Release gates G0 to G7

From expansion/DEPLOYMENT-AND-RELEASE.md: G0 contract and schema baseline with preserved
source evidence; G1 foundational state, context, control, subordinate and runtime
topology proofs; G2 neutral deterministic kernel and schema plus authoring and catalog;
G3 Deep Agents and Agent Server parity for both applications; G4 Cursor and direct
providers plus the four workflow systems and revisions; G5 knowledge data-readiness and
reproducible research, PDF and coding scenarios; G6 public skill, CLI and MCP plus web,
mobile, generative UI and MCP Apps; G7 isolation, load, recovery and security with
independently approved production qualification. Gates are a partial order; the full
release requires all of them even when early gates ship behind availability flags.
Verification distinguishes offline fixtures, real isolated infrastructure, finite
authorized live provider proofs and production deployment. No repository document
records a gate verdict per letter; `docs/qualification/two-project/LIVE_PLAN.md` calls
the live installation a "G4 concrete live installation plan" in its own numbering.

## Compute profiles and ProofBudget

`C0` deterministic local checks with no provider budget; `C1` foundational proof coding
with one strongest reasoning model plus one independent reviewer, at most two overlapping
agents per issue; `C2` bounded implementation with one writer per file region; `C3`
dataset and evaluation work, offline replay first; `C4` live qualification with zero
permitted calls until a signed `ProofBudget@1` names actual prices, ceilings and targets.
`ProofBudget@1` records scope, environments, pinned versions, price snapshots, maximum
calls, tokens, sandbox seconds, search units, storage, egress and concurrency, reserved
micros, hard stops and expiry; unit rates are exact rational micros rounded up at
settlement; unknown billing keeps its reservation and is never zero
([budgets and usage](budgets-and-usage.md)). The packet quotes no dollar amounts.

## What is live today

Per `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md` (status 2026-10-03): release
`mission_control` 1.0.0 was qualified on two local disposable clusters (run
`20261003-g3-r2`, identical fingerprint and contract digests) and then installed,
owner-approved and Biotech first, into both Supabase projects
(`biotech-research-ingestion` for `biotech`, `supabase-blue-ocean` for `ai-engineer`),
each holding `mission_control`, `mission_control_search` and `mission_control_runtime`,
six NOLOGIN capability roles, release receipts and seeds. There is no application
traffic. The "still not done" list, each item needing its own approval: application
traffic and runtime login roles; tenant, actor, issuer and audience bindings for real
users; storage buckets and their policies; Agent Server native persistence topology and
licence (the pinned server cannot use a private schema of the business database, see
ADR-0017 and `packages/mission-control-db-contract/runtime/AGENT_SERVER_TOPOLOGY.md`);
a verified recovery point; paid-provider and semantic-search qualification. The
installer's own tests, the independent two-project suite and the real-stack acceptance
run are the recorded evidence; restore remains unproven.

## Fast-track position (2026-10-08)

The fast-track packet is implemented and merged to `main` at `f8d325a`. Its evidence is local:

- **Release 1.1.0** (migrations 0025-0030) is built, locked for both applications and proven on
  scratch databases (upgrade from 1.0.0 and fresh install, both with seeds). It is **not applied to
  either live Supabase project**. Preconditions are owner decisions: `pg_trgm` in schema
  `extensions` on both projects, the storage claim name for the bundle bucket policies
  (`mc_capability_role`), approval on OVE-23 for the bucket seed, and a security review of the
  family-writer INSERT widening ([persistence](persistence.md)).
- **Lane qualification.** `lane_profile.qualified` is `true` only for `deep_agents`. `cursor_local`
  and `cursor_cloud` stay `false`: nothing flips it except a reviewed release that cites a
  live-drill record under `docs/qualification/lanes/`, and no drill has run. The offline evidence
  (`make lane-qualify PROFILE=...`: fixture replay through the real adapter and reducer,
  describe-honesty tests, Temporal replay of the captured lane histories) never flips the flag.
  Admission refuses an unqualified profile unless `MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES=true`,
  a local-proof override ([cursor lane](cursor-lane.md)).
- **No live mission.** Tickets I1, I2 and I3 (the three owner missions) have not run. A live start
  is blocked by B1 to B7 in the owner runbook (no production launch input author, missing
  capabilities in the production seeds, a narrow worker pin file and a drifted `agent-browser`
  skill pin, unqualified Cursor lanes, a Windows event loop limit for `cursor_local`, and the
  subscription event-name gap). The only paid step that is ready is the Cursor lane drill.
- **Temporal.** `temporalio` is 1.34 with Worker Deployment versioning; the server in compose is
  1.31. Captured histories replay on the current worker.

The evidence by spec is in `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md` (fast-track section).

## Multi-provider position (2026-10-10)

The multi-provider packet (`docs/specs/multi-provider-2026-10/`) is integrated as an uncommitted
working tree on the local branch `mp/integration-recovery-2026-10-09`. The per-profile statement
follows VALIDATION.md and is the [release statement pass 2](../qualification/release/multi-provider-2026-10.md);
the summary table is in [qualification](qualification.md). In short: Deep Agents, Claude Agent SDK,
Codex, Cursor local and Cursor cloud are composed through the production launch path and proven
offline, on disposable PostgreSQL 17 and on local Temporal with FIXTURE provider clients (MP-20
Stage Graph, GoalDirected and Mission Chain parity). **No profile is live-qualified** by this packet:
every G4 drill (`make lane-qualify PROFILE=... LIVE=1`, finite budget, owner-run) is unrun, and no
account is enabled. `claude_cloud` and `codex_cloud` stay Outcome 3 after the 2026-10-09
revalidation, so MP-18, MP-19 and hosted parity (MP-21) are evidence-blocked and the packet must
not be called all-provider complete. Release 1.2.0 (0001-0033, fingerprint `sha256:0113df03...`) is
built on disposable clusters only; the locks still pin 1.1.0 and the live projects hold 1.0.0
([persistence](persistence.md)). B1 and B7 are closed in code; a real start still needs the
owner's bindings file.

## A self-contradiction in the status document

The header of the status document states the release is installed live in both
projects. Its living parity checklist still carries an older row, "Common production
schema ... not applied to any live project; see blockers below". The header, the
common-component section and `LIVE_PLAN.md` are current; the parity row predates the
2026-10-03 apply and was not updated. This concept reports the inconsistency and does
not edit the status document.

# Citations

- Spec: `../mission-control-general/general-mission-control/expansion/DEPLOYMENT-AND-RELEASE.md`
  (compute allocation model; integration and release gates; release manifest and
  preflight); `../mission-control-general/general-mission-control/expansion/README.md`
  (proof statuses).
- ADRs: [0009](../adr/0009-common-release-separate-app-authority.md),
  [0017](../adr/0017-agent-server-runtime-persistence-separate-database.md),
  [0003](../adr/0003-common-sql-owned-by-db-contract-package.md).
- Status and evidence: `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md`;
  [lane qualification](../qualification/lanes/README.md);
  [multi-provider release statement](../qualification/release/multi-provider-2026-10.md);
  [multi-provider VALIDATION](../specs/multi-provider-2026-10/VALIDATION.md);
  [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md);
  `docs/qualification/two-project/LIVE_PLAN.md`;
  `docs/qualification/two-project/comparison-20261003-live-r1.json`;
  `packages/mission-control-db-contract/runtime/AGENT_SERVER_TOPOLOGY.md`.
- Tests: [lane qualification fixtures](../../tests/unit/harness/test_lane_qualification_fixtures.py),
  [describe honesty](../../tests/unit/harness/test_describe_honesty.py),
  [lane replay histories](../../tests/integration/temporal/test_lane_replay_histories.py),
  [two-project release parity](../../tests/qualification/two_project/test_release_parity.py),
  [two-project fixtures](../../tests/qualification/two_project/conftest.py),
  [PostgreSQL runtime acceptance](../../tests/acceptance/mission_control/test_postgres_runtime_parity.py).
