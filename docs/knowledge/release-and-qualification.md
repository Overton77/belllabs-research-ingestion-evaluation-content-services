---
type: Concept
title: Release gates and proof status
description: The four proof statuses, the G0 to G7 release gates, the C0 to C4 compute profiles and ProofBudget, and what is live today against the still-not-done list.
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
  `docs/qualification/two-project/LIVE_PLAN.md`;
  `docs/qualification/two-project/comparison-20261003-live-r1.json`;
  `packages/mission-control-db-contract/runtime/AGENT_SERVER_TOPOLOGY.md`.
- Tests: [two-project release parity](../../tests/qualification/two_project/test_release_parity.py),
  [two-project fixtures](../../tests/qualification/two_project/conftest.py),
  [PostgreSQL runtime acceptance](../../tests/acceptance/mission_control/test_postgres_runtime_parity.py).
