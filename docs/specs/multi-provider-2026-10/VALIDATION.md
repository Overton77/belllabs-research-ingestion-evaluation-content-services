---
type: Specification
title: "Multi-provider acceptance and qualification gates"
description: "Offline, real persistence, Temporal replay and provider-hosted acceptance criteria, with truthful partial-release reporting."
tags: [mission-control, validation, qualification]
---

# Acceptance plan

This file specifies future implementation checks. It does not claim they ran during planning. The planning validation results belong in [DELIVERY.md](DELIVERY.md).

## Gates

| Gate | Required evidence |
| --- | --- |
| G0: packet/contract | Requirements mapped to issues; acyclic dependencies; schema authority agreed; unsupported features explicit; old v1 fixtures preserved |
| G1: deterministic behavior | Contract validation, inheritance/grant intersection, reducer/property tests, adapter recorded frames and schema fixtures, materialization digests, no paid calls |
| G2: real state | Disposable PostgreSQL 17 + pgvector, migration/grant checks, outbox retry/replay, concurrent approvals, cross-tenant denial, duplicate launches, receipt/fence races |
| G3: durable execution | Real local Temporal: worker loss, interrupted activities, Continue-As-New, command races, provider ambiguity, old and new history replay |
| G4: each local/hosted profile | Finite live drill with exact version/account/environment and redacted observed capability matrix |
| G5: application verticals | Stage Graph + GoalDirected + Mission Chain acceptance for each qualified profile; explicit blocked results for hosted gaps |
| G6: realtime/operations | Two-client reconnect/replay, multi-process fanout, expiring auth, slow consumer, subordinate visibility, callbacks and local-cluster recovery |

Use `make check` or the repository's documented underlying commands. Any changed common SQL needs package release and real DB proof; unit fakes cannot qualify persistence. Any workflow change needs saved-history replay. No unrun live test becomes a pass because a mock adapter supports it.

## Scenario catalog

| ID | Trigger | Required result | Owners |
| --- | --- | --- | --- |
| V01 | Compile each profile with required unsupported hook/approval | Pointed error before provider creation; no silent fallback | MP-01/03 |
| V02 | Compile/submit/start one manifest and retry requests | One immutable binding/run; start works through production input author | MP-02 |
| V03 | Two agents acquire same writable workspace | One lease wins; loser cannot write; snapshot includes untracked/binary files | MP-04 |
| V04 | Queue instructions while a turn runs | Ordered exact-once application at declared boundary; receipts distinguish acceptance/delivery | MP-06 |
| V05 | Steer races with native turn completion | No wrong-turn injection; explicit stale-target/requeue disposition | MP-06/08 |
| V06 | Cancel with active tool and concurrent approval | Fence blocks later governed effects; old effect reconciled; no false rollback claim | MP-06/11 |
| V07 | Process dies after native send but before local acknowledgement | Reconcile native identity or park in doubt; no duplicate paid turn | MP-06/07/08/09 |
| V08 | Observation disconnects after frame persistence, before heartbeat | Resume from durable cursor; no duplicate settlement/usage | MP-06/13 |
| V09 | Human Gate approve/reject/request-changes + restart | Versioned resolution once; durable wait; feedback reaches bounded remediation | MP-10 |
| V10 | MCP client has no elicitation support | Gateway fallback only where enforceable; otherwise compile/admission rejects | MP-11 |
| V11 | Native approval callback expires or connection is replaced | Old native handle not reused; fresh correlation and grant/digest checks | MP-11 |
| V12 | Context pressure during one goal iteration | Native compaction/session transfer leaves iteration and budget counters intact | MP-12 |
| V13 | Crash at every seal/transfer phase | One activated target; mandatory context intact; old session fenced | MP-12 |
| V14 | Stage A accepted output feeds B on another provider | Digest-checked read-only artifact materialization; no leaked local path/auth | MP-20 |
| V15 | Two linked GoalDirected missions, replayed release | Consumer starts once after declared accepted evidence, with own budget/environment | MP-02/20 |
| V16 | Provider child starts/finishes with partial transcript access | Accurate lineage and visibility level; no fabricated child detail or double-counted usage | MP-13 |
| V17 | Browser disconnects across event commit and fanout failure | Durable replay without loss; duplicates deduped; scope unchanged | MP-14 |
| V18 | Expired token, forged app/room/cursor, slow client | Denial or bounded resync; no cross-tenant leak or unbounded queue | MP-14 |
| V19 | Callback retry, absent coordinator, notification-command loop | Durable inbox/dead-letter; bounded noise; no duplicate command effect | MP-15 |
| V20 | Claude/Codex hosted task full lifecycle | Actual hosted product, documented controls, environment proof; missing features remain blockers | MP-16–19/21 |
| V21 | Hosted stream expired/session failed while local caller restarts | Status reconciled separately from transport; authoritative receipts retained | MP-09/18/19 |
| V22 | Continue-As-New with queued command/human wait | Same mission frontier, new Temporal execution; handlers drained and no command lost | MP-12/22 |
| V23 | Cloud Temporal unavailable | New-run local routing only under explicit binding; active run never silently restarted elsewhere | MP-22 |
| V24 | Auth route/quota exhausted | Typed auth/capacity wait or rejection; no unnoticed switch from subscription to API billing | MP-05 |

## Bounded real fixtures

Start with deterministic no-model fixtures for the production launch/chain/Human Gate path. Then use a disposable repository with one small file edit, test command, governed no-op MCP effect, one approval, one follow-up, cancellation and restart. Qualify Stage Graph, one goal with at least two iterations, and a two-member chain. Parameterize the common suite by profile; use additional provider-specific tests for native semantics.

Each hosted fixture must record provider task/environment identity, exact repo commit, setup/config attestations, cancellation evidence and collected artifact refs. Never substitute a locally hosted SDK fixture. Hosted follow-up unsupported: fail that requirement visibly, not skip it while declaring parity.

Provider drill budgets must be finite and attributed; use the applicable repository budget policy and current owner authorization. A metered investigation may stop at the documented missing capability without starting a task. Full hosted missions still need their explicit cap. Record paid, estimated and unknown usage; a subscription-backed turn still consumes a resource budget.

Do not introduce a test helper that supplies a fake production `LaunchInputPort` or fixture-only catalog rows and then call the result production-ready. Offline fixture evidence and real deployment-profile readiness are different attestations. Exact model/auth/setup bindings remain external inputs until supplied through the registry.

## Local Temporal fallback

Use the same workflow code and contracts for local/self-hosted Temporal and Temporal Cloud. Local agents remain local regardless of scheduler placement. Pin each run to its Temporal cluster/namespace/task-queue binding.

A cloud outage does not authorize transparent failover of active executions to a separate local cluster. Pause new dispatch, preserve effects/native handles and recover the original cluster or perform an explicit reconciled successor-run procedure. New missions may target the local binding when admitted. Document persistence/backups and namespace setup; the single-process dev server is development infrastructure, not a production HA claim.

The known Windows Selector/Proactor conflict for the Cursor bridge requires an explicit WSL/Linux local worker qualification path. Do not change the API event-loop policy globally to make one lane work. Test per-profile subprocess handling and make unsupported OS combinations visible in preflight.

## Release statement

Report per profile: implemented, offline-tested, DB/Temporal-tested, live-qualified, account/environment-qualified, blocked requirements. A local baseline can ship while hosted work remains open, but it must not be called “all providers complete.” Parallel Swarm and Evaluator Optimizer remain deferred. Existing OVE-55 and OVE-59–61 are completed only when their own evidence gates are satisfied.
