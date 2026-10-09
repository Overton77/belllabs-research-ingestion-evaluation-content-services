---
type: Specification
title: Coordinator workflow for research content and ingestion
description: "This worked design illustrates the required authoring contract. Asset names are proposed seed catalog entries, not claims about currently installed endpoints. The implementation must materialize valid versioned fixtures…"
tags: [mission-control, spec, normative]
---
# Coordinator workflow for research content and ingestion

This worked design illustrates the required authoring contract. Asset names are proposed seed catalog entries, not claims about currently installed endpoints. The implementation must materialize valid versioned fixtures from this example and run them through the same compiler/API tests as other definitions.

## Human task and application context

In AI Engineer the human asks for an evidence-backed comparison of two agent toolchains and a proposal to update the knowledge base. In biotech the human asks for an evidence-backed comparison of two interventions and a proposal to update the knowledge graph. The coordinator clarifies the desired scope, limitations, deliverables, evidence and allowed writes with the human.

The same service creates a mission in the originating application scope. The goals and domain rubric differ. The program below does not. A real domain `ingestion.apply` capability is required to execute canonical writes; when absent the task can produce an ingestion proposal but must not advertise a completed ingestion.

| Component | AI Engineer binding | Biotech binding |
| --- | --- | --- |
| Mission goal | Accepted engineering comparison and admitted KB update | Accepted domain comparison and admitted graph update |
| Source guide | Engineering source and capability guide | Study and applicability guide |
| Schema workspace | App's engineering DB workspace digest/ref | App's graph/database workspace digest/ref |
| Assessment | Engineering comparison rubric/capability | Biotech evidence/applicability rubric/capability |
| Domain apply | App's governed KB ingestion endpoint | App's governed graph ingestion endpoint |
| Mission ledger | AI Engineer Supabase `mission_control` | Biotech Supabase `mission_control` |
| Execution stack | Temporal + Deep Agents/Agent Server first; Cursor Cloud in its later qualified lane | Same shared kernel and harness contracts |
| Entity store | App-owned PostgreSQL capability | App-owned Neo4j capability |

## The authored program

The root is a Stage Graph serving the comparison and ingestion objectives:

| Stage key | Behavior and exact inputs | Outputs and release/acceptance contract |
| --- | --- | --- |
| `discover` | Goal Loop with a committed source-search/read action space, source guide, topic input and finite governors | Registered source/evidence bundle; domain acquisition assessment and minimum coverage criterion must pass |
| `draft` | Deep Agent Executor; accepted discover bundle, domain context and report schema; exact skill/MCP/sandbox binding | Report artifact and claims/citations index; schema and citation custody requirements validate |
| `assess` | Registered deterministic dispatch to independent domain assessment service; draft artifacts and pinned rubric | Attributable assessment artifact/disposition; no self-approval by draft agent |
| `review` | Human Gate with report/assessment review packet, explicit assignee, deadline and on-timeout policy | Attributed resolution; rejection blocks downstream acceptance and apply |
| `plan_ingestion` | Registered domain read/plan capability; accepted report, admitted assessment, human resolution and app workspace/context refs | Typed ingestion plan/proposal with its own preconditions and evidence refs |
| `approve_write` | Human Gate if domain/app side-effect policy requires it | Approval bound to the exact plan digest; changed plan needs a new resolution |
| `apply_ingestion` | Registered domain apply capability with the approved plan and stable effect identity | Domain receipt and affected immutable refs; ambiguous response enters receipt recovery, never a new write |
| `verify_result` | Registered domain read/verification capability using receipt/affected refs | Post-write verification artifact/disposition; accepted result must match the plan/receipt |

Required data bindings imply producer acceptance and type compatibility. Human and Proof Gates are explicit program nodes. Input manifests preserve exact artifact refs/digests and permissions. A domain review-required disposition creates/holds its declared review path rather than being relabeled an execution error.

If a rejected assessment requires remediation, the coordinator either authors an Evaluator Optimizer block within the committed program or proposes a successor revision. The agent cannot improvise unlimited retry loops. Publication is a separate explicit stage/capability only when requested and authorized.

## Authoring and start calls

The coordinator uses this sequence through equivalent CLI/MCP actions:

```text
GET  /v1/applications/{app}/system
POST /v1/applications/{app}/context:select
POST /v1/applications/{app}/missions
POST /v1/applications/{app}/missions/{mission_id}:validate
POST /v1/applications/{app}/missions/{mission_id}/proposals
POST /v1/applications/{app}/missions/{mission_id}/proposals/{proposal_id}:validate
POST /v1/applications/{app}/missions/{mission_id}/proposals/{proposal_id}:resolve
POST /v1/applications/{app}/missions/{mission_id}/revisions/{revision_id}:activate
POST /v1/applications/{app}/missions/{mission_id}/runs
GET  /v1/applications/{app}/missions/{mission_id}/events?after_seq=0
```

Draft-create returns backend-generated mission/draft IDs. Proposal resolution respects the app's human-review policy; the coordinator cannot pretend to be its reviewer. Activation uses the expected scheduling head/version. Start includes `request_id`, committed `revision_id`, expected mission version, and immutable input-manifest ref/digest. Application and tenant authority come from verified request context, not from a definition-provided database location.

## What proves this is general

Run fixtures for both apps using the same core build and compiler. Catalog assets, rubric, topic, schema context and domain endpoint bindings differ; no core source change is made. Cross-app token/asset/artifact substitution must reject. Both dashboards see the same lifecycle and command meanings while displaying their own report/domain content.

If domain apply integration is unavailable, a separate research/content-only fixture ends after accepted report/review/proposal and proves that smaller mission's declared criteria. It cannot satisfy the larger ingestion goal. This distinction lets Mission Control become useful while domain services mature without claiming nonexistent capabilities.

## Subsequent Cursor SDK Cloud coding/feature mission

For either app, request a feature with a pinned repository/base commit, bounded repository/file scope, tests and review criteria. The root Stage Graph is `specify` (Deep Agents) -> `implement` (Cursor) -> `test` (deterministic sandbox test executor) -> `assess` (independent review) -> `human_review` -> `deliver`. Deliverables are immutable patch, base/head commit identities, test report, review report and instructions. The app-specific repository and test capability vary; the compiler/kernel do not. Commit/push/PR creation require explicit capability grants if part of the requested mission. Merge and deployment are separate stages with their own authorization, omitted from the default fixture.

Cursor executes in a branch/worktree or cloud workspace with exclusive writer ownership and pinned repository access. The adapter cannot reuse the operator's dirty worktree. A test failure is an assessment result; remediation requires an authored bounded optimization round or new revision, never an infrastructure retry. A provider's finished status cannot satisfy `tests_passed` or human approval.

Lifecycle fixture: observe active work; queue instruction; pause at a qualified boundary; inspect checkpoint; resume; simulate transient observation loss and reattach to the same native handle; fork from a sealed checkpoint into a new run with a fresh workspace/budget; cancel one branch and reconcile usage. Run diagnostic replay separately with effect claims disabled. The branch original's outputs, grants, active children and history stay unchanged. Cursor's native fork is not assumed: fresh-session hydration from registered artifacts is the proposed common semantic fork implementation. If continuity validation fails, return `CHECKPOINT_INVALID` rather than claiming a fork succeeded.

## Concrete public request fixture

After draft validation and revision activation, starting a run sends the following request under the authenticated application prefix. IDs and digests below are fixture placeholders; replace them with returned IDs and registered manifest digests, never send the placeholder strings to a live endpoint.

```json
{
  "schema_version": "mc.run_start.v1",
  "request_id": "<stable-client-generated-uuid>",
  "expected_version": 4,
  "revision_id": "<committed-revision-uuid>",
  "input_manifest_ref": "<registered-artifact-ref>",
  "input_digest": "sha256:<64-lowercase-hex>"
}
```

`missionctl --application biotech run start <mission_id> --request-file start.json --json` maps to `POST /v1/applications/biotech/missions/{mission_id}/runs`. The common skill uses that exact public command/API. A lost response is recovered with the same request ID and payload; generating a new ID can create a second semantic run. `GET /requests/{request_id}` recovers the receipt. The same fixture under `ai-engineer` resolves a different installation; application IDs are deployment keys, not entity-store choices.

## First-milestone async subordinate fixture

In each application's `discover` Goal Loop, admit a bounded Deep Agents child to inspect an assigned source subset. Pin the same common child policy but the application's own graph/server endpoint, artifacts and domain tools. Record required-blocking dependency, deadline, narrower grants and budget before Agent Server submission. Lose the submit response and restart the worker; recover the original native child, then validate its registered result. Repeat with parent cancellation and a late stale-generation result. Both installations must retain their own identities/receipts and never release downstream `draft` until the required accepted inputs are available. This is a required fixture, not a reported test result.
