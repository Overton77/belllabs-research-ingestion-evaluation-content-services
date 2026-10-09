---
type: Workflow Specification
title: "Mission Control — Mission Invocation, Portals, and Mission Graph"
description: "Scope: creating or attaching Missions from any caller, the Child Mission Invocation Program Node, Portals, Spawn Grants, and the relationships that form a Mission Graph"
tags: [mission-control, spec, workflow]
---
# Mission Control — Mission Invocation, Portals, and Mission Graph

Architecture: [General Mission Control](../general-mission-control/SPECIFICATION.md). Runtime/API binding: [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md). Historical milestone references do not override the [current delivery plan](../general-mission-control/IMPLEMENTATION.md).

**Status:** canonical behavior contract; earlier interview dates below are provenance
**Last updated:** 2026-10-02 (general runtime consolidation)
**Scope:** creating or attaching Missions from any caller, the Child Mission Invocation Program Node, Portals, Spawn Grants, and the relationships that form a Mission Graph

## 1. Purpose

A Mission is the unit of independent governance: its own Goals, authority, budget, lineage, owner, and completion policy. Anything that needs its own governance boundary is a Mission, reached through **Mission Invocation**. Anything that does not is a nested workflow system inside the same Mission. This document fixes how Missions are created, attached, awaited, observed, and related — from the dashboard, API, CLI, MCP, an authorized agent, or a Program Node.

## 2. Settled shared laws (inherited)

- Mission Invocation is caller-neutral. Spawn creates a new Mission identity; Attachment links an existing one.
- Adoption, dependency linkage, and use of outputs never silently change ownership or provenance.
- A nested workflow system pursues existing Objectives; independently governed Goals require another Mission.
- Child Mission success supplies evidence; it does not automatically establish parent acceptance.
- No child-to-parent token stream. Children write Artifacts and status; parents peek. Commands are the only writes.
- Mission Chaining connects Missions through typed bindings, immutable Artifact references, events, and gates.

## 3. Invocation modes (accepted Round 2)

| Mode | Sub-mode | Effect on identity, ownership, provenance |
|---|---|---|
| **Spawn** | — | New Mission identity. Owner and tenant inherit from the invoker's grant unless the grant says otherwise. `parent_of` relationship recorded. |
| **Attachment** | `adopt_as_child` | Existing Mission becomes a Child. Requires the existing Mission owner's `APPROVAL`. Records `adopted_from` lineage; prior parentage is never erased. |
| **Attachment** | `depend_on` | This Mission's release depends on the other's outcome. No ownership change. |
| **Attachment** | `use_outputs` | Typed binding to the other Mission's projected outputs. No ownership change. |

## 4. Child Mission Definition source (accepted Round 2)

```text
child_definition_source:
  inline         { definition_template, input_bindings[] }
| template       { mission_template_ref, parameters }
| agent_authored { within: spawn_grant_ref }

child_authoring_policy: inline_only | template_only | agent_within_grant
```

- The complete general system requires `inline` and `template`.
- `agent_authored` uses the canonical author/validate/commit/start operations through Agent Skill/MCP or an admitted in-graph capability. It is always validated and compiled like any Mission Definition and opens a `REVIEW` Human Task when policy requires.

## 5. Child Mission Invocation node (accepted Round 2)

```text
ChildMissionInvocation {
  mode: spawn | attach { sub_mode }
  child_definition_source | existing_mission_ref
  input_bindings[]                         // immutable Artifact references
  await_policy:
      await  { until: child_accepted | child_execution_complete | child_output { name } }
    | track
    | detach
  on_parent_cancel: cancel_child | detach_child | block
  spawn_grant_ref?                         // required for spawn and adopt
  portal: { access: peek | peek_and_command }
}
```

### 5.1 Await policy

| Policy | Node lifecycle | Node outcome |
|---|---|---|
| `await { until }` (default `child_accepted`) | `waiting` until the condition holds | mirrors the child (§5.3) |
| `track` | completes when the child **starts**; the Portal keeps projecting | `accepted` = invocation accepted; downstream work needing child results uses an Event Wait on `child_mission.accepted` |
| `detach` | completes immediately | `accepted`; lineage only, no Portal updates |

### 5.2 Cancellation cascade

`on_parent_cancel` defaults: `cancel_child` for spawned children, `detach_child` for attached Missions. `block` refuses to cancel the parent while the child is active and surfaces a blocker.

### 5.3 Outcome mirroring under `await`

| Child terminal outcome | Invocation node outcome |
|---|---|
| `accepted` | `accepted` |
| any other (`not_accepted`, `governor_exhausted`, `execution_failed`, …) | `not_accepted` with `reason: child_outcome(<value>)` |
| cancelled by this parent | `cancelled` |

## 6. Portal (accepted Round 2)

A **Portal** is the parent-side projection of a child Mission. It contains, and only contains:

- child identity, depth, and `parent_of` / `adopted_from` link;
- lifecycle, phase, and Terminal Outcome of the child's current Run;
- projected output Artifact references;
- open Human Tasks and other blockers;
- budget consumed by credit account.

Never transcripts, Artifact bodies, or raw native events. `peek` grants read; `peek_and_command` additionally allows Commands to the child through the Portal. This is the candidate specification's inter-agent communication law made concrete.

## 7. Spawn Grant (accepted Round 2)

```text
SpawnGrant {
  max_depth                 // soft default 3; platform hard cap 16
  max_children_per_node
  max_descendants
  budget_share              // fraction or absolute per credit account
  allowed_operations[]      // which Mission operations the child may perform
  justification_required: boolean
}
```

Every denial (depth, count, budget, operation) is a recorded Mission Event with its reason. A Spawn Grant is part of the invoking Program Node's authority binding and is pinned by the committed Revision; expanding it is a Revision Proposal.

## 8. Mission Graph relationships (accepted Round 2)

**Mission Relationship** kinds — distinct from Stage Graph edges and from Program Node lineage:

| Kind | Meaning |
|---|---|
| `parent_of` | Created by Spawn. |
| `adopted_from` | Attachment `adopt_as_child`; preserves prior lineage. |
| `depends_on` | Release dependency between Missions. |
| `supplies` | Typed cross-Mission output → input binding. |
| `successor_of` | Revision-boundary lineage: a material Goal/authority/tenant change created a successor Mission (`07 §6.3`). |
| `forked_from` | Fork lineage (`07`). |

A **Mission Chain** is one ordered path over `depends_on` / `supplies` relationships.

## 9. Cross-Mission binding acceptance (accepted Round 2)

A `supplies` binding releases the consumer only after **Goal Acceptance** of the Goal that owns the supplied output (default), optionally `mission_accepted`. Provisional cross-Mission bindings are unsupported in the specified general release.

## 10. State vocabularies in this document

| Vocabulary | Values |
|---|---|
| Invocation mode | `spawn \| attach` |
| Attachment sub-mode | `adopt_as_child \| depend_on \| use_outputs` |
| Child definition source | `inline \| template \| agent_authored` |
| Await policy | `await \| track \| detach` |
| Await condition | `child_accepted \| child_execution_complete \| child_output` |
| Parent cancel policy | `cancel_child \| detach_child \| block` |
| Portal access | `peek \| peek_and_command` |
| Mission Relationship | `parent_of \| adopted_from \| depends_on \| supplies \| successor_of \| forked_from` |

## 11. Questions under interview

Resolved in Round 3: Mission and Run state machines are in `07 §10a`; Portal coalescing is in `09 §6` (`child_mission.portal_updated` only; child streams are opened separately within grant).

None open.
