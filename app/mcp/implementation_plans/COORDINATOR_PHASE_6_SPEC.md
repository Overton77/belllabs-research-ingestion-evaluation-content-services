# Coordinator MCP Phase 6 specification

Date: 2026-07-27  
Status: Implementation plan  
Depends on: accepted Phase 5 handoff

## Objective

Build a reviewed, versioned coordinator sandbox package that gives an agent an efficient local
reasoning and authoring environment while fetching fresh authority through MCP. A fresh session
must be able to construct and validate a workflow or composition proposal, launch through the
governed path, and inspect available results without making the sandbox an authority.

## Current implementation evidence

A partial coordinator skill already exists at
`.agents/skills/belllabs-workflow-coordinator/`, including `SKILL.md`, advisory draft/proposal
schemas, two local validators, examples, and protocol/authority references. Phase 6 must review
and evolve these assets into the versioned base snapshot contract. It must not create a competing
skill or describe the existing advisory schemas as Phase 3 authority.

Snapshot and workspace lifecycle building blocks also exist in
`app/application/sandbox_snapshots.py` and `app/application/workspace_materialization.py`.
Coordinator-specific bundle preparation, base-snapshot packaging, stale-resource handling, and
session-level acceptance are still required.

## Base snapshot contract

The reviewed base snapshot contains:

- the coordinator Agent Skill;
- local JSON Schemas for draft file shapes;
- deterministic helper executables;
- workspace layout and read order;
- examples using exact MCP lifecycle operations;
- manifest and snapshot version metadata.

It must contain:

- no credentials or secret values;
- no tenant-specific data;
- no mutable catalog copy;
- no hard-coded current aliases or environment availability;
- no policy decisions that belong to application services;
- no previously retrieved Viome research.

Each session receives a fresh clone. Authentication, exact definitions, policy, environment
readiness, and authorized result data arrive at runtime.

## Coordinator workspace layout

```text
coordinator/
  README.md
  task/
    objective.md
    constraints.json
  catalog/
    workflow-types/
    implementations/
    contracts/
    assets/
    snapshots/
  drafts/
    workflow-design.json
    composition-plan.json
  validation/
  results/
  bin/
  skills/
    belllabs-workflow-coordinator/
```

Every materialized authoritative file records:

- exact source URI;
- logical identity and revision;
- source digest;
- retrieval time;
- tenant/request scope where relevant;
- bundle manifest identity.

Local drafts and generated summaries are visibly labeled non-authoritative.

## Agent Skill responsibilities

The skill teaches:

1. bootstrap and effective capability negotiation;
2. internal-first Workflow Type and asset discovery;
3. compact card → exact description → exact contract/schema progression;
4. Workflow Type, Implementation, Run, StageGraph, GoalDirected, and Composition distinctions;
5. governed existing-workflow launch versus novel draft/publication;
6. local draft creation and revision;
7. when to request a content-addressed bundle;
8. local advisory validation;
9. authoritative server validation and repair;
10. preparation-ticket review before consequential launch;
11. status/result navigation and unavailable-resource handling;
12. explicit refusal to fabricate refs, digests, authority, reports, or artifact locations.

The skill must not contain current catalog decisions, credentials, or an embedded answer to the
Viome mission.

## Deterministic helpers

Provide small, inspectable executables to:

- validate local workflow drafts against local shape schemas;
- validate composition-plan shape;
- verify file and bundle digests;
- detect stale exact resources and changed manifests;
- estimate context/token footprint before loading a bundle;
- create concise draft diffs;
- render server findings beside JSON paths;
- package a draft for MCP validation.

Helpers do not perform launch, grant authority, resolve aliases, install external assets, or
declare server acceptance. Local success is advisory.

## Content-addressed workspace bundles

Add `prepare_coordinator_workspace_bundle` as an application-owned use case. Input identifies
exact authorized resources and purpose; output is an immutable manifest suitable for the sandbox
adapter.

Rules:

- include only selected resources, not a catalog dump;
- resolve and authorize each resource server-side;
- preserve exact URI, revision, and digest;
- use content addressing and deterministic paths;
- enforce aggregate file/count/size limits;
- reject or mark resources unavailable according to Phase 5 retrieval state;
- never follow opaque artifact refs by guessing;
- never include secrets or credential-bearing runtime configuration;
- expire or mark stale bundles when exact authority or policy requires revalidation.

Materialization copies data into a session workspace; it does not alter authoritative records.

## Snapshot lifecycle and isolation

- Base snapshot publication is reviewed and versioned.
- Session restore always clones.
- Upgrade creates a new reviewed snapshot version.
- Concurrent sessions cannot share mutable directories.
- Tenant/session cleanup follows explicit retention.
- Network and filesystem permissions follow least privilege.
- Fresh runtime credentials are provider-managed and never persisted back to the snapshot.

Use existing snapshot compatibility and workspace materialization services rather than creating
parallel lifecycle authority.

## Application ownership

Primary areas:

- `.agents/skills/belllabs-workflow-coordinator/`
- `app/application/sandbox_snapshots.py`
- `app/application/workspace_materialization.py`
- `app/domain/control_plane/contracts.py`
- `app/application/coordinator_facade.py`
- `app/mcp/coordinator_server.py`
- `app/mcp/coordinator_resources.py`

Sandbox adapter/package files should live in the accepted existing package or deployment layout.
Do not make `sandbox-work/`, `app/experiments/`, or `app/personal_code/` product authority.

## Verification

### Snapshot security

- Base image scan finds no credentials, tokens, PHI, tenant records, or prior mission output.
- Every session receives a distinct writable clone.
- Read-only base content cannot be mutated by a session.
- Upgrade and rollback select explicit snapshot versions.
- Cleanup does not remove authoritative application records.

### Bundle integrity

- Every materialized authoritative file verifies against its manifest digest.
- Scope authorization is rechecked at bundle preparation.
- Missing/unavailable Phase 5 resources remain explicit and are not guessed.
- A stale resource is detected before preparation/launch.
- Local edits cannot change exact catalog files without digest failure.
- Large bundles are rejected or narrowed deterministically.

### Skill and helper behavior

- A clean agent follows internal-first discovery.
- The agent distinguishes proposal from authority.
- Local validation can pass while authoritative server validation rejects, and the skill handles
  that outcome.
- Helpers are deterministic and side-effect bounded.
- The skill never launches without an accepted prepared ticket and explicit consequential step.

### End-to-end session

From a clean clone:

1. bootstrap through MCP;
2. retrieve current Workflow Type/launch contract;
3. optionally prepare a narrowed bundle;
4. author and locally validate a draft/proposal;
5. submit authoritative validation;
6. prepare and launch through Phase 1 services;
7. retrieve Phase 5 status/result resources that are actually available.

## Exit criteria

Phase 6 is accepted when:

1. a reviewed base snapshot can create isolated, credential-free sessions;
2. the coordinator skill and helpers guide but never replace application authority;
3. bundle materialization is authorized, content-addressed, bounded, and stale-aware;
4. clean-clone and concurrent-session tests pass;
5. the end-to-end session reaches the real governed launch/result path;
6. unavailable Phase 5 report/artifact infrastructure remains truthful in the workspace.

## Incoming Phase 5 checks

Confirm:

- authoritative versus generated result types;
- resource URI/provider inventory;
- digest and revision semantics;
- artifact/evidence retrieval states;
- canonical S3 support by artifact class;
- official report availability;
- pagination and authorization behavior.

The sandbox may expose only the capability state recorded by the live MCP bootstrap, not a stale
handoff assertion.

## Outgoing handoff to full Viome acceptance

Include:

- base snapshot identity and manifest digest;
- coordinator skill version;
- helper executable versions/digests;
- workspace layout and permission model;
- bundle request/manifest schemas and limits;
- stale-resource behavior;
- clean-clone and isolation evidence;
- exact live MCP capabilities available to the test;
- explicit report/artifact features still unavailable.

The full Viome test must start from the reviewed base snapshot but with a fresh session and no
preloaded Viome evidence.

## Explicit non-goals

- Embedding mutable catalog authority in the snapshot.
- Auto-publishing novel Workflow Types.
- Storing credentials in helper configuration.
- Treating generated local summaries as official reports.
- Downloading snapshot bytes or arbitrary S3 objects through catalog search.
