---
type: Agent Configuration
title: "Issue tracker: Linear"
description: "Issues and specs for Mission Control live in Linear: workspace overtonbell, team OVE, project Mission Control (https://linear.app/overtonbell). Brainstorms start as local markdown and are promoted to Linear issues; the…"
tags: [mission-control, agents, process]
---
# Issue tracker: Linear

Issues and specs for Mission Control live in Linear: workspace `overtonbell`, team `OVE`, project **Mission Control** (`https://linear.app/overtonbell`). Brainstorms start as local markdown and are **promoted** to Linear issues; the Linear issue is the canonical record once it exists.

## Access

- **Claude Code**: the `linear` MCP server in `.mcp.json` (HTTP, `https://mcp.linear.app/mcp`). Authenticate once with `/mcp`.
- **Codex**: `[mcp_servers.linear]` in the workspace `../.codex/config.toml` (OAuth).
- **Cursor**: the `linear` entry in the user-level `~/.cursor/mcp.json` (`mcp-remote`).
- **Fallback for scripts**: the Linear GraphQL API (`https://api.linear.app/graphql`) with `LINEAR_API_KEY` from `.env`. Read the key from the file; never print it, paste it, or commit it.

Prefer the MCP tools when they are available in the session. The GraphQL operations below are the equivalents for scripts and for sessions without MCP.

## Conventions

- **Create an issue**: team `OVE`, project `Mission Control`, one category label (`Bug` or `Feature`, see `triage-labels.md`) and one state label. GraphQL: `issueCreate(input: {teamId, projectId, title, description, labelIds})`.
- **Read an issue**: by identifier (`OVE-123`). GraphQL: `issue(id: "OVE-123") { title description state { name } labels { nodes { name } } comments { nodes { body } } relations { nodes { type relatedIssue { identifier } } } }`.
- **List issues**: filter by project, state and labels. GraphQL: `issues(filter: {project: {name: {eq: "Mission Control"}}, labels: {name: {in: [...]}}})`.
- **Comment**: `commentCreate(input: {issueId, body})`.
- **Labels**: `issueUpdate(id, input: {labelIds})`; read the current set first and add or remove from it.
- **Close**: move to the team's `Done` state (`issueUpdate(id, input: {stateId})`), or `Canceled` for `wontfix`.
- **Sub-issues**: `parentId` on create, or `issueUpdate(id, input: {parentId})`.
- **Blocking**: Linear's native relation. `issueRelationCreate(input: {issueId: <blocker>, relatedIssueId: <blocked>, type: blocks})`. A ticket is unblocked when every blocker is `Done` or `Canceled`.

## Provenance

Every promoted issue carries a `## Provenance` section so the issue knows where its content came from. No separate database is used for this; the links are the record.

```markdown
## Provenance

- Conversation: <Claude Code / Codex / Cursor session id, or the handoff file path under .scratch/>
- Documents: <repo-relative paths with the commit they were read at, e.g. docs/adr/0007-....md @ abc1234>
- Decisions: <ADR numbers this issue depends on or changes>
- Terms: <GLOSSARY.md terms the issue relies on>
```

When a local draft is promoted, add `Linear: OVE-NNN` near the top of the local file and keep the file; the issue links back to that path. Mission Control's own `authoring_session` records can later cite the Linear identifier for missions that originate from an issue.

## Local drafts

Brainstorms, specs and ticket drafts that are not yet promoted live under `.scratch/<feature-slug>/`:

- The spec is `.scratch/<feature-slug>/spec.md`.
- Draft tickets are one file per ticket at `.scratch/<feature-slug>/issues/<NN>-<slug>.md`, numbered from `01`.
- A `Status:` line near the top records the triage state; a `Blocked by:` line records draft dependencies.

`.scratch/` is not committed. A draft is disposable until promoted.

## When a skill says "publish to the issue tracker"

Create a Linear issue as above, with a `## Provenance` section. If the user asks for a draft first, write the local file and promote later.

## When a skill says "fetch the relevant ticket"

Read the Linear issue by identifier, including comments and relations. If given a `.scratch/` path, read the file.

## Wayfinding operations

Used by `/wayfinder`. The **map** is one Linear issue; **children** are its sub-issues.

- **Map**: an issue labelled `wayfinder:map` holding the Notes / Decisions-so-far / Fog body.
- **Child ticket**: a sub-issue of the map, labelled `wayfinder:<type>` (`research`, `prototype`, `grilling`, `task`). Create the `wayfinder:*` labels on first use.
- **Blocking**: native `blocks` relations between children.
- **Frontier**: open children with no open blocker and no assignee; first in map order wins.
- **Claim**: assign yourself, before any work.
- **Resolve**: comment the answer, move to `Done`, then append a context pointer (gist + link) to the map's Decisions-so-far.
