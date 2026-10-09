# [MP-04] Generalize explicit workspace leases, worktrees and snapshots

Linear: OVE-67 — https://linear.app/overtonbell/issue/OVE-67/mp-04-generalize-explicit-workspace-leases-worktrees-and-snapshots

Status: ready-for-agent — subject to blockers

**Owner:** Environment
**Blocked by:** MP-01
**Specification:** docs/specs/multi-provider-2026-10/SPEC-02-environments.md

## Problem

Existing Cursor workspace support needs provider-neutral allocation and complete portable snapshot semantics.

## Work

- Add managed_worktree/provider_workspace policy, reuse bounds and fenced allocation from dedicated clones.
- Persist branch/base commit, patch and untracked/binary artifact manifest; validate restored digests.
- Clean up only owned released leases after artifact custody; record actual cloud branch snapshot refs.

## Source ownership

Paths relative to src/mission_control unless they begin with tests/, docs/, deployments/, infra/, contracts/ (contracts is under src), a root filename or packages/. New paths are proposed. Shared files require integrator ownership under TEAM-WORKSPACE.md.

- `application/execution/harness/leases.py`
- `adapters/cursor/ workspace modules`
- `adapters/postgres/lanes/workspace_leases.py`
- `application/workspaces/ (new)`

## Acceptance

- [ ] Concurrent lease acquisition and stale-generation writes are rejected on real PostgreSQL.
- [ ] Fork/restore preserves staged, unstaged, deleted and untracked included content.
- [ ] Resolved path/symlink escape, branch collision and dirty primary checkout cannot be silently accepted.

## Validation and handoff

Run the relevant scenarios in docs/specs/multi-provider-2026-10/VALIDATION.md and repository checks appropriate to changed code. DB/Temporal claims require real local services. Record passed, failed, blocked and unrun checks separately. Native schema/recorded fixture tests precede any finite authorized provider drill. Capture contracts changed, exact version/environment, artifact paths and usage disposition. Do not flip qualification from mock evidence.

## Provenance

- Conversation: owner multi-provider planning request and provider-hosted-only clarification, 2026-10-08.
- Source baseline: 7c9b755d75c4417ae364f33c340b9891446a391a.
- Documents: docs/specs/multi-provider-2026-10/SPEC-02-environments.md; ARCHITECTURE.md; RESEARCH.md; TEAM-WORKSPACE.md (new uncommitted packet); docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md.
- Decisions: preserve ADR-0004/0007/0008/0018/0019/0023–0034; new deltas frozen by MP-01, not assumed already accepted.
- Terms: AgentHarness, Lane Profile, Context Packet, Human Task, Stop Fence, Provider Frame, Subscription in GLOSSARY.md.
