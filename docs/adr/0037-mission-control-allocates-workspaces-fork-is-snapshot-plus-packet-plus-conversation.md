---
type: Decision Record
title: "Mission Control allocates every workspace: managed worktrees are manifest policy, and a conversation fork, a workspace snapshot and a Context Packet are three separate artifacts"
description: "Proposed for the multi-provider packet (SPEC-02, MP-04): the agent never decides whether isolation exists; mission/v2 carries a workspace policy; the kernel allocates lease-fenced worktrees from a dedicated clone; a Fork composes snapshot, packet and (only where qualified) provider conversation fork; hosted lanes use native per-task isolation and record branch and commit."
tags: [mission-control, adr, proposed, workspaces, worktrees, fork]
status: proposed
source: docs/specs/multi-provider-2026-10/SPEC-02-environments.md; docs/specs/multi-provider-2026-10/SPEC-01-runtime.md (fork command); ADR-0030 (hydrated fork); ADR-0035 (mc.workspace_snapshot.v1); application/execution/harness/leases.py
---

# Mission Control allocates every workspace: managed worktrees are manifest policy, and a conversation fork, a workspace snapshot and a Context Packet are three separate artifacts

**Status: proposed.** Becomes accepted when the owner authorizes it.

Coding agents will create their own worktrees, or work in the developer's dirty checkout, and every
provider has a different thing it calls a fork (a Codex thread fork, a Claude session resume, a Cursor
cloud task branch). We decided the agent never chooses whether isolation exists. The `mission/v2`
environment carries `workspace.policy` (`managed_worktree | provider_workspace`, reuse scope,
dirty-input rule, cleanup rule); for local lanes the kernel allocates a worktree from a dedicated clone
under an admitted workspace root, pinned to a base commit and a branch keyed to run and lease, and
refuses symlink escape, branch collisions and the owner's primary checkout. A Fork is the composition
of a Workspace Snapshot (`mc.workspace_snapshot.v1`: base commit, staged, unstaged and included
untracked content, modes, manifest digest), a Context Packet and, only where the lane qualifies it, a
provider conversation fork; none of the three is inferred from another. We rejected "encourage the
agent to use worktrees" because an encouragement cannot be fenced, audited or cleaned up, and rejected
`git diff` as the snapshot because it loses untracked and binary content. Hosted lanes use the
provider's native per-task isolation and record branch and commit; `managed_worktree` is invalid for a
hosted lane unless its adapter can actually allocate and attest one.

## Consequences

- Parallel writable operations need distinct worktrees; subordinates share a checkout only under a
  declared coordination model, otherwise they get read-only access or their own lease.
- Cleanup is idempotent, bounded to the owned lease, runs only after artifact registration and liability
  settlement, and never force-deletes a dirty user worktree.
- Repository changes cross Stage Graph nodes and Mission Chain links as snapshot, patch or commit
  artifacts, never as a shared mutable checkout.
- The Cursor `branch:<branch>@<sha>` restore convention needs a producer, not only a parser.
- Git worktree isolation is not an OS sandbox; repository metadata and network credentials are shared.
