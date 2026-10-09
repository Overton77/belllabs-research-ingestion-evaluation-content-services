"""Workspace policy admission: which mode a lane profile may use, which slot a reuse policy
leases, and where that slot lives (SPEC-02 "Worktrees and snapshots"; MP-04).

The vocabulary is the frozen `mission/v2` one (`WorkspaceMode`, `WorkspaceReuse`,
`DirtyInputPolicy`, `CleanupPolicy`) carried by `WorkspacePolicyPin` in the binding:

- `managed_worktree` (local lanes): a git worktree on a unique branch, from a dedicated clone
  under the admitted workspace root, at a pinned base commit;
- `provider_workspace` (hosted lanes): the provider's own per-task isolation; Mission Control
  records the branch and the actual head commit it reads back. Only lanes whose provider
  publishes such a branch are admitted (`cursor_cloud`); hosted Claude Code and Codex are
  unqualified (Outcome 3, `docs/qualification/lanes/*/FEASIBILITY.md`) and are refused;
- `shared_checkout` (local lanes): a participant joins a parent's leased worktree under the
  parent's fence; it never means the developer's checkout.

Reuse bounds which holders write one workspace in turn: `none` (one holder generation),
`within_session` (successive generations of one harness execution) or `within_run` (every
harness execution of the run, one at a time).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Final
from uuid import UUID

from mission_control.application.workspaces.errors import (
    WORKSPACE_PATH_ESCAPE,
    WORKSPACE_POLICY_UNSUPPORTED,
    WorkspaceError,
)
from mission_control.domain.authoring.manifest_v2 import WorkspacePolicy
from mission_control.domain.execution.bindings import WorkspacePolicyPin
from mission_control.domain.execution.lanes import HOSTED_PROFILES, LANE_PROFILES

# Hosted lanes whose provider workspace is a branch Mission Control can read back as an
# immutable commit (`branch:<branch>@<sha>`; Cursor cloud pushes to the run branch).
BRANCH_SNAPSHOT_PROFILES: Final[frozenset[str]] = frozenset({"cursor_cloud"})
# Hosted lanes with no qualified workspace lifecycle: refused, never emulated locally.
UNQUALIFIED_HOSTED_PROFILES: Final[frozenset[str]] = frozenset(
    set(HOSTED_PROFILES) - BRANCH_SNAPSHOT_PROFILES
)
BRANCH_PREFIX: Final = "mc/ws"
DEFAULT_POLICY: Final = WorkspacePolicyPin(
    mode="managed_worktree",
    reuse="within_run",
    dirty_input="reject",
    cleanup="retain_until_artifacts_registered",
)


def policy_pin(policy: WorkspacePolicy | WorkspacePolicyPin | None) -> WorkspacePolicyPin:
    """The binding pin of an authored `mission/v2` policy (defaults when absent)."""

    if policy is None:
        return DEFAULT_POLICY
    if isinstance(policy, WorkspacePolicyPin):
        return policy
    return WorkspacePolicyPin(
        mode=policy.mode,
        reuse=policy.reuse,
        dirty_input=policy.dirty_input,
        cleanup=policy.cleanup,
    )


def admit_policy(lane_profile: str, policy: WorkspacePolicyPin) -> WorkspacePolicyPin:
    """Refuse a policy the profile cannot honour; return it unchanged otherwise."""

    if lane_profile not in LANE_PROFILES:
        raise WorkspaceError(WORKSPACE_POLICY_UNSUPPORTED, f"unknown lane profile {lane_profile}")
    if lane_profile in UNQUALIFIED_HOSTED_PROFILES:
        raise WorkspaceError(
            WORKSPACE_POLICY_UNSUPPORTED,
            f"{lane_profile} has no qualified provider workspace lifecycle; it is refused, "
            "not emulated by a local worktree",
        )
    hosted = lane_profile in HOSTED_PROFILES
    if hosted and policy.mode != "provider_workspace":
        raise WorkspaceError(
            WORKSPACE_POLICY_UNSUPPORTED,
            f"{lane_profile} is provider-hosted: use provider_workspace, not {policy.mode}",
        )
    if not hosted and policy.mode == "provider_workspace":
        raise WorkspaceError(
            WORKSPACE_POLICY_UNSUPPORTED,
            f"provider_workspace needs a provider-hosted lane, not {lane_profile}",
        )
    if policy.mode == "shared_checkout" and policy.reuse == "none":
        raise WorkspaceError(
            WORKSPACE_POLICY_UNSUPPORTED,
            "shared_checkout joins a parent's lease; it needs reuse within_run or within_session",
        )
    if policy.cleanup == "immediate" and policy.reuse != "none":
        raise WorkspaceError(
            WORKSPACE_POLICY_UNSUPPORTED,
            f"cleanup immediate would destroy a workspace declared reusable {policy.reuse}",
        )
    return policy


def holder_key(lane_profile: str, harness_execution_id: UUID, generation: int) -> str:
    """The holder of a lease (the FT-G3 lease key): one generation of one execution."""

    return f"{lane_profile}:{harness_execution_id}:{generation}"


def slot_of(
    policy: WorkspacePolicyPin,
    *,
    lane_profile: str,
    harness_execution_id: UUID,
    generation: int,
    run_id: str,
) -> str:
    """The writable workspace a holder leases under its reuse policy."""

    if policy.reuse == "none":
        return f"holder:{holder_key(lane_profile, harness_execution_id, generation)}"
    if policy.reuse == "within_session":
        return f"session:{lane_profile}:{harness_execution_id}"
    return f"run:{run_id}"


def slot_digest(request_scope: str, slot: str) -> str:
    return hashlib.sha256(f"{request_scope}\n{slot}".encode()).hexdigest()


def slot_branch(request_scope: str, slot: str) -> str:
    """The slot's unique branch: every holder of a slot writes the same branch in turn."""

    return f"{BRANCH_PREFIX}/{slot_digest(request_scope, slot)[:24]}"


def resolved_root(root: Path) -> Path:
    return root.expanduser().resolve()


def contained(root: Path, candidate: Path) -> Path:
    """`candidate` fully resolved (symlinks and junctions followed) inside `root`, or refused."""

    base = resolved_root(root)
    target = candidate.expanduser().resolve()
    if target != base and base not in target.parents:
        raise WorkspaceError(
            WORKSPACE_PATH_ESCAPE, f"{candidate} resolves to {target}, outside {base}"
        )
    return target


def slot_path(root: Path, request_scope: str, slot: str) -> Path:
    """`<root>/<slot digest>/worktree`, resolved and checked inside the root."""

    digest = slot_digest(request_scope, slot)
    return contained(root, root / digest[:2] / digest[2:20] / "worktree")


def reject_nested_root(root: Path, source: Path | None) -> None:
    """The workspace root must not lie inside the source checkout, nor contain it."""

    if source is None:
        return
    base = resolved_root(root)
    checkout = source.expanduser().resolve()
    if base == checkout or checkout in base.parents or base in checkout.parents:
        raise WorkspaceError(
            WORKSPACE_PATH_ESCAPE,
            f"workspace root {base} overlaps the source checkout {checkout}",
        )


__all__ = [
    "BRANCH_PREFIX",
    "BRANCH_SNAPSHOT_PROFILES",
    "DEFAULT_POLICY",
    "UNQUALIFIED_HOSTED_PROFILES",
    "admit_policy",
    "contained",
    "holder_key",
    "policy_pin",
    "reject_nested_root",
    "resolved_root",
    "slot_branch",
    "slot_digest",
    "slot_of",
    "slot_path",
]
