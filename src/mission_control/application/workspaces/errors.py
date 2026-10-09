"""Typed refusals of workspace allocation, fencing, snapshot and cleanup (SPEC-02 "Worktrees
and snapshots"; MP-04).

Every refusal names a stable upper-snake `code` so admission, the lane adapters and the
operator surface report the same reason. None of them is retried silently: a held lease, a
stale fence, an escaping path, a colliding branch or a dirty input is an explicit outcome.
"""

from __future__ import annotations

from typing import Final

WORKSPACE_LEASE_HELD: Final = "WORKSPACE_LEASE_HELD"
WORKSPACE_LEASE_STALE: Final = "WORKSPACE_LEASE_STALE"
WORKSPACE_PATH_ESCAPE: Final = "WORKSPACE_PATH_ESCAPE"
WORKSPACE_PATH_OCCUPIED: Final = "WORKSPACE_PATH_OCCUPIED"
WORKSPACE_BRANCH_COLLISION: Final = "WORKSPACE_BRANCH_COLLISION"
WORKSPACE_DIRTY_INPUT: Final = "WORKSPACE_DIRTY_INPUT"
WORKSPACE_POLICY_UNSUPPORTED: Final = "WORKSPACE_POLICY_UNSUPPORTED"
WORKSPACE_CUSTODY_MISSING: Final = "WORKSPACE_CUSTODY_MISSING"
WORKSPACE_CUSTODY_STALE: Final = "WORKSPACE_CUSTODY_STALE"
WORKSPACE_NOT_OWNED: Final = "WORKSPACE_NOT_OWNED"
WORKSPACE_SNAPSHOT_FAILED: Final = "WORKSPACE_SNAPSHOT_FAILED"
CHECKPOINT_INVALID: Final = "CHECKPOINT_INVALID"

WORKSPACE_ERROR_CODES: Final = frozenset(
    {
        WORKSPACE_LEASE_HELD,
        WORKSPACE_LEASE_STALE,
        WORKSPACE_PATH_ESCAPE,
        WORKSPACE_PATH_OCCUPIED,
        WORKSPACE_BRANCH_COLLISION,
        WORKSPACE_DIRTY_INPUT,
        WORKSPACE_POLICY_UNSUPPORTED,
        WORKSPACE_CUSTODY_MISSING,
        WORKSPACE_CUSTODY_STALE,
        WORKSPACE_NOT_OWNED,
        WORKSPACE_SNAPSHOT_FAILED,
        CHECKPOINT_INVALID,
    }
)


class WorkspaceError(RuntimeError):
    """A workspace operation was refused; `code` is one of `WORKSPACE_ERROR_CODES`."""

    def __init__(self, code: str, message: str) -> None:
        if code not in WORKSPACE_ERROR_CODES:
            raise ValueError(f"unknown workspace error code {code}")
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class WorkspaceLeaseHeld(WorkspaceError):
    """Another holder owns the workspace slot and its lease has not expired."""

    def __init__(self, slot: str, holder_key: str) -> None:
        super().__init__(WORKSPACE_LEASE_HELD, f"workspace slot {slot} is held by {holder_key}")
        self.slot = slot
        self.holder_key = holder_key


class StaleWorkspaceLease(WorkspaceError):
    """A write carried a fence or generation that is no longer the slot's current one."""

    def __init__(self, lease_key: str, detail: str) -> None:
        super().__init__(WORKSPACE_LEASE_STALE, f"lease {lease_key} is stale: {detail}")
        self.lease_key = lease_key


__all__ = [
    "CHECKPOINT_INVALID",
    "WORKSPACE_BRANCH_COLLISION",
    "WORKSPACE_CUSTODY_MISSING",
    "WORKSPACE_CUSTODY_STALE",
    "WORKSPACE_DIRTY_INPUT",
    "WORKSPACE_ERROR_CODES",
    "WORKSPACE_LEASE_HELD",
    "WORKSPACE_LEASE_STALE",
    "WORKSPACE_NOT_OWNED",
    "WORKSPACE_PATH_ESCAPE",
    "WORKSPACE_PATH_OCCUPIED",
    "WORKSPACE_POLICY_UNSUPPORTED",
    "WORKSPACE_SNAPSHOT_FAILED",
    "StaleWorkspaceLease",
    "WorkspaceError",
    "WorkspaceLeaseHeld",
]
