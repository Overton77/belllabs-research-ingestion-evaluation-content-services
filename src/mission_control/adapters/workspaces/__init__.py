"""Provider-neutral workspace adapters (SPEC-02 "Worktrees and snapshots"; MP-04, MP-09).

`git_workspaces.GitWorkspaceBackend` implements `application.workspaces.ports.WorkspaceBackend`
for every local lane (dedicated clone, unique-branch worktrees, non-mutating capture, verified
restore). It was built under `adapters/cursor/` by MP-04 and relocated here by MP-09; the old
module path stays importable as a shim.
"""

from __future__ import annotations

from mission_control.adapters.workspaces.git_workspaces import (
    CLONES_DIR,
    GitCommandError,
    GitWorkspaceBackend,
    is_remote,
    run_git,
)

__all__ = [
    "CLONES_DIR",
    "GitCommandError",
    "GitWorkspaceBackend",
    "is_remote",
    "run_git",
]
