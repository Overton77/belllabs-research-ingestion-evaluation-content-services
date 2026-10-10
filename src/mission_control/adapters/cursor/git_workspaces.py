"""Import shim (MP-09): the git workspace backend lives in `adapters/workspaces/`.

MP-04 built the provider-neutral `GitWorkspaceBackend` here only to stay inside its claim's
writable paths and asked for the relocation (handoff item 1). The implementation is now
`mission_control.adapters.workspaces.git_workspaces`; this module re-exports its public names
so existing callers (`adapters/cursor/workspace.py`, the workspace test fixtures) keep working
until they move. New code imports the `adapters.workspaces` path.
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
