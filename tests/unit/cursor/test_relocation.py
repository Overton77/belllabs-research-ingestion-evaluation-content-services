"""MP-09: the git workspace backend moved to `adapters/workspaces/`; old imports keep working."""

from __future__ import annotations

import importlib

from mission_control.adapters import workspaces as workspaces_package
from mission_control.adapters.cursor import git_workspaces as shim
from mission_control.adapters.cursor.workspace import GitWorktreeLeaser
from mission_control.adapters.workspaces import git_workspaces as relocated


def test_the_shim_re_exports_the_relocated_objects() -> None:
    assert shim.GitWorkspaceBackend is relocated.GitWorkspaceBackend
    assert shim.run_git is relocated.run_git
    assert shim.is_remote is relocated.is_remote
    assert shim.GitCommandError is relocated.GitCommandError
    assert shim.CLONES_DIR == relocated.CLONES_DIR
    assert set(shim.__all__) == set(relocated.__all__) == set(workspaces_package.__all__)
    assert relocated.GitWorkspaceBackend.__module__ == (
        "mission_control.adapters.workspaces.git_workspaces"
    )


def test_old_and_new_import_paths_resolve_to_one_module() -> None:
    old = importlib.import_module("mission_control.adapters.cursor.git_workspaces")
    new = importlib.import_module("mission_control.adapters.workspaces.git_workspaces")
    assert old.GitWorkspaceBackend is new.GitWorkspaceBackend
    assert "relocated" in (old.__doc__ or "").lower() or "shim" in (old.__doc__ or "").lower()


def test_the_cursor_leaser_uses_the_relocated_backend(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from mission_control.application.execution.harness.leases import InMemoryWorkspaceLeaseStore

    leaser = GitWorktreeLeaser(InMemoryWorkspaceLeaseStore(), lease_root=tmp_path / "leases")
    assert isinstance(leaser._backend, relocated.GitWorkspaceBackend)
