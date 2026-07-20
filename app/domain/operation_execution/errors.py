class WorkspaceMaterializationError(ValueError):
    """A workspace request violates its compiled contract."""


class WorkspaceSlotConflict(WorkspaceMaterializationError):
    """A logical writable slot is already owned by another workspace owner."""


class WorkspaceDigestMismatch(WorkspaceMaterializationError):
    """Mounted or candidate bytes do not match their governed digest."""


class UndeclaredWorkspacePath(WorkspaceMaterializationError):
    """A path is not governed by the materialized workspace contract."""


class UnsupportedWorkspaceRequirement(WorkspaceMaterializationError):
    """The selected sandbox provider cannot enforce a required control."""
