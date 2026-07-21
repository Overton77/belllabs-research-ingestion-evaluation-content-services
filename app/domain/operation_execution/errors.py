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


class UnsupportedRuntimePolicy(ValueError):
    """The selected agent runtime cannot faithfully enforce a bound policy."""


class SandboxSnapshotError(ValueError):
    """Snapshot creation or clone restore violated a governed invariant."""


class SnapshotCompatibilityError(SandboxSnapshotError):
    """A snapshot cannot be restored into the requested runtime contract."""


class SnapshotAuthorityError(SandboxSnapshotError):
    """Present authority does not admit snapshot creation or restoration."""


class SnapshotPayloadMismatch(SandboxSnapshotError):
    """Stored snapshot bytes do not match immutable snapshot metadata."""


class SnapshotMigrationRequired(SnapshotCompatibilityError):
    """An authored migration must run as a new semantic operation."""


class SnapshotCreationInProgress(RuntimeError):
    """Another worker owns the durable snapshot-creation claim."""


class SnapshotCloneInProgress(RuntimeError):
    """Another worker owns the durable snapshot-clone claim."""
