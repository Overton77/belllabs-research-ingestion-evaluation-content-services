from __future__ import annotations

import stat
from collections.abc import Mapping
from pathlib import Path

from app.domain.operation_execution.contracts import (
    MaterializedWorkspace,
    WorkspaceMaterializationManifest,
    WorkspaceMaterializationRequest,
)
from app.domain.operation_execution.errors import (
    UndeclaredWorkspacePath,
    WorkspaceDigestMismatch,
)


class FilesystemWorkspaceProvisioner:
    """Real-filesystem conformance adapter; host paths never enter domain identity."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()
        self._roots: dict[tuple[str, str], Path] = {}

    async def provision(
        self,
        request: WorkspaceMaterializationRequest,
        manifest: WorkspaceMaterializationManifest,
        durable_inputs: Mapping[str, bytes],
    ) -> MaterializedWorkspace:
        workspace_root = self._workspace_root(request.namespace_id, request.workspace_id)
        workspace_root.mkdir(parents=True, exist_ok=True)
        self._roots[(request.namespace_id, request.workspace_id)] = workspace_root
        for slot in manifest.slots:
            host_path = self._host_path(workspace_root, slot.logical_path)
            if slot.access == "read_only":
                try:
                    content = durable_inputs[slot.logical_path]
                except KeyError as error:
                    raise WorkspaceDigestMismatch(
                        f"verified input is missing for {slot.logical_path}"
                    ) from error
                host_path.parent.mkdir(parents=True, exist_ok=True)
                if host_path.exists() and host_path.read_bytes() != content:
                    raise WorkspaceDigestMismatch(
                        f"existing mount differs from governed input: {slot.logical_path}"
                    )
                host_path.write_bytes(content)
                host_path.chmod(0o444)
            else:
                host_path.parent.mkdir(parents=True, exist_ok=True)
        return MaterializedWorkspace(
            workspace_id=request.workspace_id,
            namespace_id=request.namespace_id,
            provider=request.provider,
            runtime_digest=request.runtime_digest,
            image_digest=request.image_digest,
            mount_manifest_digest=manifest.manifest_digest,
            manifest_revision=manifest.revision,
        )

    def governed_host_path(
        self,
        manifest: WorkspaceMaterializationManifest,
        logical_path: str,
    ) -> Path:
        if logical_path not in {slot.logical_path for slot in manifest.slots}:
            raise UndeclaredWorkspacePath(f"path is not governed: {logical_path}")
        try:
            workspace_root = self._roots[(manifest.namespace_id, manifest.workspace_id)]
        except KeyError as error:
            raise UndeclaredWorkspacePath("workspace has not been provisioned") from error
        return self._host_path(workspace_root, logical_path)

    def write_candidate(
        self,
        manifest: WorkspaceMaterializationManifest,
        logical_path: str,
        content: bytes,
    ) -> Path:
        slot = next(
            (item for item in manifest.slots if item.logical_path == logical_path),
            None,
        )
        if slot is None or slot.access != "exclusive_write":
            raise UndeclaredWorkspacePath("candidate path is not a declared writable slot")
        host_path = self.governed_host_path(manifest, logical_path)
        host_path.parent.mkdir(parents=True, exist_ok=True)
        host_path.write_bytes(content)
        return host_path

    def _workspace_root(self, namespace_id: str, workspace_id: str) -> Path:
        safe_namespace = namespace_id.replace(":", "_").replace("/", "_")
        safe_workspace = workspace_id.replace(":", "_").replace("/", "_")
        return self._root / safe_namespace / safe_workspace

    @staticmethod
    def _host_path(workspace_root: Path, logical_path: str) -> Path:
        relative = Path(logical_path.lstrip("/"))
        candidate = (workspace_root / relative).resolve()
        if candidate != workspace_root and workspace_root not in candidate.parents:
            raise UndeclaredWorkspacePath("logical path escaped the workspace root")
        return candidate


def is_read_only(path: Path) -> bool:
    return not bool(path.stat().st_mode & stat.S_IWUSR)
