from __future__ import annotations

import base64
import io
import json
import re
import tarfile
from hashlib import sha256
from pathlib import PurePosixPath
from typing import Literal
from urllib.parse import quote

from agents.sandbox import Manifest, SandboxArchiveLimits
from agents.sandbox.sandboxes import DockerSandboxClient, DockerSandboxClientOptions
from agents.sandbox.session import SandboxSession
from agents.sandbox.session.dependencies import Dependencies
from agents.sandbox.snapshot import SnapshotBase
from pydantic import Field

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    MaterializedWorkspace,
    OperationExecutionBinding,
    SandboxSnapshot,
    SandboxSnapshotCapture,
    SandboxSnapshotCreateRequest,
    SnapshotCloneRequest,
)
from app.domain.operation_execution.errors import SnapshotCompatibilityError


class _ArchiveSnapshot(SnapshotBase):
    """SDK SnapshotBase backed by app-owned immutable archive bytes."""

    type: Literal["belllabs_archive"] = "belllabs_archive"
    payload: bytes = Field(default=b"", exclude=True, repr=False)
    writable: bool = Field(default=False, exclude=True, repr=False)
    max_input_bytes: int = Field(default=268_435_456, exclude=True, repr=False)

    async def persist(
        self, data: io.IOBase, *, dependencies: Dependencies | None = None
    ) -> None:
        del dependencies
        if not self.writable:
            return
        content = data.read(self.max_input_bytes + 1)
        if not isinstance(content, bytes):
            raise TypeError("sandbox snapshot archive must be binary")
        if len(content) > self.max_input_bytes:
            raise SnapshotCompatibilityError(
                "sandbox snapshot archive exceeds input size limit"
            )
        object.__setattr__(self, "payload", content)

    async def restore(self, *, dependencies: Dependencies | None = None) -> io.IOBase:
        del dependencies
        return io.BytesIO(self.payload)

    async def restorable(self, *, dependencies: Dependencies | None = None) -> bool:
        del dependencies
        return bool(self.payload)


class OpenAIAgentsSnapshotBridge:
    """Connects SDK sandbox archives to the provider-neutral snapshot service."""

    def __init__(
        self,
        client: DockerSandboxClient,
        *,
        captured_policy_refs: frozenset[str] = frozenset(),
        archive_limits: SandboxArchiveLimits | None = None,
        max_pending_captures: int = 16,
        max_pending_capture_bytes: int = 536_870_912,
        max_pending_restores: int = 10,
    ) -> None:
        self._client = client
        self._captured_policy_refs = captured_policy_refs
        self._archive_limits = archive_limits or SandboxArchiveLimits(
            max_input_bytes=268_435_456,
            max_extracted_bytes=1_073_741_824,
            max_members=10_000,
        )
        self._max_pending_captures = max_pending_captures
        self._max_pending_capture_bytes = max_pending_capture_bytes
        self._max_pending_restores = max_pending_restores
        self._captures: dict[tuple[str, str], SandboxSnapshotCapture] = {}
        self._restored: dict[tuple[str, str, str, str], SandboxSession] = {}

    @property
    def client(self) -> DockerSandboxClient:
        return self._client

    @property
    def archive_limits(self) -> SandboxArchiveLimits:
        return self._archive_limits

    def begin_capture(
        self,
        binding: OperationExecutionBinding,
        workspace: MaterializedWorkspace,
    ) -> _ArchiveSnapshot | None:
        if binding.snapshot_policy_ref not in self._captured_policy_refs:
            return None
        return _ArchiveSnapshot(
            id=f"{binding.binding_id}-{workspace.workspace_id}",
            writable=True,
            max_input_bytes=self._archive_limits.max_input_bytes or 268_435_456,
        )

    def complete_capture(
        self,
        binding: OperationExecutionBinding,
        workspace: MaterializedWorkspace,
        archive: _ArchiveSnapshot,
        *,
        sensitive_values: tuple[bytes, ...] = (),
    ) -> None:
        if not archive.payload:
            raise SnapshotCompatibilityError("OpenAI sandbox produced an empty snapshot archive")
        allowed_roots = tuple(
            slot.logical_path.lstrip("/")
            for slot in binding.workspace.slot_bindings
            if slot.access == "exclusive_write"
        ) or tuple(path.lstrip("/") for path in binding.workspace.exclusive_write_paths)
        sanitized, filesystem_digest, content_manifest_digest = _sanitize_archive(
            archive.payload,
            allowed_roots=allowed_roots,
            limits=self._archive_limits,
            sensitive_values=_secret_variants(sensitive_values),
        )
        object.__setattr__(archive, "payload", sanitized)
        key = (workspace.namespace_id or "", workspace.workspace_id)
        if key not in self._captures and len(self._captures) >= self._max_pending_captures:
            raise SnapshotCompatibilityError("too many pending sandbox snapshot captures")
        prior_size = len(self._captures[key].payload) if key in self._captures else 0
        pending_bytes = sum(len(capture.payload) for capture in self._captures.values())
        if pending_bytes - prior_size + len(sanitized) > self._max_pending_capture_bytes:
            raise SnapshotCompatibilityError(
                "pending sandbox snapshot captures exceed aggregate byte limit"
            )
        self._captures[key] = SandboxSnapshotCapture(
            provider_snapshot_id=archive.id,
            filesystem_digest=filesystem_digest,
            content_manifest_digest=content_manifest_digest,
            payload=sanitized,
        )

    async def capture(self, request: SandboxSnapshotCreateRequest) -> SandboxSnapshotCapture:
        try:
            capture = self._captures.pop(
                (request.source_namespace_id, request.source_workspace_id)
            )
        except KeyError as error:
            raise SnapshotCompatibilityError(
                "no completed OpenAI sandbox archive exists for the source workspace"
            ) from error
        return capture

    async def clone(
        self,
        *,
        snapshot: SandboxSnapshot,
        payload: bytes,
        request: SnapshotCloneRequest,
    ) -> MaterializedWorkspace:
        _sanitized, filesystem_digest, content_manifest_digest = _sanitize_archive(
            payload,
            allowed_roots=None,
            limits=self._archive_limits,
            sensitive_values=(),
        )
        if (
            filesystem_digest != snapshot.filesystem_digest
            or content_manifest_digest != snapshot.content_manifest_digest
        ):
            raise SnapshotCompatibilityError(
                "OpenAI sandbox archive does not match its canonical content manifest"
            )
        archive = _ArchiveSnapshot(
            id=snapshot.provider_snapshot_id,
            payload=payload,
            writable=False,
        )
        key = (
            request.target_namespace_id,
            request.target_workspace_id,
            request.snapshot_id,
            request.binding_id,
        )
        if key in self._restored:
            return self._workspace_for(snapshot, request)
        if len(self._restored) >= self._max_pending_restores:
            raise SnapshotCompatibilityError("too many pending restored sandbox sessions")
        session = await self._client.create(
            snapshot=archive,
            manifest=Manifest(),
            options=DockerSandboxClientOptions(image=request.image_digest),
        )
        try:
            session._set_archive_limits(self._archive_limits)
            self._restored[key] = session
        except BaseException:
            try:
                await session.aclose()
            finally:
                await self._client.delete(session)
            raise
        return self._workspace_for(snapshot, request)

    @staticmethod
    def _workspace_for(
        snapshot: SandboxSnapshot, request: SnapshotCloneRequest
    ) -> MaterializedWorkspace:
        return MaterializedWorkspace(
            workspace_id=request.target_workspace_id,
            namespace_id=request.target_namespace_id,
            provider=snapshot.provider,
            runtime_digest=request.runtime_digest,
            image_digest=request.image_digest,
            mount_manifest_digest=request.target_mount_manifest_digest,
        )

    async def take_restored_session(
        self, workspace: MaterializedWorkspace, binding: OperationExecutionBinding
    ) -> SandboxSession | None:
        return self._restored.pop(
            (
                workspace.namespace_id or "",
                workspace.workspace_id,
                binding.workspace.restore_snapshot_id or "",
                binding.binding_id,
            ),
            None,
        )

    async def discard_clone(self, request: SnapshotCloneRequest) -> None:
        session = self._restored.pop(
            (
                request.target_namespace_id,
                request.target_workspace_id,
                request.snapshot_id,
                request.binding_id,
            ),
            None,
        )
        if session is not None:
            try:
                await session.aclose()
            finally:
                await self._client.delete(session)

    async def aclose(self) -> None:
        sessions = tuple(self._restored.values())
        self._restored.clear()
        self._captures.clear()
        for session in sessions:
            try:
                await session.aclose()
            finally:
                await self._client.delete(session)


def _digest(content: bytes) -> str:
    return f"sha256:{sha256(content).hexdigest()}"


def _sanitize_archive(
    payload: bytes,
    *,
    allowed_roots: tuple[str, ...] | None,
    limits: SandboxArchiveLimits,
    sensitive_values: tuple[bytes, ...],
) -> tuple[bytes, str, str]:
    if limits.max_input_bytes is not None and len(payload) > limits.max_input_bytes:
        raise SnapshotCompatibilityError("sandbox snapshot archive exceeds input size limit")
    output = io.BytesIO()
    filesystem_entries: list[dict[str, object]] = []
    content_entries: list[dict[str, object]] = []
    extracted_bytes = 0
    try:
        source_context = tarfile.open(fileobj=io.BytesIO(payload), mode="r:*")
    except tarfile.TarError as error:
        raise SnapshotCompatibilityError("sandbox snapshot is not a valid tar archive") from error
    with source_context as source, tarfile.open(fileobj=output, mode="w") as target:
        members: list[tarfile.TarInfo] = []
        for member in source:
            members.append(member)
            if limits.max_members is not None and len(members) > limits.max_members:
                raise SnapshotCompatibilityError("sandbox snapshot has too many archive members")
        seen_paths: set[str] = set()
        for member in sorted(members, key=lambda item: item.name):
            if member.name.replace("\\", "/") in {".", "./"} and member.isdir():
                continue
            path = _safe_member_path(member)
            if path in seen_paths:
                raise SnapshotCompatibilityError(
                    "snapshot archive contains duplicate normalized paths"
                )
            seen_paths.add(path)
            if allowed_roots is not None and not any(
                path == root or path.startswith(root.rstrip("/") + "/")
                for root in allowed_roots
            ):
                continue
            _reject_sensitive_path(path)
            if member.isdir():
                content = None
                kind = "directory"
            elif member.isfile():
                extracted_bytes += member.size
                if (
                    limits.max_extracted_bytes is not None
                    and extracted_bytes > limits.max_extracted_bytes
                ):
                    raise SnapshotCompatibilityError(
                        "sandbox snapshot exceeds extracted size limit"
                    )
                extracted = source.extractfile(member)
                if extracted is None:
                    raise SnapshotCompatibilityError("snapshot file content is unavailable")
                content = extracted.read()
                if len(content) != member.size:
                    raise SnapshotCompatibilityError("snapshot file size is inconsistent")
                _reject_sensitive_content(content, sensitive_values)
                kind = "file"
            else:
                raise SnapshotCompatibilityError(
                    "snapshot links, devices, sparse entries, and special files are prohibited"
                )

            normalized = tarfile.TarInfo(path)
            normalized.mode = member.mode & 0o777
            normalized.mtime = 0
            normalized.uid = 0
            normalized.gid = 0
            normalized.uname = ""
            normalized.gname = ""
            if content is None:
                normalized.type = tarfile.DIRTYPE
                normalized.size = 0
                target.addfile(normalized)
                content_digest = None
            else:
                normalized.size = len(content)
                target.addfile(normalized, io.BytesIO(content))
                content_digest = _digest(content)
                content_entries.append(
                    {"path": path, "size_bytes": len(content), "digest": content_digest}
                )
            filesystem_entries.append(
                {
                    "path": path,
                    "kind": kind,
                    "mode": normalized.mode,
                    "size_bytes": normalized.size,
                    "content_digest": content_digest,
                }
            )
    if not filesystem_entries:
        raise SnapshotCompatibilityError(
            "snapshot contains no governed writable workspace entries"
        )
    return (
        output.getvalue(),
        sha256_digest(filesystem_entries),
        sha256_digest(content_entries),
    )


def _safe_member_path(member: tarfile.TarInfo) -> str:
    raw = member.name.replace("\\", "/")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise SnapshotCompatibilityError("snapshot archive contains an unsafe path")
    return path.as_posix()


def _reject_sensitive_path(path: str) -> None:
    parts = tuple(part.lower() for part in PurePosixPath(path).parts)
    prohibited_parts = {
        ".ssh",
        ".aws",
        ".azure",
        ".kube",
        ".netrc",
        ".npmrc",
        ".pypirc",
        "credentials",
        "credentials.json",
        "token",
        "token.json",
        "id_rsa",
        "id_ed25519",
    }
    if any(
        part in prohibited_parts or part == ".env" or part.startswith(".env.")
        for part in parts
    ):
        raise SnapshotCompatibilityError(
            "snapshot archive contains a prohibited credential-bearing path"
        )


def _secret_variants(values: tuple[bytes, ...]) -> tuple[bytes, ...]:
    variants: set[bytes] = set()
    for value in values:
        if not value:
            continue
        variants.update(
            {
                value,
                base64.b64encode(value),
                value.hex().encode(),
                quote(value.decode("utf-8", errors="ignore"), safe="").encode(),
                json.dumps(value.decode("utf-8", errors="ignore"))[1:-1].encode(),
            }
        )
    return tuple(variants)


def _reject_sensitive_content(content: bytes, sensitive_values: tuple[bytes, ...]) -> None:
    if any(value and value in content for value in sensitive_values):
        raise SnapshotCompatibilityError(
            "snapshot archive contains a resolved secret value or direct encoding"
        )
    text = content.decode("utf-8", errors="ignore")
    secret_patterns = (
        r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
        r"\bAKIA[0-9A-Z]{16}\b",
        r"\b(?:sk|rk|pk)-[A-Za-z0-9_-]{20,}\b",
        r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b",
    )
    if any(re.search(pattern, text) for pattern in secret_patterns):
        raise SnapshotCompatibilityError(
            "snapshot archive contains credential-shaped content"
        )
