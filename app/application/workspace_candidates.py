from __future__ import annotations

import asyncio
import mimetypes
import shutil
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Protocol
from uuid import NAMESPACE_URL, uuid4, uuid5

from app.application.workspace_materialization import WorkspaceMaterializationService
from app.domain.operation_execution.contracts import (
    CapturedWorkspaceCandidate,
    OperationExecutionBinding,
)
from app.domain.operation_execution.errors import (
    UndeclaredWorkspacePath,
    WorkspaceDigestMismatch,
)
from app.domain.run_control.errors import IdempotencyConflict


class WorkspaceCandidateContentPort(Protocol):
    async def put(self, candidate: CapturedWorkspaceCandidate, content: bytes) -> None: ...

    async def get(self, candidate_id: str) -> bytes: ...

    async def describe(self, candidate_id: str) -> CapturedWorkspaceCandidate: ...

    async def find(
        self, namespace_id: str, workspace_id: str, logical_path: str
    ) -> CapturedWorkspaceCandidate | None: ...


class WorkspaceCandidateCaptureService:
    def __init__(
        self,
        *,
        materializer: WorkspaceMaterializationService,
        contents: WorkspaceCandidateContentPort,
    ) -> None:
        self._materializer = materializer
        self._contents = contents

    async def capture(
        self,
        binding: OperationExecutionBinding,
        logical_path: str,
        content: bytes,
    ) -> CapturedWorkspaceCandidate:
        slot = next(
            (
                item
                for item in binding.workspace.slot_bindings
                if item.access == "exclusive_write"
                and _path_within_slot(logical_path, item.logical_path)
            ),
            None,
        )
        if slot is None:
            raise UndeclaredWorkspacePath(
                "runtime output is not a declared writable workspace slot"
            )
        digest = f"sha256:{sha256(content).hexdigest()}"
        candidate = CapturedWorkspaceCandidate(
            namespace_id=binding.workspace.namespace_id,
            workspace_id=binding.workspace.workspace_id,
            output_slot=slot.slot_name,
            logical_path=logical_path,
            owner=slot.owner,
            candidate_id=_stable_id(
                "workspace-candidate",
                binding.binding_id,
                slot.slot_name,
                logical_path,
                digest,
            ),
            content_digest=digest,
            media_type=mimetypes.guess_type(logical_path)[0] or "application/octet-stream",
            size_bytes=len(content),
        )
        await self._materializer.register_candidate(
            namespace_id=candidate.namespace_id,
            workspace_id=candidate.workspace_id,
            slot_name=candidate.output_slot,
            logical_path=candidate.logical_path,
            owner=candidate.owner,
            candidate_id=candidate.candidate_id,
            content=content,
            content_digest=candidate.content_digest,
            media_type=candidate.media_type,
        )
        await self._contents.put(candidate, content)
        return candidate

    async def get_for_path(
        self, namespace_id: str, workspace_id: str, logical_path: str
    ) -> tuple[CapturedWorkspaceCandidate, bytes]:
        manifest = await self._materializer.current_manifest(namespace_id, workspace_id)
        current = next(
            (
                entry
                for entry in manifest.entries
                if entry.kind == "local_candidate" and entry.logical_path == logical_path
            ),
            None,
        )
        if current is None:
            raise UndeclaredWorkspacePath("declared output candidate was not captured")
        candidate = await self._contents.describe(current.candidate_id)
        if (
            candidate.namespace_id != namespace_id
            or candidate.workspace_id != workspace_id
            or candidate.logical_path != logical_path
            or candidate.content_digest != current.content_digest
        ):
            raise WorkspaceDigestMismatch("workspace candidate descriptor is not current")
        return candidate, await self._contents.get(candidate.candidate_id)


class InMemoryWorkspaceCandidateContents:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._candidates: dict[str, CapturedWorkspaceCandidate] = {}
        self._contents: dict[str, bytes] = {}

    async def put(self, candidate: CapturedWorkspaceCandidate, content: bytes) -> None:
        if (
            candidate.content_digest != f"sha256:{sha256(content).hexdigest()}"
            or candidate.size_bytes != len(content)
        ):
            raise WorkspaceDigestMismatch("workspace candidate bytes do not match their descriptor")
        async with self._lock:
            prior = self._candidates.get(candidate.candidate_id)
            prior_content = self._contents.get(candidate.candidate_id)
            if prior is not None and (prior != candidate or prior_content != content):
                raise IdempotencyConflict("workspace candidate content conflict")
            self._candidates[candidate.candidate_id] = deepcopy(candidate)
            self._contents[candidate.candidate_id] = content

    async def get(self, candidate_id: str) -> bytes:
        try:
            return self._contents[candidate_id]
        except KeyError as error:
            raise UndeclaredWorkspacePath("workspace candidate content is unavailable") from error

    async def describe(self, candidate_id: str) -> CapturedWorkspaceCandidate:
        try:
            return deepcopy(self._candidates[candidate_id])
        except KeyError as error:
            raise UndeclaredWorkspacePath(
                "workspace candidate descriptor is unavailable"
            ) from error

    async def find(
        self, namespace_id: str, workspace_id: str, logical_path: str
    ) -> CapturedWorkspaceCandidate | None:
        candidate = next(
            (
                item
                for item in self._candidates.values()
                if item.namespace_id == namespace_id
                and item.workspace_id == workspace_id
                and item.logical_path == logical_path
            ),
            None,
        )
        return deepcopy(candidate)


class FilesystemWorkspaceCandidateContents:
    """Retains candidate bytes and descriptors across worker process restarts."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    async def put(self, candidate: CapturedWorkspaceCandidate, content: bytes) -> None:
        if (
            candidate.content_digest != f"sha256:{sha256(content).hexdigest()}"
            or candidate.size_bytes != len(content)
        ):
            raise WorkspaceDigestMismatch("workspace candidate bytes do not match their descriptor")
        directory = self._root / candidate.candidate_id
        content_path = directory / "content"
        metadata_path = directory / "candidate.json"
        if directory.exists():
            if not content_path.is_file() or not metadata_path.is_file():
                raise IdempotencyConflict("workspace candidate persistence requires reconciliation")
            prior = await self.get(candidate.candidate_id)
            prior_candidate = CapturedWorkspaceCandidate.model_validate_json(
                metadata_path.read_text(encoding="utf-8")
            )
            if prior != content or prior_candidate != candidate:
                raise IdempotencyConflict("workspace candidate content conflict")
            return
        temporary = self._root / f".candidate-{uuid4()}.tmp"
        temporary.mkdir()
        try:
            (temporary / "content").write_bytes(content)
            (temporary / "candidate.json").write_text(candidate.model_dump_json(), encoding="utf-8")
            try:
                temporary.replace(directory)
            except OSError:
                if not directory.is_dir():
                    raise
                await self.put(candidate, content)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    async def get(self, candidate_id: str) -> bytes:
        path = self._root / candidate_id / "content"
        if not path.is_file():
            raise UndeclaredWorkspacePath("workspace candidate content is unavailable")
        return path.read_bytes()

    async def describe(self, candidate_id: str) -> CapturedWorkspaceCandidate:
        path = self._root / candidate_id / "candidate.json"
        if not path.is_file():
            raise UndeclaredWorkspacePath("workspace candidate descriptor is unavailable")
        return CapturedWorkspaceCandidate.model_validate_json(path.read_text(encoding="utf-8"))

    async def find(
        self, namespace_id: str, workspace_id: str, logical_path: str
    ) -> CapturedWorkspaceCandidate | None:
        for metadata_path in self._root.glob("*/candidate.json"):
            if metadata_path.parent.name.startswith("."):
                continue
            candidate = CapturedWorkspaceCandidate.model_validate_json(
                metadata_path.read_text(encoding="utf-8")
            )
            if (
                candidate.namespace_id == namespace_id
                and candidate.workspace_id == workspace_id
                and candidate.logical_path == logical_path
            ):
                return candidate
        return None


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))


def _path_within_slot(logical_path: str, slot_path: str) -> bool:
    normalized_slot = slot_path.rstrip("/")
    return logical_path == normalized_slot or logical_path.startswith(normalized_slot + "/")
