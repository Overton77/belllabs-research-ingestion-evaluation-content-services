from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path

from mission_control.application.agentic_components.repository import (
    InMemoryAgenticComponentRepository,
)
from mission_control.domain.agentic_components.contracts import (
    AgenticComponentRelease,
    ComponentQuery,
    DiffCodecQualification,
)


class FilesystemAgenticComponentRepository:
    """Immutable JSON release store for local/offline harness catalogs.

    The directory is a transport adapter, not the catalog authority. Published control-plane
    definitions and immutable payload refs remain authoritative; this store makes their exact
    runnable release descriptors available in sandboxes and developer workspaces.
    """

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()
        self._release_root = self._root / "releases" / "sha256"

    async def put(self, release: AgenticComponentRelease) -> Path:
        return await asyncio.to_thread(self._put_sync, release)

    async def get_by_digest(self, digest: str) -> AgenticComponentRelease | None:
        return await asyncio.to_thread(self._get_sync, digest)

    async def search(self, query: ComponentQuery) -> tuple[AgenticComponentRelease, ...]:
        repository = InMemoryAgenticComponentRepository(await self._load_all())
        return await repository.search(query)

    async def select_diff_qualification(
        self,
        model_id: str,
    ) -> DiffCodecQualification | None:
        repository = InMemoryAgenticComponentRepository(await self._load_all())
        return await repository.select_diff_qualification(model_id)

    async def _load_all(self) -> tuple[AgenticComponentRelease, ...]:
        return await asyncio.to_thread(self._load_all_sync)

    def _put_sync(self, release: AgenticComponentRelease) -> Path:
        target = self._path_for_digest(release.coordinate.digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            json.dumps(
                release.model_dump(mode="json"),
                indent=2,
                sort_keys=True,
                separators=(",", ": "),
            )
            + "\n"
        ).encode()
        if target.exists():
            if target.read_bytes() != payload:
                raise ValueError(
                    f"immutable component release conflicts with existing digest: "
                    f"{release.coordinate.digest}"
                )
            return target
        temporary_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=".agentic-component-",
                suffix=".tmp",
                dir=target.parent,
                delete=False,
            ) as handle:
                temporary_name = handle.name
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            Path(temporary_name).replace(target)
        finally:
            if temporary_name is not None:
                Path(temporary_name).unlink(missing_ok=True)
        return target

    def _get_sync(self, digest: str) -> AgenticComponentRelease | None:
        path = self._path_for_digest(digest)
        if not path.exists():
            return None
        release = AgenticComponentRelease.model_validate_json(path.read_bytes())
        if release.coordinate.digest != digest:
            raise ValueError(f"component release path/digest mismatch: {path}")
        return release

    def _load_all_sync(self) -> tuple[AgenticComponentRelease, ...]:
        if not self._release_root.exists():
            return ()
        releases = []
        for path in sorted(self._release_root.glob("*.json")):
            release = AgenticComponentRelease.model_validate_json(path.read_bytes())
            expected = f"sha256:{path.stem}"
            if release.coordinate.digest != expected:
                raise ValueError(f"component release path/digest mismatch: {path}")
            releases.append(release)
        return tuple(releases)

    def _path_for_digest(self, digest: str) -> Path:
        prefix = "sha256:"
        value = digest.removeprefix(prefix)
        if (
            not digest.startswith(prefix)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError("component digest must be canonical sha256")
        return self._release_root / f"{value}.json"
