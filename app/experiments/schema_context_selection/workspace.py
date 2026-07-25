from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from agents.sandbox import Manifest
from agents.sandbox.entries import BaseEntry, LocalDir, LocalFile
from agents.sandbox.types import Permissions

from app.domain.schema_context.canonicalization import sha256_digest, write_json, write_text


def freeze_input(source: Path, destination: Path) -> str:
    content = source.read_bytes()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    return sha256_digest(content)


def sandbox_manifest(run_root: Path, relative_paths: Iterable[str]) -> Manifest:
    entries: dict[str, BaseEntry] = {}
    current = Path.cwd().resolve()
    for relative in sorted(set(relative_paths)):
        path = run_root / relative
        try:
            source = path.resolve().relative_to(current)
        except ValueError as error:
            raise ValueError("sandbox inputs must remain under the current project") from error
        if path.is_dir():
            entries[Path(relative).as_posix()] = LocalDir(
                src=source,
                permissions=Permissions(owner=5, directory=True),
            )
        elif path.is_file():
            entries[Path(relative).as_posix()] = LocalFile(
                src=source,
                permissions=Permissions(owner=4),
            )
        else:
            raise FileNotFoundError(path)
    return Manifest(entries=entries)


def write_source_manifest(run_root: Path, inputs: dict[str, dict[str, str]]) -> None:
    write_json(run_root / "inputs" / "source-manifest.json", {"inputs": inputs})


def reset_run_directory(path: Path) -> None:
    if path.exists():
        raise FileExistsError(f"run directory already exists: {path}")
    path.mkdir(parents=True)


__all__ = [
    "freeze_input",
    "sandbox_manifest",
    "sha256_digest",
    "write_json",
    "write_text",
    "write_source_manifest",
]
