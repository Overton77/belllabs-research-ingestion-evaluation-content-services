from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any


def canonical_json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def sha256_digest(value: bytes | str | Any) -> str:
    if isinstance(value, str):
        payload = value.encode()
    elif isinstance(value, bytes):
        payload = value
    else:
        payload = canonical_json_bytes(value)
    return f"sha256:{sha256(payload).hexdigest()}"


def write_json(path: Path, value: Any) -> str:
    content = canonical_json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return sha256_digest(content)


def write_text(path: Path, value: str) -> str:
    content = (value.rstrip() + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return sha256_digest(content)


def safe_relative_path(value: str) -> Path:
    path = Path(value.replace("\\", "/"))
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"unsafe workspace path: {value}")
    return path
