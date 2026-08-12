from __future__ import annotations

import base64
import subprocess
import uuid
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Final

from deepagents.backends.protocol import (
    ExecuteResponse,
    FileDownloadResponse,
    FileUploadResponse,
)
from deepagents.backends.sandbox import BaseSandbox

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import DeepAgentExecutionBinding
from app.domain.operation_execution.errors import DeepAgentUnsupportedPlacement

_UPLOAD_SCRIPT: Final = """
import base64, os, sys
path = sys.argv[1]
if not path.startswith('/') or '..' in path.split('/'):
    raise ValueError('invalid sandbox path')
os.makedirs(os.path.dirname(path) or '/', exist_ok=True)
with open(path, 'wb') as handle:
    handle.write(base64.b64decode(sys.stdin.buffer.read()))
"""

_DOWNLOAD_SCRIPT: Final = """
import base64, os, sys
path = sys.argv[1]
if not path.startswith('/') or '..' in path.split('/'):
    raise ValueError('invalid sandbox path')
if not os.path.isfile(path):
    raise FileNotFoundError(path)
sys.stdout.buffer.write(base64.b64encode(open(path, 'rb').read()))
"""

_DEFAULT_IMAGE: Final = (
    "python:3.12-slim@sha256:"
    "229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36"
)


class DockerSandbox(BaseSandbox):
    """Deep Agents sandbox backed by one network-isolated Docker container."""

    def __init__(self, container_id: str, *, timeout_seconds: int = 120) -> None:
        self._container_id = container_id
        self._timeout_seconds = timeout_seconds

    @property
    def id(self) -> str:
        return self._container_id

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        completed = _docker(
            "exec",
            self._container_id,
            "/bin/sh",
            "-lc",
            command,
            timeout=timeout or self._timeout_seconds,
        )
        return ExecuteResponse(
            output=_combined_output(completed),
            exit_code=completed.returncode,
        )

    def upload_files(
        self, files: list[tuple[str, bytes]]
    ) -> list[FileUploadResponse]:
        responses: list[FileUploadResponse] = []
        for path, content in files:
            completed = _docker(
                "exec",
                "-i",
                self._container_id,
                "python3",
                "-c",
                _UPLOAD_SCRIPT,
                path,
                input_bytes=base64.b64encode(content),
                timeout=self._timeout_seconds,
            )
            responses.append(
                FileUploadResponse(
                    path=path,
                    error=None if completed.returncode == 0 else _combined_output(completed),
                )
            )
        return responses

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        responses: list[FileDownloadResponse] = []
        for path in paths:
            completed = _docker(
                "exec",
                self._container_id,
                "python3",
                "-c",
                _DOWNLOAD_SCRIPT,
                path,
                timeout=self._timeout_seconds,
            )
            if completed.returncode != 0:
                responses.append(
                    FileDownloadResponse(path=path, error=_combined_output(completed))
                )
                continue
            try:
                content = base64.b64decode(completed.stdout, validate=True)
            except ValueError as error:
                responses.append(FileDownloadResponse(path=path, error=str(error)))
            else:
                responses.append(FileDownloadResponse(path=path, content=content))
        return responses


class DockerSandboxFactory:
    """Own an ephemeral, least-privilege container for one Deep Agent invocation."""

    def __init__(
        self,
        *,
        image: str = _DEFAULT_IMAGE,
        memory: str = "256m",
        cpus: str = "0.5",
        timeout_seconds: int = 120,
        workspace_root: Path | None = None,
    ) -> None:
        self._image = image
        self._memory = memory
        self._cpus = cpus
        self._timeout_seconds = timeout_seconds
        self._workspace_root = (workspace_root or Path(".sandbox-workspaces")).resolve()
        self._workspace_root.mkdir(parents=True, exist_ok=True)

    def __call__(
        self,
        binding: DeepAgentExecutionBinding,
        _secrets: Mapping[str, str],
    ):
        component = binding.sandbox
        if component.backend != "docker":
            raise DeepAgentUnsupportedPlacement("Docker factory cannot change placement")

        @asynccontextmanager
        async def context():
            name = f"belllabs-deep-agent-{uuid.uuid4().hex[:12]}"
            workspace_path = self.workspace_path(binding)
            workspace_path.mkdir(parents=True, exist_ok=True)
            mounts: list[str] = []
            for read_mount in binding.workspace.read_mounts:
                source = self._durable_workspace_path(read_mount.durable_ref)
                if not source.is_dir():
                    raise DeepAgentUnsupportedPlacement(
                        "Docker sandbox durable read workspace is unavailable"
                    )
                mounts.extend(
                    (
                        "--mount",
                        "type=bind,source="
                        f"{source},target={read_mount.logical_path},readonly",
                    )
                )
            for target in binding.workspace.exclusive_write_paths:
                mounts.extend(
                    (
                        "--mount",
                        f"type=bind,source={workspace_path},target={target}",
                    )
                )
            started = _docker(
                "run",
                "--detach",
                "--name",
                name,
                "--network",
                "none",
                "--read-only",
                "--tmpfs",
                "/workspace:rw,nosuid,nodev,size=64m",
                "--tmpfs",
                "/skills:rw,nosuid,nodev,size=16m",
                "--tmpfs",
                "/tmp:rw,nosuid,nodev,size=16m",
                "--workdir",
                "/workspace",
                "--memory",
                self._memory,
                "--cpus",
                self._cpus,
                "--pids-limit",
                "64",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                *mounts,
                self._image,
                "sleep",
                "infinity",
                timeout=self._timeout_seconds,
            )
            if started.returncode != 0:
                raise DeepAgentUnsupportedPlacement(
                    f"Docker sandbox failed to start: {_combined_output(started)}"
                )
            container_id = started.stdout.decode().strip()
            try:
                yield DockerSandbox(
                    container_id,
                    timeout_seconds=self._timeout_seconds,
                )
            finally:
                _docker(
                    "rm",
                    "--force",
                    container_id,
                    timeout=self._timeout_seconds,
                )

        return context()

    def workspace_path(self, binding: DeepAgentExecutionBinding) -> Path:
        identity = sha256_digest(
            {
                "workspace_id": binding.workspace.workspace_id,
                "namespace_id": binding.workspace.namespace_id,
            }
        ).removeprefix("sha256:")
        return self._workspace_root / identity

    def _durable_workspace_path(self, durable_ref: str) -> Path:
        prefix = "workspace://"
        identity = durable_ref.removeprefix(prefix)
        if (
            not durable_ref.startswith(prefix)
            or len(identity) != 64
            or any(character not in "0123456789abcdef" for character in identity)
        ):
            raise DeepAgentUnsupportedPlacement(
                "Docker sandbox read mount is not a governed workspace reference"
            )
        return self._workspace_root / identity


def _docker(
    *arguments: str,
    input_bytes: bytes | None = None,
    timeout: int,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["docker", *arguments],
        input=input_bytes,
        capture_output=True,
        check=False,
        timeout=timeout,
    )


def _combined_output(completed: subprocess.CompletedProcess[bytes]) -> str:
    parts: Sequence[bytes] = (completed.stdout, completed.stderr)
    return "\n".join(
        part.decode("utf-8", errors="replace").strip() for part in parts if part.strip()
    )


__all__ = ["DockerSandbox", "DockerSandboxFactory"]
