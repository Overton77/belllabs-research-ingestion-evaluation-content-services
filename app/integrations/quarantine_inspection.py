from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.application.web_research.external_candidate_inspection import (
    InspectionFinding,
    QuarantineInspectionExecution,
    QuarantineInspectionObservations,
)
from app.domain.control_plane.canonical import sha256_digest


class QuarantineInspectionDependencyError(RuntimeError):
    pass


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StaticCandidateFile(_Contract):
    path: str = Field(min_length=1, max_length=500)
    content: bytes = Field(max_length=10_000_000)

    @field_validator("path")
    @classmethod
    def path_is_relative_and_normalized(cls, value: str) -> str:
        path = PurePosixPath(value.replace("\\", "/"))
        if path.is_absolute() or ".." in path.parts or str(path) != value.replace("\\", "/"):
            raise ValueError("candidate inspection file path must be normalized and relative")
        return str(path)


class StaticCandidatePayload(_Contract):
    source_ref: str = Field(
        pattern=r"^(?:mongodb|belllabs|s3|gs|az)://",
        max_length=2_048,
    )
    metadata: dict[str, str | None]
    files: tuple[StaticCandidateFile, ...] = Field(default=(), max_length=2_000)
    provenance_verified: bool = False
    license_evidence: tuple[str, ...] = Field(default=(), max_length=32)
    secret_requirement_names: frozenset[str] = Field(default_factory=frozenset)
    network_requirement_hosts: frozenset[str] = Field(default_factory=frozenset)


class StaticScanOutput(_Contract):
    manifest_valid: bool
    findings: tuple[InspectionFinding, ...] = Field(max_length=500)
    downloaded_bytes: int = Field(ge=0)
    files_inspected: int = Field(ge=0)
    largest_file_bytes: int = Field(ge=0)
    network_requests: int = Field(ge=0)
    network_hosts_contacted: frozenset[str] = Field(max_length=32)
    tools_list_probe_used: bool


class StaticCandidatePayloadPort(Protocol):
    async def load(
        self,
        execution: QuarantineInspectionExecution,
    ) -> StaticCandidatePayload: ...


class SanitizedCandidateMetadataPayloadProvider:
    """Expose only already-governed, sanitized Mongo candidate metadata."""

    async def load(
        self,
        execution: QuarantineInspectionExecution,
    ) -> StaticCandidatePayload:
        candidate = execution.candidate.candidate
        source_ref = candidate.raw_response_ref
        if source_ref is None or not source_ref.startswith("mongodb://"):
            raise QuarantineInspectionDependencyError(
                "candidate has no governed sanitized metadata reference"
            )
        return StaticCandidatePayload(
            source_ref=source_ref,
            metadata={
                "source": candidate.source.value,
                "upstream_identity": candidate.upstream_identity,
                "upstream_version": candidate.upstream_version,
                "locator": candidate.locator,
                "publisher": candidate.publisher,
                "upstream_status": candidate.upstream_status,
            },
        )


class QuarantineSubprocessRequest(_Contract):
    executable: Path
    arguments: tuple[str, ...]
    working_directory: Path
    environment: dict[str, str]
    timeout_seconds: float = Field(ge=1, le=120)
    max_output_bytes: int = Field(ge=1_024)


class QuarantineSubprocessResult(_Contract):
    exit_code: int
    stdout: bytes
    stderr: bytes


class QuarantineSubprocessRunner(Protocol):
    async def run(
        self,
        request: QuarantineSubprocessRequest,
    ) -> QuarantineSubprocessResult: ...


class AsyncioQuarantineSubprocessRunner:
    async def run(
        self,
        request: QuarantineSubprocessRequest,
    ) -> QuarantineSubprocessResult:
        process = await asyncio.create_subprocess_exec(
            str(request.executable),
            *request.arguments,
            cwd=request.working_directory,
            env=request.environment,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert process.stdout is not None
        assert process.stderr is not None
        try:
            stdout, stderr, exit_code = await asyncio.wait_for(
                _collect_bounded_output(process, request.max_output_bytes),
                timeout=request.timeout_seconds,
            )
        except TimeoutError as error:
            process.kill()
            await process.wait()
            raise QuarantineInspectionDependencyError(
                "trusted static inspection process timed out"
            ) from error
        except asyncio.CancelledError:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        return QuarantineSubprocessResult(
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )


class StaticQuarantineInspectionRunner:
    """Run a trusted, stdlib-only static scanner; never execute candidate content."""

    def __init__(
        self,
        *,
        payloads: StaticCandidatePayloadPort,
        process_runner: QuarantineSubprocessRunner,
        python_executable: Path,
        scanner_script: Path,
        workspace_root: Path,
    ) -> None:
        self._payloads = payloads
        self._process_runner = process_runner
        self._python_executable = python_executable.resolve(strict=True)
        self._scanner_script = scanner_script.resolve(strict=True)
        self._workspace_root = workspace_root.resolve()
        self._workspace_root.mkdir(parents=True, exist_ok=True)

    async def inspect(
        self,
        execution: QuarantineInspectionExecution,
    ) -> QuarantineInspectionObservations:
        _require_non_executing_authority(execution)
        payload = await self._payloads.load(execution)
        bounds = execution.workspace.bounds
        total_bytes = sum(len(item.content) for item in payload.files)
        largest = max((len(item.content) for item in payload.files), default=0)
        if (
            len(payload.files) > bounds.max_files
            or total_bytes > bounds.max_download_bytes
            or largest > bounds.max_file_bytes
            or payload.network_requirement_hosts - execution.workspace.network_host_allowlist
        ):
            raise QuarantineInspectionDependencyError(
                "candidate payload exceeds the immutable inspection envelope"
            )
        request_payload = {
            "source_ref": payload.source_ref,
            "metadata": payload.metadata,
            "files": [
                {
                    "path": item.path,
                    "content_base64": base64.b64encode(item.content).decode("ascii"),
                }
                for item in payload.files
            ],
        }
        with tempfile.TemporaryDirectory(
            prefix="inspection-",
            dir=self._workspace_root,
        ) as directory:
            workspace = Path(directory)
            request_path = workspace / "scan-request.json"
            await asyncio.to_thread(
                request_path.write_text,
                json.dumps(request_payload, sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )
            request = QuarantineSubprocessRequest(
                executable=self._python_executable,
                arguments=(
                    "-I",
                    "-S",
                    str(self._scanner_script),
                    "--input",
                    str(request_path),
                ),
                working_directory=workspace,
                environment=_sanitized_environment(workspace),
                timeout_seconds=bounds.timeout_seconds,
                max_output_bytes=bounds.max_report_bytes,
            )
            result = await self._process_runner.run(request)
        if len(result.stdout) + len(result.stderr) > bounds.max_report_bytes:
            raise QuarantineInspectionDependencyError(
                "trusted static inspection output exceeded its configured bound"
            )
        if result.exit_code != 0:
            raise QuarantineInspectionDependencyError(
                f"trusted static inspection exited with code {result.exit_code}"
            )
        try:
            scanned = StaticScanOutput.model_validate_json(result.stdout.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as error:
            raise QuarantineInspectionDependencyError(
                "trusted static inspection returned an invalid bounded report"
            ) from error
        immutable_digest = sha256_digest(
            {
                "source_ref": payload.source_ref,
                "metadata": payload.metadata,
                "files": [
                    {
                        "path": item.path,
                        "digest": f"sha256:{hashlib.sha256(item.content).hexdigest()}",
                    }
                    for item in payload.files
                ],
            }
        )
        return QuarantineInspectionObservations(
            manifest_valid=scanned.manifest_valid,
            provenance_verified=payload.provenance_verified,
            immutable_content_digest=immutable_digest,
            license_evidence=payload.license_evidence,
            secret_requirement_names=payload.secret_requirement_names,
            network_requirement_hosts=payload.network_requirement_hosts,
            network_hosts_contacted=scanned.network_hosts_contacted,
            requested_capabilities=execution.requested_capabilities,
            findings=scanned.findings,
            downloaded_bytes=scanned.downloaded_bytes,
            files_inspected=scanned.files_inspected,
            largest_file_bytes=scanned.largest_file_bytes,
            network_requests=scanned.network_requests,
            report_size_bytes=len(result.stdout) + len(result.stderr),
            tools_list_probe_used=scanned.tools_list_probe_used,
        )


def _require_non_executing_authority(
    execution: QuarantineInspectionExecution,
) -> None:
    workspace = execution.workspace
    if (
        execution.install_allowed
        or execution.execute_candidate_allowed
        or workspace.install_allowed
        or workspace.execute_candidate_allowed
        or workspace.candidate_bundle_mounted
        or workspace.agent_environment_mounted
        or not workspace.inputs_read_only
    ):
        raise QuarantineInspectionDependencyError(
            "static inspection received executable candidate authority"
        )


def _sanitized_environment(workspace: Path) -> dict[str, str]:
    allowed: dict[str, str] = {}
    for name in ("SYSTEMROOT", "WINDIR"):
        value = os.environ.get(name)
        if value:
            allowed[name] = value
    allowed.update(
        {
            "USERPROFILE": str(workspace),
            "TEMP": str(workspace),
            "TMP": str(workspace),
            "BELLABS_QUARANTINE_ROOT": str(workspace),
            "NO_PROXY": "*",
            "HTTP_PROXY": "http://127.0.0.1:9",
            "HTTPS_PROXY": "http://127.0.0.1:9",
            "ALL_PROXY": "http://127.0.0.1:9",
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    return allowed


async def _collect_bounded_output(
    process: asyncio.subprocess.Process,
    max_output_bytes: int,
) -> tuple[bytes, bytes, int]:
    assert process.stdout is not None
    assert process.stderr is not None
    combined_size = 0
    size_lock = asyncio.Lock()

    async def read(stream: asyncio.StreamReader) -> bytes:
        nonlocal combined_size
        chunks: list[bytes] = []
        while chunk := await stream.read(8_192):
            async with size_lock:
                combined_size += len(chunk)
                if combined_size > max_output_bytes:
                    raise QuarantineInspectionDependencyError(
                        "trusted static inspection output exceeded its configured bound"
                    )
            chunks.append(chunk)
        return b"".join(chunks)

    try:
        stdout, stderr, exit_code = await asyncio.gather(
            read(process.stdout),
            read(process.stderr),
            process.wait(),
        )
    except Exception:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise
    return stdout, stderr, exit_code


class InMemoryStaticCandidatePayloadProvider:
    def __init__(
        self,
        payloads: Mapping[str, StaticCandidatePayload],
    ) -> None:
        self._payloads = dict(payloads)

    async def load(
        self,
        execution: QuarantineInspectionExecution,
    ) -> StaticCandidatePayload:
        try:
            return self._payloads[execution.candidate.candidate.candidate_id]
        except KeyError as error:
            raise QuarantineInspectionDependencyError("candidate payload is unavailable") from error


__all__ = [
    "AsyncioQuarantineSubprocessRunner",
    "InMemoryStaticCandidatePayloadProvider",
    "QuarantineInspectionDependencyError",
    "QuarantineSubprocessRequest",
    "QuarantineSubprocessResult",
    "QuarantineSubprocessRunner",
    "SanitizedCandidateMetadataPayloadProvider",
    "StaticCandidateFile",
    "StaticCandidatePayload",
    "StaticCandidatePayloadPort",
    "StaticQuarantineInspectionRunner",
    "StaticScanOutput",
]
