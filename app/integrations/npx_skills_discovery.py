from __future__ import annotations

import asyncio
import os
import re
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from app.application.web_research.external_capability_discovery import (
    ExternalDiscoveryBatch,
    ExternalDiscoveryCandidate,
    ExternalDiscoveryEvidence,
    ExternalDiscoverySource,
)

_EXACT_VERSION = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
_SKILLS_URL = re.compile(
    r"https://skills\.sh/"
    r"(?P<owner>[A-Za-z0-9_.-]+)/"
    r"(?P<repository>[A-Za-z0-9_.-]+)/"
    r"(?P<skill>[A-Za-z0-9_.-]+)"
)
_ANSI_ESCAPE = re.compile(r"\x1B(?:[@-_][0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))")


class SkillDiscoveryContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SkillDiscoverySubprocessRequest(SkillDiscoveryContract):
    executable: str = Field(min_length=1)
    arguments: tuple[str, ...]
    working_directory: Path
    environment: dict[str, str]
    timeout_seconds: float = Field(ge=1, le=120)
    max_output_bytes: int = Field(ge=1_024)


class SkillDiscoverySubprocessResult(SkillDiscoveryContract):
    exit_code: int
    stdout: bytes
    stderr: bytes


class SkillDiscoverySubprocessRunner(Protocol):
    async def run(
        self,
        request: SkillDiscoverySubprocessRequest,
    ) -> SkillDiscoverySubprocessResult: ...


class SkillDiscoveryDependencyError(RuntimeError):
    pass


class AsyncioSkillDiscoverySubprocessRunner:
    """Execute without a shell and stop when time or combined output is exhausted."""

    async def run(
        self,
        request: SkillDiscoverySubprocessRequest,
    ) -> SkillDiscoverySubprocessResult:
        process = await asyncio.create_subprocess_exec(
            request.executable,
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
                _collect_bounded_output(
                    process,
                    request.max_output_bytes,
                ),
                timeout=request.timeout_seconds,
            )
        except TimeoutError as error:
            process.kill()
            await process.wait()
            raise SkillDiscoveryDependencyError("pinned npx skills discovery timed out") from error
        except Exception:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        return SkillDiscoverySubprocessResult(
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )


class NpxSkillsDiscoveryAdapter:
    """Run only the pinned `skills find` command and parse candidate locators."""

    def __init__(
        self,
        runner: SkillDiscoverySubprocessRunner,
        *,
        executable: str,
        package_version: str,
        timeout_seconds: float,
        max_output_bytes: int,
        working_directory_root: Path | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not _EXACT_VERSION.fullmatch(package_version):
            raise ValueError("npx skills package version must be an exact SemVer")
        self._runner = runner
        self._executable = executable
        self._package_version = package_version
        self._timeout_seconds = timeout_seconds
        self._max_output_bytes = max_output_bytes
        self._working_directory_root = (
            working_directory_root.resolve() if working_directory_root is not None else None
        )
        if self._working_directory_root is not None:
            self._working_directory_root.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(UTC))

    async def search(
        self,
        query: str,
        *,
        limit: int,
        owner: str | None = None,
    ) -> ExternalDiscoveryBatch:
        normalized_query = query.strip()
        if not normalized_query or len(normalized_query) > 500:
            raise ValueError("skill discovery query must contain 1 to 500 characters")
        if limit < 1 or limit > 100:
            raise ValueError("skill discovery limit must be between 1 and 100")
        normalized_owner = owner.strip() if owner is not None else None
        if normalized_owner == "":
            normalized_owner = None
        if normalized_owner is not None and not re.fullmatch(
            r"[A-Za-z0-9_.-]+",
            normalized_owner,
        ):
            raise ValueError("skill discovery owner is invalid")

        with tempfile.TemporaryDirectory(
            prefix="belllabs-skills-discovery-",
            dir=self._working_directory_root,
        ) as directory:
            working_directory = Path(directory)
            arguments = (
                "--yes",
                f"skills@{self._package_version}",
                "find",
                normalized_query,
                *(("--owner", normalized_owner) if normalized_owner else ()),
            )
            request = SkillDiscoverySubprocessRequest(
                executable=self._executable,
                arguments=arguments,
                working_directory=working_directory,
                environment=_sanitized_environment(working_directory),
                timeout_seconds=self._timeout_seconds,
                max_output_bytes=self._max_output_bytes,
            )
            result = await self._runner.run(request)

        if len(result.stdout) + len(result.stderr) > self._max_output_bytes:
            raise SkillDiscoveryDependencyError(
                "pinned npx skills discovery exceeded the configured output limit"
            )
        raw = result.stdout + b"\n---stderr---\n" + result.stderr
        raw_digest = _digest(raw)
        discovered_at = self._clock()
        evidence = ExternalDiscoveryEvidence(
            source=ExternalDiscoverySource.NPX_SKILLS,
            source_version=f"skills@{self._package_version}",
            query=normalized_query,
            retrieved_at=discovered_at,
            raw_response_digest=raw_digest,
            raw_response_size_bytes=len(raw),
            exit_code=result.exit_code,
            stderr_size_bytes=len(result.stderr),
        )
        if result.exit_code != 0:
            raise SkillDiscoveryDependencyError(
                f"pinned npx skills discovery exited with code {result.exit_code}"
            )
        candidates = _parse_candidates(
            result.stdout,
            query=normalized_query,
            discovered_at=discovered_at,
            raw_response_digest=raw_digest,
            owner=normalized_owner,
            limit=limit,
        )
        return ExternalDiscoveryBatch(
            source=ExternalDiscoverySource.NPX_SKILLS,
            candidates=candidates,
            evidence=(evidence,),
        )


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
                    raise SkillDiscoveryDependencyError(
                        "pinned npx skills discovery exceeded the configured output limit"
                    )
            chunks.append(chunk)
        return b"".join(chunks)

    stdout, stderr, exit_code = await asyncio.gather(
        read(process.stdout),
        read(process.stderr),
        process.wait(),
    )
    return stdout, stderr, exit_code


def _sanitized_environment(working_directory: Path) -> dict[str, str]:
    allowed = ("PATH", "PATHEXT", "SYSTEMROOT", "COMSPEC")
    environment = {key: os.environ[key] for key in allowed if key in os.environ}
    environment.update(
        {
            "CI": "1",
            "NO_COLOR": "1",
            "npm_config_cache": str(working_directory / ".npm-cache"),
            "npm_config_update_notifier": "false",
        }
    )
    return environment


def _parse_candidates(
    output: bytes,
    *,
    query: str,
    discovered_at: datetime,
    raw_response_digest: str,
    owner: str | None,
    limit: int,
) -> tuple[ExternalDiscoveryCandidate, ...]:
    text = _ANSI_ESCAPE.sub("", output.decode("utf-8", errors="replace"))
    candidates: list[ExternalDiscoveryCandidate] = []
    seen: set[str] = set()
    for match in _SKILLS_URL.finditer(text):
        matched_owner = match.group("owner")
        if owner is not None and matched_owner.casefold() != owner.casefold():
            continue
        identity = f"{matched_owner}/{match.group('repository')}/{match.group('skill')}"
        if identity.casefold() in seen:
            continue
        seen.add(identity.casefold())
        locator = match.group(0)
        identity_digest = _digest(
            (f"{ExternalDiscoverySource.NPX_SKILLS.value}\0{identity}").encode()
        )
        candidates.append(
            ExternalDiscoveryCandidate(
                candidate_id=f"candidate:{identity_digest}",
                source=ExternalDiscoverySource.NPX_SKILLS,
                upstream_identity=identity,
                locator=locator,
                publisher=matched_owner,
                discovered_at=discovered_at,
                query=query,
                raw_response_digest=raw_response_digest,
            )
        )
        if len(candidates) >= limit:
            break
    return tuple(candidates)


def _digest(value: bytes) -> str:
    return f"sha256:{sha256(value).hexdigest()}"
