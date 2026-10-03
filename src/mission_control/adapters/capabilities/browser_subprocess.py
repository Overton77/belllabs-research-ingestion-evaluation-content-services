"""Bounded, framework-neutral subprocess transport for admitted browser capabilities."""

from __future__ import annotations

import asyncio
import os
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class BrowserSubprocessError(RuntimeError):
    """The admitted browser subprocess violated its execution boundary."""


class BrowserSubprocessRequest:
    def __init__(
        self,
        *,
        executable: Path,
        arguments: tuple[str, ...],
        working_directory: Path,
        environment: Mapping[str, str],
        timeout_seconds: float,
        max_output_bytes: int,
    ) -> None:
        self.executable = executable
        self.arguments = arguments
        self.working_directory = working_directory
        self.environment = dict(environment)
        self.timeout_seconds = timeout_seconds
        self.max_output_bytes = max_output_bytes


@dataclass(frozen=True)
class BrowserSubprocessResult:
    exit_code: int
    stdout: bytes
    stderr: bytes


class BrowserSubprocessRunner(Protocol):
    async def run(
        self,
        request: BrowserSubprocessRequest,
    ) -> BrowserSubprocessResult: ...


class AsyncioBrowserSubprocessRunner:
    """Run a pinned browser argv directly with bounded output and no shell."""

    async def run(
        self,
        request: BrowserSubprocessRequest,
    ) -> BrowserSubprocessResult:
        if os.name == "nt":
            try:
                return await asyncio.to_thread(_run_windows_browser_command, request)
            except subprocess.TimeoutExpired as error:
                raise BrowserSubprocessError("pinned agent-browser command timed out") from error
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
            async with asyncio.timeout(request.timeout_seconds):
                stdout, stderr, exit_code = await _collect_bounded_output(
                    process,
                    request.max_output_bytes,
                )
        except TimeoutError as error:
            process.kill()
            await process.wait()
            raise BrowserSubprocessError("pinned agent-browser command timed out") from error
        except BaseException:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        return BrowserSubprocessResult(
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )


def _run_windows_browser_command(
    request: BrowserSubprocessRequest,
) -> BrowserSubprocessResult:
    """Avoid Windows Proactor pipe inheritance across the browser daemon."""

    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        completed = subprocess.run(
            [str(request.executable), *request.arguments],
            cwd=request.working_directory,
            env=request.environment,
            stdin=subprocess.DEVNULL,
            stdout=stdout_file,
            stderr=stderr_file,
            timeout=request.timeout_seconds,
            check=False,
        )
        stdout_file.seek(0)
        stderr_file.seek(0)
        stdout = stdout_file.read(request.max_output_bytes + 1)
        remaining = max(0, request.max_output_bytes + 1 - len(stdout))
        stderr = stderr_file.read(remaining)
    if len(stdout) + len(stderr) > request.max_output_bytes:
        raise BrowserSubprocessError("pinned agent-browser exceeded its configured output limit")
    return BrowserSubprocessResult(
        exit_code=completed.returncode,
        stdout=stdout,
        stderr=stderr,
    )


async def _collect_bounded_output(
    process: asyncio.subprocess.Process,
    max_output_bytes: int,
) -> tuple[bytes, bytes, int]:
    assert process.stdout is not None
    assert process.stderr is not None
    combined_size = 0
    size_lock = asyncio.Lock()
    stdout_buffer = bytearray()
    stderr_buffer = bytearray()

    async def read(stream: asyncio.StreamReader, buffer: bytearray) -> None:
        nonlocal combined_size
        while chunk := await stream.read(8_192):
            async with size_lock:
                combined_size += len(chunk)
                if combined_size > max_output_bytes:
                    raise BrowserSubprocessError(
                        "pinned agent-browser exceeded its configured output limit"
                    )
            buffer.extend(chunk)

    readers = (
        asyncio.create_task(read(process.stdout, stdout_buffer)),
        asyncio.create_task(read(process.stderr, stderr_buffer)),
    )
    try:
        exit_code = await process.wait()
        try:
            await asyncio.wait_for(asyncio.gather(*readers), timeout=1.0)
        except TimeoutError:
            # The Windows agent-browser daemon can inherit the command's pipe
            # handles after the command process exits. Its output is complete,
            # but EOF will not arrive until the daemon closes. Bound the drain
            # instead of converting successful commands into false timeouts.
            for reader in readers:
                reader.cancel()
            await asyncio.gather(*readers, return_exceptions=True)
    except BaseException:
        for reader in readers:
            reader.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        raise
    return bytes(stdout_buffer), bytes(stderr_buffer), exit_code
