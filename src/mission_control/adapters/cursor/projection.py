"""Host Projections and the Context Packet placed into a leased Cursor workspace (FT-G3).

SPEC-07 section 5.1: the lane only *places* what SPEC-01 renders. `ProjectionSource` yields the
`HostProjection` of the binding's exact catalog pins for a lane profile (FT-A4
`render_host_files`: `AGENTS.md`, `.cursor/rules/mc-mission.mdc` with `alwaysApply`,
`.cursor/skills`, `.cursor/agents`, `.cursor/mcp.json` with secret references only,
`.cursor/hooks.json` with Kernel Hooks first and fail-closed). The packet's verified bytes are
materialized under `.mission/` and `inputs/`, `.mission/operating-contract.md` carries the
operating contract (the instruction channel, since the Python SDK has no system prompt), and
an empty `outputs/` receives declared outputs.

`projection_digests` are recorded on the Materialization; a digest that differs from the
binding's pin is `CAPABILITY_DRIFT` and nothing runs.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Final, Protocol

from mission_control.application.agentic_components.projections import render_host_files
from mission_control.application.capabilities.catalog import resolved_capability
from mission_control.domain.agentic_components.projection import (
    DEFAULT_KERNEL_HOOKS,
    HostProjection,
    KernelHook,
    ProjectedFile,
    ResolvedCapability,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.capabilities.pins import CapabilityPin
from mission_control.domain.context.render import (
    CONTEXT_INDEX_PATH,
    bytes_digest,
    is_context_input_slot,
)
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    PromptTrustClass,
)
from mission_control.domain.execution.lanes import CursorExecutionBinding

CAPABILITY_DRIFT: Final = "CAPABILITY_DRIFT"
UNSUPPORTED_BEHAVIOR: Final = "UNSUPPORTED_BEHAVIOR"
OPERATING_CONTRACT_PATH: Final = ".mission/operating-contract.md"
KERNEL_HOOK_PATH: Final = ".mission/hooks/kernel.py"
HOOK_CONTEXT_PATH: Final = ".mission/hooks/context.json"
TOKEN_PATH: Final = ".mission/bin/.token"  # noqa: S105 - a file path, not a secret
STATE_ROOT: Final = ".mission/state"
OUTPUTS_DIR: Final = "outputs"
TURN_TEXT_LIMIT: Final = 32_000
MISSION_CONTEXT_POINTER: Final = (
    "Mission context: read .mission/context.md (the context index) and .mission/inputs.json "
    "before acting; inputs are under inputs/ (read-only); write declared outputs under outputs/."
)
_INSTRUCTION_CLASSES = {PromptTrustClass.SYSTEM_AUTHORITY, PromptTrustClass.AUTHORED_INSTRUCTION}


class LaneProjectionError(ValueError):
    """Preparation refused: `code` is `CAPABILITY_DRIFT` or `UNSUPPORTED_BEHAVIOR`."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


class ProjectionSource(Protocol):
    async def project(
        self, operation: OperationExecutionRequest, *, profile: str, packet_index: str | None
    ) -> HostProjection: ...


class DurableInputReader(Protocol):
    async def retrieve(self, durable_ref: str) -> bytes: ...


RowsResolver = Callable[[OperationExecutionRequest], Awaitable[Sequence[ResolvedCapability]]]


def operating_contract(operation: OperationExecutionRequest) -> str:
    """The operation's authority and authored instructions: the lane's instruction channel."""

    parts = [
        segment.content.strip()
        for segment in operation.prompt_segments
        if segment.trust_class in _INSTRUCTION_CLASSES and segment.content.strip()
    ]
    # The packet index changes per turn; the pinned projection points at it on disk instead of
    # inlining it, so the rule and AGENTS.md digests stay those of the binding.
    parts.append(MISSION_CONTEXT_POINTER)
    return "\n\n".join(parts)


def turn_text(operation: OperationExecutionRequest, *, limit: int = TURN_TEXT_LIMIT) -> str:
    """The first send: the admitted inputs plus the pointer to the packet index on disk.

    Instructions travel through `AGENTS.md` and the always-apply rule, never inline secrets.
    """

    inputs = [
        segment.content.strip()
        for segment in operation.prompt_segments
        if segment.trust_class not in _INSTRUCTION_CLASSES and segment.content.strip()
    ]
    pointer = (
        "Read .mission/context.md (the mission's context index) and "
        f"{OPERATING_CONTRACT_PATH} before acting; write declared outputs under outputs/."
    )
    body = "\n\n".join(inputs)
    if len(body) > limit:
        body = body[:limit] + "\n\n[truncated: the full inputs are listed in .mission/context.md]"
    return f"{pointer}\n\n{body}".strip()


class RenderedProjectionSource:
    """Renders the FT-A4 Host Projection of resolved rows for a lane profile."""

    def __init__(
        self, rows: RowsResolver, *, kernel_hooks: Sequence[KernelHook] = DEFAULT_KERNEL_HOOKS
    ) -> None:
        self._rows = rows
        self._kernel_hooks = tuple(kernel_hooks)

    async def project(
        self, operation: OperationExecutionRequest, *, profile: str, packet_index: str | None
    ) -> HostProjection:
        rows = await self._rows(operation)
        return render_host_files(
            rows,
            profile,
            operating_contract(operation),
            packet_index,
            self._kernel_hooks,
        )


class CatalogRows:
    """Resolve a Cursor binding's exact catalog pins (skills, MCP servers, subagents)."""

    def __init__(self, definitions: object, *, custody: object | None = None) -> None:
        self._definitions = definitions
        self._custody = custody

    async def __call__(self, operation: OperationExecutionRequest) -> Sequence[ResolvedCapability]:
        binding = operation.cursor_binding
        if binding is None:
            return ()
        pins = (
            *binding.projections.skills,
            *binding.projections.mcp_servers,
            *binding.inline_subagents,
        )
        rows: list[ResolvedCapability] = []
        for text in dict.fromkeys(pins):
            rows.append(
                await resolved_capability(
                    self._definitions,
                    CapabilityPin.parse(text),
                    custody=self._custody,  # type: ignore[arg-type]
                )
            )
        return tuple(rows)


def static_rows(rows: Sequence[ResolvedCapability]) -> RowsResolver:
    async def resolve(_operation: OperationExecutionRequest) -> Sequence[ResolvedCapability]:
        return tuple(rows)

    return resolve


def _file(projection: HostProjection, path: str) -> bytes | None:
    try:
        return projection.file(path).content
    except KeyError:
        return None


def projection_digests(projection: HostProjection) -> dict[str, str]:
    """The digests a Cursor binding pins: the rule, the agents (with `AGENTS.md`) and hooks."""

    agents = sorted(
        (item.path, bytes_digest(item.content))
        for item in projection.files
        if item.path.startswith(".cursor/agents/") or item.path == "AGENTS.md"
    )
    return {
        "rules_digest": bytes_digest(_file(projection, ".cursor/rules/mc-mission.mdc") or b""),
        "agents_digest": sha256_digest([list(item) for item in agents]),
        "hooks_digest": bytes_digest(_file(projection, ".cursor/hooks.json") or b""),
    }


def verify_projection(binding: CursorExecutionBinding, digests: Mapping[str, str]) -> None:
    pinned = binding.projections
    for name, expected in (
        ("rules_digest", pinned.rules_digest),
        ("agents_digest", pinned.agents_digest),
        ("hooks_digest", pinned.hooks_digest),
    ):
        if digests.get(name) != expected:
            raise LaneProjectionError(
                CAPABILITY_DRIFT, f"{name} of the rendered projection differs from the binding"
            )


def safe_relative(path: str) -> PurePosixPath:
    normalized = PurePosixPath(path.replace("\\", "/").lstrip("/"))
    if not normalized.parts or ".." in normalized.parts:
        raise LaneProjectionError(CAPABILITY_DRIFT, f"unsafe workspace path: {path}")
    return normalized


def _write(root: Path, relative: PurePosixPath, content: bytes, mode: int) -> None:
    target = root.joinpath(*relative.parts)
    resolved_root = root.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if resolved_root not in target.parent.resolve().parents and target.parent.resolve() != (
        resolved_root
    ):
        raise LaneProjectionError(CAPABILITY_DRIFT, f"path escapes the workspace: {relative}")
    target.write_bytes(content)
    if os.name != "nt":
        target.chmod(mode)


def write_files(root: Path, files: Iterable[ProjectedFile]) -> tuple[str, ...]:
    written: list[str] = []
    for item in files:
        relative = safe_relative(item.path)
        _write(root, relative, item.content, item.mode)
        written.append(relative.as_posix())
    return tuple(written)


def write_secret(root: Path, path: str, value: str) -> None:
    """Write a task token readable by the workspace owner only (mode 0600 where supported)."""

    relative = safe_relative(path)
    target = root.joinpath(*relative.parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(value)


async def materialize_packet(
    root: Path,
    operation: OperationExecutionRequest,
    reader: DurableInputReader | None,
    *,
    mount_root: str = "",
) -> dict[str, str]:
    """Write the packet's verified read-only files (`.mission/`, `inputs/`), the operating
    contract and an empty `outputs/`; return path -> digest of what was written."""

    written: dict[str, str] = {}
    for slot in operation.workspace.slot_bindings:
        if not is_context_input_slot(slot):
            continue
        if reader is None:
            raise LaneProjectionError(
                UNSUPPORTED_BEHAVIOR, "context inputs are bound but no durable reader is composed"
            )
        assert slot.durable_ref is not None and slot.content_digest is not None
        content = await reader.retrieve(slot.durable_ref)
        if bytes_digest(content) != slot.content_digest:
            raise LaneProjectionError(
                CAPABILITY_DRIFT, f"context input digest mismatch for {slot.logical_path}"
            )
        logical = slot.logical_path
        if mount_root and logical.startswith(mount_root.rstrip("/") + "/"):
            logical = logical[len(mount_root.rstrip("/")) :]
        relative = safe_relative(logical)
        await asyncio.to_thread(_write, root, relative, content, 0o444)
        written[relative.as_posix()] = slot.content_digest
    contract = operating_contract(operation).encode("utf-8")
    await asyncio.to_thread(_write, root, safe_relative(OPERATING_CONTRACT_PATH), contract, 0o444)
    written[OPERATING_CONTRACT_PATH] = bytes_digest(contract)
    await asyncio.to_thread((root / OUTPUTS_DIR).mkdir, parents=True, exist_ok=True)
    return written


def read_packet_index(root: Path) -> str | None:
    target = root / CONTEXT_INDEX_PATH
    return target.read_text(encoding="utf-8") if target.is_file() else None


def hook_context_index(context: object) -> str | None:
    """`sessionStart` additional context: the lease's packet index (bounded by the caller)."""

    root = getattr(context, "workspace_root", None)
    return read_packet_index(Path(root)) if root else None


__all__ = [
    "CAPABILITY_DRIFT",
    "HOOK_CONTEXT_PATH",
    "KERNEL_HOOK_PATH",
    "OPERATING_CONTRACT_PATH",
    "OUTPUTS_DIR",
    "STATE_ROOT",
    "TOKEN_PATH",
    "UNSUPPORTED_BEHAVIOR",
    "CatalogRows",
    "DurableInputReader",
    "LaneProjectionError",
    "ProjectionSource",
    "RenderedProjectionSource",
    "hook_context_index",
    "materialize_packet",
    "operating_contract",
    "projection_digests",
    "read_packet_index",
    "safe_relative",
    "static_rows",
    "turn_text",
    "verify_projection",
    "write_files",
    "write_secret",
]
