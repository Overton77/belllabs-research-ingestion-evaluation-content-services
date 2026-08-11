from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, Protocol

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    RuntimeInvocation,
    RuntimeResult,
)

RuntimeProvider = Literal["legacy_temporal", "langgraph_agent_server"]


class RuntimeNeutralOperationAdapter(Protocol):
    async def execute(
        self,
        invocation: RuntimeInvocation,
        resolved_secrets: Mapping[str, str],
    ) -> RuntimeResult: ...


class RuntimeNeutralOperationDispatcher:
    """Routes one frozen semantic binding without allowing adapter reinterpretation."""

    def __init__(
        self,
        adapters: Mapping[RuntimeProvider, RuntimeNeutralOperationAdapter],
    ) -> None:
        self._adapters = dict(adapters)

    async def execute(
        self,
        runtime_provider: RuntimeProvider,
        invocation: RuntimeInvocation,
        resolved_secrets: Mapping[str, str],
    ) -> RuntimeResult:
        before = sha256_digest(invocation.binding)
        try:
            adapter = self._adapters[runtime_provider]
        except KeyError as error:
            raise LookupError(f"runtime provider is unavailable: {runtime_provider}") from error
        result = await adapter.execute(invocation, resolved_secrets)
        after = sha256_digest(invocation.binding)
        if before != after:
            raise ValueError("runtime adapter mutated immutable semantic operation authority")
        return result
