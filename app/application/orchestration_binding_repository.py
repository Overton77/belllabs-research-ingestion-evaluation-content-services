from __future__ import annotations

import asyncio
from typing import Protocol

from app.domain.orchestration.bindings import RunSemanticInputBinding


class SemanticInputBindingConflict(ValueError):
    """A run-scoped binding identity was reused with different immutable content."""


class SemanticInputBindingNotFound(LookupError):
    """The exact run-scoped semantic binding is unavailable."""


class RunSemanticInputBindingRepository(Protocol):
    async def create(
        self,
        binding: RunSemanticInputBinding,
    ) -> RunSemanticInputBinding: ...

    async def get(
        self,
        binding_id: str,
        *,
        request_scope: str,
        run_id: str,
    ) -> RunSemanticInputBinding | None: ...

    async def get_for_run(
        self,
        *,
        request_scope: str,
        run_id: str,
    ) -> RunSemanticInputBinding | None: ...


class InMemoryRunSemanticInputBindingRepository:
    """Concurrency-safe reference repository with PostgreSQL-equivalent semantics."""

    def __init__(self) -> None:
        self._bindings: dict[str, RunSemanticInputBinding] = {}
        self._run_index: dict[tuple[str, str], str] = {}
        self._lock = asyncio.Lock()

    async def create(
        self,
        binding: RunSemanticInputBinding,
    ) -> RunSemanticInputBinding:
        async with self._lock:
            prior = self._bindings.get(binding.binding_id)
            run_identity = (binding.request_scope, binding.run_id)
            prior_id = self._run_index.get(run_identity)
            if prior is not None:
                if prior != binding:
                    raise SemanticInputBindingConflict(
                        "semantic binding identity was reused with different content"
                    )
                return prior
            if prior_id is not None:
                prior_for_run = self._bindings[prior_id]
                if prior_for_run != binding:
                    raise SemanticInputBindingConflict(
                        "Workflow Run already has a different semantic input binding"
                    )
                return prior_for_run
            self._bindings[binding.binding_id] = binding
            self._run_index[run_identity] = binding.binding_id
            return binding

    async def get(
        self,
        binding_id: str,
        *,
        request_scope: str,
        run_id: str,
    ) -> RunSemanticInputBinding | None:
        binding = self._bindings.get(binding_id)
        if binding is None or binding.request_scope != request_scope or binding.run_id != run_id:
            return None
        return binding

    async def get_for_run(
        self,
        *,
        request_scope: str,
        run_id: str,
    ) -> RunSemanticInputBinding | None:
        binding_id = self._run_index.get((request_scope, run_id))
        return self._bindings.get(binding_id) if binding_id is not None else None


class RunSemanticInputBindingService:
    """Persists the immutable semantic routing authority before workflow dispatch."""

    def __init__(self, repository: RunSemanticInputBindingRepository) -> None:
        self._repository = repository

    async def freeze(
        self,
        binding: RunSemanticInputBinding,
    ) -> str:
        persisted = await self._repository.create(binding)
        if persisted.binding_digest != binding.binding_digest:
            raise SemanticInputBindingConflict(
                "persisted semantic input binding differs from launch intent"
            )
        return persisted.binding_id
