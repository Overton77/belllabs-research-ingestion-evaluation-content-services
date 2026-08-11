from __future__ import annotations

from app.application.runtime.runtime_bootstrap import (
    AuthoritativeRuntimeProjection,
    RuntimeBootstrapReconciler,
)
from app.domain.graph_runtime.identities import ExecutionEpochKey


class UnconfiguredBootstrapAuthority:
    async def load(self, _epoch: ExecutionEpochKey) -> AuthoritativeRuntimeProjection:
        raise RuntimeError("authoritative runtime bootstrap is not configured")


_bootstrap_reconciler = RuntimeBootstrapReconciler(UnconfiguredBootstrapAuthority())


def configure_bootstrap_reconciler(reconciler: RuntimeBootstrapReconciler) -> None:
    global _bootstrap_reconciler
    _bootstrap_reconciler = reconciler


def reset_bootstrap_reconciler() -> None:
    configure_bootstrap_reconciler(RuntimeBootstrapReconciler(UnconfiguredBootstrapAuthority()))


def get_bootstrap_reconciler() -> RuntimeBootstrapReconciler:
    return _bootstrap_reconciler
