from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from uuid import NAMESPACE_URL, uuid5

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import SecretRef
from app.domain.operation_execution.contracts import (
    MaterializedWorkspace,
    OperationExecutionBinding,
    OperationExecutionRequest,
    RuntimeInvocation,
    RuntimeResult,
    RuntimeUsage,
)


class ConformanceAuthority:
    def __init__(
        self,
        *,
        accepted_run_id: str,
        configuration_digest: str,
        control_revision: int,
        reservation_id: str,
    ) -> None:
        self.accepted_run_id = accepted_run_id
        self.configuration_digest = configuration_digest
        self.control_revision = control_revision
        self.reservation_id = reservation_id
        self.verifications = 0

    async def verify(self, request: OperationExecutionRequest) -> None:
        self.verifications += 1
        if (
            request.identity.run_id != self.accepted_run_id
            or request.effective_configuration_digest != self.configuration_digest
            or request.run_control_revision != self.control_revision
            or request.budget_reservation_id != self.reservation_id
        ):
            raise ValueError("operation authority binding is not accepted")


class ConformanceSandbox:
    def __init__(self) -> None:
        self.materializations: dict[str, MaterializedWorkspace] = {}

    async def materialize(self, binding: OperationExecutionBinding) -> MaterializedWorkspace:
        prior = self.materializations.get(binding.binding_id)
        if prior is not None:
            return prior
        manifest_digest = sha256_digest(
            {
                "workspace": binding.workspace.workspace_id,
                "read_mounts": [
                    mount.model_dump(mode="json") for mount in binding.workspace.read_mounts
                ],
                "write_paths": binding.workspace.exclusive_write_paths,
            }
        )
        workspace = MaterializedWorkspace(
            workspace_id=binding.workspace.workspace_id,
            provider=binding.workspace.provider,
            runtime_digest=binding.workspace.runtime_digest,
            image_digest=binding.workspace.image_digest,
            mount_manifest_digest=manifest_digest,
        )
        self.materializations[binding.binding_id] = workspace
        return workspace


class ConformanceAssetVerifier:
    def __init__(
        self,
        *,
        mcp_schema_digests: Mapping[str, str] | None = None,
        asset_manifest_digests: Mapping[str, str] | None = None,
    ) -> None:
        self._mcp = dict(mcp_schema_digests or {})
        self._assets = dict(asset_manifest_digests or {})
        self.verifications = 0

    async def verify(self, binding: OperationExecutionBinding) -> None:
        self.verifications += 1
        for server in binding.mcp_servers:
            if self._mcp.get(server.server_id) != server.schema_digest:
                raise ValueError(f"MCP schema digest mismatch for {server.server_id}")
        for asset in (*binding.skills, *binding.plugins):
            key = f"{asset.ref.kind.value}:{asset.ref.logical_id}:{asset.ref.revision}"
            if self._assets.get(key) != asset.manifest_digest:
                raise ValueError(f"immutable asset digest mismatch for {key}")

    async def verify_servers(self, binding: OperationExecutionBinding) -> None:
        await self.verify(binding)


class ConformanceSecretResolver:
    def __init__(self, values: Mapping[str, str] | None = None) -> None:
        self._values = dict(values or {})
        self.resolutions = 0

    async def resolve(self, refs: tuple[SecretRef, ...]) -> Mapping[str, str]:
        self.resolutions += 1
        resolved: dict[str, str] = {}
        for ref in refs:
            key = f"{ref.provider}:{ref.key}"
            if key not in self._values:
                raise ValueError("required secret reference is unavailable")
            resolved[key] = self._values[key]
        return resolved


class ConformanceRuntime:
    """Idempotent fake provider at the public operation result seam."""

    def __init__(self, output_text: str = "conformance-ok") -> None:
        self.output_text = output_text
        self.invocations: list[RuntimeInvocation] = []
        self._effects: dict[str, RuntimeResult] = {}

    @property
    def effect_count(self) -> int:
        return len(self._effects)

    async def execute(
        self,
        invocation: RuntimeInvocation,
        resolved_secrets: Mapping[str, str],
    ) -> RuntimeResult:
        self.invocations.append(invocation)
        side_effect_key = invocation.binding.side_effect_key
        prior = self._effects.get(side_effect_key)
        if prior is not None:
            return deepcopy(prior)
        # Secret values are intentionally not copied into the result, events, or invocation.
        result = RuntimeResult(
            output_text=self.output_text,
            structured_output={"status": "ok"},
            usage=RuntimeUsage(
                amounts={"model.turns": 1, "tokens.total": 3},
            ),
            provider_run_id=_stable_id("conformance-provider-run", side_effect_key),
            event_payloads=(
                {
                    "kind": "operation.completed",
                    "resolved_secret_count": len(resolved_secrets),
                },
            ),
        )
        self._effects[side_effect_key] = result
        return deepcopy(result)


class ConformanceEventSink:
    def __init__(self) -> None:
        self.events: dict[str, tuple[str, dict[str, object]]] = {}

    async def publish(self, *, event_key: str, binding_id: str, payload: dict[str, object]) -> None:
        prior = self.events.get(event_key)
        value = (binding_id, deepcopy(payload))
        if prior is not None and prior != value:
            raise ValueError("event idempotency key has conflicting payload")
        self.events[event_key] = value


class ConformanceBudgetAuthority:
    def __init__(self) -> None:
        self.settlements: dict[str, tuple[str, RuntimeUsage, bool]] = {}

    async def reconcile(
        self,
        *,
        binding: OperationExecutionBinding,
        settlement_id: str,
        usage: RuntimeUsage,
        budget_violation: bool = False,
    ) -> None:
        value = (binding.binding_id, deepcopy(usage), budget_violation)
        prior = self.settlements.get(settlement_id)
        if prior is not None and prior != value:
            raise ValueError("budget settlement identity has conflicting usage")
        for dimension, amount in usage.amounts.items():
            if not budget_violation and amount > binding.budget_limits.get(dimension, 0):
                raise ValueError(f"observed usage exceeds binding limit for {dimension}")
        self.settlements[settlement_id] = value


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))
