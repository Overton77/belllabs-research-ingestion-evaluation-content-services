"""Stop Fence port and the Kernel Hook gate that consults it (ADR-0008, ADR-0032; FT-F3).

The repository persists the fence of an immediate cancel and admits side effects against it
atomically: a fence write and an effect admission for the same run serialize on one lock, so
an admission committed after the fence is always denied and an admission committed before it
stays admitted (it was dispatched before the stop). Kernel Hooks call `KernelHookFenceGate`:
the Deep Agents `HookScriptMiddleware` kernel layer (`wrap_tool_call`, FT-A5) and the Cursor
hook callback (`POST /v1/internal/hook-callback`, FT-G3) wire their call sites to it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.policies.stop_fence import (
    FENCED_FRAME_REASON,
    EffectAdmission,
    FenceMilestone,
    FenceVerdict,
    ImmediateCancelReport,
    StopFence,
    fence_verdict,
)


class StopFenceRepository(Protocol):
    async def persist(self, fence: StopFence) -> StopFence:
        """Insert the fence for (run, generation) once; return the stored row (first wins)."""
        ...

    async def get(self, request_scope: str, run_id: str) -> StopFence | None:
        """The run's newest fence, if any."""
        ...

    async def admit_effect(self, admission: EffectAdmission) -> FenceVerdict:
        """Decide and record one effect admission atomically with respect to fence writes.

        Idempotent per (run, generation, effect_ref): a repeated admission returns the
        recorded decision.
        """
        ...

    async def record_milestone(
        self,
        request_scope: str,
        run_id: str,
        generation: int | None,
        milestone: FenceMilestone,
        *,
        unit_key: str = "",
        recorded_at: datetime | None = None,
    ) -> None:
        """Record a Delivery Report milestone on the fence of `generation` (`None`: the
        run's newest fence); a run without a fence (normal cancel) records nothing."""
        ...

    async def report(self, request_scope: str, run_id: str) -> ImmediateCancelReport | None: ...


@dataclass(frozen=True)
class HookFenceDecision:
    """What a Kernel Hook returns and persists: the verdict plus the frame payload that
    records a fenced denial (persisted by the frame sink of SPEC-03)."""

    verdict: FenceVerdict
    frame: dict[str, object] | None

    @property
    def allowed(self) -> bool:
        return self.verdict.allowed


class KernelHookFenceGate:
    """The fence check every permission Kernel Hook runs before any side effect."""

    def __init__(self, fences: StopFenceRepository) -> None:
        self._fences = fences

    async def before_effect(self, admission: EffectAdmission) -> HookFenceDecision:
        verdict = await self._fences.admit_effect(admission)
        if verdict.allowed:
            return HookFenceDecision(verdict, None)
        frame: dict[str, object] = {
            "kind": "hook",
            "decision": "deny",
            "reason": FENCED_FRAME_REASON,
            "reason_code": verdict.reason_code,
            "effect_ref": admission.effect_ref,
            "effect_kind": admission.effect_kind,
            "lane_profile": admission.lane_profile,
            "generation": admission.generation,
            "fence_command_id": verdict.fence_command_id,
        }
        frame["digest"] = sha256_digest(frame)
        return HookFenceDecision(verdict, frame)


@dataclass
class _Run:
    fences: dict[int, StopFence] = field(default_factory=dict)
    admissions: dict[tuple[int, str], FenceVerdict] = field(default_factory=dict)
    milestones: dict[tuple[int, str, str], datetime] = field(default_factory=dict)


class InMemoryStopFenceRepository:
    """Process-local repository with the same serialization contract (tests, local proof)."""

    def __init__(self) -> None:
        self._runs: dict[tuple[str, str], _Run] = {}
        self._lock = asyncio.Lock()

    def _run(self, request_scope: str, run_id: str) -> _Run:
        return self._runs.setdefault((request_scope, run_id), _Run())

    async def persist(self, fence: StopFence) -> StopFence:
        async with self._lock:
            run = self._run(fence.request_scope, fence.run_id)
            stored = run.fences.get(fence.generation)
            if stored is None:
                stored = fence.model_copy(
                    update={
                        "fenced_at": fence.fenced_at or max(datetime.now(UTC), fence.requested_at)
                    }
                )
                run.fences[fence.generation] = stored
            return stored

    async def get(self, request_scope: str, run_id: str) -> StopFence | None:
        run = self._runs.get((request_scope, run_id))
        if run is None or not run.fences:
            return None
        return run.fences[max(run.fences)]

    async def admit_effect(self, admission: EffectAdmission) -> FenceVerdict:
        async with self._lock:
            run = self._run(admission.request_scope, admission.run_id)
            key = (admission.generation, admission.effect_ref)
            prior = run.admissions.get(key)
            if prior is not None:
                return prior
            fence = run.fences[max(run.fences)] if run.fences else None
            verdict = fence_verdict(fence, admission)
            run.admissions[key] = verdict
            return verdict

    async def record_milestone(
        self,
        request_scope: str,
        run_id: str,
        generation: int | None,
        milestone: FenceMilestone,
        *,
        unit_key: str = "",
        recorded_at: datetime | None = None,
    ) -> None:
        async with self._lock:
            run = self._run(request_scope, run_id)
            if not run.fences:
                return
            generation = max(run.fences) if generation is None else generation
            if generation not in run.fences:
                return
            run.milestones.setdefault(
                (generation, milestone, unit_key), recorded_at or datetime.now(UTC)
            )

    async def report(self, request_scope: str, run_id: str) -> ImmediateCancelReport | None:
        run = self._runs.get((request_scope, run_id))
        if run is None or not run.fences:
            return None
        generation = max(run.fences)
        fence = run.fences[generation]
        acknowledged = [
            at
            for (gen, milestone, _unit), at in run.milestones.items()
            if gen == generation and milestone == "provider_acknowledged"
        ]
        settled = [
            at
            for (gen, milestone, _unit), at in run.milestones.items()
            if gen == generation and milestone == "settled"
        ]
        assert fence.fenced_at is not None
        return ImmediateCancelReport(
            command_id=fence.command_id,
            run_id=run_id,
            generation=generation,
            requested_at=fence.requested_at,
            fence_persisted_at=fence.fenced_at,
            provider_acknowledged_at=min(acknowledged) if acknowledged else None,
            settled_at=max(settled) if settled else None,
        )


__all__ = [
    "HookFenceDecision",
    "InMemoryStopFenceRepository",
    "KernelHookFenceGate",
    "StopFenceRepository",
]
