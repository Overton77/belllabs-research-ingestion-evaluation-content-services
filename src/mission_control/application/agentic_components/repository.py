from __future__ import annotations

import fnmatch
from collections.abc import Iterable
from typing import Protocol

from mission_control.domain.agentic_components.contracts import (
    AgenticComponentRelease,
    ComponentQuery,
    DiffCodecQualification,
    TrustStage,
)

_TRUST_ORDER = {
    TrustStage.QUARANTINED: 0,
    TrustStage.REVIEWED: 1,
    TrustStage.QUALIFIED: 2,
    TrustStage.ACCEPTED: 3,
}


class AgenticComponentRepository(Protocol):
    async def get_by_digest(self, digest: str) -> AgenticComponentRelease | None: ...

    async def search(self, query: ComponentQuery) -> tuple[AgenticComponentRelease, ...]: ...

    async def select_diff_qualification(
        self,
        model_id: str,
    ) -> DiffCodecQualification | None: ...


class InMemoryAgenticComponentRepository:
    """Deterministic reference repository used by the service and contract tests."""

    def __init__(self, releases: Iterable[AgenticComponentRelease] = ()) -> None:
        self._by_digest: dict[str, AgenticComponentRelease] = {}
        for release in releases:
            self.add(release)

    def add(self, release: AgenticComponentRelease) -> None:
        digest = release.coordinate.digest
        existing = self._by_digest.get(digest)
        if existing is not None and existing != release:
            raise ValueError(f"component digest collision: {digest}")
        self._by_digest[digest] = release

    async def get_by_digest(self, digest: str) -> AgenticComponentRelease | None:
        return self._by_digest.get(digest)

    async def search(self, query: ComponentQuery) -> tuple[AgenticComponentRelease, ...]:
        scored: list[tuple[int, AgenticComponentRelease]] = []
        text_tokens = tuple((query.text or "").casefold().split())
        for release in self._by_digest.values():
            if query.kinds and release.kind not in query.kinds:
                continue
            if _TRUST_ORDER[release.trust_stage] < _TRUST_ORDER[query.minimum_trust_stage]:
                continue
            compatible = release.compatibility
            if query.host is not None:
                compatible = tuple(item for item in compatible if item.host == query.host)
                if not compatible:
                    continue
            if query.operating_system is not None and not any(
                query.operating_system in item.operating_systems for item in compatible
            ):
                continue
            if query.architecture is not None and not any(
                query.architecture in item.architectures for item in compatible
            ):
                continue
            if not query.required_capabilities <= release.description.capabilities:
                continue
            haystack = " ".join(
                (
                    release.coordinate.component_id,
                    release.description.title,
                    release.description.summary,
                    *sorted(release.description.tags),
                    *sorted(release.description.biotech_domains),
                    *sorted(release.description.capabilities),
                )
            ).casefold()
            if text_tokens and not all(token in haystack for token in text_tokens):
                continue
            score = sum(haystack.count(token) for token in text_tokens)
            score += _TRUST_ORDER[release.trust_stage]
            scored.append((score, release))
        scored.sort(
            key=lambda item: (
                -item[0],
                item[1].coordinate.component_id,
                item[1].coordinate.version,
                item[1].coordinate.digest,
            )
        )
        return tuple(release for _, release in scored[: query.limit])

    async def select_diff_qualification(
        self,
        model_id: str,
    ) -> DiffCodecQualification | None:
        eligible = [
            release.diff_qualification
            for release in self._by_digest.values()
            if release.diff_qualification is not None
            and release.diff_qualification.promoted
            and release.diff_qualification.passes_gate
            and fnmatch.fnmatchcase(model_id, release.diff_qualification.model_pattern)
        ]
        if not eligible:
            return None
        return sorted(
            eligible,
            key=lambda item: (
                -item.metrics.exact_apply_rate,
                item.metrics.unintended_change_rate,
                item.metrics.median_output_tokens,
                item.qualification_id,
            ),
        )[0]
