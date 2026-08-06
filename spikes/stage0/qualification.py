"""Pure disposable models used by the Stage 0 qualification tests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from policies import ALLOWED_STORE_PURPOSES, is_allowed_store_value


def canonical_digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"


@dataclass(frozen=True)
class ContextAtom:
    ref: str
    digest: str
    kind: str
    payload: str
    protected: bool = False
    tombstone: bool = False


@dataclass(frozen=True)
class ContextManifest:
    atoms: tuple[ContextAtom, ...]
    assembly_digest: str


def assemble_context(atoms: tuple[ContextAtom, ...]) -> ContextManifest:
    """Reconstruct from immutable atoms, never from a model-written summary."""

    ordered = tuple(sorted(atoms, key=lambda item: (item.kind, item.ref, item.digest)))
    material = [
        {
            "ref": atom.ref,
            "digest": atom.digest,
            "kind": atom.kind,
            "payload": atom.payload,
            "protected": atom.protected,
            "tombstone": atom.tombstone,
        }
        for atom in ordered
    ]
    return ContextManifest(atoms=ordered, assembly_digest=canonical_digest(material))


class JournalPhase(StrEnum):
    PREPARED = "prepared"
    CLAIMED = "claimed"
    EFFECT_OBSERVED = "effect_observed"
    SETTLED = "settled"
    TERMINALIZED = "terminalized"


@dataclass
class JournalRecord:
    semantic_attempt_key: str
    effect_claim_key: str
    settlement_id: str
    phase: JournalPhase = JournalPhase.PREPARED
    effect_count: int = 0
    usage: dict[str, int] = field(default_factory=dict)
    outbox: list[str] = field(default_factory=list)


class CrashPoint(StrEnum):
    BEFORE_EFFECT = "before_effect"
    AFTER_EFFECT = "after_effect"
    AFTER_SETTLEMENT = "after_settlement"


class InjectedCrash(RuntimeError):
    pass


class OperationJournal:
    """In-memory state-machine model, not a production persistence abstraction."""

    def __init__(self) -> None:
        self._records: dict[str, JournalRecord] = {}

    def prepare(self, semantic_attempt_key: str) -> JournalRecord:
        record = self._records.get(semantic_attempt_key)
        if record is not None:
            return record
        digest = canonical_digest({"semantic_attempt_key": semantic_attempt_key})
        record = JournalRecord(
            semantic_attempt_key=semantic_attempt_key,
            effect_claim_key=f"effect:{digest}",
            settlement_id=f"settlement:{digest}",
        )
        self._records[semantic_attempt_key] = record
        return record

    def execute(
        self,
        semantic_attempt_key: str,
        *,
        crash_at: CrashPoint | None = None,
    ) -> JournalRecord:
        record = self.prepare(semantic_attempt_key)
        if record.phase == JournalPhase.TERMINALIZED:
            return record
        if record.phase == JournalPhase.PREPARED:
            record.phase = JournalPhase.CLAIMED
        if crash_at == CrashPoint.BEFORE_EFFECT and record.phase == JournalPhase.CLAIMED:
            raise InjectedCrash(crash_at)
        if record.phase == JournalPhase.CLAIMED:
            record.effect_count += 1
            record.phase = JournalPhase.EFFECT_OBSERVED
        if crash_at == CrashPoint.AFTER_EFFECT and record.phase == JournalPhase.EFFECT_OBSERVED:
            raise InjectedCrash(crash_at)
        if record.phase == JournalPhase.EFFECT_OBSERVED:
            record.usage = {"provider_calls": 1}
            record.phase = JournalPhase.SETTLED
        if crash_at == CrashPoint.AFTER_SETTLEMENT and record.phase == JournalPhase.SETTLED:
            raise InjectedCrash(crash_at)
        if record.phase == JournalPhase.SETTLED:
            event = f"operation.settled:{record.settlement_id}"
            if event not in record.outbox:
                record.outbox.append(event)
            record.phase = JournalPhase.TERMINALIZED
        return record


class TenantStore:
    """Minimal namespace model for Store isolation and authority denial."""

    def __init__(self) -> None:
        self._items: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    def put(
        self,
        *,
        tenant: str,
        environment: str,
        purpose: str,
        key: str,
        value: dict[str, Any],
    ) -> None:
        if purpose not in ALLOWED_STORE_PURPOSES or not is_allowed_store_value(
            purpose,
            value,
        ):
            raise PermissionError("Store purpose is not non-authoritative and allowed")
        self._items[(tenant, environment, purpose, key)] = dict(value)

    def get(
        self,
        *,
        tenant: str,
        environment: str,
        purpose: str,
        key: str,
    ) -> dict[str, Any] | None:
        value = self._items.get((tenant, environment, purpose, key))
        return dict(value) if value is not None else None

    def delete_namespace(self, *, tenant: str, environment: str, purpose: str) -> None:
        prefix = (tenant, environment, purpose)
        for key in tuple(self._items):
            if key[:3] == prefix:
                del self._items[key]
