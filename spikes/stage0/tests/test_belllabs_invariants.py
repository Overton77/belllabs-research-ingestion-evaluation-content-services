from __future__ import annotations

import asyncio
from contextlib import suppress

import pytest

from qualification import (
    ContextAtom,
    CrashPoint,
    InjectedCrash,
    JournalPhase,
    OperationJournal,
    TenantStore,
    assemble_context,
    canonical_digest,
)


@pytest.mark.parametrize("crash_at", list(CrashPoint))
def test_q03_stable_claim_settlement_and_reconciliation(crash_at: CrashPoint) -> None:
    journal = OperationJournal()
    with pytest.raises(InjectedCrash):
        journal.execute("run-a:operation-a:attempt-1", crash_at=crash_at)
    reconciled = journal.execute("run-a:operation-a:attempt-1")
    replayed = journal.execute("run-a:operation-a:attempt-1")

    assert reconciled is replayed
    assert reconciled.phase == JournalPhase.TERMINALIZED
    assert reconciled.effect_count == 1
    assert reconciled.usage == {"provider_calls": 1}
    assert len(reconciled.outbox) == 1
    assert reconciled.effect_claim_key.startswith("effect:sha256:")
    assert reconciled.settlement_id.startswith("settlement:sha256:")


def test_q03_conflicting_semantic_attempts_do_not_share_identity() -> None:
    journal = OperationJournal()
    first = journal.execute("run-a:operation-a:attempt-1")
    second = journal.execute("run-a:operation-a:attempt-2")
    assert first.effect_claim_key != second.effect_claim_key
    assert first.settlement_id != second.settlement_id


def test_q04_compatibility_manifest_changes_with_graph_surfaces() -> None:
    compatible = {
        "graph_family": "stagegraph",
        "state_schema": "v1",
        "nodes": ["dispatch", "join"],
        "reducers": {"results": "unique_by_operation_id:v1"},
        "interrupts": ["operation_approval:v1"],
    }
    same_surface_new_code = dict(compatible, implementation_revision="build-2")
    incompatible = dict(compatible, reducers={"results": "append:v1"})

    surface_fields = ("graph_family", "state_schema", "nodes", "reducers", "interrupts")
    compatible_digest = canonical_digest({key: compatible[key] for key in surface_fields})
    new_code_digest = canonical_digest({key: same_surface_new_code[key] for key in surface_fields})
    incompatible_digest = canonical_digest({key: incompatible[key] for key in surface_fields})

    assert compatible_digest == new_code_digest
    assert compatible_digest != incompatible_digest


def test_q05_repeated_context_reconstruction_has_zero_drift() -> None:
    atoms = (
        ContextAtom("goal:1", "sha256:g", "protected_goal", "Preserve BellLabs authority", True),
        ContextAtom("instruction:1", "sha256:i", "instruction", "Use exact bindings", True),
        ContextAtom("citation:1", "sha256:c", "citation_edge", "claim:1->source:1"),
        ContextAtom("contradiction:1", "sha256:x", "contradiction", "claim:1 conflicts claim:2"),
        ContextAtom("approval:1", "sha256:a", "approval", "approved by owner"),
        ContextAtom("attempt:1", "sha256:t", "attempt", "operation:1:attempt:1"),
        ContextAtom("source:1", "sha256:s", "source_digest", "immutable source"),
        ContextAtom("source:old", "sha256:d", "tombstone", "retracted", tombstone=True),
    )
    manifest = assemble_context(atoms)
    for _ in range(100):
        manifest = assemble_context(tuple(reversed(manifest.atoms)))
        assert manifest.assembly_digest == assemble_context(atoms).assembly_digest
        assert [atom for atom in manifest.atoms if atom.protected]
        assert [atom for atom in manifest.atoms if atom.tombstone]
        assert any(atom.kind == "contradiction" for atom in manifest.atoms)


@pytest.mark.asyncio
async def test_q06_bounded_fanout_backpressure_deadline_and_cleanup() -> None:
    active = 0
    maximum = 0
    closed = 0
    queue: asyncio.Queue[int | None] = asyncio.Queue(maxsize=3)
    values: list[int | None] = [None] * 20

    async def worker() -> None:
        nonlocal active, maximum, closed
        while True:
            index = await queue.get()
            if index is None:
                queue.task_done()
                return
            active += 1
            maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.001)
                values[index] = index
            finally:
                active -= 1
                closed += 1
                queue.task_done()

    async with asyncio.timeout(1):
        workers = [asyncio.create_task(worker()) for _ in range(3)]
        for index in range(20):
            await queue.put(index)
        for _ in workers:
            await queue.put(None)
        await queue.join()
        await asyncio.gather(*workers)
    assert values == list(range(20))
    assert maximum == 3
    assert active == 0
    assert closed == 20


@pytest.mark.asyncio
async def test_q06_deadline_cancels_and_awaits_bounded_workers() -> None:
    closed = 0
    blocker = asyncio.Event()

    async def worker() -> None:
        nonlocal closed
        try:
            await blocker.wait()
        finally:
            closed += 1

    workers = [asyncio.create_task(worker()) for _ in range(3)]
    try:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.01):
                await asyncio.gather(*workers)
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
    assert closed == 3
    assert all(task.done() for task in workers)


@pytest.mark.asyncio
async def test_q06_cancellation_propagates_and_closes_resources() -> None:
    closed = asyncio.Event()

    async def operation() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    task = asyncio.create_task(operation())
    await asyncio.sleep(0)
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    assert closed.is_set()


def test_q12_store_is_tenant_environment_and_purpose_scoped() -> None:
    store = TenantStore()
    store.put(
        tenant="tenant-a",
        environment="test",
        purpose="procedural_preference",
        key="style",
        value={"value": "concise"},
    )
    assert store.get(
        tenant="tenant-a",
        environment="test",
        purpose="procedural_preference",
        key="style",
    ) == {"value": "concise"}
    assert (
        store.get(
            tenant="tenant-b",
            environment="test",
            purpose="procedural_preference",
            key="style",
        )
        is None
    )
    for prohibited_purpose in (
        "scientific_claim_authority",
        "approval",
        "budget",
        "terminality",
    ):
        with pytest.raises(PermissionError, match="not non-authoritative"):
            store.put(
                tenant="tenant-a",
                environment="test",
                purpose=prohibited_purpose,
                key="authority",
                value={"accepted": True},
            )
    with pytest.raises(PermissionError, match="not non-authoritative"):
        store.put(
            tenant="tenant-a",
            environment="test",
            purpose="procedural_preference",
            key="authority-shaped-value",
            value={"approval": "approved"},
        )
    store.delete_namespace(
        tenant="tenant-a",
        environment="test",
        purpose="procedural_preference",
    )
    assert (
        store.get(
            tenant="tenant-a",
            environment="test",
            purpose="procedural_preference",
            key="style",
        )
        is None
    )
