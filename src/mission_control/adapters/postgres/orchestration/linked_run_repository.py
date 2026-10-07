"""Linked independent runs: canonical mission_relationship plus exact support records."""

from __future__ import annotations

from typing import Any

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.contracts.identities import uuid7
from mission_control.domain.composition.contracts import (
    LinkedChildTerminalRecord,
    LinkedRunResultAdmissionDecision,
    RunCompositionLink,
    RunDependencyRevision,
)
from mission_control.domain.policies.errors import IdempotencyConflict, RunControlNotFound

LINK_CONTRACT = "mc.run-composition-link/1"
REVISION_CONTRACT = "mc.run-dependency-revision/1"
RESULT_CONTRACT = "mc.linked-result-decision/1"
TERMINAL_CONTRACT = "mc.linked-child-terminal/1"


class PostgresLinkedRunRepository:
    """PostgreSQL authority for links and immutable parent-side decisions."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get_link(
        self, request_scope: str, request_identity: str
    ) -> RunCompositionLink | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            payload = await connection.fetchval(
                f"""
                SELECT link FROM mission_control.run_composition_link
                WHERE {SCOPE} AND request_identity = $4
                """,
                *args,
                request_identity,
            )
        return RunCompositionLink.model_validate(mc.load(payload)) if payload else None

    async def get_link_by_id(self, request_scope: str, link_id: str) -> RunCompositionLink:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            payload = await _link_payload(connection, args, link_id)
        if payload is None:
            raise RunControlNotFound(f"run composition link not found: {link_id}")
        return RunCompositionLink.model_validate(mc.load(payload))

    async def commit_link(self, link: RunCompositionLink) -> RunCompositionLink:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, link.request_scope)
            await mc.advisory_lock(connection, f"linked-request:{link.request_identity}")
            prior = await connection.fetchrow(
                f"""
                SELECT request_fingerprint, link FROM mission_control.run_composition_link
                WHERE {SCOPE} AND request_identity = $4
                """,
                *args,
                link.request_identity,
            )
            if prior is not None:
                if prior["request_fingerprint"] != link.request_fingerprint:
                    raise IdempotencyConflict(
                        "linked request identity was reused with a conflicting fingerprint"
                    )
                return RunCompositionLink.model_validate(mc.load(prior["link"]))
            # The support row first: its scoped foreign keys reject a parent, child or
            # budget account outside this tenant (no fallback); the relationship row it
            # references is checked at commit (deferred foreign key).
            await connection.execute(
                """
                INSERT INTO mission_control.run_composition_link (
                    installation_id, application_id, tenant_id, run_composition_link_id,
                    link_key, request_identity, request_fingerprint, parent_run_key,
                    child_run_key, linked_budget_account_key, link_contract, link, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $14)
                """,
                *args,
                uuid7(),
                link.link_id,
                link.request_identity,
                link.request_fingerprint,
                link.parent_run_id,
                link.child_run_id,
                link.linked_budget_account_id,
                LINK_CONTRACT,
                link.model_dump_json(),
                link.created_at,
                mc.WRITER_REF,
            )
            parent = await mc.require_run(connection, args, link.parent_run_id)
            child = await mc.require_run(connection, args, link.child_run_id)
            account_run = await connection.fetchval(
                f"""
                SELECT run_id FROM mission_control.budget_account
                WHERE {SCOPE} AND account_key = $4
                """,
                *args,
                link.linked_budget_account_id,
            )
            if account_run != child["run_id"]:
                raise IdempotencyConflict("linked budget account must belong to the child run")
            await connection.execute(
                """
                INSERT INTO mission_control.mission_relationship (
                    installation_id, application_id, tenant_id, relationship_id,
                    relationship_key, source_run_id, target_run_id, kind, invocation_ref,
                    grant_ref, projected_output_policy, detail, version, updated_at, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'composition', $8, NULL, $9::jsonb,
                        $10::jsonb, 1, $11, $11, $12)
                """,
                *args,
                uuid7(),
                link.link_id,
                parent["run_id"],
                child["run_id"],
                link.request_identity,
                mc.dump({"result_admission_policy": link.result_admission_policy}),
                link.model_dump_json(),
                link.created_at,
                mc.WRITER_REF,
            )
        return link

    async def list_parent_links(
        self, request_scope: str, parent_run_id: str
    ) -> tuple[RunCompositionLink, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT link FROM mission_control.run_composition_link
                WHERE {SCOPE} AND parent_run_key = $4 ORDER BY link_key
                """,
                *args,
                parent_run_id,
            )
        return tuple(RunCompositionLink.model_validate(mc.load(row["link"])) for row in rows)

    async def commit_dependency_revision(
        self, request_scope: str, revision: RunDependencyRevision
    ) -> RunDependencyRevision:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await mc.advisory_lock(connection, f"linked-dependency:{revision.link_id}")
            if await _link_payload(connection, args, revision.link_id) is None:
                raise RunControlNotFound(f"run composition link not found: {revision.link_id}")
            prior = await connection.fetchval(
                f"""
                SELECT decision FROM mission_control.run_dependency_revision
                WHERE {SCOPE} AND revision_key = $4
                """,
                *args,
                revision.revision_id,
            )
            if prior is not None:
                value = RunDependencyRevision.model_validate(mc.load(prior))
                if value != revision:
                    raise IdempotencyConflict(
                        "dependency revision identity has conflicting content"
                    )
                return value
            expected = await connection.fetchval(
                f"""
                SELECT COALESCE(MAX(revision), 1) + 1 FROM mission_control.run_dependency_revision
                WHERE {SCOPE} AND link_key = $4
                """,
                *args,
                revision.link_id,
            )
            if revision.revision != expected:
                raise ValueError(f"expected dependency revision {expected}")
            await connection.execute(
                """
                INSERT INTO mission_control.run_dependency_revision (
                    installation_id, application_id, tenant_id, run_dependency_revision_id,
                    revision_key, link_key, revision, decision_contract, decision, decided_at,
                    created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $10, $11)
                """,
                *args,
                uuid7(),
                revision.revision_id,
                revision.link_id,
                revision.revision,
                REVISION_CONTRACT,
                revision.model_dump_json(),
                revision.decided_at,
                revision.decided_by,
            )
        return revision

    async def list_dependency_revisions(
        self, request_scope: str, link_id: str
    ) -> tuple[RunDependencyRevision, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT decision FROM mission_control.run_dependency_revision
                WHERE {SCOPE} AND link_key = $4 ORDER BY revision
                """,
                *args,
                link_id,
            )
        return tuple(RunDependencyRevision.model_validate(mc.load(row["decision"])) for row in rows)

    async def commit_result_decision(
        self, request_scope: str, decision: LinkedRunResultAdmissionDecision
    ) -> LinkedRunResultAdmissionDecision:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await mc.advisory_lock(connection, f"linked-result:{decision.link_id}")
            link_payload = await _link_payload(connection, args, decision.link_id)
            if link_payload is None:
                raise RunControlNotFound(f"run composition link not found: {decision.link_id}")
            link = RunCompositionLink.model_validate(mc.load(link_payload))
            if (
                decision.parent_run_id != link.parent_run_id
                or decision.child_run_id != link.child_run_id
            ):
                raise IdempotencyConflict(
                    "linked result decision run identities do not match its composition link"
                )
            prior = await connection.fetchval(
                f"""
                SELECT decision FROM mission_control.linked_result_decision
                WHERE {SCOPE} AND (decision_key = $4 OR (link_key = $5 AND exact_output_ref = $6))
                """,
                *args,
                decision.decision_id,
                decision.link_id,
                decision.exact_output_ref,
            )
            if prior is not None:
                value = LinkedRunResultAdmissionDecision.model_validate(mc.load(prior))
                if value != decision:
                    raise IdempotencyConflict(
                        "exact child output already has a conflicting admission decision"
                    )
                return value
            await connection.execute(
                """
                INSERT INTO mission_control.linked_result_decision (
                    installation_id, application_id, tenant_id, linked_result_decision_id,
                    decision_key, link_key, parent_run_key, child_run_key, exact_output_ref,
                    decision_contract, decision, decided_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $12, $13)
                """,
                *args,
                uuid7(),
                decision.decision_id,
                decision.link_id,
                decision.parent_run_id,
                decision.child_run_id,
                decision.exact_output_ref,
                RESULT_CONTRACT,
                decision.model_dump_json(),
                decision.decided_at,
                decision.decided_by,
            )
        return decision

    async def list_result_decisions(
        self, request_scope: str, link_id: str
    ) -> tuple[LinkedRunResultAdmissionDecision, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT decision FROM mission_control.linked_result_decision
                WHERE {SCOPE} AND link_key = $4 ORDER BY decided_at, decision_key
                """,
                *args,
                link_id,
            )
        return tuple(
            LinkedRunResultAdmissionDecision.model_validate(mc.load(row["decision"]))
            for row in rows
        )

    async def commit_terminal_record(
        self, request_scope: str, record: LinkedChildTerminalRecord
    ) -> LinkedChildTerminalRecord:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await mc.advisory_lock(connection, f"linked-terminal:{record.link_id}")
            prior = await connection.fetchval(
                f"""
                SELECT record FROM mission_control.linked_child_terminal
                WHERE {SCOPE} AND link_key = $4
                """,
                *args,
                record.link_id,
            )
            if prior is not None:
                value = LinkedChildTerminalRecord.model_validate(mc.load(prior))
                if value != record:
                    raise IdempotencyConflict(
                        "linked child already has a conflicting terminal record"
                    )
                return value
            await connection.execute(
                """
                INSERT INTO mission_control.linked_child_terminal (
                    installation_id, application_id, tenant_id, linked_child_terminal_id,
                    terminal_record_key, link_key, child_run_key, status, record_contract, record,
                    observed_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $11, $12)
                """,
                *args,
                uuid7(),
                record.terminal_record_id,
                record.link_id,
                record.child_run_id,
                record.status,
                TERMINAL_CONTRACT,
                record.model_dump_json(),
                record.observed_at,
                mc.WRITER_REF,
            )
        return record


async def _link_payload(connection: asyncpg.Connection, args: tuple[Any, ...], link_id: str) -> Any:
    return await connection.fetchval(
        f"SELECT link FROM mission_control.run_composition_link WHERE {SCOPE} AND link_key = $4",
        *args,
        link_id,
    )
