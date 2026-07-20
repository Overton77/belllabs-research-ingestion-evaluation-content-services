from __future__ import annotations

import json
from typing import Any

import asyncpg

from app.domain.composition.contracts import (
    LinkedChildTerminalRecord,
    LinkedRunResultAdmissionDecision,
    RunCompositionLink,
    RunDependencyRevision,
)
from app.domain.run_control.errors import IdempotencyConflict, RunControlNotFound


class PostgresLinkedRunRepository:
    """PostgreSQL authority for links and immutable parent-side decisions."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get_link(
        self, request_scope: str, request_identity: str
    ) -> RunCompositionLink | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT link FROM belllabs_control.run_composition_links
                WHERE request_identity = $1
                """,
                request_identity,
            )
        return RunCompositionLink.model_validate(_json(payload)) if payload else None

    async def get_link_by_id(
        self, request_scope: str, link_id: str
    ) -> RunCompositionLink:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            payload = await connection.fetchval(
                "SELECT link FROM belllabs_control.run_composition_links WHERE link_id = $1",
                link_id,
            )
        if payload is None:
            raise RunControlNotFound(f"run composition link not found: {link_id}")
        return RunCompositionLink.model_validate(_json(payload))

    async def commit_link(self, link: RunCompositionLink) -> RunCompositionLink:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, link.request_scope)
            await _lock(connection, f"linked-request:{link.request_identity}")
            prior = await connection.fetchrow(
                """
                SELECT request_fingerprint, link
                FROM belllabs_control.run_composition_links
                WHERE request_identity = $1
                """,
                link.request_identity,
            )
            if prior is not None:
                if prior["request_fingerprint"] != link.request_fingerprint:
                    raise IdempotencyConflict(
                        "linked request identity was reused with a conflicting fingerprint"
                    )
                return RunCompositionLink.model_validate(_json(prior["link"]))
            await connection.execute(
                """
                INSERT INTO belllabs_control.run_composition_links
                    (link_id, request_identity, request_fingerprint, request_scope,
                     parent_run_id, child_run_id, linked_budget_account_id,
                     link, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
                """,
                link.link_id,
                link.request_identity,
                link.request_fingerprint,
                link.request_scope,
                link.parent_run_id,
                link.child_run_id,
                link.linked_budget_account_id,
                link.model_dump_json(),
                link.created_at,
            )
        return link

    async def list_parent_links(
        self, request_scope: str, parent_run_id: str
    ) -> tuple[RunCompositionLink, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT link FROM belllabs_control.run_composition_links
                WHERE parent_run_id = $1 ORDER BY link_id
                """,
                parent_run_id,
            )
        return tuple(RunCompositionLink.model_validate(_json(row["link"])) for row in rows)

    async def commit_dependency_revision(
        self, request_scope: str, revision: RunDependencyRevision
    ) -> RunDependencyRevision:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock(connection, f"linked-dependency:{revision.link_id}")
            link_payload = await connection.fetchval(
                "SELECT link FROM belllabs_control.run_composition_links WHERE link_id = $1",
                revision.link_id,
            )
            if link_payload is None:
                raise RunControlNotFound(
                    f"run composition link not found: {revision.link_id}"
                )
            prior = await connection.fetchrow(
                """
                SELECT decision FROM belllabs_control.run_dependency_revisions
                WHERE revision_id = $1
                """,
                revision.revision_id,
            )
            if prior is not None:
                value = RunDependencyRevision.model_validate(_json(prior["decision"]))
                if value != revision:
                    raise IdempotencyConflict(
                        "dependency revision identity has conflicting content"
                    )
                return value
            expected = (
                await connection.fetchval(
                    """
                    SELECT COALESCE(MAX(revision), 1) + 1
                    FROM belllabs_control.run_dependency_revisions
                    WHERE link_id = $1
                    """,
                    revision.link_id,
                )
            )
            if revision.revision != expected:
                raise ValueError(f"expected dependency revision {expected}")
            await connection.execute(
                """
                INSERT INTO belllabs_control.run_dependency_revisions
                    (revision_id, link_id, revision, decision, decided_at)
                VALUES ($1, $2, $3, $4::jsonb, $5)
                """,
                revision.revision_id,
                revision.link_id,
                revision.revision,
                revision.model_dump_json(),
                revision.decided_at,
            )
        return revision

    async def list_dependency_revisions(
        self, request_scope: str, link_id: str
    ) -> tuple[RunDependencyRevision, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT decision FROM belllabs_control.run_dependency_revisions
                WHERE link_id = $1 ORDER BY revision
                """,
                link_id,
            )
        return tuple(
            RunDependencyRevision.model_validate(_json(row["decision"])) for row in rows
        )

    async def commit_result_decision(
        self, request_scope: str, decision: LinkedRunResultAdmissionDecision
    ) -> LinkedRunResultAdmissionDecision:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock(connection, f"linked-result:{decision.link_id}")
            link_payload = await connection.fetchval(
                "SELECT link FROM belllabs_control.run_composition_links WHERE link_id = $1",
                decision.link_id,
            )
            if link_payload is None:
                raise RunControlNotFound(
                    f"run composition link not found: {decision.link_id}"
                )
            link = RunCompositionLink.model_validate(_json(link_payload))
            if (
                decision.parent_run_id != link.parent_run_id
                or decision.child_run_id != link.child_run_id
            ):
                raise IdempotencyConflict(
                    "linked result decision run identities do not match its composition link"
                )
            prior = await connection.fetchrow(
                """
                SELECT decision FROM belllabs_control.linked_run_result_decisions
                WHERE decision_id = $1 OR (link_id = $2 AND exact_output_ref = $3)
                """,
                decision.decision_id,
                decision.link_id,
                decision.exact_output_ref,
            )
            if prior is not None:
                value = LinkedRunResultAdmissionDecision.model_validate(
                    _json(prior["decision"])
                )
                if value != decision:
                    raise IdempotencyConflict(
                        "exact child output already has a conflicting admission decision"
                    )
                return value
            await connection.execute(
                """
                INSERT INTO belllabs_control.linked_run_result_decisions
                    (decision_id, link_id, parent_run_id, child_run_id,
                     exact_output_ref, decision, decided_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                """,
                decision.decision_id,
                decision.link_id,
                decision.parent_run_id,
                decision.child_run_id,
                decision.exact_output_ref,
                decision.model_dump_json(),
                decision.decided_at,
            )
        return decision

    async def list_result_decisions(
        self, request_scope: str, link_id: str
    ) -> tuple[LinkedRunResultAdmissionDecision, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT decision FROM belllabs_control.linked_run_result_decisions
                WHERE link_id = $1 ORDER BY decided_at, decision_id
                """,
                link_id,
            )
        return tuple(
            LinkedRunResultAdmissionDecision.model_validate(_json(row["decision"]))
            for row in rows
        )

    async def commit_terminal_record(
        self, request_scope: str, record: LinkedChildTerminalRecord
    ) -> LinkedChildTerminalRecord:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock(connection, f"linked-terminal:{record.link_id}")
            prior = await connection.fetchval(
                """
                SELECT record FROM belllabs_control.linked_child_terminal_records
                WHERE link_id = $1
                """,
                record.link_id,
            )
            if prior is not None:
                value = LinkedChildTerminalRecord.model_validate(_json(prior))
                if value != record:
                    raise IdempotencyConflict(
                        "linked child already has a conflicting terminal record"
                    )
                return value
            await connection.execute(
                """
                INSERT INTO belllabs_control.linked_child_terminal_records
                    (terminal_record_id, link_id, child_run_id, status, record, observed_at)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                """,
                record.terminal_record_id,
                record.link_id,
                record.child_run_id,
                record.status,
                record.model_dump_json(),
                record.observed_at,
            )
        return record


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute(
        "SELECT set_config('belllabs.request_scope', $1, true)",
        request_scope,
    )


async def _lock(connection: asyncpg.Connection, key: str) -> None:
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        key,
    )


def _json(value: Any) -> object:
    return json.loads(value) if isinstance(value, str) else value
