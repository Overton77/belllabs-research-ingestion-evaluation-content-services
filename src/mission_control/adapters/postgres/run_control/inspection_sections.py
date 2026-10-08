"""PostgreSQL reads for inspection sections (FT-F6): chain membership and Subscriptions.

Both read under the Run's tenant scope with forced RLS: the chain whose links name the Run's
mission (SPEC-04 `mission_chain` / `chain_link`), and the count of `active` Subscriptions
targeting the Run or its mission (SPEC-06 `mission_subscription`).
"""

from __future__ import annotations

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, scoped
from mission_control.contracts.contracts import ChainLinkView, ChainMembership


class PostgresInspectionSections:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def chain_membership(self, request_scope: str, run_id: str) -> ChainMembership | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            run = await mc.run_row(connection, args, run_id)
            if run is None:
                return None
            chain = await connection.fetchrow(
                f"""
                SELECT chain.chain_id, chain.chain_key, chain.lifecycle
                FROM mission_control.mission_chain chain
                WHERE {scoped("chain")} AND EXISTS (
                    SELECT 1 FROM mission_control.chain_link link
                    WHERE link.installation_id = chain.installation_id
                      AND link.application_id = chain.application_id
                      AND link.tenant_id = chain.tenant_id
                      AND link.chain_id = chain.chain_id
                      AND (link.from_mission_id = $4 OR link.to_mission_id = $4)
                )
                ORDER BY chain.created_at, chain.chain_id
                LIMIT 1
                """,
                *args,
                run["mission_id"],
            )
            if chain is None:
                return None
            links = await connection.fetch(
                f"""
                SELECT link.link_key, link.from_mission_key, link.to_mission_key, link.kind,
                       link.state, released.run_key AS released_run_key
                FROM mission_control.chain_link link
                LEFT JOIN mission_control.mission_run released
                  ON released.installation_id = link.installation_id
                 AND released.application_id = link.application_id
                 AND released.tenant_id = link.tenant_id
                 AND released.run_id = link.released_run_id
                WHERE {scoped("link")} AND link.chain_id = $4
                ORDER BY link.link_key
                """,
                *args,
                chain["chain_id"],
            )
        return ChainMembership(
            chain_id=str(chain["chain_id"]),
            chain_key=chain["chain_key"],
            lifecycle=chain["lifecycle"],
            links=tuple(
                ChainLinkView(
                    link_key=row["link_key"],
                    from_mission_key=row["from_mission_key"],
                    to_mission_key=row["to_mission_key"],
                    kind=row["kind"],
                    state=row["state"],
                    released_run_id=row["released_run_key"],
                )
                for row in links
            ),
        )

    async def active_subscriptions(self, request_scope: str, run_id: str) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            run = await mc.run_row(connection, args, run_id)
            if run is None:
                return 0
            count = await connection.fetchval(
                f"""
                SELECT count(*) FROM mission_control.mission_subscription
                WHERE {SCOPE} AND state = 'active'
                  AND (run_id = $4 OR (target_kind = 'mission' AND mission_id = $5))
                """,
                *args,
                run["run_id"],
                run["mission_id"],
            )
        return int(count or 0)


__all__ = ["PostgresInspectionSections"]
