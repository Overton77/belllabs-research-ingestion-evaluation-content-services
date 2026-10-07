"""A content digest of one run's authority rows and saver checkpoints.

Used by fork demonstrations to prove that taking a snapshot, admitting a fork and running the
derived run leave the source (parent) run unchanged: its canonical run, budget and effect
ledgers, family heads, runtime units (activations) with their results and checkpoint
transitions, operation claims, cognitive namespaces and LangGraph checkpoints, all read from
the common ``mission_control`` component.
"""

from __future__ import annotations

import asyncpg

from mission_control.contracts.identities import parse_request_scope

_RUN = (
    "SELECT r.run_id FROM mission_control.mission_run r"
    " WHERE (r.installation_id, r.application_id, r.tenant_id) = ($2, $3, $4)"
    " AND r.run_key = $1"
)
_UNITS = (
    "SELECT a.activation_key FROM mission_control.activation a"
    " WHERE (a.installation_id, a.application_id, a.tenant_id) = ($2, $3, $4)"
    f" AND a.run_id = ({_RUN})"
)
_SCOPED = "(installation_id, application_id, tenant_id) = ($2, $3, $4)"
_NAMESPACES = (
    "SELECT g.cognitive_namespace FROM mission_control.runtime_unit_generation g"
    f" WHERE (g.installation_id, g.application_id, g.tenant_id) = ($2, $3, $4)"
    f" AND g.unit_key IN ({_UNITS})"
)


async def run_authority_digest(
    pool: asyncpg.Pool,
    run_id: str,
    *,
    saver_schema: str = "mission_control_runtime",
    request_scope: str,
) -> dict[str, str]:
    """A content digest of every authority row of one run (owner reads bypass RLS)."""

    queries = {
        "mission_run": (
            f"SELECT * FROM mission_control.mission_run WHERE {_SCOPED} AND run_key = $1"
        ),
        "budget_account": (
            f"SELECT * FROM mission_control.budget_account WHERE {_SCOPED} AND run_id = ({_RUN})"
        ),
        "effect_ledger": (
            f"SELECT * FROM mission_control.effect_ledger WHERE {_SCOPED} AND run_key = $1"
        ),
        "family_admission_head": (
            f"SELECT * FROM mission_control.family_admission_head WHERE {_SCOPED} AND run_key = $1"
        ),
        "activation": (
            f"SELECT * FROM mission_control.activation WHERE {_SCOPED} AND run_id = ({_RUN})"
        ),
        "unit_result_observation": (
            f"SELECT * FROM mission_control.unit_result_observation"
            f" WHERE {_SCOPED} AND unit_key IN ({_UNITS})"
        ),
        "checkpoint_transition": (
            f"SELECT * FROM mission_control.checkpoint_transition"
            f" WHERE {_SCOPED} AND unit_key IN ({_UNITS})"
        ),
        "operation_claim": (
            f"SELECT * FROM mission_control.operation_claim WHERE {_SCOPED} AND run_key = $1"
        ),
        "cognitive_namespace": (
            f"SELECT * FROM mission_control.cognitive_namespace"
            f" WHERE {_SCOPED} AND namespace_key IN ({_NAMESPACES})"
        ),
        "saver_checkpoints": (
            f"SELECT c.thread_id, c.checkpoint_id, c.metadata FROM {saver_schema}.checkpoints c"
            f" WHERE c.thread_id IN ({_NAMESPACES})"
        ),
    }
    scope = parse_request_scope(request_scope)
    async with pool.acquire() as connection:
        return {
            name: await connection.fetchval(
                f"SELECT md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), ''))"
                f" FROM ({query}) t",
                run_id,
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
            )
            for name, query in queries.items()
        }


__all__ = ["run_authority_digest"]
