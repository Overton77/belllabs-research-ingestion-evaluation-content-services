"""A content digest of one run's authority rows and saver checkpoints.

Used by fork demonstrations to prove that taking a snapshot, admitting a fork and running the
derived run leave the source (parent) run unchanged: its run-control rows, units, results,
checkpoint transitions, effect claims, cognitive namespaces and LangGraph checkpoints.
"""

from __future__ import annotations

import asyncpg


async def run_authority_digest(
    pool: asyncpg.Pool, run_id: str, *, saver_schema: str
) -> dict[str, str]:
    """A content digest of every authority row of one run (owner reads bypass RLS)."""

    queries = {
        "workflow_runs": "SELECT * FROM belllabs_control.workflow_runs WHERE run_id = $1",
        "budget_accounts": "SELECT * FROM belllabs_control.budget_accounts WHERE run_id = $1",
        "effect_ledgers": "SELECT * FROM belllabs_control.effect_ledgers WHERE run_id = $1",
        "family_admission_heads": (
            "SELECT * FROM belllabs_control.family_admission_heads WHERE run_id = $1"
        ),
        "runtime_units": (
            "SELECT * FROM belllabs_control.runtime_units WHERE belllabs_run_id = $1"
        ),
        "runtime_unit_result_observations": (
            "SELECT r.* FROM belllabs_control.runtime_unit_result_observations r"
            " JOIN belllabs_control.runtime_units u USING (request_scope, unit_key)"
            " WHERE u.belllabs_run_id = $1"
        ),
        "runtime_checkpoint_transitions": (
            "SELECT t.* FROM belllabs_control.runtime_checkpoint_transitions t"
            " JOIN belllabs_control.runtime_units u USING (request_scope, unit_key)"
            " WHERE u.belllabs_run_id = $1"
        ),
        "operation_effect_claims": (
            "SELECT * FROM belllabs_control.operation_effect_claims WHERE belllabs_run_id = $1"
        ),
        "runtime_cognitive_namespaces": (
            "SELECT n.* FROM belllabs_control.runtime_cognitive_namespaces n"
            " WHERE n.cognitive_namespace IN ("
            "  SELECT g.cognitive_namespace FROM belllabs_control.runtime_unit_generations g"
            "  JOIN belllabs_control.runtime_units u USING (request_scope, unit_key)"
            "  WHERE u.belllabs_run_id = $1)"
        ),
        "saver_checkpoints": (
            f"SELECT c.thread_id, c.checkpoint_id, c.metadata FROM {saver_schema}.checkpoints c"
            " WHERE c.thread_id IN ("
            "  SELECT g.cognitive_namespace FROM belllabs_control.runtime_unit_generations g"
            "  JOIN belllabs_control.runtime_units u USING (request_scope, unit_key)"
            "  WHERE u.belllabs_run_id = $1)"
        ),
    }
    async with pool.acquire() as connection:
        return {
            name: await connection.fetchval(
                f"SELECT md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), ''))"
                f" FROM ({query}) t",
                run_id,
            )
            for name, query in queries.items()
        }


__all__ = ["run_authority_digest"]
