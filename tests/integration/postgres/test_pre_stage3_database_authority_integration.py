"""Runtime-authority release surface (migrations 0010-0017) on mission_control.

Every support table is tenant scoped (FK to tenant, forced RLS with the three-column
context policy), immutable records carry the reject_mutation trigger, and the capability
roles hold exactly the runtime-authority grants: no PUBLIC access, no DELETE or TRUNCATE
for any login capability, the family writer alone writes family admission, the read-only
role only reads, and the catalog writer has nothing here.
"""

from __future__ import annotations

import pytest

from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows

pytestmark = pytest.mark.common_db

RUNTIME = "mission_control_runtime"
FAMILY = "mission_control_family_writer"
READONLY = "mission_control_readonly"
CATALOG = "mission_control_catalog_writer"
OUTBOX = "mission_control_outbox_worker"

S, SI, SIU = (
    frozenset({"SELECT"}),
    frozenset({"SELECT", "INSERT"}),
    frozenset({"SELECT", "INSERT", "UPDATE"}),
)
NONE: frozenset[str] = frozenset()

# Support table -> (runtime, family writer, readonly) table-level privileges.
SUPPORT_TABLES: dict[str, tuple[frozenset[str], frozenset[str], frozenset[str]]] = {
    "run_lifecycle_transition": (SI, SI, S),
    # FT-D2 (ADR-0029, migration 0028): the chain reducer admits the consumer run inside the
    # family writer's append_events transaction, so the family writer may INSERT ledgers.
    "effect_ledger": (SIU, SIU, S),
    "effect_ledger_entry": (SI, SI, S),
    "family_admission_head": (S, SIU, S),
    "family_admission_journal": (S, SI, S),
    "family_admission_result": (S, SI, S),
    "operation_claim": (SI, NONE, S),
    "operation_journal_mutation": (SI, NONE, S),
    "operation_technical_attempt": (SI, NONE, S),
    "operation_settlement": (SI, NONE, S),
    "runtime_unit_generation": (SI, NONE, S),
    "cognitive_namespace": (SI, NONE, S),
    "activity_attempt_observation": (SI, NONE, S),
    "checkpoint_transition": (SI, NONE, S),
    "unit_result_observation": (SI, NONE, S),
    "lineage_write_rejection": (SI, NONE, S),
    "run_snapshot": (SI, NONE, S),
    "fork_request": (SIU, NONE, S),
    "fork_reuse_decision": (SI, NONE, S),
    "execution_lineage_record": (SI, NONE, S),
    "execution_lineage_edge": (SI, NONE, S),
    "run_composition_link": (SI, NONE, S),
    "run_dependency_revision": (SI, NONE, S),
    "linked_result_decision": (SI, NONE, S),
    "linked_child_terminal": (SI, NONE, S),
    "runtime_document": (SI, NONE, S),
    "subordinate_admission": (SIU, NONE, S),
    "subordinate_message": (SI, NONE, S),
    "async_provider_run": (SIU, NONE, S),
    "subordinate_contract": (SI, NONE, S),
    "subordinate_execution_detail": (SI, NONE, S),
    "subordinate_link_detail": (SI, NONE, S),
    "coordinator_launch_ticket": (SIU, NONE, NONE),
    "coordinator_audit_event": (SI, NONE, NONE),
    "coordinator_workflow_result": (SI, NONE, NONE),
    "runtime_execution_binding": (SIU, NONE, S),
    "runtime_execution_attempt": (SI, NONE, S),
    "runtime_intervention": (SIU, NONE, S),
}
APPEND_ONLY = {
    "run_lifecycle_transition",
    "effect_ledger_entry",
    "family_admission_journal",
    "family_admission_result",
    "operation_journal_mutation",
    "operation_technical_attempt",
    "operation_settlement",
    "activity_attempt_observation",
    "checkpoint_transition",
    "unit_result_observation",
    "lineage_write_rejection",
    "run_snapshot",
    "fork_reuse_decision",
    "execution_lineage_record",
    "execution_lineage_edge",
    "run_composition_link",
    "run_dependency_revision",
    "linked_result_decision",
    "linked_child_terminal",
    "runtime_document",
    "subordinate_message",
    "subordinate_contract",
    "coordinator_audit_event",
    "coordinator_workflow_result",
    "runtime_execution_attempt",
}
# Column-level UPDATE grants (table-level UPDATE absent) per (role, table).
COLUMN_UPDATES = {
    (RUNTIME, "operation_claim"): {
        "status",
        "heartbeat_at",
        "lease_expires_at",
        "version",
        "updated_at",
    },
    (RUNTIME, "runtime_unit_generation"): {"lease_holder", "superseded", "updated_at"},
    (RUNTIME, "cognitive_namespace"): {
        "head_checkpoint",
        "head_checkpoint_key",
        "head_transition_key",
        "head_state_schema_digest",
        "head_version",
        "in_flight_unit_key",
        "in_flight_generation",
        "updated_at",
    },
    (RUNTIME, "subordinate_execution_detail"): {"execution_generation", "payload", "updated_at"},
    (RUNTIME, "subordinate_link_detail"): {"payload", "updated_at"},
    (RUNTIME, "attempt"): {"fencing_token", "lease_expires_at", "version", "updated_at"},
    (FAMILY, "command"): {"lifecycle", "outcome", "version", "updated_at"},
    (OUTBOX, "outbox"): {
        "delivery_state",
        "lease_owner",
        "lease_expires_at",
        "attempts",
        "next_attempt_at",
        "delivered_at",
        "version",
    },
}


async def _table_privileges(db: CommonDatabase, role: str, table: str) -> frozenset[str]:
    rows = await owner_rows(
        db,
        """
        SELECT privilege FROM unnest(ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
                                            'REFERENCES', 'TRIGGER']) AS privilege
        WHERE has_table_privilege($1, 'mission_control.' || $2, privilege)
        """,
        role,
        table,
    )
    return frozenset(row["privilege"] for row in rows)


@pytest.mark.asyncio
async def test_support_tables_are_scoped_forced_rls_and_append_only(
    common_db: CommonDatabase,
) -> None:
    rows = await owner_rows(
        common_db,
        """
        SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity,
               (SELECT array_agg(pg_get_expr(p.polqual, p.polrelid))
                FROM pg_policy p WHERE p.polrelid = c.oid) AS policies,
               EXISTS (
                   SELECT 1 FROM pg_constraint k
                   WHERE k.conrelid = c.oid AND k.contype = 'f'
                     AND k.confrelid = 'mission_control.tenant'::regclass
               ) AS tenant_fk,
               EXISTS (
                   SELECT 1 FROM pg_trigger t JOIN pg_proc f ON f.oid = t.tgfoid
                   WHERE t.tgrelid = c.oid AND f.proname = 'reject_mutation'
               ) AS immutable,
               (SELECT array_agg(a.attname ORDER BY a.attnum) FROM pg_attribute a
                WHERE a.attrelid = c.oid AND a.attnum IN (1, 2, 3)) AS scope_columns
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'mission_control' AND c.relname = ANY($1::text[])
        """,
        list(SUPPORT_TABLES),
    )
    assert {row["relname"] for row in rows} == set(SUPPORT_TABLES)
    for row in rows:
        name = row["relname"]
        assert (row["relrowsecurity"], row["relforcerowsecurity"]) == (True, True), name
        assert row["tenant_fk"], name
        assert row["scope_columns"] == ["installation_id", "application_id", "tenant_id"], name
        (policy,) = row["policies"]
        for column in ("installation_id", "application_id", "tenant_id"):
            assert column in policy, (name, policy)
        assert row["immutable"] == (name in APPEND_ONLY), name


@pytest.mark.asyncio
async def test_capability_role_matrix_is_least_privilege(common_db: CommonDatabase) -> None:
    for table, (runtime, family, readonly) in SUPPORT_TABLES.items():
        assert await _table_privileges(common_db, RUNTIME, table) == runtime, table
        assert await _table_privileges(common_db, FAMILY, table) == family, table
        assert await _table_privileges(common_db, READONLY, table) == readonly, table
        assert await _table_privileges(common_db, CATALOG, table) == NONE, table
        assert await _table_privileges(common_db, OUTBOX, table) == NONE, table
        assert await _table_privileges(common_db, "public", table) == NONE, table
    # No login capability may delete or truncate any mission_control record.
    deleters = await owner_rows(
        common_db,
        """
        SELECT r.rolname, c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN (VALUES ($1), ($2), ($3), ($4), ($5)) AS r(rolname)
        WHERE n.nspname = 'mission_control' AND c.relkind = 'r'
          AND (has_table_privilege(r.rolname, c.oid, 'DELETE')
               OR has_table_privilege(r.rolname, c.oid, 'TRUNCATE'))
        """,
        RUNTIME,
        FAMILY,
        READONLY,
        CATALOG,
        OUTBOX,
    )
    assert deleters == []
    for (role, table), columns in COLUMN_UPDATES.items():
        granted = await owner_rows(
            common_db,
            """
            SELECT a.attname FROM pg_attribute a
            WHERE a.attrelid = ('mission_control.' || $2)::regclass AND a.attnum > 0
              AND NOT a.attisdropped
              AND has_column_privilege($1, a.attrelid, a.attnum, 'UPDATE')
            """,
            role,
            table,
        )
        assert {row["attname"] for row in granted} == columns, (role, table)
    # The runtime capability never inherits the family writer or catalog authority, and the
    # immutable canonical records stay insert-only for it.
    memberships = await owner_rows(
        common_db,
        "SELECT pg_has_role($1, $2, 'MEMBER') AS family, pg_has_role($1, $3, 'MEMBER') AS catalog",
        RUNTIME,
        FAMILY,
        CATALOG,
    )
    assert dict(memberships[0]) == {"family": False, "catalog": False}
    for table in (
        "definition_snapshot",
        "mission_revision",
        "compiled_program",
        "ledger_commit",
        "mission_event",
        "budget_entry",
        "delivery_report",
        "operation_receipt",
        "continuation_checkpoint",
        "fork_lineage",
        "human_resolution",
        "native_observation",
    ):
        assert await _table_privileges(common_db, RUNTIME, table) == SI, table
    sequence = await owner_rows(
        common_db,
        """
        SELECT has_sequence_privilege($1, 'mission_control.outbox_global_position', 'USAGE')
                 AS runtime,
               has_sequence_privilege($2, 'mission_control.outbox_global_position', 'USAGE')
                 AS family,
               has_sequence_privilege($3, 'mission_control.outbox_global_position', 'USAGE')
                 AS readonly
        """,
        RUNTIME,
        FAMILY,
        READONLY,
    )
    assert dict(sequence[0]) == {"runtime": True, "family": True, "readonly": False}
