-- mission-control-db-contract common release: orchestration support records.
-- Linked runs are canonical mission_relationship rows (kind 'composition') between two
-- independent mission_run rows; the support records keep the exact request identity,
-- dependency revisions, result admissions and observed child terminals. Semantic input
-- bindings are canonical immutable execution_binding manifests. Goal, StageGraph and
-- operation detail envelopes are immutable support runtime_document rows.

CREATE TABLE mission_control.run_composition_link (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    run_composition_link_id uuid PRIMARY KEY,
    link_key text NOT NULL CHECK (link_key <> ''),
    request_identity text NOT NULL CHECK (request_identity <> ''),
    request_fingerprint text NOT NULL CHECK (request_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
    parent_run_key text NOT NULL CHECK (parent_run_key <> ''),
    child_run_key text NOT NULL CHECK (child_run_key <> ''),
    linked_budget_account_key text NOT NULL CHECK (linked_budget_account_key <> ''),
    link_contract text NOT NULL CHECK (link_contract = 'mc.run-composition-link/1'),
    link jsonb NOT NULL CHECK (jsonb_typeof(link) = 'object'),
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    CHECK (parent_run_key <> child_run_key),
    UNIQUE (installation_id, application_id, tenant_id, run_composition_link_id),
    UNIQUE (installation_id, application_id, tenant_id, link_key),
    UNIQUE (installation_id, application_id, tenant_id, request_identity),
    UNIQUE (installation_id, application_id, tenant_id, child_run_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, link_key) REFERENCES mission_control.mission_relationship (installation_id, application_id, tenant_id, relationship_key) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (installation_id, application_id, tenant_id, parent_run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, child_run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, linked_budget_account_key) REFERENCES mission_control.budget_account (installation_id, application_id, tenant_id, account_key)
);
CREATE INDEX run_composition_link_parent_idx ON mission_control.run_composition_link (installation_id, application_id, tenant_id, parent_run_key, link_key);
CREATE INDEX run_composition_link_budget_fk_idx ON mission_control.run_composition_link (installation_id, application_id, tenant_id, linked_budget_account_key);
CREATE TRIGGER run_composition_link_immutable
    BEFORE UPDATE OR DELETE ON mission_control.run_composition_link
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.run_dependency_revision (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    run_dependency_revision_id uuid PRIMARY KEY,
    revision_key text NOT NULL CHECK (revision_key <> ''),
    link_key text NOT NULL CHECK (link_key <> ''),
    revision integer NOT NULL CHECK (revision >= 2),
    decision_contract text NOT NULL CHECK (decision_contract = 'mc.run-dependency-revision/1'),
    decision jsonb NOT NULL CHECK (jsonb_typeof(decision) = 'object'),
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, run_dependency_revision_id),
    UNIQUE (installation_id, application_id, tenant_id, revision_key),
    UNIQUE (installation_id, application_id, tenant_id, link_key, revision),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, link_key) REFERENCES mission_control.run_composition_link (installation_id, application_id, tenant_id, link_key)
);
CREATE TRIGGER run_dependency_revision_immutable
    BEFORE UPDATE OR DELETE ON mission_control.run_dependency_revision
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.linked_result_decision (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    linked_result_decision_id uuid PRIMARY KEY,
    decision_key text NOT NULL CHECK (decision_key <> ''),
    link_key text NOT NULL CHECK (link_key <> ''),
    parent_run_key text NOT NULL CHECK (parent_run_key <> ''),
    child_run_key text NOT NULL CHECK (child_run_key <> ''),
    exact_output_ref text NOT NULL CHECK (exact_output_ref <> ''),
    decision_contract text NOT NULL CHECK (decision_contract = 'mc.linked-result-decision/1'),
    decision jsonb NOT NULL CHECK (jsonb_typeof(decision) = 'object'),
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, linked_result_decision_id),
    UNIQUE (installation_id, application_id, tenant_id, decision_key),
    UNIQUE (installation_id, application_id, tenant_id, link_key, exact_output_ref),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, link_key) REFERENCES mission_control.run_composition_link (installation_id, application_id, tenant_id, link_key)
);
CREATE TRIGGER linked_result_decision_immutable
    BEFORE UPDATE OR DELETE ON mission_control.linked_result_decision
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.linked_child_terminal (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    linked_child_terminal_id uuid PRIMARY KEY,
    terminal_record_key text NOT NULL CHECK (terminal_record_key <> ''),
    link_key text NOT NULL CHECK (link_key <> ''),
    child_run_key text NOT NULL CHECK (child_run_key <> ''),
    status text NOT NULL CHECK (status IN ('completed', 'failed', 'cancelled', 'timed_out')),
    record_contract text NOT NULL CHECK (record_contract = 'mc.linked-child-terminal/1'),
    record jsonb NOT NULL CHECK (jsonb_typeof(record) = 'object'),
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, linked_child_terminal_id),
    UNIQUE (installation_id, application_id, tenant_id, terminal_record_key),
    UNIQUE (installation_id, application_id, tenant_id, link_key),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id),
    FOREIGN KEY (installation_id, application_id, tenant_id, link_key) REFERENCES mission_control.run_composition_link (installation_id, application_id, tenant_id, link_key),
    FOREIGN KEY (installation_id, application_id, tenant_id, child_run_key) REFERENCES mission_control.mission_run (installation_id, application_id, tenant_id, run_key)
);
CREATE INDEX linked_child_terminal_child_fk_idx ON mission_control.linked_child_terminal (installation_id, application_id, tenant_id, child_run_key);
CREATE TRIGGER linked_child_terminal_immutable
    BEFORE UPDATE OR DELETE ON mission_control.linked_child_terminal
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

CREATE TABLE mission_control.runtime_document (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_id uuid NOT NULL,
    runtime_document_id uuid PRIMARY KEY,
    contract text NOT NULL CHECK (contract IN (
        'goal.revision/1', 'goal.iteration/1', 'goal.handoff/1', 'goal.verification/1',
        'goal.template/1', 'stagegraph.template/1', 'operation.binding/1',
        'operation.binding-index/1', 'operation.settlement/1', 'operation.claim/1'
    )),
    identity text NOT NULL CHECK (identity <> ''),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    digest text NOT NULL CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    created_by_actor_ref text NOT NULL CHECK (created_by_actor_ref <> ''),
    UNIQUE (installation_id, application_id, tenant_id, runtime_document_id),
    UNIQUE (installation_id, application_id, tenant_id, contract, identity),
    FOREIGN KEY (installation_id, application_id, tenant_id) REFERENCES mission_control.tenant (installation_id, application_id, tenant_id)
);
CREATE TRIGGER runtime_document_immutable
    BEFORE UPDATE OR DELETE ON mission_control.runtime_document
    FOR EACH ROW EXECUTE FUNCTION mission_control.reject_mutation();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'run_composition_link',
        'run_dependency_revision',
        'linked_result_decision',
        'linked_child_terminal',
        'runtime_document'
    ] LOOP
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format('ALTER TABLE mission_control.%I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format(
            'CREATE POLICY %I ON mission_control.%I '
            'USING (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id() '
            'AND tenant_id = mission_control.ctx_tenant_id()) '
            'WITH CHECK (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id() '
            'AND tenant_id = mission_control.ctx_tenant_id())',
            table_name || '_scope', table_name);
    END LOOP;
END
$policies$;

REVOKE ALL ON mission_control.run_composition_link, mission_control.run_dependency_revision,
    mission_control.linked_result_decision, mission_control.linked_child_terminal,
    mission_control.runtime_document FROM PUBLIC;

GRANT SELECT, INSERT ON mission_control.mission_relationship, mission_control.execution_binding,
    mission_control.run_composition_link, mission_control.run_dependency_revision,
    mission_control.linked_result_decision, mission_control.linked_child_terminal,
    mission_control.runtime_document
TO mission_control_runtime;
GRANT SELECT ON mission_control.mission_relationship, mission_control.execution_binding,
    mission_control.run_composition_link, mission_control.run_dependency_revision,
    mission_control.linked_result_decision, mission_control.linked_child_terminal,
    mission_control.runtime_document
TO mission_control_readonly;
