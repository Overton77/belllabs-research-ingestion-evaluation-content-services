CREATE TABLE IF NOT EXISTS belllabs_control.agent_runtime_sessions (
    request_scope text NOT NULL,
    session_id text NOT NULL,
    binding_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (request_scope, binding_id, session_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.agent_runtime_messages (
    message_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_scope text NOT NULL,
    binding_id text NOT NULL,
    session_id text NOT NULL,
    message_data jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (request_scope, binding_id, session_id)
        REFERENCES belllabs_control.agent_runtime_sessions(request_scope, binding_id, session_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS agent_runtime_messages_session_idx
    ON belllabs_control.agent_runtime_messages(
        request_scope, binding_id, session_id, message_id
    );

CREATE TABLE IF NOT EXISTS belllabs_control.agent_runtime_events (
    event_id text NOT NULL,
    request_scope text NOT NULL,
    binding_id text NOT NULL,
    run_id text NOT NULL,
    operation_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence >= 1),
    event_type text NOT NULL,
    envelope jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, event_id),
    UNIQUE (request_scope, binding_id, sequence)
);

CREATE TABLE IF NOT EXISTS belllabs_control.agent_runtime_approval_requests (
    approval_id text NOT NULL,
    request_scope text NOT NULL,
    binding_id text NOT NULL,
    request_payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'rejected', 'expired')),
    requested_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, approval_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.agent_runtime_approval_decisions (
    approval_id text NOT NULL,
    request_scope text NOT NULL,
    decision_payload jsonb NOT NULL,
    decided_at timestamptz NOT NULL,
    PRIMARY KEY (request_scope, approval_id),
    FOREIGN KEY (request_scope, approval_id)
        REFERENCES belllabs_control.agent_runtime_approval_requests(request_scope, approval_id)
);

CREATE TABLE IF NOT EXISTS belllabs_control.agent_runtime_checkpoints (
    binding_id text NOT NULL,
    request_scope text NOT NULL,
    state_json text NOT NULL,
    state_mac text NOT NULL,
    status text NOT NULL CHECK (status IN ('awaiting_approval', 'resuming', 'completed')),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (request_scope, binding_id)
);

ALTER TABLE belllabs_control.agent_runtime_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.agent_runtime_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.agent_runtime_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.agent_runtime_approval_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.agent_runtime_approval_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.agent_runtime_checkpoints ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'agent_runtime_sessions',
        'agent_runtime_messages',
        'agent_runtime_events',
        'agent_runtime_approval_requests',
        'agent_runtime_approval_decisions',
        'agent_runtime_checkpoints'
    ]
    LOOP
        EXECUTE format('DROP POLICY IF EXISTS request_scope_isolation ON belllabs_control.%I',
                       table_name);
        EXECUTE format(
            'CREATE POLICY request_scope_isolation ON belllabs_control.%I
             USING (request_scope = current_setting(''belllabs.request_scope'', true))
             WITH CHECK (request_scope = current_setting(''belllabs.request_scope'', true))',
            table_name
        );
        EXECUTE format('ALTER TABLE belllabs_control.%I FORCE ROW LEVEL SECURITY', table_name);
    END LOOP;
END
$$;

GRANT SELECT, INSERT, UPDATE, DELETE
    ON belllabs_control.agent_runtime_sessions,
       belllabs_control.agent_runtime_messages
    TO belllabs_control_runtime;
GRANT SELECT, INSERT
    ON belllabs_control.agent_runtime_events,
       belllabs_control.agent_runtime_approval_requests,
       belllabs_control.agent_runtime_approval_decisions,
       belllabs_control.agent_runtime_checkpoints
    TO belllabs_control_runtime;
GRANT UPDATE (status)
    ON belllabs_control.agent_runtime_approval_requests
    TO belllabs_control_runtime;
GRANT UPDATE (state_json, state_mac, status, updated_at)
    ON belllabs_control.agent_runtime_checkpoints
    TO belllabs_control_runtime;
GRANT USAGE, SELECT ON SEQUENCE
    belllabs_control.agent_runtime_messages_message_id_seq
    TO belllabs_control_runtime;
