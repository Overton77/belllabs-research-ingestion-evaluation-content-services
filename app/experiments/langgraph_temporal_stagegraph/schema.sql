CREATE SCHEMA IF NOT EXISTS stagegraph_temporal_experiment;

CREATE TABLE IF NOT EXISTS stagegraph_temporal_experiment.runs (
    run_id text PRIMARY KEY,
    thread_id text NOT NULL UNIQUE,
    status text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS stagegraph_temporal_experiment.stage_attempts (
    attempt_id text PRIMARY KEY,
    run_id text NOT NULL REFERENCES stagegraph_temporal_experiment.runs(run_id),
    stage_id text NOT NULL,
    attempt_number integer NOT NULL,
    status text NOT NULL,
    prompt text NOT NULL,
    delay_seconds double precision NOT NULL DEFAULT 0,
    temporal_workflow_id text UNIQUE,
    temporal_run_id text,
    output_ref text,
    output_digest text,
    error_type text,
    reserved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    launched_at timestamptz,
    completed_at timestamptz,
    admitted_at timestamptz,
    UNIQUE (run_id, stage_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS stagegraph_temporal_experiment.results (
    output_ref text PRIMARY KEY,
    attempt_id text NOT NULL UNIQUE REFERENCES stagegraph_temporal_experiment.stage_attempts(attempt_id),
    output_digest text NOT NULL,
    output_text text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS stagegraph_temporal_experiment.outbox (
    event_id text PRIMARY KEY,
    run_id text NOT NULL REFERENCES stagegraph_temporal_experiment.runs(run_id),
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    delivered_at timestamptz,
    delivery_attempts integer NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS stagegraph_temporal_experiment.graph_events (
    event_id text PRIMARY KEY,
    run_id text NOT NULL REFERENCES stagegraph_temporal_experiment.runs(run_id),
    event_type text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'belllabs_app') THEN
        GRANT USAGE ON SCHEMA stagegraph_temporal_experiment TO belllabs_app;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES
            IN SCHEMA stagegraph_temporal_experiment TO belllabs_app;
    END IF;
END $$;
