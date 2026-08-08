CREATE SCHEMA IF NOT EXISTS dynamic_research_swarm_experiment;

CREATE TABLE IF NOT EXISTS dynamic_research_swarm_experiment.mission_plans (
    run_id text NOT NULL,
    revision integer NOT NULL,
    plan_json jsonb NOT NULL,
    plan_sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, revision)
);

CREATE TABLE IF NOT EXISTS dynamic_research_swarm_experiment.source_snapshots (
    source_id text NOT NULL,
    run_id text NOT NULL,
    stage_id text NOT NULL,
    url text NOT NULL,
    title text NOT NULL,
    text_content text NOT NULL,
    text_sha256 text NOT NULL,
    retrieved_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, source_id),
    UNIQUE (run_id, stage_id, url, text_sha256)
);

CREATE TABLE IF NOT EXISTS dynamic_research_swarm_experiment.claims (
    claim_id text PRIMARY KEY,
    run_id text NOT NULL,
    unit_id text NOT NULL,
    claim_json jsonb NOT NULL,
    disposition text NOT NULL,
    source_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS dynamic_research_swarm_experiment.evaluations (
    evaluation_id text PRIMARY KEY,
    claim_id text NOT NULL UNIQUE REFERENCES dynamic_research_swarm_experiment.claims(claim_id),
    policy_version text NOT NULL,
    report_json jsonb NOT NULL,
    report_sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'belllabs_app') THEN
        GRANT USAGE ON SCHEMA dynamic_research_swarm_experiment TO belllabs_app;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES
            IN SCHEMA dynamic_research_swarm_experiment TO belllabs_app;
    END IF;
END $$;
