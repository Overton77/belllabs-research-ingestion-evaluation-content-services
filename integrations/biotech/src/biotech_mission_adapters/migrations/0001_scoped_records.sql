-- OPTIONAL Biotech adapter schema. Not part of the general Mission Control migration chain.
CREATE SCHEMA IF NOT EXISTS biotech_mission_adapters;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='biotech_mission_adapter_runtime') THEN
    CREATE ROLE biotech_mission_adapter_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS biotech_mission_adapters.records (
  request_scope text NOT NULL CHECK (length(btrim(request_scope)) > 0),
  record_kind text NOT NULL CHECK (record_kind IN ('schema_grounding','web_research')),
  record_identity text NOT NULL CHECK (length(record_identity) > 0),
  run_id text,
  intent_key text,
  payload jsonb NOT NULL,
  PRIMARY KEY(request_scope,record_kind,record_identity),
  UNIQUE(request_scope,record_kind,run_id,intent_key),
  CONSTRAINT record_identity_matches CHECK ((
    jsonb_typeof(payload)='object'
    AND payload->>'request_scope'=request_scope
    AND (payload->>'run_id') IS NOT DISTINCT FROM run_id
    AND jsonb_typeof(payload->'payload')='object'
    AND payload->>'content_digest' ~ '^sha256:[0-9a-f]{64}$'
    AND jsonb_typeof(payload->'created_at')='string'
    AND CASE WHEN record_kind='schema_grounding' THEN
      (payload->>'record_type') || ':' || (payload->>'record_id')=record_identity
      AND intent_key IS NULL
    ELSE payload->>'record_id'=record_identity
      AND payload->>'intent_key'=intent_key AND run_id IS NOT NULL
    END
  ) IS TRUE)
);
ALTER TABLE biotech_mission_adapters.records ENABLE ROW LEVEL SECURITY;
ALTER TABLE biotech_mission_adapters.records FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS scoped_records ON biotech_mission_adapters.records;
CREATE POLICY scoped_records ON biotech_mission_adapters.records
  USING (request_scope=current_setting('biotech.request_scope',true))
  WITH CHECK (request_scope=current_setting('biotech.request_scope',true));
CREATE OR REPLACE FUNCTION biotech_mission_adapters.reject_record_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
  RAISE EXCEPTION 'Biotech adapter records are immutable' USING ERRCODE='55000';
END $$;
DROP TRIGGER IF EXISTS immutable_records ON biotech_mission_adapters.records;
CREATE TRIGGER immutable_records BEFORE UPDATE OR DELETE ON biotech_mission_adapters.records
  FOR EACH ROW EXECUTE FUNCTION biotech_mission_adapters.reject_record_mutation();
GRANT USAGE ON SCHEMA biotech_mission_adapters TO biotech_mission_adapter_runtime;
GRANT SELECT, INSERT ON biotech_mission_adapters.records TO biotech_mission_adapter_runtime;
