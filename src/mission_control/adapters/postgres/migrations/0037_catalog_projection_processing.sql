-- Processing state for the immutable definition catalog outbox. No historic data deletion.
CREATE TABLE belllabs_control.catalog_projection_processing (
 catalog_scope text NOT NULL CHECK(catalog_scope <> ''),
 event_id text NOT NULL CHECK(event_id ~ '^sha256:[a-f0-9]{64}$'),
 payload jsonb NOT NULL,
 PRIMARY KEY(catalog_scope,event_id),
 CHECK ((payload->>'event_id'=event_id AND payload->>'tenant_scope'=catalog_scope
   AND payload->>'state' IN ('pending','processing','retry','completed','poison')
   AND (payload->>'attempt_count')::integer >= 0) IS TRUE)
);
CREATE TABLE belllabs_control.catalog_projection_alerts (
 catalog_scope text NOT NULL CHECK(catalog_scope <> ''),
 alert_id text NOT NULL,
 payload jsonb NOT NULL,
 PRIMARY KEY(catalog_scope,alert_id),
 CHECK ((payload->>'alert_id'=alert_id) IS TRUE)
);
ALTER TABLE belllabs_control.catalog_projection_processing ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.catalog_projection_processing FORCE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.catalog_projection_alerts ENABLE ROW LEVEL SECURITY;
ALTER TABLE belllabs_control.catalog_projection_alerts FORCE ROW LEVEL SECURITY;
CREATE POLICY catalog_projection_processing_scope ON belllabs_control.catalog_projection_processing
 USING(catalog_scope=current_setting('belllabs.catalog_scope',true))
 WITH CHECK(catalog_scope=current_setting('belllabs.catalog_scope',true));
CREATE POLICY catalog_projection_alerts_scope ON belllabs_control.catalog_projection_alerts
 USING(catalog_scope=current_setting('belllabs.catalog_scope',true))
 WITH CHECK(catalog_scope=current_setting('belllabs.catalog_scope',true));
GRANT SELECT,INSERT,UPDATE ON belllabs_control.catalog_projection_processing TO belllabs_control_runtime;
GRANT SELECT,INSERT ON belllabs_control.catalog_projection_alerts TO belllabs_control_runtime;
GRANT SELECT ON belllabs_control.catalog_projection_processing,belllabs_control.catalog_projection_alerts TO belllabs_operations_readonly;
CREATE TRIGGER catalog_projection_alerts_append_only BEFORE UPDATE OR DELETE
 ON belllabs_control.catalog_projection_alerts FOR EACH ROW
 EXECUTE FUNCTION belllabs_control.reject_immutable_document_mutation();
