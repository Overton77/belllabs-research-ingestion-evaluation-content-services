-- Upgrade the transitional runtime tables without rewriting migration 0028 or data.
-- PostgreSQL CHECK permits UNKNOWN: absent or JSON-null identity keys must fail closed.
-- Adding validated constraints intentionally stops this migration if prior rows are corrupt;
-- repair/backfill requires a separately reviewed plan, never silently discard history.
ALTER TABLE belllabs_control.workspace_manifests
    ADD CONSTRAINT workspace_manifest_payload_identity_complete CHECK ((
        payload->>'namespace_id' = namespace_id
        AND payload->>'workspace_id' = workspace_id
        AND payload->>'manifest_id' = manifest_id
        AND payload->'revision' = to_jsonb(revision)
        AND payload->>'manifest_digest' = manifest_digest
        AND payload ? 'prior_manifest_digest'
        AND (payload->>'prior_manifest_digest') IS NOT DISTINCT FROM prior_manifest_digest
    ) IS TRUE);

ALTER TABLE belllabs_control.artifact_metadata_revisions
    ADD CONSTRAINT artifact_metadata_payload_identity_complete CHECK ((
        payload->>'request_scope' = request_scope
        AND payload->>'artifact_id' = artifact_id
        AND payload->>'intent_key' = intent_key
        AND payload->>'promotion_id' = promotion_id
        AND payload->'revision' = to_jsonb(revision)
        AND payload->>'state' = state
    ) IS TRUE);
