-- mission-control-db-contract common release: rebuildable capability search projection
-- in mission_control_search (catalog/artifacts lane; port of legacy capability_search
-- 0005/0007). The schema itself is created by 0001. Requires the `vector` extension in
-- schema `extensions` (release-spec required_extensions); no pgcrypto: PostgreSQL 17's
-- pg_catalog.sha256 computes the source-set digest.
--
-- RLS decision: the projection is INSTALLATION scoped. Every row carries
-- (installation_id, application_id) with an FK to application_installation, ENABLE +
-- FORCE RLS and the two-column installation/app policy, so one project's rows can never
-- satisfy another installation's context. Inside an installation, `tenant_scope` keeps
-- its legacy meaning ('global' or the exact owning scope string) and the repository
-- filters it; asset admission is re-checked against mission_control.asset_version at
-- query time. Legacy capability_search had RLS enabled but not forced and no role
-- grants; here grants are explicit and the owner is never used at runtime.
-- Embeddings: never invented. Dimension is pinned to 1536 exactly as legacy; semantic
-- search is unavailable until a generation's model/dimension/source-set checks pass.

CREATE TABLE mission_control_search.projection_generation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_scope text NOT NULL CHECK (tenant_scope <> ''),
    projection_generation text NOT NULL CHECK (projection_generation <> ''),
    embedding_model_id text NOT NULL CHECK (embedding_model_id <> ''),
    embedding_dimensions integer NOT NULL CHECK (embedding_dimensions >= 1),
    search_document_format_version integer NOT NULL CHECK (search_document_format_version >= 1),
    selected_kinds text[] NOT NULL CHECK (cardinality(selected_kinds) >= 1),
    expected_count integer NOT NULL CHECK (expected_count >= 0),
    expected_source_set_digest text NOT NULL
        CHECK (expected_source_set_digest ~ '^sha256:[0-9a-f]{64}$'),
    actual_count integer CHECK (actual_count >= 0),
    actual_source_set_digest text CHECK (actual_source_set_digest ~ '^sha256:[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN ('building', 'active', 'failed', 'superseded')),
    created_at timestamptz NOT NULL,
    verified_at timestamptz,
    activated_at timestamptz,
    PRIMARY KEY (installation_id, application_id, tenant_scope, projection_generation),
    FOREIGN KEY (installation_id, application_id)
        REFERENCES mission_control.application_installation (installation_id, application_id)
);

CREATE TABLE mission_control_search.search_document (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    search_document_id uuid NOT NULL,
    tenant_scope text NOT NULL CHECK (tenant_scope <> ''),
    asset_kind text NOT NULL CHECK (asset_kind <> ''),
    logical_id text NOT NULL CHECK (logical_id <> ''),
    revision integer NOT NULL CHECK (revision >= 1),
    source_digest text NOT NULL CHECK (source_digest ~ '^sha256:[0-9a-f]{64}$'),
    status text NOT NULL CHECK (status IN ('published', 'deprecated', 'retired', 'revoked')),
    title text NOT NULL,
    description text NOT NULL,
    search_text text NOT NULL,
    search_text_digest text NOT NULL CHECK (search_text_digest ~ '^sha256:[0-9a-f]{64}$'),
    fts tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english'::regconfig, coalesce(title, '')), 'A') ||
        setweight(to_tsvector('english'::regconfig, coalesce(logical_id, '')), 'A') ||
        setweight(to_tsvector('english'::regconfig, coalesce(search_text, '')), 'B') ||
        setweight(to_tsvector('english'::regconfig, coalesce(description, '')), 'C')
    ) STORED,
    embedding extensions.vector(1536) NOT NULL,
    embedding_model_id text NOT NULL CHECK (embedding_model_id <> ''),
    embedding_dimensions integer NOT NULL CHECK (embedding_dimensions = 1536),
    search_document_format_version integer NOT NULL,
    parent_kind text,
    parent_logical_id text,
    parent_revision integer,
    parent_source_digest text,
    mongodb_collection text NOT NULL,
    mongodb_document_id text NOT NULL,
    tags text[] NOT NULL,
    domains text[] NOT NULL,
    operation_classes text[] NOT NULL,
    workflow_type_refs jsonb NOT NULL CHECK (jsonb_typeof(workflow_type_refs) = 'array'),
    capability_requirements text[] NOT NULL,
    compatible_runtimes text[] NOT NULL,
    compatibility_summary text NOT NULL,
    schema_digest_verified boolean NOT NULL,
    source_published_at timestamptz NOT NULL,
    indexed_at timestamptz NOT NULL,
    projection_generation text NOT NULL,
    PRIMARY KEY (installation_id, application_id, search_document_id, projection_generation),
    UNIQUE (installation_id, application_id, tenant_scope, asset_kind, logical_id, revision,
        projection_generation),
    FOREIGN KEY (installation_id, application_id, tenant_scope, projection_generation)
        REFERENCES mission_control_search.projection_generation
            (installation_id, application_id, tenant_scope, projection_generation),
    CHECK (
        (asset_kind = 'mcp_tool'
            AND parent_kind = 'mcp_server'
            AND parent_logical_id IS NOT NULL
            AND parent_revision IS NOT NULL
            AND parent_source_digest ~ '^sha256:[0-9a-f]{64}$')
        OR
        (asset_kind <> 'mcp_tool'
            AND parent_kind IS NULL
            AND parent_logical_id IS NULL
            AND parent_revision IS NULL
            AND parent_source_digest IS NULL)
    )
);
CREATE INDEX search_document_fts_idx ON mission_control_search.search_document USING gin (fts);
CREATE INDEX search_document_embedding_idx ON mission_control_search.search_document
    USING hnsw (embedding extensions.vector_cosine_ops);
CREATE INDEX search_document_identity_idx ON mission_control_search.search_document
    (installation_id, application_id, tenant_scope, asset_kind, logical_id, revision);
CREATE INDEX search_document_parent_idx ON mission_control_search.search_document
    (installation_id, application_id, tenant_scope, parent_kind, parent_logical_id, parent_revision);
CREATE INDEX search_document_generation_idx ON mission_control_search.search_document
    (installation_id, application_id, tenant_scope, projection_generation, asset_kind);

CREATE TABLE mission_control_search.active_generation (
    installation_id uuid NOT NULL,
    application_id text NOT NULL,
    tenant_scope text NOT NULL,
    asset_kind text NOT NULL CHECK (asset_kind <> ''),
    projection_generation text NOT NULL,
    activated_at timestamptz NOT NULL,
    PRIMARY KEY (installation_id, application_id, tenant_scope, asset_kind),
    FOREIGN KEY (installation_id, application_id, tenant_scope, projection_generation)
        REFERENCES mission_control_search.projection_generation
            (installation_id, application_id, tenant_scope, projection_generation)
);

-- Verified activation (port of the legacy activate_generation function). SECURITY
-- INVOKER: it runs with the caller's grants and RLS; the installation partition is
-- taken from the trusted transaction context, never from a parameter.
CREATE FUNCTION mission_control_search.activate_generation(
    requested_tenant_scope text,
    requested_generation text,
    requested_expected_count integer,
    requested_expected_digest text
)
RETURNS TABLE (activated_count integer, activated_source_set_digest text)
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    ctx_installation uuid := mission_control.ctx_installation_id();
    ctx_application text := mission_control.ctx_application_id();
    generation_record mission_control_search.projection_generation%ROWTYPE;
    observed_count integer;
    observed_digest text;
    observed_model_count integer;
    observed_dimension_count integer;
    observed_format_count integer;
BEGIN
    IF ctx_installation IS NULL OR ctx_application IS NULL THEN
        RAISE EXCEPTION 'installation catalog context is required'
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    SELECT * INTO generation_record
    FROM mission_control_search.projection_generation AS g
    WHERE g.installation_id = ctx_installation
      AND g.application_id = ctx_application
      AND g.tenant_scope = requested_tenant_scope
      AND g.projection_generation = requested_generation
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'projection generation does not exist';
    END IF;
    IF generation_record.state <> 'building' THEN
        RAISE EXCEPTION 'projection generation is not building';
    END IF;
    IF generation_record.expected_count <> requested_expected_count
       OR generation_record.expected_source_set_digest <> requested_expected_digest THEN
        RAISE EXCEPTION 'projection generation expectation changed';
    END IF;

    SELECT
        count(*)::integer,
        'sha256:' || pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            coalesce(string_agg(
                d.asset_kind || '|' || d.logical_id || '|' || d.revision::text || '|'
                    || d.source_digest,
                E'\n' ORDER BY d.asset_kind, d.logical_id, d.revision, d.source_digest
            ), ''), 'UTF8')), 'hex'),
        count(DISTINCT d.embedding_model_id)::integer,
        count(DISTINCT d.embedding_dimensions)::integer,
        count(DISTINCT d.search_document_format_version)::integer
    INTO observed_count, observed_digest, observed_model_count, observed_dimension_count,
        observed_format_count
    FROM mission_control_search.search_document AS d
    WHERE d.installation_id = ctx_installation
      AND d.application_id = ctx_application
      AND d.tenant_scope = requested_tenant_scope
      AND d.projection_generation = requested_generation
      AND d.asset_kind = ANY(generation_record.selected_kinds);

    IF observed_count <> requested_expected_count
       OR observed_digest <> requested_expected_digest THEN
        RAISE EXCEPTION 'projection generation count or source digest verification failed';
    END IF;
    IF observed_count > 0 AND (
        observed_model_count <> 1 OR observed_dimension_count <> 1 OR observed_format_count <> 1
    ) THEN
        RAISE EXCEPTION 'projection generation embedding contract verification failed';
    END IF;
    IF EXISTS (
        SELECT 1 FROM mission_control_search.search_document AS d
        WHERE d.installation_id = ctx_installation
          AND d.application_id = ctx_application
          AND d.tenant_scope = requested_tenant_scope
          AND d.projection_generation = requested_generation
          AND d.asset_kind = ANY(generation_record.selected_kinds)
          AND (d.embedding_model_id <> generation_record.embedding_model_id
               OR d.embedding_dimensions <> generation_record.embedding_dimensions
               OR d.search_document_format_version
                    <> generation_record.search_document_format_version)
    ) THEN
        RAISE EXCEPTION 'projection generation contains incompatible rows';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM mission_control_search.active_generation AS a
        JOIN mission_control_search.projection_generation AS c
          ON c.installation_id = a.installation_id
         AND c.application_id = a.application_id
         AND c.tenant_scope = a.tenant_scope
         AND c.projection_generation = a.projection_generation
        WHERE a.installation_id = ctx_installation
          AND a.application_id = ctx_application
          AND a.tenant_scope = requested_tenant_scope
          AND NOT (a.asset_kind = ANY(generation_record.selected_kinds))
          AND (c.embedding_model_id <> generation_record.embedding_model_id
               OR c.embedding_dimensions <> generation_record.embedding_dimensions
               OR c.search_document_format_version
                    <> generation_record.search_document_format_version)
    ) THEN
        RAISE EXCEPTION 'projection generation is incompatible with another active kind';
    END IF;

    INSERT INTO mission_control_search.active_generation
        (installation_id, application_id, tenant_scope, asset_kind, projection_generation,
         activated_at)
    SELECT ctx_installation, ctx_application, requested_tenant_scope, selected_kind,
           requested_generation, pg_catalog.clock_timestamp()
    FROM unnest(generation_record.selected_kinds) AS selected_kind
    ON CONFLICT (installation_id, application_id, tenant_scope, asset_kind)
    DO UPDATE SET projection_generation = EXCLUDED.projection_generation,
                  activated_at = EXCLUDED.activated_at;

    UPDATE mission_control_search.projection_generation AS g
    SET actual_count = observed_count,
        actual_source_set_digest = observed_digest,
        state = 'active',
        verified_at = pg_catalog.clock_timestamp(),
        activated_at = pg_catalog.clock_timestamp()
    WHERE g.installation_id = ctx_installation
      AND g.application_id = ctx_application
      AND g.tenant_scope = requested_tenant_scope
      AND g.projection_generation = requested_generation;

    UPDATE mission_control_search.projection_generation AS prior
    SET state = 'superseded'
    WHERE prior.installation_id = ctx_installation
      AND prior.application_id = ctx_application
      AND prior.tenant_scope = requested_tenant_scope
      AND prior.projection_generation <> requested_generation
      AND prior.state = 'active'
      AND NOT EXISTS (
          SELECT 1 FROM mission_control_search.active_generation AS a
          WHERE a.installation_id = prior.installation_id
            AND a.application_id = prior.application_id
            AND a.tenant_scope = prior.tenant_scope
            AND a.projection_generation = prior.projection_generation
      );

    RETURN QUERY SELECT observed_count, observed_digest;
END;
$function$;
REVOKE ALL ON FUNCTION mission_control_search.activate_generation(text, text, integer, text)
    FROM PUBLIC;

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'projection_generation',
        'search_document',
        'active_generation'
    ] LOOP
        EXECUTE pg_catalog.format('ALTER TABLE mission_control_search.%I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format('ALTER TABLE mission_control_search.%I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format(
            'CREATE POLICY %I ON mission_control_search.%I '
            'USING (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id()) '
            'WITH CHECK (installation_id = mission_control.ctx_installation_id() '
            'AND application_id = mission_control.ctx_application_id())',
            table_name || '_scope', table_name);
    END LOOP;
END
$policies$;

REVOKE ALL ON ALL TABLES IN SCHEMA mission_control_search FROM PUBLIC;
-- Readers: API/coordinator runtime and inspection. Writers: catalog projection rebuild
-- (catalog writer) and projection event processing (outbox/projection worker). The
-- runtime keeps write access because the existing rebuild/projection tooling runs on
-- the runtime pool; it cannot bypass the installation policy.
GRANT USAGE ON SCHEMA mission_control_search TO mission_control_outbox_worker;
GRANT SELECT ON mission_control_search.projection_generation,
    mission_control_search.search_document, mission_control_search.active_generation
TO mission_control_runtime, mission_control_catalog_writer, mission_control_outbox_worker,
   mission_control_readonly;
GRANT INSERT, UPDATE ON mission_control_search.projection_generation,
    mission_control_search.search_document, mission_control_search.active_generation
TO mission_control_runtime, mission_control_catalog_writer, mission_control_outbox_worker;
GRANT EXECUTE ON FUNCTION mission_control_search.activate_generation(text, text, integer, text)
TO mission_control_runtime, mission_control_catalog_writer, mission_control_outbox_worker;
