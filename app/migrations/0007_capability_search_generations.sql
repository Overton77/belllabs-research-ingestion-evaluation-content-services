CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions;

CREATE TABLE capability_search.generations (
    tenant_scope text NOT NULL,
    projection_generation text NOT NULL,
    embedding_model_id text NOT NULL,
    embedding_dimensions integer NOT NULL CHECK (embedding_dimensions >= 1),
    search_document_format_version integer NOT NULL
        CHECK (search_document_format_version >= 1),
    selected_kinds text[] NOT NULL CHECK (cardinality(selected_kinds) >= 1),
    expected_count integer NOT NULL CHECK (expected_count >= 0),
    expected_source_set_digest text NOT NULL
        CHECK (expected_source_set_digest ~ '^sha256:[0-9a-f]{64}$'),
    actual_count integer CHECK (actual_count >= 0),
    actual_source_set_digest text
        CHECK (
            actual_source_set_digest IS NULL
            OR actual_source_set_digest ~ '^sha256:[0-9a-f]{64}$'
        ),
    state text NOT NULL
        CHECK (state IN ('building', 'active', 'failed', 'superseded')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    verified_at timestamptz,
    activated_at timestamptz,
    PRIMARY KEY (tenant_scope, projection_generation)
);

/*
 * Backfill generation records before changing the documents key. The legacy
 * table normally has one generation. If it contains more than one, all are
 * retained and the most recently indexed generation for each kind remains
 * visible after this migration.
 */
INSERT INTO capability_search.generations (
    tenant_scope,
    projection_generation,
    embedding_model_id,
    embedding_dimensions,
    search_document_format_version,
    selected_kinds,
    expected_count,
    expected_source_set_digest,
    actual_count,
    actual_source_set_digest,
    state,
    created_at,
    verified_at,
    activated_at
)
SELECT
    generation_rows.tenant_scope,
    generation_rows.projection_generation,
    min(generation_rows.embedding_model_id),
    min(generation_rows.embedding_dimensions),
    min(generation_rows.search_document_format_version),
    array_agg(DISTINCT generation_rows.asset_kind ORDER BY generation_rows.asset_kind),
    count(*)::integer,
    'sha256:' || encode(
        extensions.digest(
            coalesce(
                string_agg(
                    generation_rows.asset_kind || '|' ||
                    generation_rows.logical_id || '|' ||
                    generation_rows.revision::text || '|' ||
                    generation_rows.source_digest,
                    E'\n'
                    ORDER BY generation_rows.asset_kind,
                             generation_rows.logical_id,
                             generation_rows.revision,
                             generation_rows.source_digest
                ),
                ''
            ),
            'sha256'
        ),
        'hex'
    ),
    count(*)::integer,
    'sha256:' || encode(
        extensions.digest(
            coalesce(
                string_agg(
                    generation_rows.asset_kind || '|' ||
                    generation_rows.logical_id || '|' ||
                    generation_rows.revision::text || '|' ||
                    generation_rows.source_digest,
                    E'\n'
                    ORDER BY generation_rows.asset_kind,
                             generation_rows.logical_id,
                             generation_rows.revision,
                             generation_rows.source_digest
                ),
                ''
            ),
            'sha256'
        ),
        'hex'
    ),
    'active',
    min(generation_rows.indexed_at),
    max(generation_rows.indexed_at),
    max(generation_rows.indexed_at)
FROM capability_search.documents AS generation_rows
GROUP BY generation_rows.tenant_scope, generation_rows.projection_generation;

ALTER TABLE capability_search.documents
    DROP CONSTRAINT documents_pkey;
ALTER TABLE capability_search.documents
    DROP CONSTRAINT documents_tenant_scope_asset_kind_logical_id_revision_key;
ALTER TABLE capability_search.documents
    ADD PRIMARY KEY (search_document_id, projection_generation);
ALTER TABLE capability_search.documents
    ADD CONSTRAINT capability_search_documents_generation_identity_key
    UNIQUE (
        tenant_scope,
        asset_kind,
        logical_id,
        revision,
        projection_generation
    );
ALTER TABLE capability_search.documents
    ADD CONSTRAINT capability_search_documents_generation_fk
    FOREIGN KEY (tenant_scope, projection_generation)
    REFERENCES capability_search.generations (
        tenant_scope,
        projection_generation
    );

CREATE TABLE capability_search.active_generations (
    tenant_scope text NOT NULL,
    asset_kind text NOT NULL,
    projection_generation text NOT NULL,
    activated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_scope, asset_kind),
    FOREIGN KEY (tenant_scope, projection_generation)
        REFERENCES capability_search.generations (
            tenant_scope,
            projection_generation
        )
);

WITH ranked AS (
    SELECT
        tenant_scope,
        asset_kind,
        projection_generation,
        max(indexed_at) AS last_indexed_at,
        row_number() OVER (
            PARTITION BY tenant_scope, asset_kind
            ORDER BY max(indexed_at) DESC, projection_generation DESC
        ) AS generation_rank
    FROM capability_search.documents
    GROUP BY tenant_scope, asset_kind, projection_generation
)
INSERT INTO capability_search.active_generations (
    tenant_scope,
    asset_kind,
    projection_generation,
    activated_at
)
SELECT
    tenant_scope,
    asset_kind,
    projection_generation,
    last_indexed_at
FROM ranked
WHERE generation_rank = 1;

CREATE INDEX capability_search_documents_generation_idx
    ON capability_search.documents
    (tenant_scope, projection_generation, asset_kind);

CREATE OR REPLACE FUNCTION capability_search.activate_generation(
    requested_tenant_scope text,
    requested_generation text,
    requested_expected_count integer,
    requested_expected_digest text
)
RETURNS TABLE (
    activated_count integer,
    activated_source_set_digest text
)
LANGUAGE plpgsql
AS $function$
DECLARE
    generation_record capability_search.generations%ROWTYPE;
    observed_count integer;
    observed_digest text;
    observed_model_count integer;
    observed_dimension_count integer;
    observed_format_count integer;
BEGIN
    SELECT *
    INTO generation_record
    FROM capability_search.generations
    WHERE tenant_scope = requested_tenant_scope
      AND projection_generation = requested_generation
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
        'sha256:' || encode(
            extensions.digest(
                coalesce(
                    string_agg(
                        documents.asset_kind || '|' ||
                        documents.logical_id || '|' ||
                        documents.revision::text || '|' ||
                        documents.source_digest,
                        E'\n'
                        ORDER BY documents.asset_kind,
                                 documents.logical_id,
                                 documents.revision,
                                 documents.source_digest
                    ),
                    ''
                ),
                'sha256'
            ),
            'hex'
        ),
        count(DISTINCT documents.embedding_model_id)::integer,
        count(DISTINCT documents.embedding_dimensions)::integer,
        count(DISTINCT documents.search_document_format_version)::integer
    INTO
        observed_count,
        observed_digest,
        observed_model_count,
        observed_dimension_count,
        observed_format_count
    FROM capability_search.documents AS documents
    WHERE documents.tenant_scope = requested_tenant_scope
      AND documents.projection_generation = requested_generation
      AND documents.asset_kind = ANY(generation_record.selected_kinds);

    IF observed_count <> requested_expected_count
       OR observed_digest <> requested_expected_digest THEN
        RAISE EXCEPTION 'projection generation count or source digest verification failed';
    END IF;
    IF observed_count > 0 AND (
        observed_model_count <> 1
        OR observed_dimension_count <> 1
        OR observed_format_count <> 1
        OR NOT EXISTS (
            SELECT 1
            FROM capability_search.documents AS documents
            WHERE documents.tenant_scope = requested_tenant_scope
              AND documents.projection_generation = requested_generation
              AND documents.asset_kind = ANY(generation_record.selected_kinds)
              AND documents.embedding_model_id =
                    generation_record.embedding_model_id
              AND documents.embedding_dimensions =
                    generation_record.embedding_dimensions
              AND documents.search_document_format_version =
                    generation_record.search_document_format_version
        )
    ) THEN
        RAISE EXCEPTION 'projection generation embedding contract verification failed';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM capability_search.documents AS documents
        WHERE documents.tenant_scope = requested_tenant_scope
          AND documents.projection_generation = requested_generation
          AND documents.asset_kind = ANY(generation_record.selected_kinds)
          AND (
              documents.embedding_model_id <>
                    generation_record.embedding_model_id
              OR documents.embedding_dimensions <>
                    generation_record.embedding_dimensions
              OR documents.search_document_format_version <>
                    generation_record.search_document_format_version
          )
    ) THEN
        RAISE EXCEPTION 'projection generation contains incompatible rows';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM capability_search.active_generations AS active
        JOIN capability_search.generations AS active_contract
          ON active_contract.tenant_scope = active.tenant_scope
         AND active_contract.projection_generation =
             active.projection_generation
        WHERE active.tenant_scope = requested_tenant_scope
          AND NOT (active.asset_kind = ANY(generation_record.selected_kinds))
          AND (
              active_contract.embedding_model_id <>
                    generation_record.embedding_model_id
              OR active_contract.embedding_dimensions <>
                    generation_record.embedding_dimensions
              OR active_contract.search_document_format_version <>
                    generation_record.search_document_format_version
          )
    ) THEN
        RAISE EXCEPTION
            'projection generation is incompatible with another active kind';
    END IF;

    INSERT INTO capability_search.active_generations (
        tenant_scope,
        asset_kind,
        projection_generation,
        activated_at
    )
    SELECT
        requested_tenant_scope,
        selected_kind,
        requested_generation,
        clock_timestamp()
    FROM unnest(generation_record.selected_kinds) AS selected_kind
    ON CONFLICT (tenant_scope, asset_kind)
    DO UPDATE SET
        projection_generation = EXCLUDED.projection_generation,
        activated_at = EXCLUDED.activated_at;

    UPDATE capability_search.generations
    SET
        actual_count = observed_count,
        actual_source_set_digest = observed_digest,
        state = 'active',
        verified_at = clock_timestamp(),
        activated_at = clock_timestamp()
    WHERE tenant_scope = requested_tenant_scope
      AND projection_generation = requested_generation;

    UPDATE capability_search.generations AS prior
    SET state = 'superseded'
    WHERE prior.tenant_scope = requested_tenant_scope
      AND prior.projection_generation <> requested_generation
      AND prior.state = 'active'
      AND NOT EXISTS (
          SELECT 1
          FROM capability_search.active_generations AS active
          WHERE active.tenant_scope = prior.tenant_scope
            AND active.projection_generation =
                prior.projection_generation
      );

    RETURN QUERY SELECT observed_count, observed_digest;
END;
$function$;

ALTER TABLE capability_search.generations ENABLE ROW LEVEL SECURITY;
ALTER TABLE capability_search.active_generations ENABLE ROW LEVEL SECURITY;

CREATE POLICY capability_search_generation_tenant_visibility
    ON capability_search.generations
    USING (
        tenant_scope = 'global'
        OR tenant_scope = current_setting('belllabs.request_scope', true)
    );

CREATE POLICY capability_search_active_generation_tenant_visibility
    ON capability_search.active_generations
    USING (
        tenant_scope = 'global'
        OR tenant_scope = current_setting('belllabs.request_scope', true)
    );
