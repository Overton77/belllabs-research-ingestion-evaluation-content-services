-- mission-control-db-contract common release: hybrid capability search with a lexical
-- fallback (ADR-0025 extends ADR-0013; SPEC-01 "Hybrid search"; FT-A3). Additive only.
--
-- * search_document.embedding becomes nullable: the projection writes lexical columns
--   synchronously and embeds later in batches. embedding_model / embedding_dims record what
--   embedded a row (both NULL exactly when the vector is NULL), so a model change is a
--   re-embed, never a mixed vector space. embedding_model_id / embedding_dimensions stay
--   the generation contract checked by activate_generation.
-- * host_profiles, side_effect_class, aliases and tool_names are pre-ranking filters and the
--   trigram name surface (pg_trgm word similarity over identifiers, titles, aliases and
--   exact tool names, so `tavily_search` matches by name even when embeddings are weak).
-- * The HNSW index becomes partial (WHERE embedding IS NOT NULL).
-- pg_trgm is provisioned in schema `extensions` before application (release-spec
-- required_extensions), like `vector`; this migration never creates extensions.

ALTER TABLE mission_control_search.search_document
    ALTER COLUMN embedding DROP NOT NULL,
    ADD COLUMN embedding_model text CHECK (embedding_model IS NULL OR embedding_model <> ''),
    ADD COLUMN embedding_dims integer CHECK (embedding_dims IS NULL OR embedding_dims >= 1),
    ADD COLUMN host_profiles text[] NOT NULL DEFAULT '{}'::text[],
    ADD COLUMN side_effect_class text
        CHECK (side_effect_class IS NULL
               OR side_effect_class IN ('read_only', 'bounded_write', 'consequential')),
    ADD COLUMN aliases text[] NOT NULL DEFAULT '{}'::text[],
    ADD COLUMN tool_names text[] NOT NULL DEFAULT '{}'::text[];

-- Rows projected before 0026 were all embedded by their generation's contract model.
UPDATE mission_control_search.search_document
SET embedding_model = embedding_model_id,
    embedding_dims = embedding_dimensions
WHERE embedding IS NOT NULL AND embedding_model IS NULL;

ALTER TABLE mission_control_search.search_document
    ADD CONSTRAINT search_document_embedding_provenance CHECK (
        (embedding IS NULL) = (embedding_model IS NULL)
        AND (embedding IS NULL) = (embedding_dims IS NULL)
    ),
    ADD CONSTRAINT search_document_host_profiles_known CHECK (
        host_profiles <@ ARRAY['deep_agents', 'cursor_local', 'cursor_cloud',
                               'claude_agent_sdk', 'codex']::text[]
    );

-- Lower-cased identifier, title, aliases and tool names. array_to_string is only STABLE in
-- general, but over text[] it is deterministic, so the wrapper may be IMMUTABLE and back a
-- generated column.
CREATE FUNCTION mission_control_search.name_surface(
    logical_id text, title text, aliases text[], tool_names text[]
)
RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path = pg_catalog AS $$
    SELECT lower(concat_ws(' ', logical_id, title,
                           array_to_string(aliases, ' '), array_to_string(tool_names, ' ')))
$$;
REVOKE ALL ON FUNCTION mission_control_search.name_surface(text, text, text[], text[])
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mission_control_search.name_surface(text, text, text[], text[])
    TO mission_control_runtime, mission_control_catalog_writer, mission_control_outbox_worker,
       mission_control_readonly;

ALTER TABLE mission_control_search.search_document
    ADD COLUMN name_surface text GENERATED ALWAYS AS (
        mission_control_search.name_surface(logical_id, title, aliases, tool_names)
    ) STORED;

CREATE INDEX search_document_name_trgm_idx ON mission_control_search.search_document
    USING gin (name_surface extensions.gin_trgm_ops);
CREATE INDEX search_document_logical_id_trgm_idx ON mission_control_search.search_document
    USING gin (logical_id extensions.gin_trgm_ops);
CREATE INDEX search_document_host_profiles_idx ON mission_control_search.search_document
    USING gin (host_profiles);

DROP INDEX mission_control_search.search_document_embedding_idx;
CREATE INDEX search_document_embedding_idx ON mission_control_search.search_document
    USING hnsw (embedding extensions.vector_cosine_ops)
    WHERE embedding IS NOT NULL;
