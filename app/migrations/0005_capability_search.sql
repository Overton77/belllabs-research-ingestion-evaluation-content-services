CREATE SCHEMA IF NOT EXISTS extensions;
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA extensions;
CREATE SCHEMA IF NOT EXISTS capability_search;

CREATE TABLE IF NOT EXISTS capability_search.documents (
    search_document_id uuid PRIMARY KEY,
    tenant_scope text NOT NULL,
    asset_kind text NOT NULL,
    logical_id text NOT NULL,
    revision integer NOT NULL CHECK (revision >= 1),
    source_digest text NOT NULL CHECK (source_digest ~ '^sha256:[0-9a-f]{64}$'),
    status text NOT NULL CHECK (status IN ('published', 'deprecated', 'retired', 'revoked')),
    title text NOT NULL,
    description text NOT NULL,
    search_text text NOT NULL,
    search_text_digest text NOT NULL
        CHECK (search_text_digest ~ '^sha256:[0-9a-f]{64}$'),
    fts tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(logical_id, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(search_text, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(description, '')), 'C')
    ) STORED,
    embedding extensions.vector(1536) NOT NULL,
    embedding_model_id text NOT NULL,
    embedding_dimensions integer NOT NULL CHECK (embedding_dimensions = 1536),
    search_document_format_version integer NOT NULL,
    parent_kind text,
    parent_logical_id text,
    parent_revision integer,
    parent_source_digest text,
    mongodb_collection text NOT NULL,
    mongodb_document_id text NOT NULL,
    tags text[] NOT NULL DEFAULT '{}',
    domains text[] NOT NULL DEFAULT '{}',
    operation_classes text[] NOT NULL DEFAULT '{}',
    workflow_type_refs jsonb NOT NULL DEFAULT '[]',
    capability_requirements text[] NOT NULL DEFAULT '{}',
    compatible_runtimes text[] NOT NULL DEFAULT '{}',
    compatibility_summary text NOT NULL,
    schema_digest_verified boolean NOT NULL DEFAULT true,
    source_published_at timestamptz NOT NULL,
    indexed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    projection_generation text NOT NULL,
    UNIQUE (tenant_scope, asset_kind, logical_id, revision),
    CHECK (
        (
            asset_kind = 'mcp_tool'
            AND parent_kind = 'mcp_server'
            AND parent_logical_id IS NOT NULL
            AND parent_revision IS NOT NULL
            AND parent_source_digest ~ '^sha256:[0-9a-f]{64}$'
        )
        OR
        (
            asset_kind <> 'mcp_tool'
            AND parent_kind IS NULL
            AND parent_logical_id IS NULL
            AND parent_revision IS NULL
            AND parent_source_digest IS NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS capability_search_documents_fts_idx
    ON capability_search.documents USING gin (fts);

CREATE INDEX IF NOT EXISTS capability_search_documents_embedding_idx
    ON capability_search.documents
    USING hnsw (embedding extensions.vector_cosine_ops);

CREATE INDEX IF NOT EXISTS capability_search_documents_identity_idx
    ON capability_search.documents
    (tenant_scope, asset_kind, logical_id, revision);

CREATE INDEX IF NOT EXISTS capability_search_documents_parent_idx
    ON capability_search.documents
    (tenant_scope, parent_kind, parent_logical_id, parent_revision);

ALTER TABLE capability_search.documents ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS capability_search_tenant_visibility
    ON capability_search.documents;
CREATE POLICY capability_search_tenant_visibility
    ON capability_search.documents
    USING (
        tenant_scope = 'global'
        OR tenant_scope = current_setting('belllabs.request_scope', true)
    );
