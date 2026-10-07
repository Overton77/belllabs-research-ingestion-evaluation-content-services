"""Two deliberately different protected domain fixtures for disposable databases.

They imitate the shape (not the content) of the two live targets so the common
release is proven against unrelated pre-existing owners:

* ``biotech`` — a populated legacy ``capability_search`` projection (542 rows, the
  live row count observed at G0, with vectors and an HNSW index), a Supabase-like
  ``storage`` schema with ``buckets``/``objects`` and an RLS policy, and ``public``
  domain tables plus a view.
* ``ai-engineer`` — several domain schemas (``corpus``, ``knowledge``, ``temporal``,
  ``util``) with rows, identity columns, an explicit sequence advanced past its
  rows, an enum, a domain type, FORCE RLS whose policy calls a ``SECURITY DEFINER``
  helper in ``util``, a trigger, and schema default privileges.

Both provision ``extensions.vector`` beforehand, as the release requires. Every
identifier is fixed; role names carry a per-database random suffix because roles are
cluster-global. All values are deterministic (no ``now()``/random) so row digests are
stable. Nothing here is a real application record.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

DOMAIN_ROLE_KINDS = ("anon", "authenticated", "service")

_EXTENSIONS = """
CREATE SCHEMA IF NOT EXISTS extensions;
CREATE EXTENSION IF NOT EXISTS vector SCHEMA extensions;
GRANT USAGE ON SCHEMA extensions TO PUBLIC;
"""

_BIOTECH = """
CREATE SCHEMA capability_search;
CREATE TABLE capability_search.generations (
    generation integer PRIMARY KEY,
    model text NOT NULL,
    dimension integer NOT NULL CHECK (dimension > 0),
    active boolean NOT NULL
);
INSERT INTO capability_search.generations VALUES
    (1, 'fixture-model-a', 3, false), (2, 'fixture-model-b', 3, false),
    (3, 'fixture-model-c', 3, true);
CREATE TABLE capability_search.capability_documents (
    document_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    capability_key text NOT NULL UNIQUE,
    generation integer NOT NULL REFERENCES capability_search.generations (generation),
    body jsonb NOT NULL,
    embedding extensions.vector(3) NOT NULL,
    indexed_at timestamptz NOT NULL
);
INSERT INTO capability_search.capability_documents
    (capability_key, generation, body, embedding, indexed_at)
SELECT 'capability-' || g, 1 + g % 3,
       jsonb_build_object('title', 'Capability ' || g, 'tags', jsonb_build_array('t' || g % 7)),
       ('[' || g % 5 || ',' || g % 11 || ',' || g % 13 || ']')::extensions.vector,
       timestamptz '2026-01-01 00:00:00+00' + g * interval '1 minute'
FROM generate_series(1, 542) AS g;
CREATE INDEX capability_documents_embedding
    ON capability_search.capability_documents
    USING hnsw (embedding extensions.vector_l2_ops);
REVOKE ALL ON SCHEMA capability_search FROM PUBLIC;
GRANT USAGE ON SCHEMA capability_search TO {service};
GRANT SELECT ON ALL TABLES IN SCHEMA capability_search TO {service};

CREATE SCHEMA storage;
CREATE TABLE storage.buckets (
    id text PRIMARY KEY,
    name text NOT NULL UNIQUE,
    public boolean NOT NULL DEFAULT false,
    file_size_limit bigint,
    created_at timestamptz NOT NULL
);
INSERT INTO storage.buckets VALUES
    ('knowledge-artifacts', 'knowledge-artifacts', false, NULL, '2026-01-02 00:00:00+00'),
    ('raw-captures', 'raw-captures', false, 52428800, '2026-01-03 00:00:00+00'),
    ('public-assets', 'public-assets', true, 1048576, '2026-01-04 00:00:00+00');
CREATE TABLE storage.objects (
    id uuid PRIMARY KEY,
    bucket_id text NOT NULL REFERENCES storage.buckets (id),
    name text NOT NULL,
    owner_ref text,
    metadata jsonb NOT NULL,
    UNIQUE (bucket_id, name)
);
INSERT INTO storage.objects
SELECT md5('object-' || g)::uuid,
       (ARRAY['knowledge-artifacts', 'raw-captures', 'public-assets'])[1 + g % 3],
       'path/' || g || '.json', 'owner-' || g % 4, jsonb_build_object('size', g * 17)
FROM generate_series(1, 90) AS g;
ALTER TABLE storage.objects ENABLE ROW LEVEL SECURITY;
CREATE POLICY objects_read ON storage.objects FOR SELECT TO {authenticated}
    USING (bucket_id <> 'raw-captures');
GRANT USAGE ON SCHEMA storage TO {anon}, {authenticated};
GRANT SELECT ON storage.buckets TO {anon}, {authenticated};
GRANT SELECT ON storage.objects TO {authenticated};

CREATE TABLE public.research_entities (
    entity_id text PRIMARY KEY,
    kind text NOT NULL,
    payload jsonb NOT NULL
);
INSERT INTO public.research_entities
SELECT 'entity-' || g, (ARRAY['compound', 'gene', 'study'])[1 + g % 3],
       jsonb_build_object('score', g * 3)
FROM generate_series(1, 25) AS g;
CREATE VIEW public.research_entity_kinds AS
    SELECT kind, count(*) AS entities FROM public.research_entities GROUP BY kind;
GRANT SELECT ON public.research_entity_kinds TO {authenticated};
"""

_AI_ENGINEER = """
CREATE SCHEMA util;
CREATE SCHEMA corpus;
CREATE SCHEMA knowledge;
CREATE SCHEMA temporal;
REVOKE ALL ON SCHEMA util, corpus, knowledge, temporal FROM PUBLIC;

CREATE FUNCTION util.current_tenant() RETURNS uuid
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT NULLIF(pg_catalog.current_setting('app.tenant_id', true), '')::uuid
$$;
REVOKE ALL ON FUNCTION util.current_tenant() FROM PUBLIC;
GRANT USAGE ON SCHEMA util TO {authenticated};
GRANT EXECUTE ON FUNCTION util.current_tenant() TO {authenticated};
CREATE FUNCTION util.touch_updated_at() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    NEW.updated_at := pg_catalog.clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE DOMAIN corpus.sha256_digest AS text CHECK (VALUE ~ '^sha256:[0-9a-f]{{64}}$');
CREATE TABLE corpus.documents (
    document_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    digest corpus.sha256_digest NOT NULL,
    title text NOT NULL,
    created_at timestamptz NOT NULL
);
INSERT INTO corpus.documents
SELECT md5('document-' || g)::uuid, md5('tenant-' || g % 3)::uuid,
       'sha256:' || encode(sha256(convert_to('document-' || g, 'UTF8')), 'hex'),
       'Document ' || g, timestamptz '2026-02-01 00:00:00+00' + g * interval '1 hour'
FROM generate_series(1, 120) AS g;
CREATE TABLE corpus.chunks (
    chunk_id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    document_id uuid NOT NULL REFERENCES corpus.documents (document_id),
    ordinal integer NOT NULL,
    content text NOT NULL,
    embedding extensions.vector(3) NOT NULL,
    UNIQUE (document_id, ordinal)
);
INSERT INTO corpus.chunks (document_id, ordinal, content, embedding)
SELECT md5('document-' || (1 + g % 120))::uuid, g / 120, 'chunk ' || g,
       ('[' || g % 3 || ',' || g % 7 || ',' || g % 9 || ']')::extensions.vector
FROM generate_series(0, 479) AS g;
ALTER DEFAULT PRIVILEGES IN SCHEMA corpus GRANT SELECT ON TABLES TO {authenticated};
GRANT USAGE ON SCHEMA corpus TO {authenticated};
GRANT SELECT ON ALL TABLES IN SCHEMA corpus TO {authenticated};

CREATE TYPE knowledge.entity_kind AS ENUM ('compound', 'gene', 'pathway', 'paper');
CREATE TABLE knowledge.entities (
    entity_id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    kind knowledge.entity_kind NOT NULL,
    label text NOT NULL,
    updated_at timestamptz NOT NULL
);
INSERT INTO knowledge.entities
SELECT md5('entity-' || g)::uuid, md5('tenant-' || g % 3)::uuid,
       (ARRAY['compound', 'gene', 'pathway', 'paper']::knowledge.entity_kind[])[1 + g % 4],
       'Entity ' || g, timestamptz '2026-03-01 00:00:00+00' + g * interval '1 day'
FROM generate_series(1, 60) AS g;
ALTER TABLE knowledge.entities ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge.entities FORCE ROW LEVEL SECURITY;
CREATE POLICY entities_tenant ON knowledge.entities FOR ALL TO {authenticated}
    USING (tenant_id = util.current_tenant())
    WITH CHECK (tenant_id = util.current_tenant());
CREATE TRIGGER entities_touch BEFORE UPDATE ON knowledge.entities
    FOR EACH ROW EXECUTE FUNCTION util.touch_updated_at();
GRANT USAGE ON SCHEMA knowledge TO {authenticated};
GRANT SELECT, INSERT, UPDATE ON knowledge.entities TO {authenticated};

CREATE SEQUENCE temporal.event_seq START 1000 INCREMENT 7;
CREATE TABLE temporal.events (
    event_no bigint PRIMARY KEY DEFAULT nextval('temporal.event_seq'),
    occurred_at timestamptz NOT NULL,
    kind text NOT NULL,
    body jsonb NOT NULL
);
INSERT INTO temporal.events (occurred_at, kind, body)
SELECT timestamptz '2026-04-01 00:00:00+00' + g * interval '5 minutes',
       (ARRAY['started', 'progressed', 'completed'])[1 + g % 3], jsonb_build_object('n', g)
FROM generate_series(1, 40) AS g;
SELECT nextval('temporal.event_seq') FROM generate_series(1, 3);
GRANT USAGE ON SCHEMA temporal TO {service};
GRANT SELECT ON ALL TABLES IN SCHEMA temporal TO {service};

CREATE TABLE public.app_settings (key text PRIMARY KEY, value jsonb NOT NULL);
INSERT INTO public.app_settings
SELECT 'setting-' || g, jsonb_build_object('enabled', g % 2 = 0) FROM generate_series(1, 5) AS g;
"""

DOMAIN_SQL = {"biotech": _BIOTECH, "ai-engineer": _AI_ENGINEER}


@dataclass(frozen=True)
class DomainFixture:
    application_id: str
    roles: dict[str, str]

    @property
    def role_names(self) -> tuple[str, ...]:
        return tuple(self.roles.values())


async def create_domain_roles(admin: Any, suffix: str) -> dict[str, str]:
    roles = {kind: f"mcqd_{suffix}_{kind}" for kind in DOMAIN_ROLE_KINDS}
    for name in roles.values():
        await admin.execute(f'CREATE ROLE "{name}" NOLOGIN NOINHERIT')
    return roles


async def install_domain(connection: Any, application_id: str, roles: dict[str, str]) -> None:
    """Create the protected pre-existing domain in one transaction (as its owner)."""
    sql = DOMAIN_SQL[application_id].format(**{kind: f'"{name}"' for kind, name in roles.items()})
    async with connection.transaction():
        await connection.execute(_EXTENSIONS)
        await connection.execute(sql)
