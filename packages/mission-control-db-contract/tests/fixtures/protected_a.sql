-- Container A protected domain fixture (SYNTHETIC; mimics a Supabase project with a
-- corpus domain schema, storage bucket metadata and an RLS-enabled app table).
CREATE SCHEMA extensions;
CREATE EXTENSION vector SCHEMA extensions;
CREATE EXTENSION pgcrypto SCHEMA extensions;
CREATE SCHEMA corpus;
CREATE TABLE corpus.document (
    document_id bigint PRIMARY KEY,
    title text NOT NULL,
    body text NOT NULL,
    embedding extensions.vector(3)
);
CREATE TABLE corpus.note (note text);
CREATE SEQUENCE corpus.ingest_seq;
INSERT INTO corpus.document
SELECT g, 'title ' || g, 'CORPUS-SECRET-BODY-' || g, '[1,2,3]' FROM generate_series(1, 2500) g;
INSERT INTO corpus.note VALUES ('CORPUS-NOTE-SENTINEL'), ('CORPUS-NOTE-SENTINEL'), ('b');
SELECT nextval('corpus.ingest_seq') FROM generate_series(1, 7);
CREATE SCHEMA storage;
CREATE TABLE storage.buckets (
    id text PRIMARY KEY,
    name text NOT NULL,
    public boolean NOT NULL DEFAULT false,
    file_size_limit bigint,
    allowed_mime_types text[],
    owner uuid,
    created_at timestamptz DEFAULT now()
);
INSERT INTO storage.buckets VALUES
    ('knowledge-artifacts', 'knowledge-artifacts', false, 52428800, ARRAY['application/pdf'],
     gen_random_uuid(), now());
CREATE TABLE public.app_profile (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), email text NOT NULL);
INSERT INTO public.app_profile (email) VALUES ('PERSON-A-EMAIL-SENTINEL@example.invalid');
ALTER TABLE public.app_profile ENABLE ROW LEVEL SECURITY;
