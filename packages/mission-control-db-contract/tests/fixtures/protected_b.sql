-- Container B protected domain fixture (SYNTHETIC): a legacy capability_search schema
-- with data and a belllabs_control poison schema with sentinel rows and REVOKE ALL.
CREATE SCHEMA extensions;
CREATE EXTENSION vector SCHEMA extensions;
CREATE EXTENSION pgcrypto SCHEMA extensions;
CREATE SCHEMA capability_search;
CREATE TABLE capability_search.search_document (
    document_key text PRIMARY KEY,
    content text NOT NULL,
    embedding extensions.vector(3)
);
INSERT INTO capability_search.search_document
SELECT 'doc-' || g, 'LEGACY-SEARCH-CONTENT-' || g, '[0,1,0]' FROM generate_series(1, 40) g;
CREATE SCHEMA belllabs_control;
CREATE TABLE belllabs_control.immutable_documents (document_id text PRIMARY KEY, body jsonb NOT NULL);
INSERT INTO belllabs_control.immutable_documents VALUES
    ('poison-1', '{"sentinel": "BELLLABS-POISON-SENTINEL"}'),
    ('poison-2', '{"sentinel": "BELLLABS-POISON-SENTINEL"}');
CREATE SEQUENCE belllabs_control.event_seq;
SELECT nextval('belllabs_control.event_seq') FROM generate_series(1, 3);
REVOKE ALL ON SCHEMA belllabs_control FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA belllabs_control FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA belllabs_control FROM PUBLIC;
