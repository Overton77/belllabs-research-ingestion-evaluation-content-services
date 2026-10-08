# [FT-A3] Hybrid search with lexical fallback wired into the public API

Linear: OVE-24

**Epic:** Capabilities (SPEC-01)
**Team:** T1
**Blocked by:** FT-A1
**Status:** ready-for-agent

**What to build:** `missionctl catalog search --query "pubmed literature retrieval" --kind mcp_server --host cursor_cloud --json` against the configured public API returns ranked, admitted capabilities with their pin, kind, host support and rank provenance, and reports `search_mode: hybrid` when an embedding route is configured or `search_mode: lexical` when it is not, never a 503. Grant, admission, kind and lane filters apply before ranking; exact tool names match through trigram similarity; rows without an embedding still rank. A fixed evaluation set guards recall.

**Spec sections:** SPEC-01 "Hybrid search", "Contracts" (search request and response additions), "Persistence" (0026), "Insertion points" (search).

**Writable regions:** `src/mission_control/application/capabilities/capability_search.py`, `src/mission_control/adapters/postgres/capability/capability_search_repository.py`, `src/mission_control/adapters/capabilities/capability_embeddings.py`, `src/mission_control/domain/coordinator/search_document.py`, `catalog_projection*.py`, `packages/mission-control-db-contract/component/migrations/0026_search_projection_nullable_embedding.sql`, `tests/fixtures/capability_search_eval.json`; shared: `src/mission_control/bootstrap/api.py`, `src/mission_control/bootstrap/catalog.py`.

**Acceptance criteria:**
- [ ] Migration 0026 makes `search_document.embedding` nullable, adds `embedding_model`, `embedding_dims`, `host_profiles text[]`, `side_effect_class`, `aliases text[]`, a generated `name_surface` with `pg_trgm` GIN index, a GIN index on `host_profiles`, and a partial HNSW index `WHERE embedding IS NOT NULL`; applies and replays as a no-op.
- [ ] `CapabilitySearchService.search` filters by scope, `admitted`, `kinds`, `host_profiles`, `side_effect_classes` first, then runs lexical, trigram and (when available) vector rankings, fuses with RRF `k=60` and weights `1.0 / 0.5 / 1.0`, verifies hits against the definition digest, and returns `search_mode` and per-hit `rank_provenance`.
- [ ] With no embedding route configured the service returns lexical results; `bootstrap/api.py` wires `OpenAICapabilityEmbeddingAdapter` when the application configuration names the `embedding.openai.text-embedding-3-small` model profile and a secret ref, and composes the service in both cases; `catalog_search_unavailable` is returned only when the projection is unreachable.
- [ ] Projection rebuild writes lexical columns synchronously and embeds asynchronously in batches, recording `embedding_model` and `embedding_dims`; a row whose model differs from the configured route is re-embedded.
- [ ] `ComponentQuery` search reads the projection with kind filters; the in-memory substring matcher is deleted.
- [ ] `tests/fixtures/capability_search_eval.json` has ≥ 25 queries with expected top-3 ids across kinds; lexical recall@3 ≥ 0.9 in a unit test over an in-memory projection built from the seeds; hybrid recall@3 ≥ 0.9 in a `common_db` test with recorded embeddings.
- [ ] CLI, HTTP and MCP return identical hit sets for the same request (parity test).

**Verification:** `make check`; `uv run --group biotech pytest tests/unit/capability/test_capability_search.py tests/unit/capability/test_search_eval.py -q`; `uv run --group biotech pytest -m common_db tests/integration/postgres/test_capability_search_hybrid.py -q`; `uv run uvicorn mission_control.bootstrap.api:create_app --factory` then `uv run missionctl catalog search --query "web search" --json` and confirm `search_mode`.

**Notes:** `OPENAI_API_KEY` is set; embedding the seeded catalog (a few hundred rows) is within the default budget; embedding a full projection rebuild repeatedly is not. ADR-0025 extends ADR-0013; cite both in the module docstring. Supabase's documented `hybrid_search` caps at 30 results; ours caps candidates per list at `2 × max_results`, maximum 60. Use `vector_cosine_ops` (normalization of OpenAI embeddings is UNVERIFIED). Check the installed pgvector version on the disposable cluster (`select extversion from pg_extension where extname='vector'`) and record it in the handoff; `hnsw.iterative_scan` needs ≥ 0.8. No live OpenAI calls in tests; embeddings are recorded fixtures.
