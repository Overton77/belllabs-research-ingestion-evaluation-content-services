---
type: Decision Record
title: Capability search is hybrid by default (full-text plus pgvector fused by reciprocal rank fusion) with a lexical-only fallback
description: "Extends ADR-0013: pgvector is installed in both application databases, so the search projection fuses a websearch_to_tsquery ranking and a cosine ranking with RRF after grant and host-support filtering; when no embedding route is configured the service returns lexical results instead of 503, and embeddings are nullable in the projection."
tags: [mission-control, adr, decision, catalog, search]
status: accepted
source: fast-track interview 2026-10-07 (requirement 1, hybrid search); ADR-0013; docs/specs/fast-track-2026-10/research/codebase-map.md (CapabilitySearchService already fuses; bootstrap wires no embeddings; search_document.embedding NOT NULL)
---

# Capability search is hybrid by default (full-text plus pgvector fused by reciprocal rank fusion) with a lexical-only fallback

ADR-0013 ordered full-text first and treated vectors as an optional reranker because no extension was assumed installed. Both live installations now carry `pgvector` with an HNSW cosine index on the search projection, and the code already fuses lexical and semantic rankings with RRF. We therefore make hybrid the default: filter by scope, admission status, kind and lane `host_support` first; run the lexical and vector rankings over that candidate set; fuse with RRF (k=60, equal weights, both tunable per application); verify each hit against the authoritative definition digest; return scores with rank provenance. The embedding route is a Model Profile (OpenAI `text-embedding-3-small`, 1536 dimensions, as the first qualified route) whose absence degrades the service to lexical-only with a `search_mode: lexical` field, never a 503. We rejected vector-only because grant filtering must precede retrieval and because lexical matches on exact tool names matter for agents.

## Consequences

- `search_document.embedding` becomes nullable; projection rebuilds embed asynchronously and rows without an embedding still rank lexically.
- Agentic component search drops its in-memory substring matcher and reads the same projection.
- A fixed evaluation set (queries with expected capability ids) gates changes to weights or models.
