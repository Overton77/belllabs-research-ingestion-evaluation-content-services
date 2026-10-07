---
type: Decision Record
title: Catalog search starts with PostgreSQL full-text search; vector search is a replaceable reranker
description: "Capability search runs on a scoped PostgreSQL full-text projection first; vector similarity, when enabled, reranks that candidate set and no feature assumes a vector extension is installed. We chose this because grant…"
tags: [mission-control, adr, decision]
status: accepted
source: expansion/CATALOG-AND-ENVIRONMENTS.md
---

# Catalog search starts with PostgreSQL full-text search; vector search is a replaceable reranker

Capability search runs on a scoped PostgreSQL full-text projection first; vector similarity, when enabled, reranks that candidate set and no feature assumes a vector extension is installed. We chose this because grant filtering must happen before content retrieval, which a plain SQL projection makes trivial, and because paid embeddings are an unqualified cost until a proof budget is approved.
