---
type: Decision Record
title: "One release, separate application authority, never a shared checkpoint namespace"
description: "The same runtime, schema and operation contract is released to two separate Supabase projects with app-bound server and worker identities. Catalog, search and vector indexes are scoped projections, never authorization…"
tags: [mission-control, adr, decision]
status: accepted
source: ARCHITECTURE-AND-ADRS.md (ADR-X05)
---

# One release, separate application authority, never a shared checkpoint namespace

The same runtime, schema and operation contract is released to two separate Supabase projects with app-bound server and worker identities. Catalog, search and vector indexes are scoped projections, never authorization stores; grant filtering happens before retrieval and reranking. If supported runtime tooling cannot run app-local, admission is blocked and a reviewed infrastructure alternative is presented (see ADR-0017); the two applications are never silently connected to one checkpoint namespace.
