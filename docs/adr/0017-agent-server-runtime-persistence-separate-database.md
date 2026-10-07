---
type: Decision Record
title: "Agent Server runtime persistence lives in a physically separate, app-bound PostgreSQL database"
description: "The specification assumed LangGraph saver and store state could live in a private schema (mission_control_runtime) inside each application's business database. The pinned Agent Server (langgraph-api 0.12) cannot use a…"
tags: [mission-control, adr, decision]
status: accepted
supersedes: the app-local private-schema assumption in SPECIFICATION.md (Fixed architecture) and DATABASE.md
source: docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md (Still not done, item 4), ADR-0009, interview 2026-10-07 (Q9)
---

# Agent Server runtime persistence lives in a physically separate, app-bound PostgreSQL database

The specification assumed LangGraph saver and store state could live in a private schema (`mission_control_runtime`) inside each application's business database. The pinned Agent Server (langgraph-api 0.12) cannot use a private schema of that database, which is exactly the condition ADR-0009 says blocks admission. Decision: each application gets its own small runtime PostgreSQL database, bound to that application only and never shared; the provider (a second Supabase project, Neon, RDS) is an operator input, not an architecture choice. The `mission_control_runtime` schema that `mission-db runtime-apply` provisions remains the descriptor for whatever database hosts it.
