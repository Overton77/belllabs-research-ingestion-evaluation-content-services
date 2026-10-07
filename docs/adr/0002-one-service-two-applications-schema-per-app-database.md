---
type: Decision Record
title: "One shared service, two application bindings, the same schema in each application's own database"
description: "One control service serves ai-engineer and biotech through immutable operator-managed application bindings, and the identical common schema is installed into each application's existing Supabase PostgreSQL, with private…"
tags: [mission-control, adr, decision]
status: accepted
source: SPECIFICATION.md (Deployment and application binding; Accepted storage), EVIDENCE.md (D02, D03), ARCHITECTURE-AND-ADRS.md (ADR-X05)
---

# One shared service, two application bindings, the same schema in each application's own database

One control service serves `ai-engineer` and `biotech` through immutable operator-managed application bindings, and the identical common schema is installed into each application's existing Supabase PostgreSQL, with private Storage buckets in the same project. We rejected a third central mission database because it would pool two applications' authority and data behind one connection. Consequence: resource identity is always (installation, application, tenant, resource); a token for one application can never acquire the other's authority, and no worker ever assumes another application's binding.
