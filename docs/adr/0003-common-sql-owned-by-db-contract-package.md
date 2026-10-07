---
type: Decision Record
title: "The common SQL is owned by one installer package, not by the runtime or either application"
description: "All Mission Control tables, roles, seeds and runtime-persistence descriptors are owned by packages/mission-control-db-contract (CLI mission-db), which the runtime never imports; each application installs that release…"
tags: [mission-control, adr, decision]
status: accepted
source: SPECIFICATION.md (Repositories schema ownership, amendment 2026-10-03), EVIDENCE.md (D09 superseding D05), DATABASE.md
---

# The common SQL is owned by one installer package, not by the runtime or either application

All Mission Control tables, roles, seeds and runtime-persistence descriptors are owned by `packages/mission-control-db-contract` (CLI `mission-db`), which the runtime never imports; each application installs that release exactly once and adds only its own seeds. `ai-engineer-db-contract` keeps AI Engineer entity tables only, and `biotech-postgres-db-contract` is a thin consumer. Consequence: schema changes ship as new component versions with receipts and fingerprints; migration bytes and applied seeds are never edited in place (`mc.app.bindings@1.0.0` is frozen as applied).
