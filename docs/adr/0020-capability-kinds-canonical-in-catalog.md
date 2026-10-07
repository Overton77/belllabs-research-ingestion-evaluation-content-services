---
type: Decision Record
title: "Capability kinds and capabilities are canonical in the PostgreSQL catalog, and documents reference it"
description: "Two documents list different capability-kind vocabularies. Neither is canonical: the catalog tables and seeds in the common component record the kinds and the admitted capabilities per application, and every document…"
tags: [mission-control, adr, decision]
status: accepted
source: interview 2026-10-07 (Q11); packages/mission-control-db-contract (catalog definitions and search projection); expansion/CATALOG-AND-ENVIRONMENTS.md
---

# Capability kinds and capabilities are canonical in the PostgreSQL catalog, and documents reference it

Two documents list different capability-kind vocabularies. Neither is canonical: the catalog tables and seeds in the common component record the kinds and the admitted capabilities per application, and every document, skill and contract references that record instead of restating it. We chose this because capability search followed by binding into contracts is a core feature, and a list that lives in prose would drift from the one the compiler enforces.
