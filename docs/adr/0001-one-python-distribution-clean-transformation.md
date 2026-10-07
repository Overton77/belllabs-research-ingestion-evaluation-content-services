---
type: Decision Record
title: "One Python distribution, transformed cleanly from the Biotech backend"
description: "The Biotech research-ingestion backend was transformed into a single neutral Python distribution, mission_control (FastAPI, Pydantic v2 frozen contracts, Temporal worker), with no Mongo/Beanie dependency, no legacy API…"
tags: [mission-control, adr, decision]
status: accepted
source: mission-control-general SPECIFICATION.md (Fixed architecture), expansion/ARCHITECTURE-AND-ADRS.md (ADR-X01), EVIDENCE.md (D01, D06)
---

# One Python distribution, transformed cleanly from the Biotech backend

The Biotech research-ingestion backend was transformed into a single neutral Python distribution, `mission_control` (FastAPI, Pydantic v2 frozen contracts, Temporal worker), with no Mongo/Beanie dependency, no legacy API aliases and no migration of old execution history. We chose a clean break over coexistence because a second kernel or compatibility layer would have made one scheduler and one writer impossible to prove. Consequence: replay and upgrade compatibility are mandatory for runs created by the new engine only; old-engine data is provenance, not a migration target.
