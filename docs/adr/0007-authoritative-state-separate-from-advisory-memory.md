---
type: Decision Record
title: Authoritative state is separate from advisory memory and from runtime checkpoints
description: "The mission journal, typed state, command frontier, effect receipts and acceptance are the source of truth; native graph checkpoints only recover graph execution; sandbox files carry scratch and selected context…"
tags: [mission-control, adr, decision]
status: accepted
source: ARCHITECTURE-AND-ADRS.md (ADR-X03), workflow-types/02 and 08
---

# Authoritative state is separate from advisory memory and from runtime checkpoints

The mission journal, typed state, command frontier, effect receipts and acceptance are the source of truth; native graph checkpoints only recover graph execution; sandbox files carry scratch and selected context; recalled knowledge is advisory until revalidated. We never checkpoint authority by trusting model messages and never auto-ingest chat summaries into knowledge. Consequence: state handoff requires a typed delta, base version and digest through a deterministic reducer, and context selection is reproducible from captured candidate identities.
