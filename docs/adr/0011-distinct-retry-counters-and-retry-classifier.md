---
type: Decision Record
title: "Retry, revisit, iteration, round and continuation are distinct counters, and only transient failures retry"
description: "Technical retries keep the same attempt semantics and effect identities and are permitted only for transient_transport, worker_lost and provider_busy (infrastructure, timeout, provider error, capacity) failures, with…"
tags: [mission-control, adr, decision]
status: accepted
source: workflow-types/00 (sections 5.4, 6.5), RUNTIME-CONTRACTS.md (retry classifier), EVIDENCE.md (D06)
---

# Retry, revisit, iteration, round and continuation are distinct counters, and only transient failures retry

Technical retries keep the same attempt semantics and effect identities and are permitted only for `transient_transport`, `worker_lost` and `provider_busy` (infrastructure, timeout, provider error, capacity) failures, with bounds. Quality rejection, changed inputs or instructions, and governor exhaustion are never retries; they are new semantic work, a revisit, a loop iteration, an optimization round or a continuation, each with its own identity and limit. Consequence: no unlimited retry exists anywhere, and a run stops at a governor with a governed outcome instead of asking for more budget.
