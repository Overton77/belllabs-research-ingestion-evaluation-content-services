---
type: Decision Record
title: "Temporal is the only scheduler, fed by a transactional outbox, with no distributed transaction"
description: "Durable scheduling, waits and recovery mechanics belong to Temporal alone; business transitions, budgets and effects are decided in application-database transactions and reach Temporal through an outbox relay that…"
tags: [mission-control, adr, decision]
status: accepted
source: SPECIFICATION.md (Temporal and transactional execution), workflow-types/05 and 09
---

# Temporal is the only scheduler, fed by a transactional outbox, with no distributed transaction

Durable scheduling, waits and recovery mechanics belong to Temporal alone; business transitions, budgets and effects are decided in application-database transactions and reach Temporal through an outbox relay that starts or attaches the root workflow idempotently. We chose this over a two-phase commit between PostgreSQL and Temporal, and over letting an agent runtime (Deep Agents, Agent Server) schedule anything, because exactly one scheduler and one writer is what makes recovery provable. Consequence: all I/O happens in activities, every external effect is journaled first, and an ambiguous effect enters reconciliation before any re-execution.
