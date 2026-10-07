---
type: Decision Record
title: "Commands are durably ordered; urgent stop is a persisted fence, not a latency promise"
description: "Commands enter a durable ordered admission queue; an urgent cancel first persists a stop fence that rejects new effect claims, then requests provider or process cancellation. stop_now guarantees no new effects, not that…"
tags: [mission-control, adr, decision]
status: accepted
source: ARCHITECTURE-AND-ADRS.md (ADR-X04), workflow-types/09
---

# Commands are durably ordered; urgent stop is a persisted fence, not a latency promise

Commands enter a durable ordered admission queue; an urgent cancel first persists a stop fence that rejects new effect claims, then requests provider or process cancellation. `stop_now` guarantees no new effects, not that already-dispatched remote tools halt, and clients show requested, observed and settled states separately. We chose truthful cooperative control over advertising native suspension that no lane can prove. Consequence: pause requires checkpoint or quiescence validation, and required human review is never satisfied by notification delivery.
