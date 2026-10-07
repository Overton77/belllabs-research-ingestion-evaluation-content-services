---
type: Decision Record
title: "Deep Agents is the first lane, extended through public interfaces rather than forked"
description: "Bounded cognition runs on Deep Agents (pinned 0.7.5 while it qualifies) with admission wrappers, typed tools and ordered middleware added at its extension points. A fork is allowed only after a minimized failing test…"
tags: [mission-control, adr, decision]
status: accepted
source: SPECIFICATION.md (Exact agent and sandbox binding), ARCHITECTURE-AND-ADRS.md (ADR-X02)
---

# Deep Agents is the first lane, extended through public interfaces rather than forked

Bounded cognition runs on Deep Agents (pinned 0.7.5 while it qualifies) with admission wrappers, typed tools and ordered middleware added at its extension points. A fork is allowed only after a minimized failing test, pinned-source analysis, an adapter-alternative cost, a minimal patch proposal and an owner decision, and it must keep public-contract parity and never become a second scheduler. We preferred this to a fork because a private runtime would make every upstream security and version change our maintenance burden.
