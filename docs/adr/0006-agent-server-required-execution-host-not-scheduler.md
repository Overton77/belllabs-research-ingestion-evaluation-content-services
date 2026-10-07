---
type: Decision Record
title: Agent Server hosts asynchronous subordinates in both applications and schedules nothing
description: "Asynchronous subordinate graphs run on a required, app-bound Agent Server with registered graph identities, authenticated submission, isolated persistence and pinned versions; Temporal remains the sole mission…"
tags: [mission-control, adr, decision]
status: accepted
source: SPECIFICATION.md (Harness continuation and subordinates), EVIDENCE.md (D04), RUNTIME-CONTRACTS.md
---

# Agent Server hosts asynchronous subordinates in both applications and schedules nothing

Asynchronous subordinate graphs run on a required, app-bound Agent Server with registered graph identities, authenticated submission, isolated persistence and pinned versions; Temporal remains the sole mission scheduler. An async launch is admitted only if its native identity can be recovered or the provider supplies an idempotency contract; a lost launch is never replaced by a duplicate paid run. We rejected in-process subagent tools as the async mechanism because they cannot survive worker loss.
