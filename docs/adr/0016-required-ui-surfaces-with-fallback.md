---
type: Decision Record
title: "Web, mobile, generative UI and MCP Apps are required release surfaces, each with a non-UI fallback; desktop is later"
description: "First-party web and mobile clients, generative UI over a closed authorized view and action schema, MCP Apps compatibility, SSE with cursor replay and a required WebSocket adapter are all part of the complete release…"
tags: [mission-control, adr, decision]
status: accepted
source: ARCHITECTURE-AND-ADRS.md (ADR-X06), expansion/EXPERIENCE-AND-STREAMS.md
---

# Web, mobile, generative UI and MCP Apps are required release surfaces, each with a non-UI fallback; desktop is later

First-party web and mobile clients, generative UI over a closed authorized view and action schema, MCP Apps compatibility, SSE with cursor replay and a required WebSocket adapter are all part of the complete release; desktop is explicitly later. No remote UI can mint grants, resolve a human task without an attributed action, or bypass the API, and every embedded UI has a plain text or link fallback the host can render. We chose required-with-fallback over optional UI so that a review is never blocked by a host that lacks an embedded runtime.
