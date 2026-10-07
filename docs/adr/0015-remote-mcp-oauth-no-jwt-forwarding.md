---
type: Decision Record
title: Remote MCP onboarding uses resource-bound OAuth with PKCE and never forwards application JWTs
description: "The common MCP endpoint is a protected resource with authorization-server discovery and a maintained OAuth adapter; it issues Mission Control resource tokens scoped to app, tenant and scopes after delegating identity to…"
tags: [mission-control, adr, decision]
status: accepted
source: SPECIFICATION.md (Public skill CLI MCP and dashboard contract)
---

# Remote MCP onboarding uses resource-bound OAuth with PKCE and never forwards application JWTs

The common MCP endpoint is a protected resource with authorization-server discovery and a maintained OAuth adapter; it issues Mission Control resource tokens scoped to app, tenant and scopes after delegating identity to the application's own login. Passing an application JWT to a domain MCP server, or accepting arbitrary upstream tokens, is prohibited. Headless callers use provisioned scoped service credentials, and a stdio bridge only calls the deployed service. Onboarding is verified in both Codex and Claude Code as part of interface acceptance.
