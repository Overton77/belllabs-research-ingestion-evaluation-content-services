---
type: Research Note
title: "Provider lifecycle evidence and hosted-product qualification gaps"
description: "Primary documentation checked October 8, 2026, with code-confirmed facts separated from implementation decisions and unresolved account-specific capabilities."
tags: [mission-control, research, providers]
---

# Research evidence

Checked 2026-10-08 through official documentation retrieval and Context7 for python-socketio. The earlier [coding-lane survey](../../research/2026-10-07-coding-lane-surfaces.md) is useful historical evidence, but its version/feature claims are not a substitute for current source or a recorded live drill. No live provider run was performed. The following is a capability assessment, not a qualification attestation.

## Integration comparison

N = documented native primitive; E = Mission Control implementation required; Q = incomplete evidence/qualification; U = not offered by the selected integration as far as the researched surface establishes. Q is not proof that a feature can never exist.

| Concern | Claude local | Codex local | Cursor local | Cursor hosted | Claude hosted | Codex hosted |
| --- | --- | --- | --- | --- | --- | --- |
| Chosen entry | Python Agent SDK | app-server | Python SDK/bridge | Cloud SDK/API | Cloud product CLI, pending full integration evidence | Cloud product CLI, pending full integration evidence |
| Start and follow-up | N | N | N | N, serialize runs | Start/message submission N; application/delivery semantics Q | Start/list N; follow-up lifecycle Q |
| Durable queue | E | E | E | E | E; native delivery proof Q | E; native delivery proof Q |
| Interrupt/cancel | N + settlement E | N + settlement E | N + settlement E | N + settlement E | Q | Q |
| Live observation/replay | N stream + E persistence | N events + E persistence | N offsets + E persistence | N SSE + E persistence | Q for distributable full lifecycle interface | Q for full lifecycle stream |
| Workflow Human Gate | E | E | E | E | E only after sufficient lifecycle integration | E only after sufficient lifecycle integration |
| Native tool approvals | Callback N; effective policy matters | RPC N on covered paths | Q; no assumed headless ask | Q; gateway needed for governed effects | Q | Q |
| Native context compaction | Observations Q by SDK language/version | Documented explicit primitive | Provider-managed; explicit trigger Q | Provider-managed; explicit trigger Q | Q | Q |
| Portable continuation | E checkpoint + hydration | E checkpoint + hydration | E checkpoint + hydration | E artifacts + new/resumed task | Q required hosted surfaces | Q required hosted surfaces |
| Workspaces | E managed worktree | E managed worktree | Existing lease adapter | Native task workspace + E attestation | Native cloud environment + Q config injection | Native cloud environment + Q task config injection |

All new cells require recorded conformance evidence before production admission. Local provider integration does not certify the matching cloud product. Hosted launch support alone cannot fulfill the requested full lifecycle parity.

## Findings that materially change the implementation

**Claude SDK language mismatch.** Current hooks documentation marks `PostCompact` as TypeScript-supported but not Python-supported. Existing Python-targeted projection cannot assume every Claude CLI/TypeScript hook exists in the SDK callback union. Use the actual pinned callback types, qualified command hooks or explicit capability rejection. [SDK hooks](https://code.claude.com/docs/en/agent-sdk/hooks).

**Claude hosted messages are submissions.** The cloud product documents terminal launch and a print-mode message submission to an existing hosted session; it exits without waiting for the answer. Those facts do not establish a complete public event/recovery/cancel transport. The hosted qualification issue must settle that gap before an adapter can be marked ready. [Claude cloud](https://code.claude.com/docs/en/claude-code-on-the-web).

**Codex local is an embedding surface.** App-server exposes local threads/turns/events and control RPCs. That is the appropriate local integration. It does not by itself establish access to provider-hosted Codex product sessions. [App-server](https://learn.chatgpt.com/docs/app-server), [platform architecture](https://developers.openai.com/blog/codex-as-a-platform).

**Codex hooks have coverage limits.** Local tool hooks can intercept covered tool paths; hosted tools and specialized paths are exceptions, and malformed/unsupported output can fail without blocking a call. The transcript-file format is not stable. Treat hook coverage and failure behavior as explicit qualification dimensions. [Codex hooks](https://learn.chatgpt.com/docs/hooks).

**Codex cloud CLI is a limited proven surface.** The official reference documents task submission and JSON list/status information and labels the cloud command experimental. Complete programmatic cancellation, mid-turn control, approval continuation and subagent stream access were not established by these pages. [CLI reference](https://learn.chatgpt.com/docs/cli/reference).

**Cursor cloud differs before its writable environment exists.** Cloud command hooks omit local session/MCP hooks and may not run during initial read-only exploration. Some pool-specific behavior differs again, so don't generalize pool evidence to the requested Cursor-hosted profile. [Cursor hooks](https://cursor.com/docs/hooks).

**Cursor queue/recovery is adapter work.** The Python SDK documents one active cloud run per agent and a non-retryable busy response. Its resumable observation and terminal state are distinct from prompt submission. [Python SDK](https://cursor.com/docs/sdk/python). Cloud API event/retention and endpoint details need fixtures from the selected version/account. [Cloud API](https://cursor.com/docs/cloud-agent/api/endpoints).

**Human waits are not uniformly durable inside providers.** LangGraph checkpoints support interrupt/resume; replay can re-execute node code preceding an interrupt. Mission Control's effects must therefore be idempotent and approval state independent of an in-memory callback. [LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts).

**MCP capability negotiation is mandatory.** Elicitation support must be declared by the client, with separate form/URL capabilities. It is not equivalent to provider permission hooks. [MCP elicitation](https://modelcontextprotocol.io/specification/2025-11-25/client/elicitation).

**Socket.IO needs our own durability.** Its default delivery is at most once, so reconnect recovery needs persisted IDs/cursors. ASGI wrapping and rooms fit the existing FastAPI stack. [Delivery](https://socket.io/docs/v4/delivery-guarantees/), [Python server](https://python-socketio.readthedocs.io/en/stable/server.html). Context7 resolved `/miguelgrinberg/python-socketio` and confirmed the ASGIApp/room integration from upstream documentation.

**Temporal rollover and cancellation do not replace application reconciliation.** Continue-As-New must account for active message handlers; long activities use heartbeat/cancellation mechanics and explicit error classification. [Message passing](https://docs.temporal.io/develop/python/workflows/message-passing), [Continue-As-New](https://docs.temporal.io/develop/python/workflows/continue-as-new), [failure handling](https://docs.temporal.io/develop/python/best-practices/error-handling).

## Provider-hosted feasibility exit criteria

Produce a machine-readable matrix for the exact account, product version and environment. For every feature record official source, invocation/schema, authentication mode, native IDs, deduplication strategy, failure/retention behavior, observed result and qualification evidence. The required checklist is launch, inspect/status, observe/replay, follow-up, cancel, usage, output custody, environment selection, configuration materialization, approval suspension/resume, subordinate lineage and bounded continuation.

Outcomes: (1) implement documented adapter, (2) expose explicitly limited integration with incompatible workflows rejected, or (3) keep profile unqualified with precise missing vendor operations. A successful UI action proves the product can do something, not that Mission Control has a supported automation API. Do not make browser automation, scraped session cookies, private endpoints or unrelated OpenAI/Anthropic managed-agent services the production lane.

## Version strategy

Code pins observed: `cursor-sdk==1.0.37`, `deepagents==0.7.5`, `langgraph==1.2.10`, `mcp==1.29.0`, `python-socketio>=5.13,<6`, `temporalio[opentelemetry]>=1.34,<2`. Claude/Codex runtime dependencies are absent from `pyproject.toml`. Select exact implementation pins during adapter qualification; capture CLI/bundled binary, SDK, generated schema and OS together. Do not install or upgrade dependencies merely to write the architecture.

Provider documentation is mutable. Every live qualification record must preserve the exact schema/fixture used. The earlier survey's package-release numbers are historical candidates, not newly verified recommendations.
