---
type: Decision Record
title: "Two origins of human control over one Human Task service: the Human Gate is a kernel-executed program node, native provider approvals bind to Human Tasks through an Approval Binding, and governed effects fall back to prepare, review, execute"
description: "Proposed for the multi-provider packet (SPEC-03, MP-10, MP-11): no provider callback is the authority; HumanGateNode lowers into a control activation with one deterministic Human Task; native permission, question, elicitation and governed-effect requests are correlated by mc.approval_binding.v1 before anyone waits; a lane that cannot enforce a required gate is rejected at admission."
tags: [mission-control, adr, proposed, human-tasks, approvals, mcp]
status: proposed
source: docs/specs/multi-provider-2026-10/SPEC-03-human-control.md; docs/specs/multi-provider-2026-10/RESEARCH.md (LangGraph interrupts, MCP elicitation, Claude user input, Codex approval RPCs); ADR-0035 (mc.approval_binding.v1); workflow-types/05-EXECUTORS_AND_DURABLE_CONTROLS
---

# Two origins of human control over one Human Task service: the Human Gate is a kernel-executed program node, native provider approvals bind to Human Tasks through an Approval Binding, and governed effects fall back to prepare, review, execute

**Status: proposed.** Becomes accepted when the owner authorizes it.

Deep Agents makes human-in-the-loop a first-class interrupt in graph state; Claude, Codex and Cursor
each expose a different permission or question callback with different coverage; MCP elicitation is a
negotiated client capability, not a permission gate; and a hosted provider's tools may bypass hooks
entirely. We decided that no provider mechanism is the authority. The manifest's `HumanGateNode` lowers
into a typed control activation that opens exactly one deterministic Human Task per activation and
attempt; Temporal waits on the persisted resolution or deadline and holds no cognition slot. A native
request of any origin (`provider_permission`, `provider_question`, `mcp_elicitation`,
`governed_effect`) becomes an Approval Binding (`mc.approval_binding.v1`) that correlates the Human Task
with the generation, native session, turn and request, tool name, input digest and policy digest, written
before anyone waits; the decision authorizes only that digest, and after a restart a stored decision is
revalidated against generation, current grants and the Stop Fence rather than answered into a new
request by ID coincidence. For Mission Control-owned tools the governed MCP gateway offers prepare
(returns `pending_approval` and executes nothing), review and execute/resume of the bound intent. A
lane that cannot enforce a gate the workflow requires (hooks bypassed, elicitation not forwarded, no
deferral for long waits) is rejected at admission for that workflow, never downgraded silently. We
rejected "approval lives in the provider" (not durable across process death, not attributable, not
uniform) and rejected a second approval ledger beside `human_task` (two state machines for one fact).

## Consequences

- Timeout is never implicit approval; `keep_waiting` leaves the task pending and defaults apply only
  when the node's policy admits them.
- `request_changes` activates a declared remediation route under a governor and never mutates the
  mission revision or acceptance criteria.
- The HTTP resolution endpoint is the one mutation path; MCP and Socket.IO reuse the same service, and
  stale or duplicate answers fail the expected-version check idempotently.
- Qualification must prove, per provider and per tool family, both tool interception and whether
  elicitation reaches an API the adapter controls; "supports MCP" proves neither.
- A third-party tool that executed before asking leaves a liability to reconcile, not an approval to
  grant retroactively.
