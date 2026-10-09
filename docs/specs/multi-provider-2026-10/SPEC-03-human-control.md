---
type: Specification
title: "Human Gate and tool-level human control"
description: "A durable Human Task model with separate workflow-node and provider/MCP correlations, safe resumption and bounded approval waits."
tags: [mission-control, human-tasks, mcp]
---

# Human control

Build two origins of human interaction over one durable Human Task service. The existing general Human Gate is an executable program node to implement, not merely a provider permission callback. Native tool requests are pending runtime operations, not graph revisions.

## Explicit Human Gate

Lower `HumanGateNode` into a typed control activation with one deterministic human-task identity per activation/attempt. Reuse common `human_task`/`human_resolution` tables and actor/version checks; do not add a competing approval ledger. Retain public outcomes and timeout rules from the accepted durable-controls annex.

On entry, capture immutable review packet refs and digests, reviewer policy, permitted resolution kinds, deadline and remediation target. Commit task-open plus event/outbox before waiting. Temporal waits on persisted resolution or deadline, consuming no cognitive activity slot.

- Approve/review-accept authorizes only the exact packet/version represented by the task.
- Reject closes the gate as not accepted. Feedback is an attributed artifact.
- Request changes resolves the review with feedback and activates an explicitly declared remediation route; bound rounds with a governor. It does not silently mutate the mission's revision or acceptance criteria.
- Stop/cancel expires or cancels outstanding tasks consistently. Stale and double answers fail expected-version checks; retries of the same resolution are idempotent.
- A task timed out with `keep_waiting` remains pending; timeout is not implicit approval. Defaults are permitted only when explicitly admitted by the node's policy.

Stage Graph can wait at a review node between executor nodes. GoalDirected can call a bounded review/control action and use feedback in the next iteration without resetting its budget. A goal loop's existing `acceptance.human` predicate requires an attributable task resolution; notification delivery cannot satisfy it.

## Native provider requests

Proposed `ApprovalBinding` correlates `(human_task_id, origin, execution_id, generation, native_session, native_turn, native_request/tool_call, tool_name, input_digest, policy_digest, deadline, replay_strategy)`. Origins: `workflow_gate`, `provider_permission`, `provider_question`, `mcp_elicitation`, `governed_effect`.

Write the binding before waiting for the human. Translate only admitted resolution shapes. Approval applies to the exact normalized arguments and policy. Edited arguments produce a new digest and, where necessary, a new review. Feedback is an instruction/result; it is not permission to elevate authority.

Short waits may retain a live native callback under a bounded session manager. Long waits require a documented provider defer/checkpoint/resume mechanism or a deny/interrupt + durable parked operation. Do not keep an Activity or native hook process sleeping indefinitely. SDK timeout/disconnection expires the native correlation even if the durable Human Task remains; reissue a pending request or restart at a safe boundary only under the recorded replay strategy.

After restart, an approved task cannot be answered into a new native request by ID coincidence. Revalidate generation, arguments, current grants and stop fence. Pending request lost with the local process: reconcile effects, then obtain a fresh request/correlation or park for reconciliation. Preserve the original human decision as evidence; never reuse an opaque connection-scoped request handle.

Claude uses permission callbacks/user-question handling when the selected configuration actually invokes them; automatically allowed tools may not call a permission callback. Codex exposes approval/user-input RPCs for particular paths. Cursor headless `ask` and cloud tool-gate coverage require qualification. Deep Agents uses its interrupt/checkpoint mechanism, but business approval still belongs to Mission Control. [Claude user input](https://code.claude.com/docs/en/agent-sdk/user-input), [Deep Agents HITL](https://docs.langchain.com/oss/python/deepagents/human-in-the-loop), [LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts).

## MCP is two different approval problems

**Pre-execution authorization:** a tool has not run; Mission Control must decide whether the effect may be admitted. A governed MCP gateway resolves capability/grants, persists the effect intent and tool-input digest, checks the Stop Fence and requires a valid Human Task resolution where policy says so. Domain-effect idempotency lives in the capability/receipt contract.

**Server-requested elicitation:** an MCP server asks its client for structured information or a URL-mediated interaction during a call. Elicitation is negotiated protocol capability, not a universal permission gate, and an `accept` response is not proof of downstream effect success. Translate accepted, declined and cancelled responses distinctly. URL workflows remain authenticated provider/server flows; do not ask the model to collect credentials into an ordinary form. [MCP elicitation](https://modelcontextprotocol.io/specification/2025-11-25/client/elicitation).

For each provider, qualification must prove both tool interception and whether it forwards MCP elicitation to an API the adapter controls. “Supports MCP” proves neither. If it cannot forward elicitation, expose only tools that don't require it or route through a Mission Control-owned gateway that terminates the protocol and owns the user interaction.

## Durable prepare/review/execute fallback

For Mission Control-owned domain tools, support a typed prepare operation that returns `pending_approval`, intent ID and Human Task reference **without executing the effect**. Review resolves the task. An explicit execute/resume operation consumes the approval-bound intent and returns its stable receipt. This application protocol is not presented as a standard MCP feature.

The gateway serializes intent consumption and fence checks; repeated client tool calls return the pending state or the existing receipt. A human approval permits at most the admitted intent, not arbitrary retries or changed arguments. If a third-party MCP tool has already executed before requesting input, its effects are liabilities to reconcile, not retroactively preventable approvals.

This fallback cannot govern arbitrary shell/network writes unless credentials and egress force those writes through the gateway. Qualify enforcement coverage by tool family. If the workflow requires all writes to be human-gated and the hosted provider allows uncontrolled writes, reject that combination.

## Transport and authorization

Proposed HTTP endpoints under the existing application prefix: list/read Human Tasks and `POST /human-tasks/{id}/resolutions`, with `request_id`, `expected_task_version`, typed decision, feedback artifact refs and current principal. MCP and Socket.IO call the same application service. Assignee/reviewer permissions are enforced in the service; socket room membership is never authorization.

Expose task state, origin, pending tool preview, review packet and expiry through inspection. Publish opened/resolved/expired/cancelled transitions transactionally; do not display raw tokens, unredacted tool credentials or native transport request IDs as user-facing capabilities.

Acceptance tests must cover two concurrent reviewers, stale approval after compaction/cancel/restart, callback timeout, missing client elicitation capability, repeated pending tools, denied effects, review-with-feedback remediation and an HTTP/Socket.IO retry of the same resolution.
