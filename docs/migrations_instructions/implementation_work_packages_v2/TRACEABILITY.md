# Control-plane foundation traceability projection

Status: generated-style projection from canonical specification metadata; requirements are authored only in their owning specifications.  
Evidence status: planned until the owning work package is implemented and accepted.

| Requirement | Source anchors | ADR | Canonical spec | Contract | Work package | GitHub/local issue | Test/qualification | Evidence | Status |
|---|---|---|---|---|---|---|---|---|---|
| REQ-CP-DEF-001 | Pre-research F1 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-DEFINITION-REF-V1 | WP-CP-010 | local Markdown | QUAL-CP-DETERMINISTIC-COMPILATION | evidence_v2/WP-CP-010 | planned |
| REQ-CP-DEF-002 | ADR-0001; F1 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-DEFINITION-REF-V1 | WP-CP-010 | local Markdown | publication/compilation integration | evidence_v2/WP-CP-010 | planned |
| REQ-CP-DEF-003 | Architecture proposal §7 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010 | local Markdown | blueprint-binding validation | evidence_v2/WP-CP-010 | planned |
| REQ-CP-DEF-004 | Governing invariant | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010 | local Markdown | QUAL-CP-DETERMINISTIC-COMPILATION | evidence_v2/WP-CP-010 | planned |
| REQ-CP-DEF-005 | Foundation interview decisions 3-8 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010, WP-CP-050 | local Markdown | tracer ERC execution | evidence_v2/WP-CP-050 | planned |
| REQ-CP-DEF-006 | Foundation interview decision 5 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010 | local Markdown | composition/collision tests | evidence_v2/WP-CP-010 | planned |
| REQ-CP-DEF-007 | Pre-research F1 overlays | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010 | local Markdown | overlay decision tests | evidence_v2/WP-CP-010 | planned |
| REQ-CP-DEF-008 | Foundation interview decisions 5-8 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010, WP-CP-050 | local Markdown | capability failure/degradation | evidence_v2/WP-CP-050 | planned |
| REQ-CP-DEF-009 | Architecture proposal §7; F1 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010 | local Markdown | child compilation integration | evidence_v2/WP-CP-010 | planned |
| REQ-CP-DEF-010 | Architecture proposal §17 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-DEFINITION-REF-V1 | WP-CP-010 | local Markdown | secret serialization tests | evidence_v2/WP-CP-010 | planned |
| REQ-CP-RUN-001 | Pre-research F2 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-RUN-REQUEST-V1 | WP-CP-020, WP-CP-050 | local Markdown | QUAL-CP-TRANSACTIONAL-AUTHORITY | evidence_v2/WP-CP-020 | planned |
| REQ-CP-RUN-002 | Pre-research F2 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-DOMAIN-EVENT-V1 | WP-CP-020 | local Markdown | transaction failure injection | evidence_v2/WP-CP-020 | planned |
| REQ-CP-RUN-003 | Pre-research F2 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-LIFECYCLE-V1 | WP-CP-020 | local Markdown | concurrent CAS tests | evidence_v2/WP-CP-020 | planned |
| REQ-CP-RUN-004 | Pre-research F2 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-LIFECYCLE-V1 | WP-CP-020 | local Markdown | lifecycle-axis tests | evidence_v2/WP-CP-020 | planned |
| REQ-CP-RUN-005 | Architecture proposal §§5-6 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-LIFECYCLE-V1 | WP-CP-020, WP-CP-050 | local Markdown | terminalization evidence tests | evidence_v2/WP-CP-050 | planned |
| REQ-CP-RUN-006 | Pre-research F2 budgets | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-BUDGET-LEDGER-V1 | WP-CP-020 | local Markdown | concurrency/settlement tests | evidence_v2/WP-CP-020 | planned |
| REQ-CP-RUN-007 | Architecture proposal §10 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-BUDGET-LEDGER-V1 | WP-CP-020 | local Markdown | effect ambiguity tests | evidence_v2/WP-CP-020 | planned |
| REQ-CP-RUN-008 | Pre-research F2 outbox | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-DOMAIN-EVENT-V1 | WP-CP-020 | local Markdown | redelivery/gap tests | evidence_v2/WP-CP-020 | planned |
| REQ-CP-RUN-009 | Foundation interview decisions 9-10 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-ASYNC-SUBAGENT-V1 | WP-CP-020, WP-CP-045 | local Markdown | QUAL-CP-ASYNC-SUBAGENT-LIFECYCLE | evidence_v2/WP-CP-045 | planned |
| REQ-CP-RUN-010 | Pre-research F2 finalization | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-LIFECYCLE-V1 | WP-CP-020 | local Markdown | bounded finalization tests | evidence_v2/WP-CP-020 | planned |
| REQ-CP-EXEC-001 | Proposal §7.1 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-TEMPORAL-IDENTITY-V1 | WP-CP-030, WP-CP-050 | local Markdown | root idempotency/replay | evidence_v2/WP-CP-030 | planned |
| REQ-CP-EXEC-002 | Proposal §§7.1-7.4 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-TEMPORAL-IDENTITY-V1 | WP-CP-030 | local Markdown | cross-family root tests | evidence_v2/WP-CP-030 | planned |
| REQ-CP-EXEC-003 | Proposal §7.3 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-TEMPORAL-IDENTITY-V1 | WP-CP-030, WP-CP-050 | local Markdown | operation recovery tests | evidence_v2/WP-CP-050 | planned |
| REQ-CP-EXEC-004 | Proposal §§6-8 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-TEMPORAL-IDENTITY-V1 | WP-CP-030 | local Markdown | captured-history replay | evidence_v2/WP-CP-030 | planned |
| REQ-CP-EXEC-005 | Proposal §§8-10 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-TEMPORAL-IDENTITY-V1 | WP-CP-030, WP-CP-045 | local Markdown | retry/generation tests | evidence_v2/WP-CP-045 | planned |
| REQ-CP-EXEC-006 | Proposal §9 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-WORKFLOW-MESSAGE-V1 | WP-CP-030, WP-CP-045, WP-CP-050 | local Markdown | ordered receipt tests | evidence_v2/WP-CP-045 | planned |
| REQ-CP-EXEC-007 | Proposal §9 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-WORKFLOW-MESSAGE-V1 | WP-CP-030 | local Markdown | Signal/Update/Query tests | evidence_v2/WP-CP-030 | planned |
| REQ-CP-EXEC-008 | Proposal §§9-10 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-WORKFLOW-MESSAGE-V1 | WP-CP-030, WP-CP-045, WP-CP-050 | local Markdown | cancellation saga tests | evidence_v2/WP-CP-045 | planned |
| REQ-CP-EXEC-009 | Proposal §7; pre-research F3 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-LINKED-RUN-V1 | WP-CP-030 | local Markdown | QUAL-CP-LINKED-RUN-SEMANTICS | evidence_v2/WP-CP-030 | planned |
| REQ-CP-EXEC-010 | Pre-research F3 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-LINKED-RUN-V1 | WP-CP-030 | local Markdown | result/late-result tests | evidence_v2/WP-CP-030 | planned |
| REQ-CP-EXEC-011 | Proposal §12 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-CONTINUATION-V1 | WP-CP-030, WP-CP-050 | local Markdown | forced Continue-As-New | evidence_v2/WP-CP-050 | planned |
| REQ-CP-EXEC-012 | Proposal §11 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-CONTINUATION-V1 | WP-CP-030 | local Markdown | semantic fork tests | evidence_v2/WP-CP-030 | planned |
| REQ-CP-DA-001..007 | Proposal §8; foundation decisions 2-8 | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-DEEP-AGENT-PROFILE/PLACEMENT/BINDING-V1 | WP-CP-040 | local Markdown | QUAL-CP-DEEP-AGENT-MATERIALIZATION | evidence_v2/WP-CP-040 | planned |
| REQ-CP-DA-008..012 | Foundation decisions 9-11 | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-ASYNC-SUBAGENT-V1 | WP-CP-045 | local Markdown | QUAL-CP-ASYNC-SUBAGENT-LIFECYCLE | evidence_v2/WP-CP-045 | planned |
| REQ-CP-DA-013..015 | Pre-research F4 workspace/artifact/snapshot | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-WORKSPACE/ARTIFACT/SNAPSHOT-V1 | WP-CP-040 | local Markdown | workspace/promotion/restore tests | evidence_v2/WP-CP-040 | planned |
| REQ-BP-SG-001..003 | Pre-research F3 StageGraph | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGEGRAPH-V1 | WP-BP-010 | local Markdown | interpreter validation/determinism | evidence_v2/WP-BP-010 | planned |
| REQ-BP-SG-004 | Proposal §§2, 7.2 | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGE-DECISION-V1 | WP-BP-010, WP-CP-050 | local Markdown | early-release timing proof | evidence_v2/WP-CP-050 | planned |
| REQ-BP-SG-005..010 | Pre-research F3; proposal §7.2 | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGE-DECISION-V1 | WP-BP-010 | local Markdown | QUAL-BP-STAGEGRAPH-PARITY-RECOVERY | evidence_v2/WP-BP-010 | planned |
| REQ-BP-GD-001..003 | Pre-research F3; proposal §7.4 | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-DIRECTED-V1 | WP-BP-020 | local Markdown | revision/iteration tests | evidence_v2/WP-BP-020 | planned |
| REQ-BP-GD-004 | Proposal §7.4 | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-VERIFICATION-V1 | WP-BP-020, WP-CP-050 | local Markdown | independent verifier proof | evidence_v2/WP-CP-050 | planned |
| REQ-BP-GD-005..006 | Proposal §§7.4, 12 | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-HANDOFF-V1 | WP-BP-020, WP-CP-050 | local Markdown | fresh-session/rollover proof | evidence_v2/WP-CP-050 | planned |
| REQ-BP-GD-007..010 | Pre-research F3; foundation decisions 9-10 | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-DIRECTED-V1 | WP-BP-020 | local Markdown | QUAL-BP-GOAL-DIRECTED-CONVERGENCE | evidence_v2/WP-BP-020 | planned |

## Validation rule

Ranges in this projection are compact views only. The owning specification lists and defines every atomic requirement individually. A release script or later registry implementation must expand ranges mechanically and reject any requirement without exactly one owning specification and at least one implementing work package.

