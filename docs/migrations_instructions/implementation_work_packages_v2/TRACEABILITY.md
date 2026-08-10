# Control-plane foundation traceability projection

Status: generated-style projection from canonical specification metadata; requirements are authored only in their owning specifications.  
Evidence status: WP-CP-010, WP-CP-020, WP-CP-030, WP-CP-040, and WP-CP-045 are accepted; later packages remain planned.
Canonical metadata revision: `c48867a240d09a98db9cdfb4937f55176f30adf1`.

| Requirement | Source anchors | ADR | Canonical spec | Contract | Work package | GitHub/local issue | Test/qualification | Evidence | Status |
|---|---|---|---|---|---|---|---|---|---|
| REQ-CP-DEF-001 | Pre-research F1 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-DEFINITION-REF-V1 | WP-CP-010 | local Markdown | lifecycle and Mongo repository tests | `evidence_v2/WP-CP-010/README.md` | accepted |
| REQ-CP-DEF-002 | ADR-0001; F1 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-DEFINITION-REF-V1 | WP-CP-010 | local Markdown | Workflow Type publication/compilation tests | `evidence_v2/WP-CP-010/README.md` | accepted |
| REQ-CP-DEF-003 | Architecture proposal §7 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010 | local Markdown | GoalDirected and StageGraph compile fixtures | `evidence_v2/WP-CP-010/README.md` | accepted |
| REQ-CP-DEF-004 | Governing invariant | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010 | local Markdown | deterministic compilation and drift guard | `evidence_v2/WP-CP-010/README.md` | accepted |
| REQ-CP-DEF-005 | Foundation interview decisions 3-8 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010, WP-CP-050 | local Markdown | complete ERC qualification assertions | `evidence_v2/WP-CP-010/README.md` | accepted |
| REQ-CP-DEF-006 | Foundation interview decision 5 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010 | local Markdown | deterministic flattening/collision tests | `evidence_v2/WP-CP-010/README.md` | accepted |
| REQ-CP-DEF-007 | Pre-research F1 overlays | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010 | local Markdown | accepted/rejected/omitted/degraded decisions | `evidence_v2/WP-CP-010/README.md` | accepted |
| REQ-CP-DEF-008 | Foundation interview decisions 5-8 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010, WP-CP-050 | local Markdown | six exact capability families; fail/omit/degrade | `evidence_v2/WP-CP-010/README.md` | accepted |
| REQ-CP-DEF-009 | Architecture proposal §7; F1 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-ERC-V1 | WP-CP-010 | local Markdown | `tests/test_linked_runs.py` independent child compilation | `evidence_v2/WP-CP-010/README.md` | accepted |
| REQ-CP-DEF-010 | Architecture proposal §17 | ADR-0003 | SPEC-CP-DEFINITIONS | CON-CP-DEFINITION-REF-V1 | WP-CP-010 | local Markdown | secret rejection/serialization/payload tests | `evidence_v2/WP-CP-010/README.md` | accepted |
| REQ-CP-RUN-001 | Pre-research F2 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-RUN-REQUEST-V1 | WP-CP-020, WP-CP-050 | local Markdown | QUAL-CP-TRANSACTIONAL-AUTHORITY | `evidence_v2/WP-CP-020/README.md` | accepted |
| REQ-CP-RUN-002 | Pre-research F2 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-DOMAIN-EVENT-V1 | WP-CP-020 | local Markdown | transaction failure injection | `evidence_v2/WP-CP-020/README.md` | accepted |
| REQ-CP-RUN-003 | Pre-research F2 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-LIFECYCLE-V1 | WP-CP-020 | local Markdown | concurrent CAS tests | `evidence_v2/WP-CP-020/README.md` | accepted |
| REQ-CP-RUN-004 | Pre-research F2 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-LIFECYCLE-V1 | WP-CP-020 | local Markdown | lifecycle-axis tests | `evidence_v2/WP-CP-020/README.md` | accepted |
| REQ-CP-RUN-005 | Architecture proposal §§5-6 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-LIFECYCLE-V1 | WP-CP-020, WP-CP-050 | local Markdown | terminalization evidence tests | `evidence_v2/WP-CP-020/README.md` | accepted |
| REQ-CP-RUN-006 | Pre-research F2 budgets | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-BUDGET-LEDGER-V1 | WP-CP-020 | local Markdown | concurrency/settlement tests | `evidence_v2/WP-CP-020/README.md` | accepted |
| REQ-CP-RUN-007 | Architecture proposal §10 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-BUDGET-LEDGER-V1 | WP-CP-020 | local Markdown | effect ambiguity tests | `evidence_v2/WP-CP-020/README.md` | accepted |
| REQ-CP-RUN-008 | Pre-research F2 outbox | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-DOMAIN-EVENT-V1 | WP-CP-020 | local Markdown | redelivery/gap tests | `evidence_v2/WP-CP-020/README.md` | accepted |
| REQ-CP-RUN-009 | Foundation interview decisions 9-10 | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-ASYNC-SUBAGENT-V1 | WP-CP-020, WP-CP-045 | local Markdown | parent-authority acceptance test; QUAL-CP-ASYNC-SUBAGENT-LIFECYCLE remains WP-CP-045 | `evidence_v2/WP-CP-020/README.md` | accepted |
| REQ-CP-RUN-010 | Pre-research F2 finalization | ADR-0003 | SPEC-CP-RUN-CONTROL | CON-CP-LIFECYCLE-V1 | WP-CP-020 | local Markdown | bounded finalization tests | `evidence_v2/WP-CP-020/README.md` | accepted |
| REQ-CP-EXEC-001 | Proposal §7.1 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-TEMPORAL-IDENTITY-V1 | WP-CP-030, WP-CP-050 | local Markdown | root idempotency/replay | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-002 | Proposal §§7.1-7.4 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-TEMPORAL-IDENTITY-V1 | WP-CP-030 | local Markdown | cross-family root tests | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-003 | Proposal §7.3 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-TEMPORAL-IDENTITY-V1 | WP-CP-030, WP-CP-050 | local Markdown | operation recovery tests | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-004 | Proposal §§6-8 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-TEMPORAL-IDENTITY-V1 | WP-CP-030 | local Markdown | captured-history replay | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-005 | Proposal §§8-10 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-TEMPORAL-IDENTITY-V1 | WP-CP-030, WP-CP-045 | local Markdown | retry/generation tests | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-006 | Proposal §9 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-WORKFLOW-MESSAGE-V1 | WP-CP-030, WP-CP-045, WP-CP-050 | local Markdown | ordered receipt tests | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-007 | Proposal §9 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-WORKFLOW-MESSAGE-V1 | WP-CP-030 | local Markdown | Signal/Update/Query tests | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-008 | Proposal §§9-10 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-WORKFLOW-MESSAGE-V1 | WP-CP-030, WP-CP-045, WP-CP-050 | local Markdown | cancellation saga tests | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-009 | Proposal §7; pre-research F3 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-LINKED-RUN-V1 | WP-CP-030 | local Markdown | QUAL-CP-LINKED-RUN-SEMANTICS | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-010 | Pre-research F3 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-LINKED-RUN-V1 | WP-CP-030 | local Markdown | result/late-result tests | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-011 | Proposal §12 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-CONTINUATION-V1 | WP-CP-030, WP-CP-050 | local Markdown | forced Continue-As-New | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-EXEC-012 | Proposal §11 | ADR-0003 | SPEC-CP-DURABLE-EXECUTION | CON-CP-CONTINUATION-V1 | WP-CP-030 | local Markdown | semantic fork tests | `evidence_v2/WP-CP-030/README.md` | accepted |
| REQ-CP-DA-001 | Proposal §8; provider-neutral seam | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-DEEP-AGENT-BINDING-V1 | WP-CP-040 | local Markdown | executor conformance | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-DA-002 | Foundation profile decision | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-DEEP-AGENT-PROFILE-V1 | WP-CP-040 | local Markdown | strict profile schema | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-DA-003 | Foundation flattening decision | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-DEEP-AGENT-BINDING-V1 | WP-CP-040, WP-CP-050 | local Markdown | binding drift tests | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-DA-004 | Foundation placement decision | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-DEEP-AGENT-PLACEMENT-V1 | WP-CP-040 | local Markdown | placement/fallback tests | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-DA-005 | Foundation capability decision | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-DEEP-AGENT-BINDING-V1 | WP-CP-040, WP-CP-050 | local Markdown | exact attachment vertical | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-DA-006 | Catalog/runtime boundary | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-DEEP-AGENT-BINDING-V1 | WP-CP-040 | local Markdown | runtime access guards | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-DA-007 | Foundation sync-subagent decision | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-DEEP-AGENT-PROFILE-V1 | WP-CP-040 | local Markdown | delegation ceiling tests | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-DA-008 | Foundation async-child decision | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-ASYNC-SUBAGENT-V1 | WP-CP-045, WP-CP-050 | local Markdown | spawn-before-submit tests | `evidence_v2/WP-CP-045/README.md` | accepted |
| REQ-CP-DA-009 | Foundation dependency-class decision | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-ASYNC-SUBAGENT-V1 | WP-CP-045 | local Markdown | four dependency classes | `evidence_v2/WP-CP-045/README.md` | accepted |
| REQ-CP-DA-010 | Foundation async messaging decision | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-ASYNC-SUBAGENT-V1 | WP-CP-045 | local Markdown | ordered message tests | `evidence_v2/WP-CP-045/README.md` | accepted |
| REQ-CP-DA-011 | Foundation result-admission decision | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-ASYNC-SUBAGENT-V1 | WP-CP-045, WP-CP-050 | local Markdown | result/late-result tests | `evidence_v2/WP-CP-045/README.md` | accepted |
| REQ-CP-DA-012 | Foundation escalation decision | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-ASYNC-SUBAGENT-V1 | WP-CP-045 | local Markdown | classifier fixtures | `evidence_v2/WP-CP-045/README.md` | accepted |
| REQ-CP-DA-013 | Pre-research F4 workspace | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-WORKSPACE-MANIFEST-V1 | WP-CP-040 | local Markdown | workspace ownership tests | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-DA-014 | Pre-research F4 artifact | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-ARTIFACT-PROMOTION-V1 | WP-CP-040 | local Markdown | promotion/idempotency tests | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-DA-015 | Pre-research F4 snapshot | ADR-0003 | SPEC-CP-DEEP-AGENT-RUNTIME | CON-CP-SNAPSHOT-V1 | WP-CP-040 | local Markdown | clone/reauthorization tests | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-CS-001 | Cognitive schema foundation | ADR-0004 | SPEC-CP-COGNITIVE-SCHEMAS | CON-CP-COGNITIVE-STATE-SCHEMA-V1 | WP-CP-040 | local Markdown | distinct state/context contracts | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-CS-002 | Cognitive composition root | ADR-0004 | SPEC-CP-COGNITIVE-SCHEMAS | CON-CP-COGNITIVE-STATE-SCHEMA-V1 | WP-CP-040 | local Markdown | adapter-only type composition | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-CS-003 | Minimum base channels | ADR-0004 | SPEC-CP-COGNITIVE-SCHEMAS | CON-CP-COGNITIVE-STATE-SCHEMA-V1 | WP-CP-040 | local Markdown | actual state inspection | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-CS-004 | Exact channel packs | ADR-0004 | SPEC-CP-COGNITIVE-SCHEMAS | CON-CP-COGNITIVE-CHANNEL-PACK-V1 | WP-CP-040 | local Markdown | digest/collision tests | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-CS-005 | Frozen middleware channels | ADR-0004 | SPEC-CP-COGNITIVE-SCHEMAS | CON-CP-COGNITIVE-CHANNEL-PACK-V1 | WP-CP-040 | local Markdown | Skills middleware ownership test | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-CS-006 | Subagent projections | ADR-0004 | SPEC-CP-COGNITIVE-SCHEMAS | CON-CP-COGNITIVE-STATE-SCHEMA-V1 | WP-CP-040 | local Markdown | exact slice ceiling test | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-CP-CS-007 | Checkpoint digest gate | ADR-0004 | SPEC-CP-COGNITIVE-SCHEMAS | CON-CP-COGNITIVE-CONTEXT-SCHEMA-V1 | WP-CP-040 | local Markdown | binding/checkpoint schema digests | `evidence_v2/WP-CP-040/README.md` | accepted |
| REQ-BP-SG-001 | Pre-research F3 StageGraph | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGEGRAPH-V2 | WP-BP-010 | local Markdown | structural validation; normalization; complete ordering-key registry; digest stability | evidence_v2/WP-BP-010 | planned |
| REQ-BP-SG-002 | Pre-research F3 joins | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGEGRAPH-V2 | WP-BP-010 | local Markdown | complete dependency-disposition and join truth tables | evidence_v2/WP-BP-010 | planned |
| REQ-BP-SG-003 | Pre-research F3 determinism | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGE-DECISION-V1 | WP-BP-010 | local Markdown | randomized-order determinism | evidence_v2/WP-BP-010 | planned |
| REQ-BP-SG-004 | Proposal §§2, 7.2 | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGEGRAPH-V2; CON-BP-STAGE-DECISION-V1 | WP-BP-010, WP-CP-050 | local Markdown | controlled early-release ordering and slow-sibling liability proof; cohesive vertical | evidence_v2/WP-BP-010; evidence_v2/WP-CP-050 | planned |
| REQ-BP-SG-005 | Pre-research F3 fairness | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGEGRAPH-V2; CON-BP-STAGE-DECISION-V1 | WP-BP-010 | local Markdown | initial/resumed weighted-ring cursors; admission-only movement; saturation/starvation | evidence_v2/WP-BP-010 | planned |
| REQ-BP-SG-006 | Proposal §7.2 retry identity | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGE-DECISION-V1 | WP-BP-010 | local Markdown | retry/cycle lineage | evidence_v2/WP-BP-010 | planned |
| REQ-BP-SG-007 | Pre-research F3 stage cycles | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGEGRAPH-V2; CON-BP-STAGE-DECISION-V1 | WP-BP-010 | local Markdown | bounded repair and semantic-precedence tests | evidence_v2/WP-BP-010 | planned |
| REQ-BP-SG-008 | Pre-research F3 invalidation | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGEGRAPH-V2; CON-BP-STAGE-DECISION-V1 | WP-BP-010 | local Markdown | minimal reuse and invalid-generation lineage tests | evidence_v2/WP-BP-010 | planned |
| REQ-BP-SG-009 | Pre-research F3 waits/cancellation | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGEGRAPH-V2; CON-BP-STAGE-DECISION-V1 | WP-BP-010 | local Markdown | wait/cancel; slow-sibling routing; veto/rule precedence; late-result effects | evidence_v2/WP-BP-010 | planned |
| REQ-BP-SG-010 | Pre-research F3 obligations | ADR-0003 | SPEC-BP-STAGEGRAPH | CON-BP-STAGEGRAPH-V2; CON-BP-STAGE-DECISION-V1 | WP-BP-010, WP-CP-050 | local Markdown | obligation completion and producer-liability closure | evidence_v2/WP-BP-010 | planned |
| REQ-BP-GD-001 | Pre-research F3 objective envelope | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-DIRECTED-V1 | WP-BP-020 | local Markdown | envelope publication/control | evidence_v2/WP-BP-020 | planned |
| REQ-BP-GD-002 | Proposal §7.4 revisions | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-DIRECTED-V1 | WP-BP-020 | local Markdown | revision-boundary tests | evidence_v2/WP-BP-020 | planned |
| REQ-BP-GD-003 | Proposal §7.4 iterations | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-DIRECTED-V1 | WP-BP-020 | local Markdown | operation durability tests | evidence_v2/WP-BP-020 | planned |
| REQ-BP-GD-004 | Proposal §7.4 | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-VERIFICATION-V1 | WP-BP-020, WP-CP-050 | local Markdown | independent verifier proof; cohesive vertical | evidence_v2/WP-BP-020; evidence_v2/WP-CP-050 | planned |
| REQ-BP-GD-005 | Proposal §7.4 handoff | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-HANDOFF-V1 | WP-BP-020, WP-CP-050 | local Markdown | empty-session resume; cohesive vertical | evidence_v2/WP-BP-020; evidence_v2/WP-CP-050 | planned |
| REQ-BP-GD-006 | Proposal §12 rollover | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-HANDOFF-V1 | WP-BP-020, WP-CP-050 | local Markdown | rollover/protected facts; cohesive vertical | evidence_v2/WP-BP-020; evidence_v2/WP-CP-050 | planned |
| REQ-BP-GD-007 | Pre-research F3 convergence | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-DIRECTED-V1 | WP-BP-020, WP-CP-050 | local Markdown | precedence/property tests | evidence_v2/WP-BP-020 | planned |
| REQ-BP-GD-008 | Foundation async-subgoal decision | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-CP-ASYNC-SUBAGENT-V1 | WP-BP-020 | local Markdown | delegation classifier tests | evidence_v2/WP-BP-020 | planned |
| REQ-BP-GD-009 | Pre-research F3 fork boundary | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-DIRECTED-V1 | WP-BP-020 | local Markdown | protected-field/fork tests | evidence_v2/WP-BP-020 | planned |
| REQ-BP-GD-010 | Pre-research F3 terminality | ADR-0003 | SPEC-BP-GOAL-DIRECTED | CON-BP-GOAL-DIRECTED-V1 | WP-BP-020 | local Markdown | proposal/reducer tests | evidence_v2/WP-BP-020 | planned |

## Blueprint runtime qualification projection

`WP-BP-010` and `WP-BP-020` each require deterministic semantic suites plus a credential-gated,
narrow runtime-logic acceptance vertical. Each vertical starts at the BellLabs API, passes through
admission, `BellLabsRunWorkflow`, the real family workflow, and `OperationWorkflow`, and makes real
LLM calls through the accepted Deep Agents adapter. These tests qualify family mechanics; they are
not the complete production-shaped foundation vertical. `WP-BP-010` must capture branching,
incremental-release, obligation, and terminalization evidence. `WP-BP-020` must capture separate
executor/verifier operations, convergence or revision behavior, and terminalization evidence.

These blueprint-package verticals prove family runtime logic. `WP-CP-050` still owns the later
cohesive proof that combines MCP, Skills, sandboxes, snapshots, sync/async subagents, both
families, and recovery.

## Validation rule

Every atomic requirement is listed separately. A release script or later registry implementation
must reject duplicate ownership, missing work packages, or a requirement without at least one exact
test/evidence mapping.
