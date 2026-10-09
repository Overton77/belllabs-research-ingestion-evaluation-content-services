---
type: Specification
title: "Owner requirement coverage and completion boundaries"
description: "Traceability from the pasted brief and hosted-only clarification to contracts, implementation issues and executable acceptance scenarios."
tags: [mission-control, requirements, coverage]
---

# Requirement coverage

All references below describe the target implementation. The architecture/planning work is complete; runtime parity is not claimed by this packet.

| Owner requirement | Specification | Issues | Proof |
| --- | --- | --- | --- |
| Stage Graph on every provider, local and provider-hosted | SPEC-01 runtime | MP-02/07/08/09/18/19/20/21 | V02/V14/V20 |
| GoalDirected and linked GoalDirected missions | SPEC-01 runtime | MP-02/12/20/21 | V12/V15/V20 |
| Queue, abrupt cancel and intervention | SPEC-01 runtime | MP-06/07/08/09/18/19 | V04–08/V21 |
| Separate goal iteration from context compaction | SPEC-01 runtime | MP-12 | V12/V13/V22 |
| Transfer context, artifacts and workspace state | SPEC-01 and SPEC-02 | MP-03/04/12/20/21 | V03/V13–15 |
| First-class workflow approval/review/reject with feedback | SPEC-03 human control | MP-10/20/21 | V09 |
| Provider tool approvals and MCP human input | SPEC-03 human control | MP-07/08/09/11/18/19 | V06/V10/V11 |
| Skills, plugins, MCP servers, subagents and executable hooks | SPEC-02 environments | MP-01/03/07/08/09/18/19 | V01/V10/V16/V20 |
| Explicit repo/branch, cloud setup and environment config | SPEC-02 environments | MP-03/04/16–19/22 | V01/V03/V20 |
| Explicit managed worktrees | SPEC-02 environments | MP-04 | V03/V14 |
| Coordinator inspection and callbacks | SPEC-04 realtime | MP-13/14/15 | V16–19 |
| Socket.IO routes/handlers with subagent events | SPEC-04 realtime | MP-13/14/15 | V16–18 |
| Provider-specific events plus a common useful model | SPEC-04 realtime | MP-13 | V08/V16 |
| Local Temporal fallback | VALIDATION | MP-22 | V22/V23 |
| Clean contracts and YAML; modular adapters | ARCHITECTURE and SPEC-02 | MP-01/03/23 | G0/G1 and V01 |
| Make subscription-aware execution choices | SPEC-02 authentication | MP-05 | V24 |
| Specs, issues and implementation-team workspace | README, ISSUES and TEAM-WORKSPACE | Planning delivery | DELIVERY record |

Cloud is restricted to the providers' hosted coding products by the owner's explicit clarification. Self-hosted remote SDK workers, unrelated managed-agent APIs and private desktop tools are excluded as substitutions. GitHub repository tooling is reused; a new repository hosting/PR product is not scope.

“Emit” is treated as observation/event publication in SPEC-01; the existing intervention verbs remain the state-changing contract. A future distinct signal-injection requirement would need its own typed admitted command.

Parallel Swarm and Evaluator Optimizer are deferred. Independent mission concurrency remains allowed under existing governors, so a coordinator can compose multiple workflows without waiting for those future program behaviors.

## Completion boundary

MP-20 can prove the local/Cursor baseline. Full requested all-provider parity additionally requires MP-21 and MP-23. If MP-16 or MP-17 finds a missing vendor operation, retain the hosted implementation/release blocker and specify it exactly. Do not collapse the requirement to “can launch a cloud task,” and do not silently remove the hosted requirement to finish the backlog.
