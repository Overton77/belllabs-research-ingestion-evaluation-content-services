---
type: Architecture Reference
title: Source ownership
description: How the general runtime separates contracts, decisions, adapters and application integrations.
tags: [mission-control, implementation]
---

# Source ownership

One distribution imports `mission_control` from `src/mission_control`. Transport
enters through interfaces, application handlers use domain rules and ports, concrete
adapters implement those ports, and bootstrap supplies trusted dependencies.

| Package | Reason to change it |
| --- | --- |
| contracts | Public request/receipt shapes, canonical bytes, resource identity |
| domain | Pure program interpretation, admission/lifecycle meaning and invariants |
| application | Orchestrate use cases, transitions, subordinate settlement and recovery |
| adapters | SQL, Temporal, Deep Agents, Agent Server, storage and provider mechanics |
| interfaces | Parse/authorize transport requests and return typed outcomes |
| bootstrap | Bind installation configuration to restricted pools and process roles |

A new program is not a new scheduler. Both existing families use the same governed
operation execution, budgets and settlement. An application-specific capability
runs behind an admitted interface; the general kernel cannot import Biotech
implementation. The optional `integrations/biotech/` package is a transition home,
not completion of the deferred Knowledge Services generalization audit.

Do not move application logic into HTTP dependencies or provider callbacks.
Definitions and compiled bindings are immutable; runtime state belongs to scoped
repositories. See [lifecycle](lifecycle.md) and [persistence](persistence.md).

# Citations

- [Composition](../../src/mission_control/bootstrap/composition.py).
- [Scoped mission service](../../src/mission_control/application/missions/service.py).
- [Public contracts](../../src/mission_control/contracts/contracts.py).
- Accepted package direction: `../mission-control-general/general-mission-control/CODEBASE-ORGANIZATION.md`.
