---
type: Workflow
title: Run admission and lifecycle
description: How authenticated scope, immutable admission and durable command effects connect.
tags: [mission-control, implementation]
---

# Run admission and lifecycle

The API authenticates a signed identity and resolves a trusted installation,
application and tenant binding. A route cannot select another pool or grant itself
permissions. The bound service's request scope must match the resolved identity.

Admission injects actor and scope from trusted context, validates immutable ERC
and workflow references, and delegates to the existing run-control reducer.
Launch verifies the admitted family and exact input binding before Temporal start.
Repeated starts verify the actual first history event and immutable root payload.

Commands carry a stable request ID, target, expected version and generation.
Permission checks precede replay lookup. Exact repeats replay the receipt; changed
content conflicts. The reducer commits accepted commands and the delivery ledger;
Temporal later receives and applies them at supported boundaries.

Accepted, delivered and applied are different facts. Pause/resume/satisfy-wait
follow persisted boundary semantics; normal-urgency cancellation delegates to the
existing runtime contract. Unsupported generic retry, instruction injection and expanded immediate
semantics fail explicitly rather than pretending to enqueue supported work.

Inspection reports execution lifecycle and outcome separately. A completed
execution is not a new proof of the general architecture's mission acceptance.

# Citations

- [Mission lifecycle facade](../../src/mission_control/application/missions/service.py).
- [Admission wrapper](../../src/mission_control/application/missions/admission.py).
- [Run reducer service](../../src/mission_control/application/execution/service.py).
- [Boundary relay](../../src/mission_control/application/execution/boundary_relay.py).
- [PostgreSQL HTTP lifecycle proof](../../tests/integration/postgres/test_mission_control_lifecycle_postgres.py).
