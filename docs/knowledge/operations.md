---
type: Playbook
title: Trusted startup and process roles
description: How operators bind API workers and Agent Server without fabricating readiness.
tags: [mission-control, implementation]
---

# Trusted startup and process roles

Start the API at `mission_control.bootstrap.api:create_app`, worker at
`mission_control.bootstrap.worker`, and read-only preflight at
`mission_control.bootstrap.preflight`. The missionctl client calls the authenticated
application-scoped API. Detailed configuration lives in the operator guide outside
this bundle.

Operator deployment configuration names secret environment references, accepted
issuers/audiences, public verification keys, tenant grants and immutable bindings.
Token permissions and user-editable metadata do not create authority.
Installing the common release, its runtime phase and seeds are explicit
`mission-db` administrative actions with reviewed plans; startup never performs them.

Workers select one application and binding digest, verify actual persisted
installation identity and role separation, and then connect to the configured
Temporal namespace and queues. Runtime option factories are installed trusted
code; requests and skill bundles cannot select them.

Agent Server's canonical configuration is `agent_server/langgraph.json`. Its
graphs execute bounded subordinate cognition and qualification surfaces. Serving
a graph does not make it a mission scheduler or certify deployed production state.
The runtime, qualification and N1-only profiles share one authentication entrypoint;
profile checks also guard native thread run creation and exact assistant UUIDs.
Disallowed registered graphs return 403.

# Citations

- [API bootstrap](../../src/mission_control/bootstrap/api.py).
- [Worker bootstrap](../../src/mission_control/bootstrap/worker.py).
- [Common installation readiness](../../src/mission_control/bootstrap/common_installation.py).
- [Canonical Agent Server profile proofs](../../tests/integration/agent_server/test_canonical_server_local.py).
- Operator setup: `docs/MISSION_CONTROL_LOCAL_API.md`.
