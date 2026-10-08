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

The fast-track packet adds process behavior, configured by environment names only (the list is
in the operator guide). The API process runs the subscription relay only when
`MISSION_CONTROL_SUBSCRIPTION_RELAY=1` ([events and commands](events-and-commands.md)). The
worker registers Cursor lane profiles only when `CURSOR_API_KEY` is bound, refuses unqualified
lanes unless `MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES=true`, serves the Kernel Hook callback on
`127.0.0.1` (`MISSION_CONTROL_HOOK_CALLBACK_PORT`), serves `mc.frames_expire.v1` on its
maintenance queue with a daily Schedule (`MISSION_CONTROL_FRAMES_EXPIRE_SCHEDULE`), and polls as
a Worker Deployment version (`TEMPORAL_WORKER_VERSIONING`, `TEMPORAL_DEPLOYMENT_NAME`,
`TEMPORAL_BUILD_ID`, `TEMPORAL_PROMOTE_ON_START`) on `temporalio` 1.34
([cursor lane](cursor-lane.md), `adapters/temporal/versioning.py`). The worker can launch only
capabilities in its pin file (`CAPABILITY_PINS_PATH`), and a drifted pinned skill stops startup
with `CapabilityPinError`. A live mission start is still blocked
([authoring](authoring.md)).

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
- [Settings](../../src/mission_control/bootstrap/settings.py),
  [subscription composition](../../src/mission_control/bootstrap/subscriptions.py),
  [Worker Deployment versioning](../../src/mission_control/adapters/temporal/versioning.py),
  [worker maintenance tests](../../tests/unit/mission_control/test_worker_maintenance.py),
  [versioning tests](../../tests/integration/temporal/test_worker_versioning.py).
- [Canonical Agent Server profile proofs](../../tests/integration/agent_server/test_canonical_server_local.py).
- Operator setup: `docs/MISSION_CONTROL_LOCAL_API.md`.
