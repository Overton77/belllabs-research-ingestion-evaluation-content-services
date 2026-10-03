# Public contracts

contracts.py defines scoped command/inspection envelopes; admission_contracts.py
defines typed admission/launch requests; runtime_contracts.py defines snapshot,
fork and reconciliation requests. canonical.py defines deterministic digests;
identities.py defines installation/application/tenant scoped runtime identities.

Changing canonical bytes changes idempotency and immutable pins. Reject ambiguous
Unicode keys, nonfinite values, illegal scope and payload-kind mismatches.
A contract request cannot inject trusted actor, pool or installation authority.

Evidence: tests/unit/run_control/test_mission_control_canonical.py,
test_mission_control_facade.py and tests/unit/mission_control/test_temporal_identities.py.
