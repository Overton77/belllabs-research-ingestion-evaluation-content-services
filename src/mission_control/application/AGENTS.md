# Application logic

missions/service.py maps public controls to execution/service.py and boundary
delivery. missions/admission.py injects trusted admission fields and uses
execution/run_launch.py. missions/runtime.py delegates snapshots, forks and
reconciliation to recovery services.

authoring compiles definitions; programs coordinates family repositories;
execution owns lifecycle/journal/effects; subordinates coordinates delegated
submission and settlement; capabilities resolves admitted artifacts and tools.
installations/registry.py resolves immutable application bindings.

ports/payloads owns neutral content addresses and payload storage interfaces.
capabilities/catalog.py binds definition/search/discovery/inspection services;
external candidates remain unadmitted until governed registration.

Ports and framework-neutral logic belong here; concrete SDK clients and SQL
implementations belong in adapters. Composition belongs in bootstrap. Never choose
a provider fallback or install a permissive policy because a binding is missing.

Key proof: tests/unit/run_control/test_mission_control_*.py plus
tests/integration/postgres/test_mission_control_lifecycle_postgres.py and
test_mission_control_composition_postgres.py.
