# Pure rules and decisions

programs contains family interpretation; execution owns operation/run invariants;
authoring owns immutable definitions and compilation; composition owns delegated
run contracts. Policies remain explicit admitted inputs.

Domain imports neither Temporal, database clients, FastAPI nor model/provider SDKs.
Do not turn interpreter proposals into authoritative lifecycle changes. Preserve
determinism and immutable content identity across retries and forks.

Start with the relevant tests/unit/orchestration, run_control, operations or
control_plane tests. Use docs/knowledge/execution.md for the request-to-settlement path.
