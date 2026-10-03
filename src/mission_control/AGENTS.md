# General runtime navigation

Read root AGENTS.md and docs/knowledge/index.md before broad changes.
contracts owns public envelopes; domain owns meaning; application owns use cases;
adapters owns concrete I/O; interfaces owns transport; bootstrap owns composition.

A request becomes an admitted immutable binding, then a scoped Temporal root,
family decisions, bounded operations and durable settlements. Interpreters propose;
reducers authorize and terminalize. Keep provider objects outside domain.
Application-specific integrations belong outside this package.

Source-relative files are runtime code; experiments/ at repository root is not
shipped as the production kernel. Check tests/unit for contracts and reducers,
tests/integration for stores/runtime and tests/acceptance/mission_control for
authenticated end-to-end proofs.
