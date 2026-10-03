# Concrete adapters

postgres implements scoped repositories and immutable persistence; temporal
implements deterministic workflows/activities and client transport; deep_agents
materializes bounded cognition; agent_server hosts subordinate graphs;
capabilities and supabase_storage handle admitted bundles and object custody.
auth/jwt.py verifies public identity; storage owns payload I/O; realtime owns
PostgreSQL/Redis event transport; operations owns concrete execution ports. There
is no general infrastructure catchall for new implementations.

Adapters implement application ports. They cannot invent completion semantics,
privileged grants or cross-application routing. Scoped command delivery is separate
from command admission and application. Preserve exact immutable binding receipts.

Run real PostgreSQL/Temporal tests for durability claims. SDK calls must match
pinned official source. No paid provider experiment without finite authorization.
Application database migrations are under postgres/migrations; runtime startup
never applies them.
