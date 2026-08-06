status: REWORK_REQUIRED
prepared_at / actor / base revision / diff ref:
  2026-08-06 / Cursor implementing agent / 6e49ef1e49670c626956bfe0a9b1e65699dd279b / uncommitted dirty worktree

contract amendment:
  schema versions and digests:
    legacy-readable: belllabs.graph-assembly-spec.v1 and belllabs.run-plan.v2
    amended: belllabs.graph-assembly-spec.v2, belllabs.operation-assembly.v2, belllabs.run-plan.v3
    exported schema inventory digest: sha256:8ce3a973da4995799c71ff07194ec9311506d0e8d8c4b3c7aaf7a9abe295b3a3
  compatibility/migration result:
    v1/v2 contracts remain unchanged and readable; v2/v3 are additive, versioned contracts that remove
    global runtime defaults in favor of per-stage bindings.
  Stage 2 graph/introspection impact:
    stable graph IDs, nodes, state channels, and reducers are unchanged; the side-effect-free schema export
    now includes v2 stage requirement, assembly, binding, resource, lineage, graph assembly, and RunPlan schemas.
  exact tests and results:
    uv run pytest -q tests/test_graph_runtime_contracts.py tests/test_graph_runtime_dispatch.py tests/test_agent_server_stage2.py
    30 passed, 1 warning
    uv run ruff check app/domain/graph_runtime/definitions.py app/application/runtime_run_plan.py app/api/graph_runtime_schemas.py app/domain/graph_runtime/governance.py tests/test_graph_runtime_contracts.py
    pass
  result:
    partial; structural compiler coverage, exact-ref/digest drift, and disabled-surface prediction are tested.
    Authority/maturity intersection and complete v2 RunPlan preparation integration remain unproven.

database authority proof:
  sanitized disposable topology:
    PostgreSQL: Docker Compose application-postgres at 127.0.0.1:55432 / belllabs / belllabs role.
    MongoDB: Docker Compose application-mongodb replica set rs0 at 127.0.0.1:27017;
    per-test databases are UUID-suffixed. Primary Supabase and primary Atlas were not targeted.
  migrations tested:
    clean-apply and second idempotent apply of 0012_graph_runtime_operation_journal.sql and
    0013_legacy_operation_journal_backfill.sql on disposable PostgreSQL: pass.
  RLS/grant/tenant evidence:
    disposable PostgreSQL cross-tenant runtime-role query denial/pass partition: pass in
    tests/test_run_control_postgres_integration.py.
  crash/idempotency/backfill/quarantine/rollback results:
    claim/attempt/settlement idempotency, injected pre-commit rollback, and lifecycle version conflict: pass.
    immutable Mongo bindings and claim/settlement records: pass.
    real Mongo-to-PostgreSQL backfill count/digest, malformed-source quarantine, process crash/resume,
    and rollback-window read-routing: not yet exercised across both disposable stores.
  exact tests and results:
    TEST_APPLICATION_POSTGRES_DSN=<sanitized local disposable> uv run pytest -q
    tests/test_run_control_postgres_integration.py tests/test_operation_journal_stage1.py
    tests/test_artifact_promotion_postgres_integration.py
    6 passed
    TEST_MONGODB_URI=<sanitized local disposable replica-set> uv run pytest -q
    tests/test_operation_execution_mongodb_integration.py tests/test_artifact_promotion_mongodb_integration.py
    tests/test_control_plane_mongodb_integration.py
    3 passed

runtime mechanics qualification:
  exact versions/server mode/persistence:
    langgraph=1.2.10; langgraph-checkpoint-postgres=3.1.1; langgraph-api=0.12.0;
    langgraph-runtime-inmem=0.32.0. Existing Stage 2 evidence covers Agent Server in-memory mode only.
  restart/interrupt/parallel interrupt/concurrent strategy/fork evidence:
    not qualified on an async PostgreSQL saver through a real separate server-process restart.
  N/N+1 compatibility result:
    not qualified; no blue/green persistent-checkpoint fixture exists.
  cancellation/tenant/introspection result:
    Stage 2 in-memory tenant and introspection tests pass; persistent-server cancellation/cleanup remains unqualified.

accepted Stage 3 policy decisions:
  decision table with actor/date:
    2026-08-06 / work-package recommended default / active-run intervention -> reject; typed enqueue only when declared
    2026-08-06 / work-package recommended default / interrupt response -> persist BellLabs decision before same-thread resume
    2026-08-06 / work-package recommended default / arbitrary state update -> deny except audited privileged Overwrite
    2026-08-06 / work-package recommended default / retry versus fork -> technical retry retains identity; fork creates new identity
    2026-08-06 / work-package recommended default / epoch rollover -> disabled without exact compatible policy
    2026-08-06 / work-package recommended default / cancellation -> cooperative cascade; linked authoritative runs need accepted command
    2026-08-06 / work-package recommended default / durable waits -> release worker; retain only declared leases
    2026-08-06 / work-package recommended default / event reconnect -> BellLabs outbox durable; Agent Server cursor transient
    2026-08-06 / work-package recommended default / checkpoint visibility -> redacted by default; debug restricted to operator
    2026-08-06 / work-package recommended default / reconciliation -> automatic only for idempotent version-checked repairs
    2026-08-06 / owner via Cursor / interim retention -> retain checkpoint, event, incident, and lineage records
    for 90 days; delete only through an audited, tenant-scoped operational job
    2026-08-06 / work-package recommended default / blue-green -> running epochs stay on exact compatible endpoint/assembly

deferred non-entry blockers:
  live LangSmith trace/evaluators -> Stage 7 -> tracing remains disabled by default
  MCP/Store/sandbox/Deep Agents/async subagents/QuickJS -> Stages 5-6 -> corresponding features remain disabled
  production deployment/cutover -> Stages 7-8 -> no production routing enabled

Stage 3 entry:
  all required rows pass: no
  remaining blocker, if any:
    Block B lacks real two-store Mongo-to-PostgreSQL backfill, malformed-source quarantine, crash/resume,
    and rollback-routing evidence; Block C lacks the required persistent async-PostgreSQL-saver, real
    process-restart, interrupt/concurrency/fork, compatibility, cancellation, and tenant-isolation qualification.
  exact Stage 3-compatible schema/endpoint/fixture instructions:
    do not start Stage 3. Use only GraphAssemblySpecV2/RunPlanV3 after the missing Block B/C evidence is
    completed on the documented disposable Compose PostgreSQL and MongoDB targets.
