status: ACCEPTED
prepared_at / actor / base revision / diff ref:
  2026-08-06 / Cursor implementing agent / d40d5862b06bb74789a349087112162f0a879094 /
  uncommitted prerequisite-closure diff

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
    Agent Server auth and custom-app entry points now use importable module paths so the Windows-built
    production-like Linux image does not retain host backslashes.
  exact tests and results:
    focused Block A and Stage 2 compatibility suite: 55 passed, two third-party warnings.
    full repository suite after closure: 462 passed, 24 explicitly optional/external skips,
    11 third-party warnings.
    uv run ruff check app tests: pass.
    uv run mypy app: pass (269 source files).
  result:
    pass. The v3 structural compiler intersects ERC authority, exact capability-manifest
    maturity/flags/readiness, resource and delegation constraints, and compatibility digests.
    Typed unavailable surfaces fail closed. Production coordinator composition requires a
    RuntimePlanPreparer and a frozen RunPlanV3; current business launch compositions deliberately
    use an unavailable preparer until governed per-stage runtime assets are published, so no alias,
    package discovery, or placeholder silently widens authority.

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
    real Mongo-to-PostgreSQL backfill count/digest, malformed-source quarantine, process exit after a
    committed batch, restart/resume from the durable cursor, PostgreSQL-first cutover reads, explicit
    legacy rollback-window fallback, and no source mutation/deletion: pass.
    The service-backed drill exposed and fixed non-serializable ClaimAdmission/SettlementAdmission
    values in the canonical batch digest.
  exact tests and results:
    TEST_APPLICATION_POSTGRES_DSN=<sanitized local disposable> uv run pytest -q
    tests/test_run_control_postgres_integration.py tests/test_operation_journal_stage1.py
    tests/test_artifact_promotion_postgres_integration.py
    6 passed
    TEST_MONGODB_URI=<sanitized local disposable replica-set> uv run pytest -q
    tests/test_operation_execution_mongodb_integration.py tests/test_artifact_promotion_mongodb_integration.py
    tests/test_control_plane_mongodb_integration.py
    3 passed
    BELL_LABS_REQUIRE_STAGE3_ENTRY_SERVICES=1
    TEST_APPLICATION_POSTGRES_DSN=<sanitized local disposable>
    TEST_MONGODB_URI=<sanitized local disposable replica-set> uv run pytest -q
    tests/test_operation_journal_backfill_integration.py
    1 passed
    combined prerequisite suite: 47 passed, 1 third-party deprecation warning
    uv run ruff check app tests: pass
    uv run mypy app: pass (258 source files)

runtime mechanics qualification:
  exact versions/server mode/persistence:
    langgraph=1.2.10; langgraph-checkpoint-postgres=3.1.1; langgraph-api=0.12.0;
    langgraph-sdk=0.4.2. Pinned licensed `langgraph up` images ran as separate Docker
    processes on ports 8133 (N) and 8134 (N+1), used distinct Compose/Redis projects, and shared only
    the isolated `belllabs_langgraph_stage3` PostgreSQL database. Qualification auth used an
    ephemeral RSA pair supplied only through process environment; no key/token value was persisted.
  restart/interrupt/parallel interrupt/concurrent strategy/fork evidence:
    pass, non-skipped. A PostgreSQL-backed interrupt survived `docker restart` of the API
    container and resumed once on the same thread with one stable claim. Single and parallel
    interrupt IDs, duplicate resume, active-run reject/enqueue behavior, strict thread copy with
    parent immutability, typed cancellation cleanup, tenant denial, and state/history/schema/graph
    introspection side-effect freedom passed.
  N/N+1 compatibility result:
    pass with a mandatory routing constraint. Direct same-server N-to-N+1 resume is provider
    fail-open and can change the visible checkpoint schema; BellLabs must never use it. In the
    accepted separate-deployment topology, N+1 can see the shared thread row but fails state
    inspection without the N graph. The guarded route invoked only the exact N endpoint/assistant,
    resumed successfully, and never called N+1. A disposable direct cross-deployment attempt was
    isolated and asserted as either fail-closed or explicit schema contamination.
  cancellation/tenant/introspection result:
    pass on the persistent server. Cancellation records `wait_status=cancelled` and
    `resource_open=false`; tenant B cannot access tenant A state/history/copy; repeated
    assistant/thread/run/state/history/Store introspection leaves counts and checkpoint digests
    unchanged.
  exact tests and results:
    core persistent mechanics: 11 passed, one restart-phase test deselected.
    restart prepare phase: 1 passed, 13 deselected; API container restarted; restart resume phase:
    1 passed, 13 deselected.
    separate N/N+1 deployment qualification: 2 passed, 12 deselected.
  executable provenance:
    N launch:
      COMPOSE_PROJECT_NAME=belllabs-block-c-qualification uv run langgraph up
      --config langgraph.block_c.json --postgres-uri "$BLOCK_C_POSTGRES_URI"
      --port 8133 --wait --no-pull
    N+1 launch:
      COMPOSE_PROJECT_NAME=belllabs-block-c-qualification-n1 uv run langgraph up
      --config langgraph.block_c_n1.json --postgres-uri "$BLOCK_C_POSTGRES_URI"
      --port 8134 --wait --no-pull
    core drill:
      BLOCK_C_RUN_NN1_PHASE=1 uv run pytest -q
      tests/test_agent_server_block_c_persistent.py
      -m "block_c_live and not block_c_restart"
    restart drill:
      BLOCK_C_RUN_RESTART_PHASE=prepare uv run pytest -q
      tests/test_agent_server_block_c_persistent.py -m block_c_restart;
      docker restart belllabs-block-c-qualification-langgraph-api-1;
      BLOCK_C_RUN_RESTART_PHASE=resume uv run pytest -q
      tests/test_agent_server_block_c_persistent.py -m block_c_restart
    blue/green drill:
      BLOCK_C_RUN_NN1_DEPLOYMENT=1 uv run pytest -q
      tests/test_agent_server_block_c_persistent.py -m
      "block_c_live and block_c_nn1_deploy"
    restart evidence:
      container=belllabs-block-c-qualification-langgraph-api-1;
      container_id=0366f720a948…; restarted_at=2026-08-06T22:23:38Z;
      thread=019fd92c…4363; run=019fd92c…7ba5;
      checkpoint=1f191e57…af07; assistant=8c5bb6b1…62eb.
      The post-restart resume passed with one stable claim and an idle terminal thread.
    cleanup:
      obsolete belllabs-stage3-qualification Redis was stopped. N and N+1 API/Redis
      containers remain healthy for immediate Stage 3 work; application PostgreSQL remains healthy.

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
  all required rows pass: yes
  remaining blocker, if any:
    none for Stage 3 entry. Governed business-specific per-stage runtime assets remain disabled and
    are not permission to begin StageGraph business scheduling or later-stage capabilities.
  exact Stage 3-compatible schema/endpoint/fixture instructions:
    begin Stage 3 using only GraphAssemblySpecV2/RunPlanV3. Mirror the qualification
    route-then-invoke guard in the authoritative dispatcher: every resume stays on its exact
    compatible endpoint/graph/assembly. Never submit an N checkpoint directly to an N+1 assistant.
    Qualification configs are `langgraph.block_c.json` and `langgraph.block_c_n1.json`; the tracked
    `langgraph.block_c.env` contains variable references only. Keep both Compose projects and the
    disposable PostgreSQL database isolated from application and production services.
