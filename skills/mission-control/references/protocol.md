# Supported public protocol

All endpoints are under `/v1/applications/{application_id}` and require deployment
authentication. A changed application path never grants another application's scope.
Common `--application`, `--url`, and `--json` flags work at every command depth.
`--wait SECONDS` belongs to run inspection and requires 0 < seconds <= 3600.

Rows marked `Availability: FT-xx` are specified by the fast-track packet
(`docs/specs/fast-track-2026-10`) and ship with that ticket; until the ticket is `Done` the
command returns an error rather than a partial result. Rows without the marker exist today.

## Runs and commands

| Operation | CLI | HTTP | Availability |
| --- | --- | --- | --- |
| Admit run | `run admit --request-file FILE` | `POST /run-requests` | |
| Launch admitted run | `run start ID --request-file FILE` | `POST /runs/ID/launch` | |
| Inspect | `run inspect ID [--wait S]` | `GET /runs/ID/inspection` | enriched in FT-F6 |
| List and query runs | `run list --query 'mc_lane="cursor_local" AND mc_phase="running"'` | `GET /runs?query=` | FT-C4 |
| Transcript | `run transcript ID --format jsonl\|md [--since CURSOR]` | `GET /runs/ID/transcript` | FT-C3 |
| Search a run | `run search ID --query TEXT` | `GET /runs/ID/transcript/search?q=` | FT-C4 |
| Send control | `command send ID --request-file FILE` | `POST /runs/ID/commands` | kinds added in FT-F1, FT-F2, FT-F3 |
| Queue instruction or context | `command queue ID --file FILE` | same endpoint, `kind: queue_instruction \| add_context` | FT-F1 |
| Interrupt and inject | `command inject ID --file FILE` | same endpoint, `kind: interrupt_and_inject` | FT-F2 |
| Cancel | `command cancel ID --urgency normal\|immediate --reason TEXT` | same endpoint, `kind: cancel` | immediate in FT-F3 |
| Observe controls | `command list ID` | `GET /runs/ID/commands` | |
| Safe snapshot | `run snapshot ID --request-file FILE` | `POST /runs/ID/snapshots` | |
| Semantic fork | `run fork ID --request-file FILE` or `run fork ID --from-snapshot SNAPSHOT_ID --instruction-file FILE` | `POST /runs/ID/forks` | flags in FT-F4 |
| Privileged reconciliation | `run reconcile ID --request-file FILE` | `POST /runs/ID/reconcile-unit` | |

## Missions and chains

| Operation | CLI | HTTP | Availability |
| --- | --- | --- | --- |
| Compile a manifest | `mission compile FILE --json` | `POST /missions:compile` | FT-E2 |
| Submit (commit revision, admit run or chain) | `mission submit FILE --json` | `POST /missions:submit` | FT-E3 |
| Start the admitted run | `mission start RUN_ID --json` | `POST /missions/MISSION_ID/runs` (alias for the head revision's admitted run) | FT-E3 |
| Inspect a chain | `chain inspect CHAIN_ID --json` | `GET /chains/CHAIN_ID` | FT-D2 |

## Events and subscriptions

| Operation | CLI | HTTP | Availability |
| --- | --- | --- | --- |
| Watch events | `events watch MISSION_ID --after-seq N` | `GET /missions/MISSION_ID/events?after_seq=` (SSE) | FT-F5 |
| Subscribe | `subscribe --run RUN_ID \| --mission MISSION_ID --events TYPES --webhook URL --secret-ref REF` | `POST /subscriptions` (channels `webhook`, `stream_ticket`, `mcp_session`) | FT-F5 |
| List and close subscriptions | `subscribe list`, `subscribe close ID` | `GET /subscriptions`, `DELETE /subscriptions/ID` | FT-F5 |

## Catalog

| Operation | CLI | HTTP | Availability |
| --- | --- | --- | --- |
| List definitions | `catalog list` | `GET /catalog/definitions` | |
| Resolve exact ref | `catalog resolve --request-file FILE` | `POST /catalog/resolve` | |
| Search | `catalog search --request-file FILE` or `catalog search --query TEXT --kind KIND --host HOST --limit N --json` | `POST /catalog/search` | flags in FT-A8 |
| Discover (quarantined) | `catalog discover --request-file FILE` | `POST /catalog/discover` | |
| Inspect candidate | `catalog inspect --request-file FILE` | `POST /catalog/inspect` | |
| Components | `catalog components --request-file FILE` | `POST /catalog/components/search` | |
| Pin | `catalog pin --query TEXT --kind KIND --host HOST --json` (one pin or `AMBIGUOUS_CAPABILITY`); exact ref today via `catalog resolve` | `GET /catalog/pins/PIN` | FT-A8 |
| Inspect pin, preview projection | `catalog inspect --pin PIN`, `catalog render --pin PIN --host HOST` | `GET /catalog/pins/PIN` | FT-A8 |
| Publish a bundle | `catalog publish --dir PATH --kind skill_bundle --json` | `POST /catalog/publish` | FT-A2 |

## Request conventions

Admission uses `mc.runtime_admission.v1` with a request UUID, exact configuration digest,
workflow type/input references, budget, and granted sponsorship/approval references. The
response contains `application_id` and `admission`; obtain `run_id` from the accepted admission.
Start uses `mc.runtime_launch.v1`, `family` of `StageGraph` or `GoalDirected`, and exactly the
matching typed family input. A committed definition alone does not start a run.

Commands have `schema_version: mc.command.v1`, a UUID `request_id`, `expected_version`,
`expected_generation`, `target: {kind: run, id: RUN_ID}`, `kind`, typed `payload`, and `reason`.
A normal cancellation payload is:

```json
{
  "schema_version": "mc.command.v1",
  "request_id": "e158f328-2ad1-4c24-a52c-21dc4ef2a160",
  "expected_version": 1,
  "expected_generation": 1,
  "target": {"kind": "run", "id": "REPLACE_WITH_INSPECTED_RUN"},
  "kind": "cancel",
  "payload": {"urgency": "normal"},
  "reason": "Operator requested cancellation"
}
```

Replace the identity, version and generation from inspection; create a new request ID for a
new action. Preserve the full original body on retry. When a write response is lost, retry
only that identical request; never invent a new request ID to work around uncertainty.

Every command response carries a Delivery Report naming what the lane did:
`turn_boundary_guaranteed | cooperative_inject | cancel_and_replace | wait_then_send |
pause_at_tool_gate | emulated | unsupported`. Requested and delivered semantics are shown
separately; cancellation is asynchronous and does not promise provider interruption.

Exit codes: 0 successful read/admission or completed execution; 2 invalid request;
3 denied; 4 version/idempotency conflict; 5 unavailable; 6 wait timeout or completed
execution with unsuccessful outcome. JSON receipt and error bodies are preserved.

Snapshots use `mc.runtime_snapshot.v1` and `expected_version`; they fail if the source is not
at a declared safe boundary. Semantic forks use `mc.runtime_fork.v1`, `request_id`, exact
`snapshot_id` and `snapshot_digest`, typed `changes`, `invalidation_frontier`,
`sponsorship_ref`, authorized `approval_refs`, and `reason`. A fork admits a distinct derived
run from authorized source state without launching it, copying authority, cloning in-flight
commands or mutating the source history. Privileged reconciliation uses
`mc.unit_reconciliation.v1`, `request_id`, `expected_version`, a typed `ReconcileUnitAction`,
and `reason`; only separately granted operators may use it.

Technical retry creates a new execution attempt; diagnostic replay inspects recorded history
without repeating business effects. Neither is a CLI command.
