# Supported public protocol

All endpoints are under `/v1/applications/{application_id}` and require deployment
authentication. A changed application path never grants another application's scope.

| Operation | CLI | HTTP |
| --- | --- | --- |
| Admit run | `run admit --request-file FILE` | `POST /run-requests` |
| Launch admitted run | `run start ID --request-file FILE` | `POST /runs/ID/launch` |
| Inspect | `run inspect ID` | `GET /runs/ID/inspection` |
| Send control | `command send ID --request-file FILE` | `POST /runs/ID/commands` |
| Observe controls | `command list ID` | `GET /runs/ID/commands` |
| Safe snapshot | `run snapshot ID --request-file FILE` | `POST /runs/ID/snapshots` |
| Semantic fork | `run fork ID --request-file FILE` | `POST /runs/ID/forks` |
| Privileged reconciliation | `run reconcile ID --request-file FILE` | `POST /runs/ID/reconcile-unit` |

Common `--application`, `--url`, and `--json` flags work at every command depth.
`--wait SECONDS` belongs to run inspection and requires 0 < seconds <= 3600.

Admission uses `mc.runtime_admission.v1` with a request UUID, exact configuration
digest, workflow type/input references, budget, and granted sponsorship/approval
references. The response contains `application_id` and `admission`; obtain `run_id`
from the accepted admission. Start uses `mc.runtime_launch.v1`, `family` of
`StageGraph` or `GoalDirected`, and exactly the matching typed family input.
The server verifies it against admitted authority. Obtain complete schemas from
OpenAPI; a committed definition alone does not start a run.

Commands have `schema_version: mc.command.v1`, a UUID `request_id`,
`expected_version`, `expected_generation`, `target: {kind: run, id: RUN_ID}`,
`kind`, typed `payload`, and `reason`. Use API OpenAPI schemas for exact pause,
resume, and wait evidence requirements. A normal cancellation payload is:

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

Replace the example identity, version and generation from inspection; create a new
request ID for a new action. Preserve the full original body on retry. When a write
response is lost, retry only that identical request; never invent a new request ID
to work around uncertainty. Request lookup is not provided by this parity slice.

Exit codes: 0 successful read/admission or completed execution; 2 invalid request;
3 denied; 4 version/idempotency conflict; 5 unavailable; 6 wait timeout or completed
execution with unsuccessful outcome. JSON receipt and error bodies are preserved.
Cancellation is asynchronous and does not promise provider interruption.

Snapshots use `mc.runtime_snapshot.v1` and `expected_version`; they fail if the
source is not at a declared safe boundary. Semantic forks use `mc.runtime_fork.v1`,
`request_id`, exact `snapshot_id` and `snapshot_digest`, typed `changes`,
`invalidation_frontier`, `sponsorship_ref`, authorized `approval_refs`, and `reason`.
Inspect OpenAPI for exact fields. They preserve the inherited runtime fork contract,
not the broader generalized mission-fork specification. Privileged reconciliation
uses `mc.unit_reconciliation.v1`, `request_id`, `expected_version`, a typed
`ReconcileUnitAction`, and `reason`; only separately granted operators may use it.
Snapshot responses wrap the immutable manifest under `snapshot`; reconciliation
responses wrap the inherited ledger result under `result`. Scope metadata never
changes the digested snapshot content.

Technical retry would create a new execution attempt; diagnostic replay would
inspect recorded history without repeating business effects. Neither is implemented
by this CLI slice. Fork creates a distinct run from authorized source state without
launching it, copying authority, or mutating the source history.
