# Command templates (`mc.command.v1`)

Replace every `REPLACE_*` value from `missionctl run inspect RUN_ID --json`. Generate a fresh
UUID per new action; keep the same UUID when retrying a lost write. Exact payload schemas come
from OpenAPI; these are the shapes.

## queue_instruction (FT-F1)

```json
{
  "schema_version": "mc.command.v1",
  "request_id": "REPLACE_UUID",
  "expected_version": 3,
  "expected_generation": 1,
  "target": {"kind": "run", "id": "REPLACE_RUN_ID"},
  "kind": "queue_instruction",
  "payload": {"content_ref": "artifact://…/note.md", "content_digest": "sha256:…", "boundary": "next_turn"},
  "reason": "Owner asked to prefer primary literature"
}
```

`boundary` is `next_turn` or `next_iteration`. The instruction becomes an `inline` item of the
next Context Packet and is consumed once.

## add_context (FT-F1)

```json
{
  "schema_version": "mc.command.v1",
  "request_id": "REPLACE_UUID",
  "expected_version": 3,
  "expected_generation": 1,
  "target": {"kind": "run", "id": "REPLACE_RUN_ID"},
  "kind": "add_context",
  "payload": {"artifact_ref": "artifact://…/review-notes.md", "content_digest": "sha256:…",
              "expand": "materialize", "boundary": "next_iteration"},
  "reason": "Reviewer notes from the first pass"
}
```

## interrupt_and_inject (FT-F2)

```json
{
  "schema_version": "mc.command.v1",
  "request_id": "REPLACE_UUID",
  "expected_version": 3,
  "expected_generation": 1,
  "target": {"kind": "run", "id": "REPLACE_RUN_ID"},
  "kind": "interrupt_and_inject",
  "payload": {"content_ref": "artifact://…/redirect.md", "content_digest": "sha256:…",
              "accept_emulated": true},
  "reason": "Wrong repository branch; redirect to release/2.3"
}
```

`accept_emulated: false` rejects the command when the lane can only `cancel_and_replace`.

## pause / resume

```json
{"schema_version": "mc.command.v1", "request_id": "REPLACE_UUID", "expected_version": 3,
 "expected_generation": 1, "target": {"kind": "run", "id": "REPLACE_RUN_ID"},
 "kind": "pause", "payload": {}, "reason": "Hold while the budget is reviewed"}
```

```json
{"schema_version": "mc.command.v1", "request_id": "REPLACE_UUID", "expected_version": 4,
 "expected_generation": 1, "target": {"kind": "run", "id": "REPLACE_RUN_ID"},
 "kind": "resume", "payload": {"instruction_ref": null}, "reason": "Budget approved"}
```

## cancel

```json
{"schema_version": "mc.command.v1", "request_id": "REPLACE_UUID", "expected_version": 3,
 "expected_generation": 1, "target": {"kind": "run", "id": "REPLACE_RUN_ID"},
 "kind": "cancel", "payload": {"urgency": "normal"}, "reason": "Operator requested cancellation"}
```

`"urgency": "immediate"` (FT-F3) persists a Stop Fence before the provider cancel; the
receipt reports `fence_persisted_at` and, later, `settled_at`.

## satisfy_wait

```json
{"schema_version": "mc.command.v1", "request_id": "REPLACE_UUID", "expected_version": 3,
 "expected_generation": 1, "target": {"kind": "run", "id": "REPLACE_RUN_ID"},
 "kind": "satisfy_wait", "payload": {"wait_id": "REPLACE_WAIT_ID", "evidence_refs": ["artifact://…"]},
 "reason": "External approval recorded"}
```

## snapshot (`mc.runtime_snapshot.v1`)

```json
{"schema_version": "mc.runtime_snapshot.v1", "request_id": "REPLACE_UUID", "expected_version": 3,
 "reason": "Branch point before the synthesis rewrite"}
```

## fork (`mc.runtime_fork.v1`; FT-F4 adds the CLI flags)

```json
{"schema_version": "mc.runtime_fork.v1", "request_id": "REPLACE_UUID",
 "snapshot_id": "REPLACE_SNAPSHOT_ID", "snapshot_digest": "sha256:…",
 "changes": [{"kind": "queue_instruction", "content_ref": "artifact://…/note.md", "content_digest": "sha256:…"}],
 "invalidation_frontier": [], "sponsorship_ref": "REPLACE_SPONSORSHIP", "approval_refs": [],
 "reason": "Try the alternative extraction schema"}
```

## reconcile (`mc.unit_reconciliation.v1`)

```json
{"schema_version": "mc.unit_reconciliation.v1", "request_id": "REPLACE_UUID", "expected_version": 3,
 "action": {"kind": "accept_descendant", "unit_key": "REPLACE_UNIT", "checkpoint_key": "REPLACE_CHECKPOINT"},
 "reason": "Operator verified the descendant checkpoint after the worker crash"}
```
