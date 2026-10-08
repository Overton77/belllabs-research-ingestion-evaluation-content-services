# Transcript format

`missionctl run transcript RUN_ID --format jsonl` emits one `mc.transcript_entry.v1` per line,
ordered by mission event `seq` and, inside a turn, by provider frame arrival ordinal.

```json
{"schema_version": "mc.transcript_entry.v1", "cursor": "000123:0007", "at": "2026-10-07T18:02:11Z",
 "source": "frame", "run_id": "…", "node_key": "collect", "activation_id": "…", "attempt_no": 1,
 "session_ref": "agent-…", "turn_ref": "run-…", "kind": "tool_call.completed",
 "summary": "tavily_search(query=\"NMN muscle\") → 10 results", "args_digest": "sha256:…",
 "result_digest": "sha256:…", "excerpt": "…first 2 KB…", "artifact_refs": [], "native_event_ref": "frame:…"}
```

| Field | Meaning |
| --- | --- |
| `cursor` | Opaque position; pass to `--since` to continue |
| `source` | `event` (a mission event), `frame` (a provider frame), `artifact` (a registration) |
| `kind` | Mission event type (`activation.phase_changed`, `session.turn_completed`, `command.delivered`, `human_task.opened`, …) or frame kind (`assistant.text`, `tool_call.started`, `tool_call.completed`, `hook.invoked`, `usage`, `summary`, `status`) |
| `summary` | One line, redacted, never chain-of-thought |
| `excerpt` | Size-capped body for frames; full bodies that matter are artifacts |
| `artifact_refs` | Registered outputs at this point |
| `native_event_ref` | Pointer into the Native Event Store for audit |

`--format md` renders the same entries as a document: one heading per activation, one
subsection per turn, tool calls as a list with digests, mission events as callouts, artifacts as
links to `missionctl artifact get`.

Retention: frames expire per application policy (30 days by default); mission events do not.
An expired frame renders as its mission event with `excerpt: null`.
