---
name: mission-control-intervene
description: Change a running Mission Control run with missionctl commands - queue an instruction or context for the next turn or iteration, interrupt and inject, pause, resume, cancel (normal or immediate), snapshot, fork from a snapshot with an instruction, or reconcile an in-doubt unit. Use when a human wants to steer, stop, branch or repair a run that is already running.
---

# Intervene in a run

A command is an admitted, ordered instruction to a run. Its lifecycle is `accepted → queued →
delivered → observed → completed`; its outcome is `applied | failed | rejected | expired`.
Admission is never delivery. Every command carries the `expected_version` and
`expected_generation` you read from inspection, a UUID `request_id`, and a `reason`.
Templates for every body are in [command-templates.md](references/command-templates.md).

## 1. Inspect first

`missionctl run inspect RUN_ID --json`. Copy `version` and `execution_generation`. Confirm the
lifecycle allows the command (a completed run accepts nothing but fork). Done when the template
has real values.

## 2. Choose the command

| Intent | Command | Delivered as |
| --- | --- | --- |
| Add guidance without stopping | `command queue` (`kind: queue_instruction`), `boundary: next_turn \| next_iteration` | `turn_boundary_guaranteed` (Deep Agents) or `wait_then_send` (Cursor); consumed once into the next Context Packet. Available (FT-F1) |
| Add a document or artifact to context | `command queue --add-context` (`kind: add_context`) with a short note or an artifact ref | same. Available (FT-F1) |
| Stop the current turn and redirect | `command inject` with `kind: interrupt_and_inject` | `cooperative_inject` where native, otherwise `cancel_and_replace` after uncertain effects settle. Availability: FT-F2 |
| Stop releasing new work | `command send` with `kind: pause` | quiescence at the next safe boundary; the run parks |
| Continue | `command send` with `kind: resume` (optionally with an instruction) | never new authority or budget |
| Stop | `command cancel --urgency normal` | settlement after children, effects and usage reconcile |
| Stop now | `command cancel --urgency immediate` | a Stop Fence is persisted first, then provider cancel; no new effects, no promise that dispatched tools halt. Availability: FT-F3 |
| Satisfy a declared wait | `command send` with `kind: satisfy_wait` and the evidence the wait requires | boundary application |

```text
missionctl command send RUN_ID --request-file command.json --json
missionctl command queue RUN_ID --file queue.json --json
missionctl command queue RUN_ID --file note.md --add-context --boundary next_iteration --json
missionctl command inject RUN_ID --file inject.json --json
missionctl command cancel RUN_ID --urgency immediate --reason "…" --json
```

`command queue` reads the run's `version` and `execution_generation` itself and sends a
fresh `request_id` (or the file's). Its `--file` is either JSON (`text` or `content`,
`boundary`, `node_key`, `expand`, `deadline`, `reason`, `request_id`) or plain text that
becomes the instruction. Inline text is capped at 8 KiB (`content_too_large`, exit 2); put
larger content in an artifact and send its ref. Without a `node_key` a Goal Loop delivers to
the next executor turn (the verifier stays independent); StageGraph delivers to the next
admitted stage. A cancel admitted first expires the entry (`superseded`), and a Generation
that moved on expires it (`stale_generation`).

## 3. Confirm

`missionctl command list RUN_ID --json` shows receipts; `run inspect` shows the effect. A
queued command reads `accepted, queued`, then `delivered` when a boundary takes it into a
packet, `observed` when the carrying turn starts, and `applied` (or `failed`) when that turn
settles; `command.delivered` and `command.completed` mission events carry the Delivery Report.
A command is done when its receipt is `applied` and the Delivery Report names the semantics the
lane used. Report `requested` and `delivered` separately; `emulated` carries a note.

## 4. Branch

```text
missionctl run snapshot RUN_ID --request-file snapshot.json --json
missionctl run fork RUN_ID --from-snapshot SNAPSHOT_ID --instruction-file note.json --json
```

A snapshot captures a safe boundary and fails elsewhere. A fork admits a new run seeded from the
snapshot's Context Packet (workspace tier restores files); it never clones in-flight commands or
active children and never launches by itself. Start the fork with `run start`. Availability of
the `--from-snapshot` and `--instruction-file` flags: FT-F4 (today `run fork --request-file`).

## 5. Repair

`missionctl run reconcile RUN_ID --request-file reconciliation.json --json` names an accepted
checkpoint for an in-doubt unit. It needs a separate grant; it is never a retry.

## Retry rule

If a write's response is lost, resend the identical body with the same `request_id`; a 200 is
the replay, a 409 means the run moved and you must inspect again. Never invent a new request id
to escape uncertainty.

## Exit codes

0 success; 2 invalid request; 3 denied; 4 version or idempotency conflict (inspect and rebuild
the body); 5 unavailable; 6 wait timeout or unsuccessful completed execution.
