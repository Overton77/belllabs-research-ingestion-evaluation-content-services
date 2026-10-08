---
name: mission-control-observe
description: Inspect Mission Control runs and chains, list and search runs, read a run's transcript, watch mission events and register subscriptions (webhook or stream) with missionctl. Use when a human asks what a run is doing, whether a control took effect, what an agent did, why a mission stopped, or wants callbacks when a human task opens or a run completes.
---

# Observe a run

Everything you can read comes from one sequenced stream of mission events plus the Native
Event Store of provider frames. Reads never change execution. Three fields describe any
record's state and you report them separately: `lifecycle` (where it is), `phase` (what it is
doing), `terminal_outcome` (how it ended; absent until completion).

## 1. Inspect

```text
missionctl run inspect RUN_ID --json
missionctl run inspect RUN_ID --wait 60 --json     # block until the next change, max 3600
```

Read `version` and `execution_generation` before authoring any command. The inspection carries
the run projection (waits, pauses, readiness, outputs, async children, reconciliation incidents)
and, after FT-F6, the lane profile, agent sessions and turns, the command mailbox and delivery
reports. Done when you can say lifecycle, phase and outcome in one sentence each.

## 2. List and search runs

```text
missionctl run list --query 'mc_lane = "cursor_local" AND mc_phase = "running"' --json
missionctl run search RUN_ID --query "pytest failed" --json
```

Availability: FT-C4. `run list` is a Temporal visibility query over the typed search attributes
`mc_mission_id`, `mc_run_id`, `mc_lane`, `mc_application_id`, `mc_phase`,
`mc_forked_from_run_id`; `run search` is a text search over a run's
transcript index and returns transcript cursors you can pass to `--since`.

## 3. Transcript

```text
missionctl run transcript RUN_ID --format md             # for a human
missionctl run transcript RUN_ID --format jsonl --since CURSOR   # for an agent
```

Availability: FT-C3. The transcript is a materialized, read-only join of mission events,
provider frames and artifact references in order; see
[transcript-format.md](references/transcript-format.md). It is evidence, never state: a tool
call in the transcript proves the agent attempted it; the effect ledger proves it landed.

## 4. Chains

```text
missionctl chain inspect CHAIN_ID --json
```

Availability: FT-D2. Shows each mission in the chain, each link's state (`armed | released |
blocked | detached | cancelled`), the acceptance condition it waits for and the Context Packet digest it
released with.

## 5. Watch and subscribe

```text
missionctl events watch MISSION_ID --after-seq 0
missionctl subscribe --mission MISSION_ID --events human_task.opened,run.completed \n  --webhook https://… --secret-ref WEBHOOK_SECRET_REF
missionctl subscribe --run RUN_ID --events run.completed --webhook https://… --secret-ref REF
missionctl subscribe list
missionctl subscribe close SUBSCRIPTION_ID
```

Availability: FT-F5. Events replay from `after_seq` then follow live; dedupe on `event_id`,
detect gaps on `seq`, and on `CURSOR_EXPIRED` resync from inspection rather than skipping.
Webhook deliveries are at least once and HMAC-signed with a secret the deployment resolves from
`--secret-ref` (never pass the secret itself); a subscription is a durable row you can list and
close. Stream consumers use `events watch` (SSE); an MCP session uses `mission_subscribe`. Done when the subscription id is recorded with the mission.

## Reading a Delivery Report

Every command shows `requested` and `delivered` semantics separately:
`turn_boundary_guaranteed`, `cooperative_inject`, `cancel_and_replace`, `wait_then_send`,
`pause_at_tool_gate`, `emulated` (with a note naming the emulation), `unsupported`. A report of
`delivered` means the lane acknowledged; `applied` means the reducer recorded the effect;
cancellation is settled only when children, effects and usage are reconciled.

## Boundaries

- Transcripts are redacted by policy; secrets never appear, tool bodies above the cap appear as
  digest plus excerpt.
- Child mission events do not flow into the parent stream; open the child's stream within grant.
- Usage marked `estimated` is not billing; `settled` is.
