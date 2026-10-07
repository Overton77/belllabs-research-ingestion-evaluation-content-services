---
type: Decision Record
title: "Claude Code, Codex and Cursor are execution lanes behind the one harness protocol"
description: "The canonical pack models only Cursor SDK Cloud as a coding lane and treats Claude Code and Codex as coordinator hosts. The owner's intent is that all three are controllable by Mission Control as lanes implementing the…"
tags: [mission-control, adr, decision]
status: proposed
source: interview 2026-10-07 (Q7); restores intent from the historical MISSION_CONTROL_PRESPEC.md section 10.3 and MISSION_CONTROL_SPEC.md sections 10.5 and 17
---

# Claude Code, Codex and Cursor are execution lanes behind the one harness protocol

The canonical pack models only Cursor SDK Cloud as a coding lane and treats Claude Code and Codex as coordinator hosts. The owner's intent is that all three are controllable by Mission Control as lanes implementing the same harness protocol (prepare, start, send_turn, cancel_turn, observe, snapshot, usage, end_session), with Deep Agents parity qualified first, then the Claude Agent SDK lane, then Cursor Cloud, then Codex. Each lane's command delivery semantics (queue at turn boundary, emulated pause and fork) are declared per profile, never assumed. Tentative: the CLI versus SDK surface for Claude Code and Codex, and the hook-based trace path, are open until the next interview round.

## Considered options

Keep Claude Code and Codex as hosts only (the pack's current reading). Rejected: it forbids the coding missions the owner runs daily.
