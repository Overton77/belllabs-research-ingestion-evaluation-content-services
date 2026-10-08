---
type: Decision Record
title: Coding lanes qualify as worker-hosted local processes first; cloud placement is a second profile per lane
description: "The first profile of each coding lane runs on a Mission Control worker as a managed process with a leased workspace, because that can be qualified today without new accounts and keeps secrets server-side. Cloud…"
tags: [mission-control, adr, decision]
status: accepted
source: interview 2026-10-07 (Q8)
supplement: accepted for Cursor by ADR-0030 (2026-10-07)
---

# Coding lanes qualify as worker-hosted local processes first; cloud placement is a second profile per lane

The first profile of each coding lane runs on a Mission Control worker as a managed process with a leased workspace, because that can be qualified today without new accounts and keeps secrets server-side. Cloud placement (Cursor Cloud, Claude Code cloud sessions, Codex cloud) is a separate profile with its own workspace, credential and control-latency declaration. This reverses the pack's "cloud placement first" for Cursor; the Cursor Cloud profile stays required for the complete release.
