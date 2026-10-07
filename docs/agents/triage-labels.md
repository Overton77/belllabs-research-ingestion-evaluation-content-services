---
type: Agent Configuration
title: Triage Labels
description: The skills speak in terms of canonical triage roles. This file maps those roles to the label strings used in the Linear workspace overtonbell.
tags: [mission-control, agents, process]
---
# Triage Labels

The skills speak in terms of canonical triage roles. This file maps those roles to the label strings used in the Linear workspace `overtonbell`.

| Role in mattpocock/skills | Label in Linear   | Meaning                                  |
| ------------------------- | ----------------- | ---------------------------------------- |
| `needs-triage`            | `needs-triage`    | Maintainer needs to evaluate this issue  |
| `needs-info`              | `needs-info`      | Waiting on reporter for more information |
| `ready-for-agent`         | `ready-for-agent` | Fully specified, ready for an AFK agent  |
| `ready-for-human`         | `ready-for-human` | Requires human implementation            |
| `wontfix`                 | `wontfix`         | Will not be actioned                     |
| `bug`                     | `Bug`             | Something is broken                      |
| `enhancement`             | `Feature`         | New feature or improvement               |

The five state labels are workspace labels created on 2026-10-07. `Bug` and `Feature` already existed and are reused instead of creating duplicates. `Improvement` also exists; treat it as a synonym of `Feature` when reading, and do not apply it.

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use the corresponding label string from this table.
