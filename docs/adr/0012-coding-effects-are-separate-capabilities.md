---
type: Decision Record
title: "Repository checkout, patch, test, review, push, merge and deploy are separate capabilities"
description: "A coding mission binds a repository, base commit, allowed paths, tests, egress and publication policy; starting it authorizes none of push, pull request, merge or deploy, each of which is its own admitted capability…"
tags: [mission-control, adr, decision]
status: accepted
source: SPECIFICATION.md (Final contract and scope amendments), RUNTIME-CONTRACTS.md (coding workspace binding), WORKED-MISSION.md
---

# Repository checkout, patch, test, review, push, merge and deploy are separate capabilities

A coding mission binds a repository, base commit, allowed paths, tests, egress and publication policy; starting it authorizes none of push, pull request, merge or deploy, each of which is its own admitted capability with its own side-effect class and review. We chose this over a single "coding agent" capability so that a lane (Cursor, Claude Agent SDK, Codex) can run with the same contract as research work and so that irreversible publication is always a reviewed step.
