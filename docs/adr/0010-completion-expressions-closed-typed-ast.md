---
type: Decision Record
title: "Completion is computed from a closed typed expression, never from supplied code or prose"
description: "Completion contracts are a closed AST over all, any, schema and integrity checks, named assessment dispositions, human resolutions and criterion results. They cannot execute SQL, Python, JavaScript or natural-language…"
tags: [mission-control, adr, decision]
status: accepted
source: SPECIFICATION.md (Definition and compiler contracts)
---

# Completion is computed from a closed typed expression, never from supplied code or prose

Completion contracts are a closed AST over all, any, schema and integrity checks, named assessment dispositions, human resolutions and criterion results. They cannot execute SQL, Python, JavaScript or natural-language predicates; semantic judgment goes through a registered assessment capability and rubric that return a typed disposition. We chose this over expression languages because acceptance must be replayable, auditable and immune to prompt injection.
