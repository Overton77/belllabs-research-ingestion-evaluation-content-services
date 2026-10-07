# Documentation ownership

Start at README.md. Keep accepted external architecture separate from local
implementation evidence and blocked production gates. Update the operator guide
when entrypoints/configuration change and REMOVAL_GUIDE.md when removing behavior.

knowledge/ is an Open Knowledge Format bundle. Every concept has YAML type,
title and description; index.md is navigation and log.md is dated history.
Do not put AGENTS.md inside knowledge/: it would become a concept. Use one concept
per file and cite actual source/tests. Do not label explanations accepted specs.

Historical subtrees retain provenance only. Preserve prior user deletions, private
scratch and untracked files. Do not silently rewrite historical proof as current proof.

Validate knowledge/ with `python docs/tools/validate_okf.py` (repo-aware links are allowed
to the glossary, ADRs and the sibling spec pack). After adding or renaming any document,
run `python docs/tools/agents_docs_index.py` so the compressed index in the root AGENTS.md
stays current; `--check` verifies it. Search everything with `python docs/tools/okf_search.py`.
