# Ledger (machine-local workspace)

Everything in this directory except this README is gitignored. It is the skill's working
memory on *this machine*: what was done, when, with what evidence, and the raw snapshots the
audit script produced. Tracked knowledge belongs in `../references/`.

## Files

| Path | Purpose | Tracked |
| --- | --- | --- |
| `README.md` | this format note | yes |
| `progress.md` | dated entries, newest first | no |
| `snapshots/<UTC timestamp>.json` | output of `scripts/audit_local_setup.py` | no |
| `notes/` | scratch investigations worth keeping locally | no |

## Entry format for `progress.md`

```markdown
## 2026-10-07  tooling refresh

**Changed:** one line per change (file or target).
**Verified:** passed / failed / blocked / unrun, each listed separately with the command.
**Residue:** what is still open because of this work.
**Next:** the single most useful next step.
```

Rules: no secret values, no DSNs, no tokens; reference variable names only. Record blocked
checks as blocked, never as passed. When an entry changes the described state, update
`references/current-state.md` and `references/recommendations.md` in the same session.
