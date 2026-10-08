# [FT-I4] Knowledge bundle, implementation status, AGENTS.md index and glossary reconciliation

Linear: OVE-62

**Epic:** Missions (00-ARCHITECTURE §1, missions/)
**Team:** Integrator
**Blocked by:** FT-I1, FT-I2, FT-I3
**Status:** ready-for-agent

**What to build:** The documentation catches up with the delivered code so the next session reasons from retrieval, not memory. Update the OKF concepts that now describe built behaviour (`capabilities`, `context-and-continuation`, `lanes-and-harness`, `events-and-commands`, `recovery`, `interfaces`), add `docs/knowledge/cursor-lane.md` and `docs/knowledge/mission-chains.md`, each separating implemented (cited code and tests) from specified-only (cited spec); refresh `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md` with the three mission acceptance results and remaining gates; reconcile `GLOSSARY.md` terms against the code's names; set ADR-0018 and ADR-0019 to `accepted` and note ADR-0013 as extended by ADR-0025; regenerate the compressed index in `AGENTS.md`; append `docs/knowledge/log.md`.

**Spec sections:** 00-ARCHITECTURE §11 (precedence); `docs/agents/domain.md`; `docs/knowledge/index.md` conventions; ADR-0021 (OKF plus compressed index).

**Writable regions:** `docs/knowledge/`, `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md`, `GLOSSARY.md`, `docs/adr/` (status fields only), `AGENTS.md` (generated index block only), `docs/specs/fast-track-2026-10/README.md` (status line).

**Acceptance criteria:**
- [ ] Every new or changed concept has frontmatter (`type`, `title`, `description`), an "Implemented" and a "Specified only" section where applicable, and a `# Citations` section pointing at code, tests and spec.
- [ ] `python docs/tools/validate_okf.py` passes; `python docs/tools/agents_docs_index.py` regenerates the `AGENTS.md` block and the diff contains only the generated region.
- [ ] `docs/knowledge/index.md` lists `cursor-lane.md` and `mission-chains.md` under the right task rows.
- [ ] Implementation status records passed, failed, blocked and unrun checks for I1, I2 and I3 separately, and lists remaining live gates (buckets, paid qualifications, Supabase roles).
- [ ] `GLOSSARY.md` terms match the names used in code and specs; any drift is fixed on the wrong side and noted in the log.
- [ ] ADR statuses updated; no ADR body rewritten.
- [ ] `python docs/tools/okf_search.py "cursor lane describe"` ranks the new concept in the top three.
- [ ] `make check` passes.

**Verification:** `python docs/tools/validate_okf.py`; `python docs/tools/agents_docs_index.py`; `python docs/tools/okf_search.py "mission chain link" --limit 6`; `make check`.

**Notes:** Do not edit the spec pack in `../mission-control-general`; record any conflict in `docs/knowledge/log.md` and as a Linear comment. No paid calls.
