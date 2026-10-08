# [FT-C3] Transcript materialization on CLI, HTTP and MCP

Linear: OVE-36

**Epic:** Mission state (SPEC-03)
**Team:** T2
**Blocked by:** FT-C2
**Status:** ready-for-agent

**What to build:** A run's Transcript as a materialized read view. `application/frames/transcript.py::materialize(run_id, since, filters)` streams a merge of mission events (by `seq`), frames (by `arrival_ordinal`, placed under their governing event) and artifact registrations into `mc.transcript_entry.v1` entries with an opaque monotonic `cursor`; `to_jsonl` and `to_markdown` renderers apply redaction again and never inline referenced bodies; `--since` resumes, `--follow` tails, and a cursor older than retention yields `frame_expired` placeholders rather than an error. Exposed as `missionctl run transcript`, `GET .../runs/{id}/transcript`, an MCP resource and tool, plus the non-canonical `GET .../runs/{id}/frames/tail` SSE for live text. Demo: `missionctl run transcript RUN_ID --format md` on C1's integration run prints activations, turns, tool calls with digests, artifacts and usage in order; `--format jsonl --since <cursor>` returns only newer entries.

**Spec sections:** SPEC-03 §Contracts (`mc.transcript_entry.v1`), §Implementation Decisions (Transcript materialization, Non-canonical live tail), §Interfaces

**Writable regions:** `src/mission_control/application/frames/transcript.py`, `src/mission_control/domain/frames/render.py`, `src/mission_control/interfaces/http/transcript.py`, `src/mission_control/interfaces/mcp/coordinator_resources.py` and `coordinator_server.py` (read-only transcript resource and tool only), `tests/unit/frames/`, `tests/integration/postgres/`; shared (integrator): `interfaces/cli/main.py` (`run transcript`, `run frames --tail`), `bootstrap/api.py` (include router)

**Acceptance criteria:**
- [ ] `TranscriptEntry` model matches SPEC-03; `cursor` encodes `(governing mission seq, arrival ordinal)` and round-trips; entries are strictly increasing by cursor.
- [ ] Merge ordering test: interleaved events and frames render in the specified order; artifact registrations appear with digest and media type; human task resolutions and commands render with `role: human` / `mission_control`; canonical entries are marked.
- [ ] `since` beyond retention returns canonical entries plus `frame_expired` placeholders carrying the digest; a malformed cursor returns `CURSOR_EXPIRED` (HTTP 409, CLI exit 2).
- [ ] Redaction is applied at render time as well as write time; `--full` fetches referenced bodies only through the artifact API under the caller's grant; no secret pattern appears in golden outputs (test scans).
- [ ] Markdown golden: heading per activation, sub-heading per turn, `HH:MM:SS · role · title` lines, collapsed tool blocks with digest and excerpt, `mc://` artifact links, `✓` for canonical, `·` for frames. JSONL golden: one canonical JSON object per line.
- [ ] Filters `--activation`, `--node`, `--kinds`, `--canonical-only`, `--subordinate`, `--limit` work on CLI and HTTP; `--follow` streams new entries and exits 6 on wait timeout.
- [ ] HTTP `GET /v1/applications/{app}/runs/{run_id}/transcript` streams JSONL or returns Markdown on `Accept: text/markdown`; scope and `mission.read` enforced; `GET .../frames/tail` is SSE labelled `canonical: false`.
- [ ] MCP resource `mc://applications/{app}/runs/{run_id}/transcript` and tool `mission_run_transcript` return the same entries; they are annotated read-only.
- [ ] Integration test on the C1 run: Markdown and JSONL outputs match goldens modulo ids and timestamps; `make check` passes.

**Verification:** `make check`; `uv run pytest tests/unit/frames -k transcript -q`; `MISSION_CONTROL_TEST_ADMIN_DSN=... uv run --group biotech pytest -m common_db tests/integration/postgres -k transcript -q`; `uv run missionctl run transcript <RUN_ID> --format md`

**Notes:** The transcript is a view, not a table; do not persist rendered entries (the search projection in C4 is separate). Keep the renderers pure in `domain/frames/render.py`. `MissionInspection.sessions[]` enrichment is F6 (T5); expose a `summarize_sessions(run_id)` helper here that F6 can call. The coordinator skill `mission-control-observe` (SPEC-08) documents these commands; update its `Availability` line in your handoff comment, not in the skill file (T1 owns `skills/`).
