# [FT-A8] Catalog CLI and MCP parity for kinds, pins and plugin composition

Linear: OVE-29

**Epic:** Capabilities (SPEC-01)
**Team:** T1
**Blocked by:** FT-A3, FT-A4
**Status:** ready-for-agent

**What to build:** A coordinator (human on `missionctl`, or an agent through the coordinator MCP server) searches with kind and lane filters, resolves one query to exactly one Capability Pin, inspects a pin to see its body, host support and plugin members, and previews what a pin will render for a given lane profile, all with identical results across CLI, HTTP and MCP. `missionctl catalog pin --query "pubmed" --kind mcp_server --host deep_agents` prints `mcp.pubmed@2.10.20#sha256:...` or a typed ambiguity error listing the candidates.

**Spec sections:** SPEC-01 "Interfaces", "Contracts" (search additions, pin string), "Host projection" (preview), "Discovery stays quarantined".

**Writable regions:** `src/mission_control/application/capabilities/catalog.py`, `src/mission_control/interfaces/http/catalog.py`, `src/mission_control/interfaces/mcp/coordinator_server.py` (catalog tools only), `src/mission_control/interfaces/mcp/coordinator_resources.py`, `tests/unit/coordinator/test_coordinator_mcp_read_surface.py`; shared: `src/mission_control/interfaces/cli/main.py` (`catalog` group), `skills/mission-control-catalog/` references (text only, with H1's owner).

**Acceptance criteria:**
- [ ] `missionctl catalog search --query TEXT [--kind K]... [--host P]... [--limit N] --json` works without a request file and prints `search_mode`, and per hit `pin`, `kind`, `host_support`, `rank_provenance`; the request-file form still works.
- [ ] `missionctl catalog pin --query --kind --host` returns one pin when the top hit's fused score exceeds the second by the configured margin, otherwise exit 2 with a typed `AMBIGUOUS_CAPABILITY` error listing candidates; HTTP `GET /catalog/pins/{pin}` returns the row.
- [ ] `missionctl catalog inspect --pin PIN` shows body, host support per profile with overlays, secret ref names (never values), plugin members with their pins and roles.
- [ ] `missionctl catalog render --pin PIN --host P` prints the projected files (paths and bytes) from A4 for preview; secrets render as references.
- [ ] MCP tools `search_capabilities` (gains `kinds`, `host_profiles`), `get_capability` (host support, members) and new `pin_capability` call the same application handlers; resources `belllabs://catalog/{kind}/{id}/{rev}` serve the new kinds; `discover_*` results remain candidate-only and never appear in `search_capabilities`.
- [ ] Parity test: for ten fixture requests CLI, HTTP and MCP return identical hit sets and pins.
- [ ] Exit codes follow the existing scheme (0 success, 2 invalid or ambiguous, 3 denied, 5 unavailable); errors use the common envelope.

**Verification:** `make check`; `uv run --group biotech pytest tests/unit/coordinator tests/unit/capability/test_catalog_cli.py -q`; `uv run uvicorn mission_control.bootstrap.api:create_app --factory` then `uv run missionctl catalog search --query "web search" --kind mcp_server --host cursor_local --json` and `uv run missionctl catalog pin --query "sec filings" --kind mcp_server --host deep_agents`.

**Notes:** `catalog discover` and `catalog inspect` of external candidates keep their separate grants; this ticket does not change admission or promotion. The `mission-control-catalog` skill (H1) documents these commands with `Availability: FT-A8` lines; update that line to shipped in the same branch only if H1 is already `Done`, otherwise leave a note in the handoff for the integrator.
