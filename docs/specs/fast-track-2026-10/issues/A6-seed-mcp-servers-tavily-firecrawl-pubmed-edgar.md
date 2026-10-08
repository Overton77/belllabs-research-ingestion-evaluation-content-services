# [FT-A6] Seed MCP servers: Tavily, Firecrawl, PubMed, EDGAR

Linear: OVE-27

**Epic:** Capabilities (SPEC-01)
**Team:** T1
**Blocked by:** FT-A1
**Status:** ready-for-agent

**What to build:** After `mission-db seed-apply`, both application catalogs contain admitted `mcp_server` rows (with `mcp_tool` children) for Tavily, Firecrawl and agent-browser, Biotech additionally has PubMed (cyanheads) and BioMCP, and AI Engineer additionally has edgartools, each with exact package pins, secret references by name, the full tool list, a recorded `tools/list` digest and a `host_support` matrix. `missionctl catalog list --json` shows them; `make seeds-validate` proves every pin parses and every digest matches its fixture. No key is stored anywhere.

**Spec sections:** SPEC-01 "Seeds" (table and rejections), "The five kinds" (`mcp_server` body), "Discovery stays quarantined".

**Writable regions:** `packages/mission-control-db-contract/seeds/common/mc.catalog.agent-capabilities-1.0.0.json`, `packages/mission-control-db-contract/seeds/biotech/mc.app.biotech.agent-capabilities-1.0.0.json`, `packages/mission-control-db-contract/seeds/ai-engineer/mc.app.ai-engineer.agent-capabilities-1.0.0.json`, `tests/fixtures/mcp/*.tools.json`, `scripts/seeds_validate.py`; shared: `Makefile` (`seeds-validate`).

**Acceptance criteria:**
- [ ] `mcp.tavily` (`npm:tavily-mcp@0.2.22`, remote `https://mcp.tavily.com/mcp/` Bearer; `TAVILY_API_KEY`; tools `tavily_search`, `tavily_extract`, `tavily_crawl`, `tavily_map`, `tavily_research`) admitted in both applications; `tavily_feedback` absent (not in the published build).
- [ ] `mcp.firecrawl` (`npm:firecrawl-mcp@3.28.2`, remote `https://mcp.firecrawl.dev/v2/mcp` Bearer, env `FIRECRAWL_NO_SEARCH_FEEDBACK=1` and `FIRECRAWL_NO_ENDPOINT_FEEDBACK=1`; `FIRECRAWL_API_KEY`) with the SPEC-01 tool list and `tool_allowlist` excluding `firecrawl_extract`; both applications.
- [ ] `mcp.agent-browser` (`npm:agent-browser@0.38.2`, `agent-browser mcp --tools core`, no secrets, Environment Profile requirement `agent-browser install`) with `cursor_cloud` marked `unqualified`; both applications.
- [ ] `mcp.pubmed` (`npm:@cyanheads/pubmed-mcp-server@2.10.20`, `MCP_TRANSPORT_TYPE=stdio`, `NCBI_API_KEY`, config `NCBI_ADMIN_EMAIL`, `UNPAYWALL_EMAIL`; 11 tools) and `mcp.biomcp` (`pypi:biomcp-cli==0.9.1` with wheel sha256, `biomcp serve`; 7 read-only tools; optional secret refs listed) admitted in Biotech only; the author-hosted public endpoint is not referenced.
- [ ] `mcp.edgartools` (`uvx --from "edgartools[ai]==5.61.1" edgartools-mcp`; `EDGAR_IDENTITY`; 13 tools) admitted in AI Engineer only.
- [ ] Each server has `tests/fixtures/mcp/<id>.tools.json` captured from `tools/list` (recorded once, with the capture date and version) and the row's `tools_list_digest` equals the fixture digest (`make seeds-validate`).
- [ ] Seed bundles apply and replay with receipts on both disposable targets (`tests/qualification/two_project` extended); `tests/unit/control_plane/test_catalog_seed_bundles.py` covers the new bundles.
- [ ] A seed notes file records the rejected candidates and why (`sec-edgar-mcp` AGPL and unauthenticated HTTP, `pubmedmcp` stale, unscoped `pubmed-mcp-server`, key-in-URL forms).

**Verification:** `make check`; `make seeds-validate`; `uv run --group biotech pytest tests/unit/control_plane/test_catalog_seed_bundles.py -q`; `uv run mission-db seed-apply --target <disposable-biotech>` and `--target <disposable-ai-engineer>` then `mission-db qualify`.

**Notes:** `TAVILY_API_KEY` and `FIRECRAWL_API_KEY` are set; `NCBI_API_KEY` and `EDGAR_IDENTITY` are not: ask the owner by Linear comment before a live EDGAR smoke test, run PubMed keyless. One real `tools/list` per seeded server is permitted to record the tool digest. Capturing `tools/list` requires running each server once locally (Node ≥ 24 for the cyanheads server, Node ≥ 20 for Tavily and Firecrawl, `uv` for edgartools and BioMCP); keyless modes suffice for Tavily and Firecrawl captures. NCBI: 3 req/s without a key, 10 with one. SEC: 10 req/s and a declared identity. Confirm (UNVERIFIED) that `uvx --from "edgartools[ai]==5.61.1"` resolves the extra as expected and record the wheel sha256. Never place a real key in a seed, fixture or test.
