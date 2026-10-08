---
type: Research Note
title: "Seed capabilities and open formats fact sheet (read 2026-10-07)"
description: "Tavily, Firecrawl, agent-browser, PubMed (cyanheads server and BioMCP) and edgartools MCP servers with tools, env vars and limits; Agent Skills, Claude Code plugin and hooks, Codex hooks, MCP config shapes per host, Supabase Storage limits for immutable bundles, and the Postgres hybrid search pattern; ends with a seed manifest proposal."
tags: [mission-control, research, fast-track, seeds]
---

# Seed capabilities and open formats — primary-source fact sheet

Read date for every source below: **2026-10-07** (registry calls made the same day). Claims not confirmed against a primary source are marked **UNVERIFIED**. Citation keys `[Cn]` resolve in the Citations section.

Terminology follows `mission-control/GLOSSARY.md` (Catalog, Capability, Capability Kind, Capability Pin, MCP Server, Skill Bundle, Plugin, Hook Script, Lane, Host).

---

## 0. Version snapshot (registries, 2026-10-07)

| Package | Registry | Latest | Published | License | Source |
|---|---|---|---|---|---|
| `tavily-mcp` | npm | 0.2.22 | 2026-08-05 | MIT | [C1] |
| `firecrawl-mcp` | npm | 3.28.2 | 2026-10-06 | MIT | [C1] |
| `agent-browser` | npm | 0.38.2 | 2026-10-01 | Apache-2.0 | [C1] |
| `@cyanheads/pubmed-mcp-server` | npm | 2.10.20 | 2026-10-04 | Apache-2.0 | [C1] |
| `biomcp-cli` (GenomOncology BioMCP) | PyPI | 0.9.1 | 2026-10-02 | MIT | [C2] |
| `edgartools` | PyPI | 5.61.1 | 2026-10-06 | MIT, Python >=3.10 | [C2] |
| `sec-edgar-mcp` | PyPI | 1.1.0 | 2026-08-20 | AGPL-3.0, Python >=3.11 | [C2] |
| `skills` (Vercel Labs CLI) | npm | 1.7.1 | 2026-10-06 | MIT | [C1] |
| `supabase` (supabase-py) | PyPI | 2.32.0 | — | MIT | [C2] |
| `deepagents` | PyPI | 0.7.23 | 2026-10-07 | — | [C2] |
| pgvector | GitHub tags | v0.8.7 | — | PostgreSQL | [C40] |

Name traps found:
- PyPI `biomcp` (0.0.1) is **unrelated**; the real package is `biomcp-cli` [C11].
- npm `biomcp` (1.5.0) is a different project (`yeyuan98/biomcp-ts`) [C1].
- npm `pubmed-mcp-server` (unscoped, 1.0.0, last modified 2025-09-27) is not cyanheads' server; use the scoped `@cyanheads/pubmed-mcp-server` [C1].
- npm `mcp-server-pubmed` does not exist (registry 404) [C1].

---

# PART A — MCP servers to seed

## 1. Tavily MCP

| Item | Fact | Src |
|---|---|---|
| Repo / package | `github.com/tavily-ai/tavily-mcp`, npm `tavily-mcp` 0.2.22, MIT. Repo pushed 2026-10-05. | [C3][C1] |
| stdio run | `npx -y tavily-mcp@latest` (Node >= 20). | [C3] |
| Remote HTTP | `https://mcp.tavily.com/mcp/` — auth by `?tavilyApiKey=<key>` query param, **or** `Authorization: Bearer <key>` header, **or** OAuth (`https://mcp.tavily.com/mcp` without key; MCP OAuth flow with metadata discovery + dynamic client registration). | [C3] |
| Claude Code | `claude mcp add --transport http tavily https://mcp.tavily.com/mcp/?tavilyApiKey=<key>` or OAuth via `/mcp`. | [C3] |
| Env vars | `TAVILY_API_KEY` (required for full tool set); `DEFAULT_PARAMETERS` (JSON defaults for search, e.g. `{"include_images":true,"max_results":15,"search_depth":"advanced"}`); `TAVILY_HUMAN_ID` (optional, forwarded as `X-Human-Id`, hashed server-side). The remote server also accepts a `DEFAULT_PARAMETERS` **header**. | [C3][C4] |
| Keyless mode | If `TAVILY_API_KEY` is unset the server runs "keyless": search and extract work; other tools return an "API key required" message. | [C4] |
| OAuth key selection | After OAuth, a key named `mcp_auth_default` (personal over team) is used, else `default`, else the first key. | [C3] |

**Tools (published 0.2.22, verified in `build/index.js`)** [C4]:

| Tool | Purpose |
|---|---|
| `tavily_search` | Web search; `search_depth` basic/advanced/fast/ultra-fast, `topic`, `time_range`, `start_date`/`end_date`, `max_results`, images, raw content, include/exclude domains, `country`, favicon, exact-match. |
| `tavily_extract` | Extract page content (markdown/text) from a URL list; `extract_depth` advanced for protected sites; optional `query` to rerank chunks. |
| `tavily_crawl` | Crawl from a root URL with `max_depth`, `max_breadth`, `limit`, natural-language `instructions`, path/domain regex selectors. |
| `tavily_map` | Return a site's URL structure from a base URL. |
| `tavily_research` | Multi-source research task (`input`, `model` = mini/pro/auto). Tool description states "Rate limit: 20 requests per minute." |

- Repo `main` additionally defines `tavily_feedback` (relevance feedback for a request/session). **Not in published 0.2.22** — pin accordingly [C4].
- Tool names use underscores (`tavily_search`), not hyphens as in the brief.
- Notable limits: `tavily_research` 20 req/min (tool text). Account credit limits are plan-dependent — **UNVERIFIED** here.
- Security note: the README's primary remote example puts the API key in the URL query string. For Mission Control prefer the `Authorization: Bearer` header form (keeps secrets out of URLs and logs).

## 2. Firecrawl MCP

| Item | Fact | Src |
|---|---|---|
| Repo / package | `github.com/firecrawl/firecrawl-mcp-server`, npm `firecrawl-mcp` 3.28.2, MIT. Docker `ghcr.io/firecrawl/firecrawl-mcp-server` (tags `latest`, `v<ver>` (moving), `sha-<commit>`; README: pin `sha-` or a digest). | [C5][C1] |
| stdio run | `env FIRECRAWL_API_KEY=fc-... npx -y firecrawl-mcp`. Windows: `cmd /c "set FIRECRAWL_API_KEY=... && npx -y firecrawl-mcp"`. | [C5] |
| Local HTTP | `HTTP_STREAMABLE_SERVER=true` (Docker image sets `HOST=0.0.0.0`, `PORT=3000`); `SSE_LOCAL=true` for SSE. | [C5] |
| Hosted HTTP | `https://mcp.firecrawl.dev/v2/mcp` — keyless free tier exposes only `firecrawl_scrape`, `firecrawl_search`, `firecrawl_parse` (rate-limited). With `Authorization: Bearer <FIRECRAWL_API_KEY>`: full set. OAuth endpoint: `https://mcp.firecrawl.dev/v2/mcp-oauth`. Search-only fixed surface: `https://mcp.firecrawl.dev/v2/mcp-search` (9 tools, no page fetch). | [C5][C6] |
| Key-in-path URL | The brief's `https://mcp.firecrawl.dev/{key}/v2/mcp` form is **not documented** in the current README/docs; README says "Never put an API key in the server URL." Treat as legacy — **UNVERIFIED** whether still served. | [C5][C6] |
| Env vars | `FIRECRAWL_API_KEY` (cloud); `FIRECRAWL_API_URL` (self-hosted; key optional then); `FIRECRAWL_OAUTH_TOKEN` (stdio static OAuth token); `FIRECRAWL_NO_SEARCH_FEEDBACK=1`, `FIRECRAWL_NO_ENDPOINT_FEEDBACK=1` (remove feedback tools); `CLOUD_SERVICE`, `FIRECRAWL_MCP_SEARCH_*` (hosted-operator only). | [C5] |
| Auth | API key (stdio env, or HTTP bearer / `x-firecrawl-api-key` / `x-api-key`), or MCP OAuth bearer (`fco_` token, takes precedence). | [C5] |

**Tools (registered names extracted from published `dist/index.js` 3.28.2)** [C7] — README states 27 tools in the full default profile [C5]:

| Tool | Purpose |
|---|---|
| `firecrawl_scrape` | Single URL → markdown/html/links/screenshot/JSON-by-schema; also executes Alexandria catalogued capabilities. |
| `firecrawl_map` | Discover URLs on a site without fetching content. |
| `firecrawl_search` | Web search (+ optional `scrapeOptions` to fetch content); categories research/pdf/developer/gov. |
| `firecrawl_search_feedback` | Feedback on search results (opt-out env). |
| `firecrawl_feedback` | Generic endpoint feedback (opt-out env). |
| `firecrawl_crawl` | Multi-page crawl with limits and include/exclude paths. |
| `firecrawl_check_crawl_status` | Poll a crawl job. |
| `firecrawl_parse` | Parse PDF/DOCX/XLSX/HTML; hosted mode uses a two-step upload-ref; local direct file parse needs self-hosted `FIRECRAWL_API_URL`. |
| `firecrawl_extract` | **Deprecated** ("use scrape JSON"), still registered. |
| `firecrawl_agent` / `firecrawl_agent_status` | Autonomous multi-source research returning structured JSON; async status. |
| `firecrawl_interact` / `firecrawl_interact_stop` | Browser actions (click/type/navigate) per call on a URL or `scrapeId`; stop session. |
| `firecrawl_research_search_papers`, `_read_paper`, `_inspect_paper`, `_related_papers`, `_search_github` | Paper index (PubMed/bioRxiv/medRxiv/arXiv) and repo research. |
| `firecrawl_monitor_create`, `_list`, `_get`, `_update`, `_delete`, `_run`, `_check`, `_checks` | Scheduled change monitors with diffs and webhook/email alerts. |
| `firecrawl_developer_search` | Developer index: repos, issues, merged PRs, READMEs, docs. |
| `firecrawl_gov_search` | US federal/state/local law and regulatory index. |
| `firecrawl_find_tools` | Alexandria catalogue browse/contracts. |
| `firecrawl_credit_usage` | Current/historical credits (optionally by API key). |

- Notable limits: keyless hosted tier is rate-limited and has 3 tools; `firecrawl_interact` runs one prompt/code turn per call (not step-driven from the client); per-team rate limits/credits are plan-dependent (values **UNVERIFIED**).
- "research?" in the brief: yes — `firecrawl_research_*` tools exist [C5][C7].

## 3. Vercel `agent-browser`

**It is both a CLI and an MCP server.** The primary form is a native Rust CLI; `agent-browser mcp` starts an MCP stdio server [C8].

| Item | Fact | Src |
|---|---|---|
| Repo / package | `github.com/vercel-labs/agent-browser`, npm `agent-browser` 0.38.2, Apache-2.0. Also `brew install agent-browser`, `cargo install agent-browser`. | [C8][C1] |
| Install | `npm install -g agent-browser && agent-browser install` (downloads Chrome for Testing; existing Chrome/Brave/Playwright/Puppeteer installs are auto-detected). Linux: `agent-browser install --with-deps`. No Node/Playwright needed for the daemon. Update: `agent-browser upgrade`. Diagnose: `agent-browser doctor [--fix] [--json]`. | [C8] |
| Architecture | Rust CLI → Rust daemon speaking CDP directly. The daemon auto-starts on first command and idle-shuts after **1 h** (`--idle-timeout`, `AGENT_BROWSER_IDLE_TIMEOUT_MS`, `0` disables). Engines: chrome (default), lightpanda, obscura; Safari via WebDriver (iOS). | [C8] |
| Platforms | Native binaries: macOS arm64/x64, Linux arm64/x64, **Windows x64**. On Windows headless Chrome runs on a private desktop; launched Chrome processes sit in a Job Object killed with the daemon. | [C8] |
| Headless/headed | Headless by default; `--headed` / `AGENT_BROWSER_HEADED`; on display-less Linux with `--headed` it auto-starts Xvfb (`AGENT_BROWSER_NO_XVFB=1` opts out). | [C8] |
| Safety knobs | `--allowed-domains` / `AGENT_BROWSER_ALLOWED_DOMAINS` (blocks subresources/WS/beacons to other domains, disables WebRTC; incompatible with CDP attach, profiles, restore). Sessions: `--session` / `AGENT_BROWSER_SESSION`. | [C8] |
| Optional key | The `chat` subcommand (natural-language control) needs `AI_GATEWAY_API_KEY` (+ optional `AI_GATEWAY_MODEL`, `AI_GATEWAY_URL`). Core automation needs **no** API key. | [C8] |

**Core CLI commands** [C8]: `open [url]` (aliases goto/navigate), `read [url]`, `snapshot` (accessibility tree with `@eN` refs; `-i` interactive only; `--json`), `click <sel|@ref>`, `dblclick`, `focus`, `type <sel> <text>`, `fill <sel> <text>`, `press <key>`, `keyboard type|inserttext`, `hover`, `select`, `check`/`uncheck`, `scroll`, `scrollintoview`, `drag`, `upload`, `screenshot [path] [--full|--annotate|--if-changed]`, `pdf <path>`, `eval <js>`, `connect <port>` (CDP), `get text @e1`, `is visible @e2`, `find role button click --name ...`, `close [--all]`, `stream enable|status|disable`, `webmcp list|invoke` (experimental), `chat`.

**Agent workflow** (README "Optimal AI Workflow"): `open` → `snapshot -i --json` → act on refs (`click @e2`, `fill @e3 "..."`) → re-snapshot after page changes. Commands can be chained with `&&` because the daemon persists [C8].

**MCP mode** [C8]: `{"command":"agent-browser","args":["mcp"]}`; default tool profile `core`; `--tools all|core,network,state,debug,tabs,react,mobile`. Common tools: `agent_browser_tools_profiles`, `agent_browser_open`, `_snapshot`, `_click`, `_fill`, `_type`, `_press`, `_wait_for_selector`, `_screenshot`, `_get_url`, `_eval`, `_close`. Each tool has typed fields (`url`, `selector`, `text`, `key`, `session`, `allowedDomains`) plus `extraArgs` for CLI parity; MCP protocol 2025-11-25 by default; paginated tool discovery.

**Skill file** [C8][C9]:
- Install: `npx skills add vercel-labs/agent-browser` (works for Claude Code, Codex, Cursor, Gemini CLI, Copilot, Goose, OpenCode, Windsurf). README: "Do not copy `SKILL.md` from `node_modules`."
- The published `skills/agent-browser/SKILL.md` is a **thin discovery stub**: frontmatter `name: agent-browser`, a long trigger `description`, `allowed-tools: Bash(agent-browser:*), Bash(npx agent-browser:*)`, `hidden: true`. The body says to run `agent-browser skills get core` (or `--full`) to load version-matched instructions; specialised skills are `electron`, `slack`, `dogfood`, `derive-client`, `vercel-sandbox`, `protected-vercel-deployments`, `agentcore`.
- The CLI serves bundled skills: `agent-browser skills list|get <name> [--full]|get --all|path`; override the directory with `AGENT_BROWSER_SKILLS_DIR`.

**Recommendation (CLI vs MCP):** for coding-agent lanes with a shell (Claude Code, Codex, Cursor local), the vendor-recommended path is **CLI via shell + skill stub** (small context, version-matched instructions). Use **MCP (`core` profile)** for lanes without a shell or where typed approval prompts matter (deepagents without sandbox execute, Cursor Cloud team MCP). Seed it as a Plugin composed of: skill bundle (stub) + MCP server definition (`agent-browser mcp`) + install precondition (`agent-browser install`).

## 4. PubMed MCP — candidate comparison

| Candidate | Status (2026-10-07) | Verdict |
|---|---|---|
| `@cyanheads/pubmed-mcp-server` 2.10.20 (Apache-2.0) | Repo pushed 2026-10-04; npm published 2026-10-04; 11 tools, 1 resource, 1 prompt; stdio + Streamable HTTP; public hosted endpoint. | **Best PubMed-specific choice.** [C10] |
| GenomOncology BioMCP (`biomcp-cli` 0.9.1, MIT, Rust single binary) | Repo pushed 2026-10-08 UTC; 70 sources (PubMed/PubTator3/Europe PMC, ClinVar, ClinicalTrials.gov, OncoKB, ...); stdio + Streamable HTTP; Claude Code plugin + skills. | **Best broad biomedical choice** (literature + trials + variants). [C11][C12] |
| `pubmedmcp` (PyPI 0.1.4) | Last upload 2025-09-02; no license/author metadata on PyPI. | Not recommended. [C2] |
| `pubmed-mcp-server` (unscoped npm 1.0.0) | Last modified 2025-09-27; unrelated to cyanheads. | Not recommended. [C1] |
| `mcp-server-pubmed` (npm) | Does not exist. | — [C1] |

Recommendation: seed **both** cyanheads (precise PubMed/PMC/EPMC literature operations, citations, MeSH) and BioMCP (cross-entity pivots). They overlap only on article search.

### 4a. cyanheads `@cyanheads/pubmed-mcp-server` [C10]

- stdio: `npx -y @cyanheads/pubmed-mcp-server@latest` (or `bunx`), env `MCP_TRANSPORT_TYPE=stdio`. Requires Bun >= 1.4.0 **or** Node >= 24. A Docker image is available.
- Public hosted (third-party, operated by the author): `https://pubmed.caseyjhand.com/mcp` (Streamable HTTP, no key). Treat as an untrusted third party for production; prefer self-hosting.
- Framework `@cyanheads/mcp-ts-core`: HTTP auth modes `none`/`jwt`/`oauth`.
- Env: `NCBI_API_KEY` (3 → 10 req/s), `NCBI_ADMIN_EMAIL`, `NCBI_REQUEST_DELAY_MS` (400, or 100 with key), `NCBI_MAX_CONCURRENT` (8), `NCBI_MAX_RETRIES` (6), `NCBI_TIMEOUT_MS` (30000), `NCBI_TOTAL_DEADLINE_MS` (60000), `UNPAYWALL_EMAIL` (enables the Unpaywall full-text tier), `UNPAYWALL_TIMEOUT_MS`, `EUROPEPMC_ENABLED` (false removes the EPMC tools).
- Tools:

| Tool | Purpose |
|---|---|
| `pubmed_search_articles` | PubMed query syntax + filters + date ranges; up to 1,000/page, offset ≤ 9,998; optional summaries. |
| `pubmed_fetch_articles` | Metadata by PMID (≤ 200/call): abstract, authors, MeSH, grants, retraction/correction links. |
| `pubmed_fetch_fulltext` | Full text by PMID/PMCID/DOI (≤ 10/call), PMC → Europe PMC → Unpaywall fallback. |
| `pubmed_europepmc_search` | Europe PMC incl. preprints (PPR), patents (PAT), Agricola; cursor pagination. |
| `pubmed_europepmc_fetch` | Full Europe PMC records (≤ 25/call). |
| `pubmed_format_citations` | APA 7, MLA 9, BibTeX, RIS, Vancouver (≤ 50 PMIDs). |
| `pubmed_find_related` | Similar / cited_by / references; falls back to Europe PMC then OpenAlex. |
| `pubmed_spell_check` | NCBI ESpell query correction. |
| `pubmed_lookup_mesh` | MeSH descriptors, tree numbers, scope notes. |
| `pubmed_lookup_citation` | Partial citations → PMIDs via ECitMatch (≤ 25). |
| `pubmed_convert_ids` | DOI/PMID/PMCID conversion via PMC ID Converter (≤ 50). |
| Resource `pubmed://database/info`; Prompt `research_plan` | EInfo metadata; four-phase research plan. |

### 4b. BioMCP (GenomOncology) [C11][C12]

- Install: `uv tool install biomcp-cli` (or `pip install biomcp-cli`), `curl -fsSL https://biomcp.org/install.sh | bash`, Homebrew tap, Docker `ghcr.io/genomoncology/biomcp`.
- stdio MCP: `biomcp serve` (legacy alias `biomcp mcp`); Docker: `docker run --rm -i ghcr.io/genomoncology/biomcp serve`.
- HTTP: `biomcp serve-http --host <h> --port <p> [--allowed-hosts ...]` → `/mcp` (Streamable HTTP, POST body ≤ 65,536 bytes), `/health`, `/readyz`, `/`. Non-loopback binds require explicit `--allowed-hosts`. No hosted public endpoint is documented.
- Claude Code: `/plugin marketplace add genomoncology/biomcp` + `/plugin install biomcp@biomcp`. Codex: `codex mcp add biomcp -- biomcp serve`. Skills: `biomcp skill install ~/.claude --force`.
- MCP Registry name: `io.github.genomoncology/biomcp`.
- Env (all optional): `NCBI_API_KEY` (ClinVar, PubTator, PubMed efetch, PMC OA, ID converter), `S2_API_KEY` (Semantic Scholar, 1 req/s dedicated), `OPENFDA_API_KEY`, `NCI_API_KEY`, `ONCOKB_TOKEN`, `ALPHAGENOME_API_KEY`, `DISGENET_API_KEY`.
- MCP tools: a bounded **seven-tool** catalog, all `readOnlyHint: true`: `search`, `get`, `variant_normalize_car`, `variant_erepo`, `gene_cspec`, `variant_articles`, plus the raw `biomcp` escape-hatch tool (read-only CLI allowlist). CI caps the catalog at 22,600 bytes / 5,800 cl100k tokens. Pass `json: true` for structured output with `_meta.evidence_urls` and similar fields.
- Data terms: upstream source terms govern reuse (some restrict commercial/clinical use).

### 4c. NCBI E-utilities facts [C13]

- "Without an API key, any site (IP address) posting more than **3 requests per second** to the E-utilities will receive an error message. By including an API key, a site can post up to **10 requests per second** by default. Higher rates are available by request."
- The key is passed as the `api_key` URL parameter. The `NCBI_API_KEY` env var is a convention of the MCP servers, not an NCBI standard.
- NCBI asks for registered `tool` and `email` parameters, and for large jobs to run on weekends or 21:00–05:00 ET on weekdays.

## 5. edgartools (SEC EDGAR)

| Item | Fact | Src |
|---|---|---|
| Package | PyPI `edgartools` 5.61.1 (2026-10-06), MIT, Python >= 3.10; repo `dgunning/edgartools` pushed 2026-10-07. Docs: `edgartools.readthedocs.io`. | [C2][C14] |
| Identity | `from edgar import *; set_identity("your.name@example.com")` — required; "EDGAR requires an email with every request. No key, no signup." The MCP server reads `EDGAR_IDENTITY="Name email"`. | [C14][C15] |
| Main API | `Company("AAPL").get_financials().income_statement()` / `.balance_sheet()`; `Company(...).get_filings(form="4").latest().obj()` (typed objects); `get_filings(form="13F-HR")`; `Company(...).get_facts().query().by_concept("Revenue").to_dataframe()` (XBRL facts); typed objects for 20+ forms, section extraction, full-text search. | [C14] |
| SEC fair access | "Current max request rate: **10 requests/second**." Declare a `User-Agent` such as "Sample Company Name AdminContact@<domain>.com". | [C16] |
| Built-in MCP server | Yes: `uvx --from "edgartools[ai]" edgartools-mcp` (or `pip install "edgartools[ai]"` then `python -m edgar.ai`); `claude mcp add edgartools -- uvx --from "edgartools[ai]" edgartools-mcp`; HTTP: `edgartools-mcp --transport streamable-http --port 8000` (`/mcp`). Module path `edgar/ai/mcp/`. | [C14][C15] |
| Hosted alternative | edgar.tools hosted MCP (commercial platform on the same engine) — terms/pricing **UNVERIFIED**. | [C15] |
| Skills | `from edgar.ai import install_skill; install_skill()` → `~/.claude/skills/edgartools/`; `package_skill()` → ZIP. Domains: core, financials, holdings, ownership, reports, xbrl. | [C17] |

**edgartools MCP tools (13)** [C18]: `edgar_company` (profile + financials + filings + ownership in one call), `edgar_search` (metadata search), `edgar_screen` (industry/exchange/state from local data, zero API calls), `edgar_text_search` (EFTS full-text), `edgar_monitor` (live filing feed), `edgar_filing` (parse any filing into typed data), `edgar_read` (extract sections), `edgar_notes` (notes/disclosures drill-down), `edgar_trends` (XBRL time series + growth), `edgar_compare` (side-by-side or industry), `edgar_ownership` (Form 4 / 13F), `edgar_fund` (mutual funds/ETFs/BDCs/MMFs), `edgar_proxy` (DEF 14A compensation).

**Alternative:** `sec-edgar-mcp` (stefanoamorelli) 1.1.0, **AGPL-3.0** (commercial license by email), Docker `stefanoamorelli/sec-edgar-mcp:latest`, env `SEC_EDGAR_USER_AGENT="Name (email)"`, HTTP via `python -m sec_edgar_mcp.server --transport streamable-http --port 9870` with **no auth**. Tool categories: company (CIK lookup, info, facts), filings (10-K/10-Q/8-K, sections), financials (XBRL statements), insider (Forms 3/4/5) [C19]. AGPL is a licensing risk for a hosted Mission Control; prefer edgartools (MIT).

**Recommended exposure:** edgartools' own docs say "Skills improve code generation. The MCP Server provides data access … Install both" [C17]. For Mission Control: seed the **MCP server** (`edgartools-mcp`, stdio for local lanes, streamable-http for cloud lanes) as the data-access capability, plus the **skill bundle** for code-writing lanes (Python sandbox with `edgartools` installed). Pin with `uvx --from "edgartools[ai]==5.61.1" edgartools-mcp` (exact-pin syntax with extras is standard uv behaviour — **UNVERIFIED** against edgartools docs).

---

# PART B — Formats to store and project

## 6. Agent Skills open standard (agentskills.io)

**Directory layout** [C20]:
```
skill-name/
├── SKILL.md        # required: YAML frontmatter + Markdown body
├── scripts/        # optional executable code
├── references/     # optional docs (REFERENCE.md, FORMS.md, domain files)
├── assets/         # optional templates, images, data
└── ...             # any other files
```

**Frontmatter (spec)** [C20]:

| Field | Req | Constraint |
|---|---|---|
| `name` | yes | 1–64 chars; `a-z`, `0-9`, `-`; no leading/trailing or consecutive hyphens; **must match the parent directory name**. |
| `description` | yes | 1–1024 chars; what it does + when to use it. |
| `license` | no | License name or bundled-file reference. |
| `compatibility` | no | 1–500 chars; environment requirements. |
| `metadata` | no | Map string → string. |
| `allowed-tools` | no | Space-separated pre-approved tools. **Experimental**; support varies. |

- Progressive disclosure: metadata (~100 tokens) at startup for all skills; full body on activation (**< 5,000 tokens recommended**; keep `SKILL.md` **under 500 lines**); resources loaded on demand. Keep file references one level deep, with relative paths [C20].
- Validation: `skills-ref validate ./my-skill` (`agentskills/agentskills` repo) [C20].
- Client guidance: scan project and user scopes; `.agents/skills/` is "a widely-adopted convention for cross-client skill sharing" (the spec does not mandate locations); many clients also scan `.claude/skills/`; **project-level overrides user-level** on a name collision; gate project skills on workspace trust [C21].

**Host support / locations**:

| Host | Project | User | Extra fields honoured | Src |
|---|---|---|---|---|
| Claude Code | `.claude/skills/<name>/SKILL.md` (walks up dirs) | `~/.claude/skills/` | `when_to_use`, `argument-hint`, `arguments`, `disable-model-invocation`, `user-invocable`, `allowed-tools`, `disallowed-tools`, `model`, `effort`, `context: fork`, `agent`, `background`, `hooks`, `paths`, `shell`, `metadata`, `license`, `compatibility`. `description` + `when_to_use` are capped at 1,536 chars in the listing; `name` is optional (defaults to the dir name). `.claude/commands/*.md` are merged into skills. | [C22] |
| Cursor | `.agents/skills/`, `.cursor/skills/` | `~/.agents/skills/`, `~/.cursor/skills/` | Also loads `.claude/skills/`, `.codex/skills/`, `~/.claude/skills/`, `~/.codex/skills/`. Fields: `name`, `description`, `paths`, `disable-model-invocation`, `icon`, `color`, `metadata`. | [C23] |
| Codex | `.agents/skills` in every dir from CWD up to the repo root | `$HOME/.agents/skills`; admin `/etc/codex/skills`; system bundled | Optional `agents/openai.yaml` (`interface.*`, `policy.allow_implicit_invocation`, `dependencies.tools[]` incl. MCP). The skill list budget is ≤ 2% of context (8,000 chars if unknown). Disable via `[[skills.config]]` in `~/.codex/config.toml`. Symlinks are followed. | [C24] |
| deepagents (LangChain) | Configured paths on a backend: `create_deep_agent(backend=FilesystemBackend(root_dir=...), skills=["./skills/"])`; the default `StateBackend` takes files via `invoke(files={...})`; `StoreBackend` is supported | — | Spec fields (`name`, `description`, `license`, `compatibility`, `metadata`, `allowed-tools`); `SkillsMiddleware` injects name + description, and the agent reads the body via `read_file`. The path must point at a dir **containing** skill dirs. | [C25] |

**Vercel `skills` CLI** (npm `skills` 1.7.1, MIT) [C9][C26]:
- `npx skills add <owner/repo | URL | git URL | local path | direct SKILL.md/archive URL>`; options `-g/--global`, `-a/--agent <ids...>` (`claude-code`, `codex`, `cursor`, `deepagents`, `universal`, ...), `-s/--skill <names|'*'>`, `-l/--list`, `--copy` (the default is a symlink to a canonical copy), `-y`, `--all`, `--full-depth`.
- Other commands: `skills use` (print the prompt without installing), `list|ls`, `find`, `update [-g|-p]`, `init`, `remove|rm`, `experimental_install` (restore from the lockfile).
- Agent path table: `universal`/`amp`/`replit` → project `.agents/skills/`, global `~/.config/agents/skills/`; `claude-code` → `.claude/skills/` / `~/.claude/skills/`; `codex` → `.agents/skills/` / `~/.agents/skills/`; `cursor` → `.agents/skills/` / `~/.cursor/skills/`; `deepagents` → `.agents/skills/` / `~/.deepagents/agent/skills/`.
- **Lockfiles (from published source)**: project `skills-lock.json` (`version: 1`, `skills: {name: {source, sourceType, sourceUrl?, skillPath?, computedHash, ...}}`, keys sorted, local sources made relative); global `~/.agents/.skill-lock.json` (or `$XDG_STATE_HOME/skills/.skill-lock.json`, `version: 3`). `computedHash` = SHA-256 over the skill folder: files sorted by relative path (forward slashes), hashing `relativePath` then the bytes of each file, skipping `.git` and `node_modules`. This repo's own `skills-lock.json` already uses this shape (`source`, `sourceType: github`, `skillPath`, `computedHash`).
- The README does not document the lockfile or a "universal mode" beyond the agent table; the lockfile facts above come from the shipped `dist/cli.mjs` [C26].
- Discovery inside a source repo: root `SKILL.md`, `skills/`, `skills/.curated|.experimental|.system/`, `.agents/skills/`, `.claude/skills/`, ~60 agent dirs; ≤ 3 levels deep; also `.claude-plugin/marketplace.json` / `plugin.json` `skills` entries. Optional `metadata.internal: true` hides a skill unless `INSTALL_INTERNAL_SKILLS=1`.
- Env: `GITHUB_TOKEN`/`GH_TOKEN`, `DISABLE_TELEMETRY`/`DO_NOT_TRACK` (anonymous telemetry is on by default).
- Compatibility matrix (README): `allowed-tools` is honoured by Claude Code, Codex, Cursor (not Kiro, Zencoder); `context: fork` only by Claude Code; hooks in skills by Claude Code, Cline, Kiro.

**Mission Control implication:** store skill bundles as the canonical directory tree + `computedHash` (same algorithm as the `skills` CLI, so pins interoperate) and project to host paths: `.agents/skills/` covers Codex, Cursor, deepagents (via backend path) and the universal convention; also project `.claude/skills/` for Claude Code, which does **not** document reading `.agents/skills`.

## 7. Claude Code plugin, subagent and hooks formats

### 7a. Plugin manifest `.claude-plugin/plugin.json` [C27]

- The manifest is **optional**; when present only `name` (kebab-case) is required. Everything except the manifest lives at the plugin root, not inside `.claude-plugin/`.
- Fields: `$schema`, `name`, `displayName`, `version` (keeps users on that version until changed), `description`, `author {name, email?, url?}`, `homepage` (must parse as a URL), `repository`, `license` (SPDX), `keywords`, `metadata` (free-form, ignored), directory-listing fields (`icon`, `documentationUrl`, `supportUrl`, `privacyPolicyUrl`, `termsOfServiceUrl`), `defaultEnabled` (default true), `dependencies`, `settings` (only `agent` and `subagentStatusLine` take effect), `userConfig` (prompted values; `sensitive: true`; referenced as `${user_config.KEY}`; exported to hooks as `CLAUDE_PLUGIN_OPTION_<KEY>`), `types`, `channels`, and the component keys `skills` (adds to the default scan), `commands` (path/array/object map; replaces the default), `agents` (files only; replaces the default), `hooks` (path/inline/array; merged with `hooks/hooks.json`), `mcpServers` (path, `.mcpb`/`.dxt` bundle path or URL, inline, or array; merged with `.mcp.json`, later name wins), `lspServers` (merged with `.lsp.json`), `outputStyles`, `workflows`, `experimental.{themes, monitors, evals}`.
- Unknown top-level keys are stripped with a warning; unknown keys inside `userConfig`/`channels`/`lspServers`/`monitors` entries fail the load. Validate with `claude plugin validate ./my-plugin [--strict]` (MCP entry checks need v2.1.281+).
- **Standard layout**:

| Component | Default location |
|---|---|
| Manifest | `.claude-plugin/plugin.json` |
| Skills | `skills/<name>/SKILL.md` (a root `SKILL.md` alone ⇒ single-skill plugin) |
| Commands | `commands/*.md` (prefer skills) |
| Agents | `agents/*.md` |
| Hooks | `hooks/hooks.json` |
| MCP servers | `.mcp.json` |
| LSP servers | `.lsp.json` |
| Output styles / Workflows / Themes / Monitors | `output-styles/`, `workflows/`, `themes/`, `monitors/monitors.json` |
| Executables | `bin/` (on the Bash tool PATH while enabled; claude.ai and Cowork refuse plugins with a top-level `bin/`) |
| Settings | `settings.json` |

- A `CLAUDE.md` at the plugin root is **not** loaded (the validator warns).
- **Hooks inside a plugin**: `hooks/hooks.json` (and any hooks file referenced from `plugin.json`) must wrap the event map in a top-level `"hooks"` key; inline `hooks` objects in `plugin.json` are the bare event map. Example: `{"hooks":{"PreToolUse":[{"matcher":"Bash","hooks":[{"type":"command","command":"\"${CLAUDE_PLUGIN_ROOT}\"/scripts/check-command.sh"}]}]}}`.
- **Path variables**: `${CLAUDE_PLUGIN_ROOT}` (installed version dir; changes on update — don't write state there), `${CLAUDE_PLUGIN_DATA}` (`~/.claude/plugins/data/<id>/`, persists across updates, deleted on last uninstall unless `--keep-data`), `${CLAUDE_PROJECT_DIR}`. Where they resolve: hook `command`/`args` (also exported to the process); MCP stdio `command`/`args`/`env` (exports ROOT and DATA); MCP http/sse/ws `url`/`headers`/`headersHelper`; LSP fields; skill/command/agent Markdown bodies (inline substitution). They are **not** present in Bash-tool commands. Use exec form (`args`) or quote `"${CLAUDE_PLUGIN_ROOT}"` in shell form; on Windows substituted paths use forward slashes.

### 7b. Marketplace `.claude-plugin/marketplace.json` [C28][C29]

- Required: `name` (letters/digits/`.`/`_`/`-`; official names reserved), `owner {name, email?, url?}`, `plugins[]`. Optional: `$schema`, `description`, `version`, `metadata.{description,version,pluginRoot}`, `forceRemoveDeletedPlugins`, `allowCrossMarketplaceDependenciesOn`, `renames`.
- Plugin entry: `name` + `source` required; plus `description`, `version` (plugin.json wins), `category`, `tags`, `strict` (default true), `relevance`, `dependencies`, `defaultEnabled`, `displayName`, `metadata`, `headers`, `headersHelper`, and any `plugin.json` field.
- Plugin `source` types: relative path (`./...`), `github {repo, ref?, sha?}`, `url {url, ref?, sha?}`, `git-subdir {url, path, ref?, sha?}`, `npm {package, version?, registry?}` (no install scripts run), `archive {url, sha256}` (v2.1.224+), `command {command, timeout?, mode?}` (v2.1.229+). `sha` is a full 40-char commit; when both `ref` and `sha` are set, `sha` is checked out.
- Strict mode: with `plugin.json` present and `strict: true`, entry component fields are appended (entry `hooks` matchers replace per event); `strict: false` + entry components ⇒ load conflict.
- Install id: `<entry-name>@<marketplace>`; keep the entry name equal to the manifest name.

### 7c. Subagents `.claude/agents/*.md` [C30]

- Scopes/priority: managed (1) > `--agents` CLI JSON (2) > `.claude/agents/` walking up to the repo root (3) > `~/.claude/agents/` (4) > plugin `agents/` (5).
- Frontmatter (only `name` and `description` are required; camelCase; unknown fields are silently ignored):

| Field | Notes |
|---|---|
| `name` | ≤ 256 chars, no `:` (reserved for `plugin:agent`); the filename need not match; hooks see it as `agent_type`. |
| `description` | When to delegate. |
| `tools` | Comma string or YAML list; omitted ⇒ inherit all. |
| `disallowedTools` | Removed from the inherited/specified list; a specifier like `Bash(git push *)` removes the whole tool. |
| `model` | `sonnet`/`opus`/`haiku`/`fable`/full id/`inherit`. |
| `permissionMode` | `default`, `acceptEdits`, `auto`, `dontAsk`, `bypassPermissions`, `plan`, `manual` (= default). **Ignored for plugin subagents.** |
| `maxTurns` | Partial output is marked when hit (v2.1.246+). |
| `skills` | Preload full skill content. |
| `mcpServers` | Names or inline configs. **Ignored for plugin subagents.** |
| `hooks` | Scoped lifecycle hooks. **Ignored for plugin subagents.** |
| `memory` | `user` / `project` / `local`. |
| `background` | `true` forces background. |
| `omitClaudeMd` | v2.1.271+. |
| `effort` | `low` … `max`. |
| `isolation` | `worktree` (temporary git worktree from the default branch; auto-cleaned if unchanged). |
| `color`, `initialPrompt` (ignored for plugin subagents), `experimental.cacheTtl` (`5m`/`1h`) | |

Implication: plugin-shipped subagents cannot carry `permissionMode`, `mcpServers` or `hooks`; Mission Control must project those subagent profiles to `.claude/agents/` (project scope) when it needs them.

### 7d. Hooks (Claude Code) [C31]

- Locations: `~/.claude/settings.json`, `.claude/settings.json`, `.claude/settings.local.json`, managed policy, plugin `hooks/hooks.json`, skill frontmatter, subagent frontmatter. Entries **merge** across levels. Cloud sessions don't read `~/.claude/settings.json`. `allowManagedHooksOnly` blocks user/project/local/plugin hooks.
- Shape: `{"hooks": {"<Event>": [{"matcher": "...", "hooks": [ {handler} ]}]}}`.
- **Events (33)**: `SessionStart`, `Setup`, `InstructionsLoaded`, `UserPromptSubmit`, `UserPromptExpansion`, `MessageDisplay`, `PreToolUse`, `PermissionRequest`, `PermissionDenied`, `PostToolUse`, `PostToolUseFailure`, `PostToolBatch`, `Notification`, `SubagentStart`, `SubagentStop`, `TaskCreated`, `TaskCompleted`, `Stop`, `StopFailure`, `TeammateIdle`, `ConfigChange`, `CwdChanged`, `DirectoryAdded`, `FileChanged`, `WorktreeCreate`, `WorktreeRemove`, `PreCompact`, `PostCompact`, `PreModelSwitch`, `PostModelSwitch`, `Elicitation`, `ElicitationResult`, `SessionEnd`.
- Matcher: `*`, empty or omitted = all; only `[A-Za-z0-9_- ,|]` = exact name(s) split on `|` or `,`; anything else = unanchored JS regex (e.g. `mcp__memory__.*`). `FileChanged`/`StopFailure` use a narrower exact-match set.
- Handler types: `command`, `http`, `mcp_tool`, `prompt`, `agent` (experimental).
  - Common: `type`, `if` (one permission rule, e.g. `Bash(git *)`, tool events only), `timeout` in seconds (defaults 600 for command/http/mcp_tool, 30 for prompt, 60 for agent; lowered on some events), `statusMessage`, `once` (skill frontmatter only).
  - `command`: `command`, `args` (exec form, no shell), `async`, `asyncRewake` (wake Claude on exit 2), `shell` (`bash`/`powershell`; on Windows defaults to Git Bash, else PowerShell).
  - `http`: `url`, `headers` (`$VAR`/`${VAR}` only for vars in `allowedEnvVars`), `allowedEnvVars`. The settings `allowedHttpHookUrls` and `httpHookAllowedEnvVars` constrain them.
  - `mcp_tool`: `server` (plugin servers as `plugin:<plugin>:<server>`), `tool`, `input` (`${tool_input.file_path}` substitution).
  - `prompt`/`agent`: `prompt` (`$ARGUMENTS` = hook input JSON), `model`.
- All matching handlers run in parallel; identical handlers across settings files are deduplicated.
- **Common stdin fields**: `session_id`, `prompt_id`, `transcript_path`, `cwd`, `scratchpad_dir` (v2.1.257+), `permission_mode`, `effort.level`, `hook_event_name`; inside subagents or with `--agent`: `agent_id`, `agent_type`. Tool events add `tool_name`, `tool_input`, `tool_use_id`; `PostToolUse` adds `tool_response`.
- **Exit codes**: `0` = success (stdout parsed as JSON if it starts with `{` and ends with `}`; plain stdout becomes context only for `UserPromptSubmit`, `UserPromptExpansion`, `SessionStart`, `PostModelSwitch`); `2` = blocking error on blockable events (stderr or the JSON reason is the message; a JSON `allow` cannot override it); any other code = non-blocking error unless valid JSON decides. **Exit 1 does not block.** Timed-out command hooks on `PreToolUse` do **not** block.
- **JSON output**: universal `continue` (false stops processing), `stopReason`, `suppressOutput` (no effect), `systemMessage` (shown to the user), `terminalSequence` (allowlisted OSC/BEL); top-level `decision: "block"` + `reason` (UserPromptSubmit, UserPromptExpansion, PostToolUse, PostToolUseFailure, PostToolBatch, Stop, SubagentStop, ConfigChange, PreCompact); `hookSpecificOutput {hookEventName, ...}`:
  - PreToolUse: `permissionDecision` = `allow`/`deny`/`ask`/`defer` (precedence deny > defer > ask > allow), `permissionDecisionReason`, `updatedInput` (replaces the whole input), `additionalContext`.
  - PermissionRequest: `decision.behavior` allow/deny (+ `updatedInput`); PostToolUse: `updatedToolOutput`; PermissionDenied: `retry`; SessionStart: `additionalContext`, `initialUserMessage`, `watchPaths`, `sessionTitle`, `reloadSkills`; WorktreeCreate: path on stdout / `worktreePath`.
  - `additionalContext`, `systemMessage`, `initialUserMessage` and plain stdout are each capped at **10,000 chars** (overflow is saved to a file with a 2,000-char preview).
- HTTP hooks: 2xx empty = success; 2xx JSON = same schema; non-2xx or connection failure = non-blocking; they cannot block by status code alone.

## 8. Codex formats

Note: `developers.openai.com/codex/*` now **308-redirects to `learn.chatgpt.com/docs/*`**; append `.md` for Markdown [C32].

### 8a. Hooks [C32]
- Sources: `hooks.json` or inline `[hooks]` tables in `config.toml`, next to each active config layer: `~/.codex/hooks.json`, `~/.codex/config.toml`, `<repo>/.codex/hooks.json`, `<repo>/.codex/config.toml`; plus plugin-bundled `hooks/hooks.json`. All sources load (no override); both forms in one layer merge with a warning. Project hooks load only when the project `.codex/` layer is trusted.
- **Trust**: non-managed hooks must be reviewed and trusted per **hash** via `/hooks`; changed hooks are skipped until re-trusted. `--dangerously-bypass-hook-trust` exists for vetted automation. Managed hooks (system/MDM/cloud/`requirements.toml`) are trusted by policy.
- Shape: the same three-level structure as Claude Code: `{"description"?, "hooks": {"<Event>": [{"matcher": "...", "hooks": [{"type": "command", "command": "...", "timeout": 30, "statusMessage": "...", "additionalContextLimit": 5000, "commandWindows"?: "...", "async"?: true}]}]}}`. TOML: `[[hooks.PreToolUse]] matcher = "^Bash$"` + `[[hooks.PreToolUse.hooks]] type = "command" ...`.
- Handler types: `command` and `mcp_tool` run; `prompt` and `agent` are **parsed but skipped**.
- Events (12): `SessionStart`, `SessionEnd`, `SubagentStart`, `SubagentStop`, `PreToolUse`, `PermissionRequest`, `PostToolUse`, `PreCompact`, `PostCompact`, `UserPromptSubmit`, `Stop`, `Interrupt`.
- Timeouts: default 600 s; `SessionEnd`/`Interrupt` default 1 s, max 3 s.
- Common input: `session_id` (subagents use the parent id), `transcript_path` (not a stable format), `cwd`, `hook_event_name`, `model` (Codex extension); turn-scoped hooks add `turn_id`; most add `permission_mode`. PreToolUse adds `tool_name` (`Bash`, `apply_patch`, `mcp__server__tool`), `tool_use_id`, `tool_input`. Matcher aliases: `apply_patch` also matches `Edit`/`Write`.
- Output: `continue`, `stopReason`, `systemMessage`, `suppressOutput` (parsed, not implemented). PreToolUse supports `hookSpecificOutput.permissionDecision: "deny"` (+ reason), legacy `{"decision":"block"}`, exit 2 + stderr, `additionalContext`, and `permissionDecision: "allow"` + `updatedInput` (Bash/apply_patch need a string `command`). **`ask` is parsed but not supported** (the hook run is marked failed and the call continues).

### 8b. Skills [C24] — see the §6 table (`.agents/skills` walk-up, `$HOME/.agents/skills`, `/etc/codex/skills`, `agents/openai.yaml`).

### 8c. AGENTS.md [C33]
- Global: `~/.codex/AGENTS.override.md`, else `AGENTS.md` (or under `$CODEX_HOME`); only the first non-empty file is used.
- Project: walk from the project root (git root) down to CWD; in each dir check `AGENTS.override.md`, `AGENTS.md`, then `project_doc_fallback_filenames`; concatenate root → leaf (later = higher priority); stop at `project_doc_max_bytes` (**32 KiB** default).

### 8d. MCP in `config.toml` [C34]
- `~/.codex/config.toml` or project `.codex/config.toml`; one `[mcp_servers.<name>]` table per server.
- stdio: `command` (required), `args`, `env` (literal map), `env_vars` (allow-list of names to forward; `{name, source = "local"|"remote"}`), `cwd`, `experimental_environment = "remote"`.
- Streamable HTTP: `url` (required), `bearer_token_env_var`, `http_headers` (static), `env_http_headers` (header → env var name), `http_headers_helper`, OAuth sub-table.
- Common: `startup_timeout_sec` (10), `tool_timeout_sec` (60), `enabled`, `required`, `enabled_tools`, `disabled_tools` (applied after), `default_tools_approval_mode`, per-tool `[mcp_servers.X.tools.Y] approval_mode`, `output_token_limit`. Plugin servers live under `plugins."name@mp".mcp_servers.<server>`.
- CLI: `codex mcp add <name> -- <command...>`, `codex mcp login`.
- **No `${VAR}` string interpolation is documented**; secrets flow via `env_vars`, `bearer_token_env_var` and `env_http_headers` (the config reference contains no interpolation syntax) [C34][C35].

## 9. MCP server config shapes across hosts

| Host | File(s) | Shape | Env interpolation | Src |
|---|---|---|---|---|
| Claude Code | project `.mcp.json`; local + user scopes in `~/.claude.json`; plugin `.mcp.json`/`mcpServers` | `{"mcpServers": {"name": {"type": "stdio"/"http"/"sse"/"ws", "command", "args", "env", "url", "headers", "headersHelper"}}}` | `${VAR}` and `${VAR:-default}` in `command`, `args`, `env`, `url`, `headers`. An unset var without a default ⇒ warning, literal `${VAR}` kept. Plugins add `${CLAUDE_PLUGIN_ROOT}`, `${CLAUDE_PLUGIN_DATA}`, `${user_config.KEY}`. Project servers need interactive approval (auto-loaded in `-p`, SDK and cloud sessions). | [C36][C27] |
| Cursor | project `.cursor/mcp.json`; global `~/.cursor/mcp.json` | stdio: `type: "stdio"`, `command`, `args`, `env`, `envFile` (stdio only); remote: `url`, `headers`, `auth {CLIENT_ID, CLIENT_SECRET?, scopes?}` (static OAuth) | `${env:NAME}`, `${userHome}`, `${workspaceFolder}`, `${workspaceFolderBasename}`, `${pathSeparator}`/`${/}` in `command`, `args`, `env`, `url`, `headers` (and `auth`). | [C37] |
| Cursor Cloud Agents | Team/personal MCP via the dropdown at cursor.com/agents; team MCPs via the Default team marketplace | HTTP and stdio both supported; OAuth supported; built-in Cursor Cloud MCP for diagnostics | Whether cloud agents read the repo's `.cursor/mcp.json` — **UNVERIFIED** | [C38] |
| Codex | `~/.codex/config.toml`, `.codex/config.toml` | TOML `[mcp_servers.<name>]` (see §8d) | None; env forwarding by name | [C34] |
| deepagents / LangChain | code | `MCPAdapter(target)` where target = http(s) URL, script `Path`, transport object, in-process FastMCP, or a `{"mcpServers": {...}}` dict (`langchain[mcp]>=1.4.0`, beta; supersedes `langchain-mcp-adapters`) | Python-side (no file interpolation) | [C39] |

**Projection rule for Mission Control:** store one neutral MCP Server capability record `{transport, command, args, env_refs[], url, header_refs{}, oauth?}` holding secret **references**, and render per host: Claude `${VAR}`, Cursor `${env:VAR}`, Codex `env_vars=[...]` / `bearer_token_env_var` / `env_http_headers`, deepagents a dict resolved in-process. Never render literal secrets into checked-in files.

## 10. Supabase Storage for bundles

| Topic | Fact | Src |
|---|---|---|
| Create bucket | JS `storage.createBucket('b', {public, allowedMimeTypes, fileSizeLimit})`; Python `supabase.storage.create_bucket('b', options={"public": False, "allowed_mime_types": [...], "file_size_limit": 1024})`; SQL `insert into storage.buckets (id, name, public) ...`. Default `public: false`. | [C41][C43] |
| Size limits | Global limit: Free ≤ 50 MB, Pro/Team ≤ 500 GB, Enterprise custom; a per-bucket limit must be ≤ global. Filename character allow-list: alphanumerics, `_ - . ' ,`, `! * & $ @ = ; : + ? ( )`, whitespace. | [C42] |
| Standard upload | Recommended ≤ 6 MB; up to 5 GB possible. Python `storage.from_('b').upload(path=..., file=f, file_options={"cache-control": "3600", "upsert": "false", "content-type": ...})`. Overwriting needs `upsert` (`x-upsert`) plus RLS SELECT + UPDATE. | [C44][C43] |
| Resumable (TUS) | Endpoint `https://<project>.storage.supabase.co/storage/v1/upload/resumable` (direct storage hostname recommended); **chunk size must be 6 MB**; metadata `bucketName`, `objectName`, `contentType`, `cacheControl`, `metadata` (JSON → `user_metadata`); upload URL valid **≤ 24 h**; concurrent uploads to the same path ⇒ `409 Conflict` (first wins; last wins with `x-upsert`); an existing path ⇒ `400 Asset Already Exists`. Python via `tus-py-client`. Signed resumable: put the `create_signed_upload_url` token in the `x-signature` header. | [C45] |
| Signed upload URL | `create_signed_upload_url(path, options?)` — valid **2 hours**; needs RLS INSERT; then `upload_to_signed_url(path, token, file)` (no RLS needed). | [C43] |
| Signed download URL | `create_signed_url(path, expires_in_seconds, options?)` and `create_signed_urls(paths, expires_in)`; need RLS SELECT. Private download: `download(path)` → bytes. | [C43] |
| List and other ops | `list("folder", {"limit": 100, "offset": 0, "sortBy": {"column": "name", "order": "desc"}})` (RLS SELECT). Also `remove([...])` (DELETE + SELECT), `move`, `copy`, `update`. | [C43] |
| RLS | No uploads are allowed without policies on `storage.objects`. Prefix-scoped example: `create policy "..." on storage.objects for insert to authenticated with check (bucket_id = 'my_bucket_id' and (storage.foldername(name))[1] = 'private');` Helpers: `storage.foldername()`, `storage.filename()`, `storage.extension()`, `storage.allow_only_operation()`, `storage.allow_any_operation()`. The service key bypasses RLS entirely. | [C46][C47] |
| Schema | `storage.buckets(id, name, public, file_size_limit, allowed_mime_types, owner_id, ...)`, `storage.objects(id, bucket_id, name, metadata jsonb, path_tokens, version, owner_id, ...)`. Treat as **read-only** and operate via the API (deleting rows orphans billed objects). | [C48] |
| Versioning | "**S3 versioning is not supported.** … Deleted objects are permanently removed and cannot be restored." CDN propagation makes overwrites stale-prone, and the docs recommend uploading to a **new path**. ⇒ immutability must come from content-addressed paths (`bundles/<kind>/<sha256>/...`), no `upsert`, and RLS that grants writers INSERT but not UPDATE/DELETE. | [C49][C45] |

Suggested private-bucket policy set for bundles (derived from [C46], not copied): INSERT for the publisher role where `bucket_id = 'capability-bundles' and (storage.foldername(name))[1] = 'sha256'`; SELECT for readers; no UPDATE/DELETE policies, so upsert or overwrite is impossible except with the service key.

## 11. Hybrid search in Postgres (Supabase)

- **Supabase's documented function** (shape as published) [C50]: table `documents(id bigint identity, content text, fts tsvector generated always as (to_tsvector('english', content)) stored, embedding extensions.vector(512))`; indexes `using gin(fts)` and `using hnsw (embedding vector_ip_ops)`; function `hybrid_search(query_text text, query_embedding extensions.vector(512), match_count int, full_text_weight float = 1, semantic_weight float = 1, rrf_k int = 50) returns setof documents language sql` with CTEs `full_text` (`row_number() over (order by ts_rank_cd(fts, websearch_to_tsquery(query_text)) desc)`, `where fts @@ websearch_to_tsquery(query_text)`, `limit least(match_count, 30) * 2`) and `semantic` (`row_number() over (order by embedding <#> query_embedding)`), a `full outer join`, ordered by `coalesce(1.0/(rrf_k + full_text.rank_ix), 0.0) * full_text_weight + coalesce(1.0/(rrf_k + semantic.rank_ix), 0.0) * semantic_weight desc`, `limit least(match_count, 30)`. It is called via `supabase.rpc('hybrid_search', {...})`. Note the hard cap of 30 results. (The doc's Edge Function sample declares `supabaseSecretKey` but uses `supabaseServiceRoleKey` — a doc typo.)
- **Operators/opclasses** [C51][C40]: `<->` L2 (`vector_l2_ops`), `<#>` negative inner product (`vector_ip_ops`), `<=>` cosine (`vector_cosine_ops`); `halfvec_*_ops` and `sparsevec_*_ops` for those types. Use `vector_ip_ops` only with normalized embeddings (whether OpenAI/Voyage outputs are normalized is **UNVERIFIED** here); `vector_cosine_ops` is the safe default.
- **Dimension limits for indexes** (HNSW and IVFFlat) [C40]: `vector` ≤ 2,000; `halfvec` ≤ 4,000; `bit` ≤ 64,000. Half-precision indexing pattern: `create index on items using hnsw ((embedding::halfvec(3072)) halfvec_cosine_ops)`; queries must cast the same way.
- HNSW tuning: `hnsw.ef_search` (default 40); filtering is applied after the index scan, so use `hnsw.iterative_scan = strict_order|relaxed_order` (pgvector ≥ 0.8) for filtered queries [C40]. The pgvector version installed on the Supabase project is **UNVERIFIED** (check `select extversion from pg_extension where extname='vector'`).
- **pg_trgm** [C52]: a trusted extension (a non-superuser with CREATE can install it); `similarity()`, `word_similarity()`, the `%` operator (threshold `pg_trgm.similarity_threshold`, default **0.3**), `<%`; index with `gin (name gin_trgm_ops)` or `gist (name gist_trgm_ops)`. Use it for fuzzy capability-name/alias matching, either as a third RRF list or as a pre-filter.
- **Embedding options**:

| Model | Dims | Context | License / access | Src |
|---|---|---|---|---|
| OpenAI `text-embedding-3-small` | 1536 default (shortenable via `dimensions`) | 8192 | API (`OPENAI_API_KEY`) | [C53] |
| OpenAI `text-embedding-3-large` | 3072 default (needs `dimensions` ≤ 2000 or `halfvec` to index) | 8192 | API | [C53] |
| Voyage `voyage-4` / `-4-large` / `-4-lite` | 1024 default; 256/512/2048 | 32,000 | API (`VOYAGE_API_KEY` — env name **UNVERIFIED**); 4-series embeddings are mutually compatible | [C54] |
| Voyage `voyage-4-nano` | 1024 default; 256/512/2048 | 32,000 | Open weights on Hugging Face | [C54] |
| `BAAI/bge-m3` | 1024 | 8192 | MIT; dense + sparse + ColBERT; 100+ languages; sentence-transformers; `ollama pull bge-m3` | [C55][C57] |
| `nomic-ai/nomic-embed-text-v1.5` | 768 (Matryoshka, e.g. 512/256) | 2048 native, extendable to 8192 | Apache-2.0; requires `search_query:` / `search_document:` prefixes; `ollama pull nomic-embed-text`, `POST localhost:11434/api/embed` | [C56][C57] |

Recommendation: store `embedding_model` + `embedding_dims` on every row (the projection is rebuildable and never an authorization store, per GLOSSARY "Search Projection"); default to 1024 dims (bge-m3 locally or voyage-4 via API) to stay well under the 2,000-dim `vector` index limit; add a `pg_trgm` list on `name`/`aliases` and a `tsvector` on description and tool names; fuse with RRF (`rrf_k` 50–60).

---

# Seed manifest proposal

Kinds use GLOSSARY vocabulary: `mcp_server`, `skill_bundle`, `plugin`, `hook_script`. "Pin strategy" = how the Capability Pin is formed. Host columns: **DA** deepagents, **CL** Cursor local, **CC** Cursor cloud, **CCode** Claude Code, **CX** Codex. ✔ = documented path; (h) = via remote HTTP endpoint; (s) = needs a shell/sandbox exec; ? = UNVERIFIED.

| Capability id | Kind | Source | Pin strategy | Secrets needed | DA | CL | CC | CCode | CX |
|---|---|---|---|---|---|---|---|---|---|
| `mcp.tavily` | mcp_server | npm `tavily-mcp@0.2.22` (stdio) / `https://mcp.tavily.com/mcp/` (http) | npm exact version + tarball integrity (sha512 from registry); remote: endpoint URL + recorded `tools/list` digest | `TAVILY_API_KEY` (bearer header for http) | ✔ | ✔ | ✔(h) | ✔ | ✔ |
| `mcp.firecrawl` | mcp_server | npm `firecrawl-mcp@3.28.2`; Docker `ghcr.io/firecrawl/firecrawl-mcp-server:sha-<commit>`; hosted `https://mcp.firecrawl.dev/v2/mcp` | npm exact + integrity, or image digest; hosted: tools/list digest + `FIRECRAWL_NO_*_FEEDBACK=1` | `FIRECRAWL_API_KEY` (or OAuth); optional `FIRECRAWL_API_URL` | ✔ | ✔ | ✔(h) | ✔ | ✔ |
| `mcp.firecrawl-search` | mcp_server | hosted `https://mcp.firecrawl.dev/v2/mcp-search` (fixed 9 tools) | tools/list digest (contractually fixed) | OAuth or key | ✔(h) | ✔(h) | ✔(h) | ✔(h) | ✔(h) |
| `plugin.agent-browser` | plugin | npm `agent-browser@0.38.2` + `vercel-labs/agent-browser` skill stub | npm exact + integrity; skill `computedHash`; precondition `agent-browser install` (Chrome for Testing) | none (optional `AI_GATEWAY_API_KEY` for `chat`) | (s)? | ✔ | ? (needs Chrome in the VM) | ✔ | ✔ |
| `mcp.agent-browser` | mcp_server | `agent-browser mcp --tools core` | same binary pin as above | none | ✔ | ✔ | ? | ✔ | ✔ |
| `skill.agent-browser` | skill_bundle | `npx skills add vercel-labs/agent-browser` | git commit sha + `computedHash` (skills-lock.json) | none | ✔ | ✔ | ✔ | ✔ | ✔ |
| `mcp.pubmed` | mcp_server | npm `@cyanheads/pubmed-mcp-server@2.10.20` (stdio or self-hosted Streamable HTTP) | npm exact + integrity; Node ≥ 24 or Bun ≥ 1.4 | `NCBI_API_KEY`; `NCBI_ADMIN_EMAIL`, `UNPAYWALL_EMAIL` (config, not secrets) | ✔ | ✔ | ✔(h, self-host) | ✔ | ✔ |
| `mcp.biomcp` | mcp_server | PyPI `biomcp-cli==0.9.1` (`biomcp serve` / `serve-http`) or `ghcr.io/genomoncology/biomcp@sha256:...` | PyPI exact + wheel sha256, or image digest | optional `NCBI_API_KEY`, `S2_API_KEY`, `OPENFDA_API_KEY`, `NCI_API_KEY`, `ONCOKB_TOKEN`, `ALPHAGENOME_API_KEY`, `DISGENET_API_KEY` | ✔ | ✔ | ✔(h, self-host) | ✔ (also plugin) | ✔ |
| `skill.biomcp` | skill_bundle | `biomcp skill install <dir>` output | `computedHash` of the installed tree | none | ✔ | ✔ | ✔ | ✔ | ✔ |
| `mcp.edgartools` | mcp_server | PyPI `edgartools[ai]==5.61.1` → `edgartools-mcp` (stdio / `--transport streamable-http`) | PyPI exact + wheel sha256; uv lock | `EDGAR_IDENTITY` ("Name email" — an identity, not a secret) | ✔ | ✔ | ✔(h, self-host) | ✔ | ✔ |
| `skill.edgartools` | skill_bundle | `edgar.ai.install_skill()` output | `computedHash`; tied to the edgartools version | none | ✔ | ✔ | ✔ | ✔ | ✔ |
| `plugin.research-web` (optional) | plugin | composition: `mcp.tavily` + `mcp.firecrawl` + `skill.agent-browser` | digest of member pins | union of members | ✔ | ✔ | ✔ | ✔ | ✔ |
| `hook.pre-tool-use-policy` (template) | hook_script | Mission Control-authored script | content sha256; rendered per host (Claude `hooks.json`; Codex `hooks.json`, which requires per-hash trust) | none | ✔ (middleware) | ? (Cursor hooks not researched) | ? | ✔ | ✔ |

Rejected for seeding: `sec-edgar-mcp` (AGPL-3.0, unauthenticated HTTP), `pubmedmcp` (stale, unlicensed), unscoped `pubmed-mcp-server`, and the cyanheads public endpoint `pubmed.caseyjhand.com` for production (a third-party personal host; acceptable for dev).

Open items (**UNVERIFIED**, need follow-up): the Cursor hooks format; whether Cursor Cloud agents read the repo's `.cursor/mcp.json` / `.agents/skills`; deepagents sandbox `execute` for CLI-based capabilities; the Supabase project's pgvector version; the Voyage env var name; the Firecrawl key-in-path legacy URL.

---

# Citations

All read 2026-10-07.

- [C1] npm registry JSON: `https://registry.npmjs.org/{tavily-mcp,firecrawl-mcp,agent-browser,@cyanheads/pubmed-mcp-server,skills,biomcp,pubmed-mcp-server,mcp-server-pubmed}` (latest + time).
- [C2] PyPI JSON: `https://pypi.org/pypi/{edgartools,biomcp-cli,biomcp,sec-edgar-mcp,pubmedmcp,supabase,deepagents,langchain-mcp-adapters}/json`.
- [C3] Tavily MCP README — https://raw.githubusercontent.com/tavily-ai/tavily-mcp/main/README.md
- [C4] Tavily MCP source — https://raw.githubusercontent.com/tavily-ai/tavily-mcp/main/src/index.ts ; published build https://unpkg.com/tavily-mcp@0.2.22/build/index.js
- [C5] Firecrawl MCP README — https://raw.githubusercontent.com/firecrawl/firecrawl-mcp-server/main/README.md
- [C6] Firecrawl MCP docs — https://docs.firecrawl.dev/mcp-server.md
- [C7] firecrawl-mcp 3.28.2 dist — https://unpkg.com/firecrawl-mcp@3.28.2/dist/index.js
- [C8] agent-browser README — https://raw.githubusercontent.com/vercel-labs/agent-browser/main/README.md
- [C9] agent-browser skill stub — https://raw.githubusercontent.com/vercel-labs/agent-browser/main/skills/agent-browser/SKILL.md
- [C10] cyanheads pubmed-mcp-server README (v2.10.20) — https://raw.githubusercontent.com/cyanheads/pubmed-mcp-server/main/README.md ; GitHub API repo metadata.
- [C11] BioMCP README — https://raw.githubusercontent.com/genomoncology/biomcp/main/README.md
- [C12] BioMCP MCP server reference — https://raw.githubusercontent.com/genomoncology/biomcp/main/docs/reference/mcp-server.md
- [C13] NCBI, "A General Introduction to the E-utilities" — https://www.ncbi.nlm.nih.gov/sites/books/NBK25497/
- [C14] edgartools README — https://raw.githubusercontent.com/dgunning/edgartools/main/README.md
- [C15] edgartools MCP setup — https://raw.githubusercontent.com/dgunning/edgartools/main/docs/ai/mcp-setup.md
- [C16] SEC, Accessing EDGAR Data — https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data
- [C17] edgartools Skills — https://raw.githubusercontent.com/dgunning/edgartools/main/docs/ai/skills.md
- [C18] edgartools MCP tools reference — https://raw.githubusercontent.com/dgunning/edgartools/main/docs/ai/mcp-tools.md
- [C19] sec-edgar-mcp README — https://raw.githubusercontent.com/stefanoamorelli/sec-edgar-mcp/main/README.md
- [C20] Agent Skills specification — https://agentskills.io/specification.md
- [C21] Agent Skills, adding skills support — https://agentskills.io/client-implementation/adding-skills-support.md ; clients https://agentskills.io/clients.md
- [C22] Claude Code skills — https://code.claude.com/docs/en/skills.md
- [C23] Cursor Agent Skills — https://cursor.com/docs/context/skills (→ /docs/skills)
- [C24] Codex skills — https://developers.openai.com/codex/skills.md (→ learn.chatgpt.com)
- [C25] Deep Agents skills — https://docs.langchain.com/oss/python/deepagents/skills.md
- [C26] Vercel `skills` CLI README — https://raw.githubusercontent.com/vercel-labs/skills/main/README.md ; published source https://unpkg.com/skills@1.7.1/dist/cli.mjs
- [C27] Claude Code plugin manifest reference — https://code.claude.com/docs/en/plugins-reference.md
- [C28] Claude Code, create a marketplace — https://code.claude.com/docs/en/plugin-marketplaces.md
- [C29] Claude Code marketplace reference — https://code.claude.com/docs/en/plugins/marketplace-reference.md
- [C30] Claude Code subagents — https://code.claude.com/docs/en/sub-agents.md
- [C31] Claude Code hooks reference — https://code.claude.com/docs/en/hooks.md
- [C32] Codex hooks — https://developers.openai.com/codex/hooks.md (308 → https://learn.chatgpt.com/docs/hooks.md)
- [C33] Codex AGENTS.md guide — https://developers.openai.com/codex/guides/agents-md.md
- [C34] Codex MCP — https://developers.openai.com/codex/mcp.md
- [C35] Codex configuration reference — https://developers.openai.com/codex/config-reference.md
- [C36] Claude Code MCP — https://code.claude.com/docs/en/mcp.md
- [C37] Cursor MCP — https://cursor.com/docs/mcp.md
- [C38] Cursor Cloud Agents / capabilities — https://cursor.com/docs/cloud-agent , https://cursor.com/docs/cloud-agent/capabilities (search excerpts)
- [C39] LangChain MCP — https://docs.langchain.com/oss/python/langchain/mcp.md
- [C40] pgvector README — https://raw.githubusercontent.com/pgvector/pgvector/master/README.md ; tags via GitHub API (v0.8.7 latest)
- [C41] Supabase, Creating Buckets — https://supabase.com/docs/guides/storage/buckets/creating-buckets.md
- [C42] Supabase, Storage Limits — https://supabase.com/docs/guides/storage/uploads/file-limits.md
- [C43] Supabase Python reference (storage) — https://supabase.com/docs/reference/python/storage-createbucket
- [C44] Supabase, Standard Uploads — https://supabase.com/docs/guides/storage/uploads/standard-uploads.md
- [C45] Supabase, Resumable Uploads — https://supabase.com/docs/guides/storage/uploads/resumable-uploads.md
- [C46] Supabase, Storage Access Control — https://supabase.com/docs/guides/storage/security/access-control.md
- [C47] Supabase, Storage helper functions — https://supabase.com/docs/guides/storage/schema/helper-functions.md
- [C48] Supabase, The Storage Schema — https://supabase.com/docs/guides/storage/schema/design.md
- [C49] Supabase, S3 Compatibility — https://supabase.com/docs/guides/storage/s3/compatibility.md
- [C50] Supabase, Hybrid search — https://supabase.com/docs/guides/ai/hybrid-search.md
- [C51] Supabase, HNSW indexes — https://supabase.com/docs/guides/ai/vector-indexes/hnsw-indexes.md
- [C52] PostgreSQL docs, pg_trgm — https://www.postgresql.org/docs/current/pgtrgm.html
- [C53] OpenAI, Vector embeddings — https://developers.openai.com/api/docs/guides/embeddings
- [C54] Voyage AI, Text Embeddings — https://docs.voyageai.com/docs/embeddings (page modified 2026-08-10)
- [C55] BAAI/bge-m3 model card — https://huggingface.co/BAAI/bge-m3/raw/main/README.md
- [C56] nomic-embed-text-v1.5 model card — https://huggingface.co/nomic-ai/nomic-embed-text-v1.5/raw/main/README.md
- [C57] Ollama library — https://ollama.com/library/nomic-embed-text , https://ollama.com/library/bge-m3
