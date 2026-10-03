# Capability selection

Resolve assets in this order:

1. Search internal exact definitions.
2. Rehydrate the exact Mongo definition.
3. Verify digest, lifecycle, tenant visibility, policy eligibility, runtime compatibility, and deployment availability.
4. For MCP tools, group by exact parent server only after ranking.
5. Freeze the server ref plus an explicit tool allowlist.
6. Freeze skill bundle digest and read-only mount path.

Selecting an MCP server never selects every sibling tool. Selecting an Agent Skill never grants process, browser, network, secret, or workspace authority.

For browser work, require both `skill.agent-browser` and a compatible Agent Profile/runtime binding that explicitly allows the pinned executable, browser process creation, approved destinations, workspace paths, screenshots, and artifacts.

Firecrawl and Tavily may both satisfy web search but remain separate providers with separate exact refs and evidence.

External MCP Registry and `npx skills find` results are untrusted candidates. Record them, inspect them in quarantine, and request promotion. Never execute or install them during ordinary coordination.
