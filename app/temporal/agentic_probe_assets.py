"""Locked, minimal copies of the upstream skills used by the live sandbox probe."""

from __future__ import annotations

TAVILY_BEST_PRACTICES_SKILL = b"""\
---
name: tavily-best-practices
description: Use the Tavily CLI for web search, extraction, crawling, and research.
---
# Tavily

Tavily is a search API designed for LLMs and real-time web data. Use the
installed `tvly` CLI, which reads `TAVILY_API_KEY` from the environment.

Use `tvly search` for search results, `tvly extract` for specific URLs, `tvly
crawl` for site-wide extraction, `tvly map` for URL discovery, and `tvly
research` for an end-to-end cited report. Never put credentials in a workspace
file or final response.
"""

VERCEL_AGENT_BROWSER_SKILL = b"""\
---
name: agent-browser
description: Browser automation CLI for AI agents.
allowed-tools: Bash(agent-browser:*), Bash(npx agent-browser:*)
---
# agent-browser

Use agent-browser for browser automation, including web navigation, form
interaction, screenshots, extraction, and testing. Before browser commands,
load the installed command's current workflow with:

    agent-browser skills get core

Install with `npm i -g agent-browser && agent-browser install` only when the
environment explicitly permits package installation and network access.
"""
