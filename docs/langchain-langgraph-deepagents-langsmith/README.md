# LangChain / LangGraph / Deep Agents / LangSmith ecosystem notes

This directory is a maintained, project-local knowledge base for design and implementation decisions that span LangChain, LangGraph, Deep Agents, and LangSmith. It is intentionally concise: the official documentation remains authoritative, while these notes preserve the implications for this research-ingestion system.

## Status

- Started: 2026-08-06
- First focus: LangGraph streaming API, typed v2 format, custom streaming, and subgraph propagation.
- Source posture: record the URL, package/version assumptions, and an "as checked" date for every version-sensitive note.

## Index

- [LangGraph streaming and custom LLM integration](LANGGRAPH_STREAMING.md) - implementation guidance and migration reference.
- [Documentation process](DOCUMENTATION_PROCESS.md) - how this knowledge base should grow.

## Working conventions

1. Prefer `version="v2"` for newly written stream consumers.
2. Treat `StreamPart["type"]` and `StreamPart["ns"]` as the stable routing interface at application boundaries.
3. Never expose internal model output merely because it is available in a graph stream; use `nostream` or a distinct custom event contract when appropriate.
4. Include `subgraphs=True` when a parent-level client must receive events from nested agents/graphs.
5. Keep external-provider adapters behind a custom-event schema rather than leaking provider-specific chunks into UI or API consumers.

## Primary source

- LangGraph: [Streaming](https://docs.langchain.com/oss/python/langgraph/streaming), checked 2026-08-06.
