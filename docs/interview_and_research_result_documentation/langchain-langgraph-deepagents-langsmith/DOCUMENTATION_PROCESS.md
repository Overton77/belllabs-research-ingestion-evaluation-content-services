# Documentation process

## Purpose

Capture ecosystem behavior that affects this system before it becomes tribal knowledge or an implementation surprise. These notes are not a replacement for version-pinned API documentation.

## Entry template

Each substantial entry should include:

- **Claim** - the concise behavior or decision.
- **Scope** - package, version threshold, Python/runtime conditions, and affected component.
- **Recommended use here** - the implementation rule for this repository.
- **Example or test** - a minimal executable pattern, or a link to a local test/spike.
- **Source** - primary documentation URL and date checked.
- **Open questions** - anything that requires validation against the installed dependency lock.

## Initial backlog

- Map current LangGraph, LangChain, Deep Agents, and LangSmith versions from `pyproject.toml` and lockfiles to these notes.
- Establish one project-level custom stream-event schema for client-safe progress, status, and provider-token events.
- Add a streaming integration test that covers root events, nested-agent events, `nostream`, and async behavior.
- Document Deep Agents composition and its graph/subgraph boundary.
- Document LangSmith tracing, run metadata, and links between stream events and traces.

## Review cadence

Review an entry whenever upgrading a related package or changing a graph/client streaming boundary. Add the exact dependency version validated in code-oriented entries.
