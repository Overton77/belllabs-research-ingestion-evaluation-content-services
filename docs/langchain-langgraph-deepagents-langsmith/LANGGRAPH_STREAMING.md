# LangGraph streaming and custom LLM integration

**Status:** initial consolidated note  
**Primary source:** [LangGraph streaming](https://docs.langchain.com/oss/python/langgraph/streaming)  
**Checked:** 2026-08-06

## Project runtime baseline

The current project dependency declaration pins Python `>=3.12`, `langgraph==1.2.10`, `langchain==1.3.14`, `deepagents==0.7.4`, and `langsmith==0.10.15`. Therefore the documented v2 stream format and the Python 3.11 async compatibility workaround are relevant as migration context, but the Python < 3.11 workaround is not expected to apply to this project's supported runtime.

## Decision summary

For new code, prefer LangGraph's event-streaming API when its separate typed projections (`messages`, `values`, `subgraphs`, and output) fit the consumer. Use the graph `stream()` / `astream()` stream-mode API when a consumer needs direct graph-runtime events such as `updates`, `custom`, `checkpoints`, `tasks`, or `debug`.

When using the stream-mode API, new consumers should pass `version="v2"`. It avoids format changes based on the selected modes or subgraph setting and gives every event the same envelope:

```python
{
    "type": "values" | "updates" | "messages" | "custom" | "checkpoints" | "tasks" | "debug",
    "ns": (),  # namespace path; non-empty for an event emitted in a subgraph
    "data": ...,  # payload determined by `type`
}
```

`StreamPart` is a discriminated union from `langgraph.types`; branch on `part["type"]` so type checkers can narrow `part["data"]` safely.

## v1 to v2 migration map

| Scenario | v1/default behavior | v2 behavior |
| --- | --- | --- |
| One mode | Raw payload | `StreamPart` envelope |
| Multiple modes | `(mode, data)` tuple | Same envelope; route on `part["type"]` |
| Subgraphs | `(namespace, data)` tuple | Same envelope; source is `part["ns"]` |
| Modes plus subgraphs | `(namespace, mode, data)` tuple | Same envelope |
| `invoke` / `ainvoke` | Plain state dictionary | `GraphOutput` with `.value` and `.interrupts` |
| Interrupt | `__interrupt__` in a dictionary | `interrupts` on stream values parts or `GraphOutput.interrupts` |

The v2 format requires LangGraph 1.1 or later. This project currently pins LangGraph 1.2.10, so it is an available runtime capability; re-check this note whenever dependencies change.

## Stream modes

| Mode | Intended payload |
| --- | --- |
| `values` | Complete state after each step |
| `updates` | State deltas returned by each node |
| `messages` | `(message_chunk, metadata)` emitted by LLM calls |
| `custom` | Application-defined data emitted with a writer |
| `checkpoints` | Checkpointer state events |
| `tasks` | Task start/finish events, results, and errors |
| `debug` | Checkpoint/task data plus diagnostic metadata |

`messages` may be emitted even where the model call uses `.invoke()` rather than `.stream()`.

## Recommended consumer pattern

```python
for part in graph.stream(
    inputs,
    stream_mode=["updates", "messages", "custom"],
    subgraphs=True,
    version="v2",
):
    if part["type"] == "messages":
        message_chunk, metadata = part["data"]
        # Render only client-safe token content.
    elif part["type"] == "custom":
        # Validate the event against the application's custom-event contract.
        handle_progress(part["data"], namespace=part["ns"])
    elif part["type"] == "updates":
        persist_graph_delta(part["data"], namespace=part["ns"])
```

Do not couple a client to tuple layouts from v1. At a boundary (SSE/WebSocket/API), map the `StreamPart` envelope to an explicit, versioned application event schema.

## Custom data and arbitrary LLM clients

Use `get_stream_writer()` inside a synchronous node or tool to emit application-defined events, and include `"custom"` in `stream_mode` to receive them.

```python
from langgraph.config import get_stream_writer

def call_arbitrary_model(state):
    writer = get_stream_writer()
    for provider_chunk in custom_client.stream(state["prompt"]):
        writer({"type": "provider_token", "content": provider_chunk.text})
    return {"result": "completed"}
```

This is the adapter route for an LLM API that does not implement the LangChain chat-model interface. Normalize provider chunks before emitting them; do not make downstream consumers understand provider SDK objects.

### Async and Python versions

On Python earlier than 3.11, context-variable propagation does not support the usual async streaming path. Explicitly pass `RunnableConfig` through `ainvoke()` calls, and do not use `get_stream_writer()` in async nodes/tools. Instead, declare a typed writer argument and use that supplied writer. Prefer Python 3.11+ for new async graph services.

## Filtering and privacy controls

- Tag model invocations and use message-event metadata to select a particular model or graph node.
- Add the `nostream` tag to model invocations whose output is needed internally but must not be emitted in `messages` mode.
- `nostream` is useful for structured/internal reasoning stages and for avoiding duplicate client output when the user-facing content is sent through `custom` events.

## Nested agents and subgraphs

Pass `subgraphs=True` to a parent graph stream when clients need output from nested graphs. In v2, the root event has `ns == ()`; a nested event identifies its invocation path in `ns` (for example `("agent:<task_id>",)`).

This is particularly important for agent composition: `create_agent(...)` returns a compiled graph, so using it as a parent node creates a subgraph. Parent `stream_mode="messages"` does not include inner-agent tokens unless `subgraphs=True`; streaming the agent directly does.

## Implementation checklist

- [ ] Verify installed LangGraph version supports v2 before adopting it in runtime code.
- [ ] Use `version="v2"` at every new graph streaming boundary.
- [ ] Define and validate a client-safe custom event schema.
- [ ] Add `subgraphs=True` for parent-level nested-agent observability.
- [ ] Apply `nostream` to internal-only model calls.
- [ ] For async runtime, standardize on Python 3.11+; otherwise pass config/writer explicitly.
- [ ] Link traces and relevant run identifiers to LangSmith observability without treating raw token streams as an audit log.
