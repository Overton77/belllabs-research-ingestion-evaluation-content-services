# Coordinator protocol

Use this order:

```text
coordinator_bootstrap
search_capabilities(kinds=["workflow_type"])
get_capability / exact contract resources
search_capabilities(kinds=["prompt","skill","mcp_server","mcp_tool","agent_profile"])
validate_workflow_design
prepare_workflow_launch
launch_workflow
get_workflow_result
```

Call external discovery only between internal search and design validation, and only for a demonstrated gap.

Every response uses:

```text
ok
schema_version
correlation_id
data?
error? { code, message, retryable, details }
```

Preserve the correlation ID when reporting failures. Never expose dependency stack traces or secret-bearing messages.

Search rank is retrieval evidence. Selection requires an exact ref, source digest match, selectable authorization state, and positive compatibility/availability reasons.

Launch is a separate mutation boundary. A prepared ticket is caller-, tenant-, proposal-, approval-, policy-, and environment-bound; it cannot be edited into a different run.
