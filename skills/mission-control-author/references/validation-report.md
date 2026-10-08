# Reading a Validation Report

`missionctl mission compile FILE --json` returns `mc.manifest_resolution.v1` wrapped in the
common response envelope. Nothing launches during compile.

```json
{
  "schema_version": "mc.manifest_resolution.v1",
  "manifest_digest": "sha256:…",
  "definition_digest": "sha256:…",
  "blockers": [
    {"code": "CAPABILITY_UNAVAILABLE", "pointer": "/mission/environment/capabilities/0",
     "message": "search 'pubmed literature retrieval' (kind mcp_server) has no admitted hit for lane deep_agents",
     "candidates": []}
  ],
  "warnings": [
    {"code": "BUDGET_NEAR_CEILING", "pointer": "/mission/environment/budget/usd", "message": "…"}
  ],
  "resolutions": [
    {"pointer": "/mission/environment/capabilities/1", "search": "web search with extraction",
     "kind": "mcp_server", "resolved": "mcp.tavily@0.2.22#sha256:…", "score": 0.81,
     "rank_provenance": {"lexical": 1, "vector": 2}, "host_support": ["deep_agents", "cursor_local", "cursor_cloud"]}
  ],
  "inheritance": [
    {"node": "collect", "key": "budget.usd", "mission": 25, "node_value": 10, "effective": 10}
  ],
  "lane_support": [
    {"node": "collect", "behavior": "goal_loop", "lane": "deep_agents", "supported": true}
  ],
  "unsupported_on_lane": [
    {"pointer": "/mission/environment/hooks/0", "event": "session_start", "lane": "cursor_cloud"}
  ]
}
```

## How to act

| Field | Means | Do |
| --- | --- | --- |
| `blockers[]` | The definition cannot be committed | Fix the manifest at `pointer`. For `CAPABILITY_UNAVAILABLE` search the catalog with `mission-control-catalog`; a quarantined discovery is not a fix. |
| `warnings[]` | Committed if you proceed | Show each to the human before submit. |
| `resolutions[]` | Exact pin each `search:` became | Copy into the manifest as `pin:` when the human wants it frozen; otherwise the same resolution is stored with the revision. |
| `inheritance[]` | Effective environment per node | Confirm narrowing is intended; widening never appears here because it is a blocker. |
| `lane_support[]` | Behavior allowed on the chosen lane | A `false` row appears as blocker `UNSUPPORTED_BEHAVIOR`. |
| `unsupported_on_lane[]` | Hook events the lane cannot fire | Decide whether the mission still meets its side-effect policy without that event. |

Blocker codes you will see: `INVALID_DEFINITION`, `CAPABILITY_UNAVAILABLE`, `CAPABILITY_DRIFT`,
`UNSUPPORTED_BEHAVIOR`, `ENVIRONMENT_WIDENS_AUTHORITY`, `BUDGET_EXHAUSTED`,
`AUTHORITY_DENIED`, `OBJECTIVE_UNCOVERED` (a goal with no node serving it), `UNREACHABLE_NODE`.

The report is evidence of what the compiler resolved at that moment; the materializer rechecks
pins, grants and versions when a run starts and refuses drift.
