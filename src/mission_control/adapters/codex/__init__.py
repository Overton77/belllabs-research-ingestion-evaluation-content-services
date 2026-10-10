"""The `codex` Lane Profile: Codex app-server (JSON-RPC over the CLI's stdio) on a worker (MP-08).

The pinned protocol is `codex-cli 0.162.0` app-server protocol v2, generated offline with
`codex app-server generate-json-schema` and committed under
`tests/fixtures/provider_frames/codex/schema/` (`PIN.json` carries every digest). The
package is layered as:

- `protocol.py`: the typed subset of that schema the lane speaks, the method registries and
  the limit-refusal classification;
- `transport.py`: the JSON-RPC connection with separated correlation of responses,
  notifications and server-originated requests over any message channel (the CLI's stdio in
  production, an in-process channel in tests);
- `launcher.py`: the subprocess launcher (Linux/WSL workers; a Windows selector loop cannot
  spawn; a CLI other than the pin is refused) and the credential-free child environment
  built through the injected `provider_child_environment`;
- `approvals.py`: the adapter over MP-11's `ApprovalBroker` that binds a pending server
  request (persisted before it is held or awaited) and maps broker replies into native
  responses;
- `frames.py`: app-server events to `LaneFrame`s, cursors, closing facts and occupancy;
- `harness.py`: the `SessionLane` (`DispatchReconcilingLane`, `SteeringLane`,
  `TurnTextStaging`, MP-12 `CompactingLane` / `ContextOccupancyLane`) over threads, turns
  and items;
- `compose.py`: `compose_codex_local` for the integrator's worker composition;
- `describe.py`: the declared describe matrix, checked against the pin (`qualified=False`).

Nothing here logs in, starts a model turn on its own or reads a credential value.
"""
