---
type: Specification
title: "Manifest, environment and materialization contracts"
description: "Proposed v2 authoring additions, provider-hosted environment qualification, explicit worktree policy and reproducible skills, MCP, plugins and hooks."
tags: [mission-control, environments, manifest, worktrees]
---

# Environments and materialization

Reuse the existing catalog, bundle custody, Host Projection, capability pins and materialization receipts. Extend `Environment` rather than creating a second configuration system. The new v2 authoring surface adds explicit execution environment, authentication profile, workspace policy, continuation policy and required features. Existing capabilities/agents/hooks/plugins syntax and inheritance rules remain recognizable.

## Proposed authoring fragment

This is an **environment fragment for the proposed `mission/v2` schema**, not a runnable v1 mission. The names in angle brackets are unresolved deployment values. The schema does not exist yet; MP-01 owns it and MP-02 exports checked examples for all profiles.

Copyable design fragments: [Claude local](examples/claude-local.environment.yml), [Claude hosted](examples/claude-cloud.environment.yml), [Codex hosted](examples/codex-cloud.environment.yml), [Cursor hosted](examples/cursor-cloud.environment.yml). These files are YAML syntax examples, not executable manifests or qualification fixtures.

```yaml
environment:
  lane: claude_agent_sdk
  model: {profile: "<admitted-model-profile>"}
  auth: {profile: "<owner-local-auth-profile>"}
  execution_environment:
    kind: local_workspace
    profile: "<admitted-local-host-profile>"
  workspace:
    repo: {url: "https://github.com/<owner>/<repo>", ref: "<branch>"}
    policy:
      mode: managed_worktree
      reuse: within_run
      dirty_input: reject
      cleanup: retain_until_artifacts_registered
    skills: [{pin: "<skill-version-pin>"}]
  capabilities:
    - {pin: "<mcp-server-version-pin>"}
  agents:
    - {pin: "<subagent-profile-version-pin>"}
  hooks:
    - pin: "<hook-script-version-pin>"
      events: [before_tool]
      fail_closed: true
  plugins: [{pin: "<plugin-version-pin>"}]
  continuation:
    native_compaction: preferred
    fallback: sealed_checkpoint
    soft_context_ratio: 0.70
    hard_context_ratio: 0.85
    max_session_turns: 12
    max_transfers: 4
  requires:
    controls: [queue_instruction, cancel, request_continuation]
    approvals: [workflow_gate, governed_mcp]
    observation: [terminal_result, subordinate_lifecycle]
  budget: {tokens: 200000, wall_clock: 1h, tool_calls: 100}
```

For a provider-hosted profile replace only the relevant selections:

```yaml
environment:
  lane: codex_cloud
  auth: {profile: "<codex-cloud-account-profile>"}
  execution_environment:
    kind: provider_hosted
    provider: openai
    environment_ref: "<published-provider-environment-id>"
    expected_revision: "<provider-revision-or-attested-digest>"
    setup: {pin: "<setup-bundle-version-pin>"}
  workspace:
    repo: {url: "https://github.com/<owner>/<repo>", ref: "<branch>"}
    policy:
      mode: provider_workspace
      reuse: within_session
      dirty_input: reject
      cleanup: retain_until_artifacts_registered
```

`provider` must match the lane. A local `path` is invalid for provider-hosted work. `managed_worktree` is not valid for a hosted provider unless its adapter can actually allocate and attest it. A hosted workspace uses native per-task isolation and records branch/commit; it need not literally be a Git worktree.

An alias may resolve during compile, but the committed binding pins its exact asset version/digest. The compiler can parse and resolve the proposal without contacting a paid provider; it cannot issue a launch ticket until required environment/readiness evidence exists. Distinguish structural validity, resolved binding and launch readiness.

Inheritance must preserve current replacement/overlay semantics. A child cannot weaken a parent's required approvals, widen egress/secret access, remove Kernel Hooks or increase its budget ceiling. Switching lane revalidates the full inherited capability set. Explicit empty lists may clear optional capabilities only; they cannot clear mandatory policy.

## Environment profiles

Persist the requested profile and an immutable resolved binding containing repository identity, requested branch/ref, resolved commit, provider/account/environment identity, setup artifact digests, runtime/toolchain pins, egress policy, credential references, storage policy and timeout ceilings. Native immutable revision unavailable: store an observed configuration digest, method/time of verification and qualified freshness policy; require re-attestation on change. Never invent a provider revision API.

Provider environment creation/publication is a separately admitted operation from starting a task. Prefer binding existing environments initially. A setup file in the repo is insufficient proof that a hosted product loaded it. Preparation must report effective config, executed setup phase, checks, artifacts and omissions.

| Target | Materialization path | Required proof |
| --- | --- | --- |
| Local providers | Dedicated clone/worktree; pinned bundle extraction and config rendering before session start | Files/digests, executable/interpreter availability, MCP readiness, expected cwd, exact CLI/SDK versions |
| Cursor-hosted | Supported repo/setup surface and agent/run configuration; project hooks only where supported | Repo commit/branch, reachable callback/MCP endpoints, hydrated packet, actual native configuration |
| Claude-hosted | Existing cloud environment plus documented repository/setup mechanism | Launch selection and setup attestations; per-session configuration injection remains a hosted qualification question |
| Codex-hosted | Published cloud environment plus repository/install/start configuration | Environment identity and setup revision/digest; per-task overrides require separate evidence |

Current Codex documentation distinguishes install scripts and start skills in published cloud environments; older legacy environment guidance must not be silently substituted. [Current cloud environments](https://learn.chatgpt.com/docs/environments/cloud-environments). Claude's cloud environment is likewise separate from its local SDK configuration. [Claude cloud environments](https://code.claude.com/docs/en/claude-code-on-the-web).

For ephemeral config that a hosted provider can only read from Git, materialize into an explicitly admitted integration branch/commit using the existing Git capability. Never push secrets or modify the user's branch merely to inject config. A signed artifact bootstrap is acceptable only if the provider setup phase can fetch it before agent work and that ordering is proven. No first-prompt instruction asking the agent to install its own mandatory controls counts as enforcement.

## Worktrees and snapshots

Mission Control allocates the workspace; the agent does not choose whether isolation exists. Default local policy: managed worktree from a dedicated clone under an admitted workspace root, pinned base commit and unique branch keyed to run/lease. Validate resolved paths remain within that root; reject symlink escape and branch collisions. Do not operate on the developer's primary dirty checkout.

Reuse within one run is explicit and lease-fenced. Parallel writable operations require distinct worktrees. Subagents share a parent checkout only where policy declares that coordination model; otherwise give them read-only access or separate leases. Git worktree isolation is not an OS sandbox and does not isolate repository metadata or network credentials.

Snapshot base commit, staged and unstaged changes, tracked deletions, untracked included files, file modes, relevant LFS/submodule identities and a manifest digest. Record exclusion rules. A plain `git diff` cannot capture all untracked/binary content. Native cloud branch refs must be recorded as actual immutable commit artifacts; the current Cursor restore convention `branch:<branch>@<sha>` needs a producer, not just a parser.

Conversation fork + workspace snapshot + Context Packet produce a new forked execution. Never infer one from another. Cleanup is idempotent and bounded to the owned lease after artifact registration and liability settlement; preserve failed work for the retention interval. No automatic force-delete of dirty user worktrees.

## Capabilities and hook scripts

Skills are full directory bundles, not pasted descriptions. Keep relative resources/scripts, verify content-addressed custody and reject archive traversal/symlink escape. Reuse current roots: `.claude/skills`, `.agents/skills`, `.cursor/skills`. Instruction and subagent files use the existing renderer, then are tested against actual runtime discovery. A generated file does not establish that a provider loaded it.

Plugins remain pinned compositions in the Mission Control catalog. Expand their dependencies deterministically and reject cycles, name collisions or incompatible host support. Do not assume Claude, Codex and Cursor plugin formats are interchangeable; use native plugin loading only for a separately qualified exact format. Provider auto-discovery of ambient plugins/settings is disabled or explicitly scoped and recorded.

MCP materialization distinguishes local stdio launchers and remotely reachable HTTP endpoints. `localhost` in a provider-hosted workspace is not this computer. Resolve OAuth/token references in a scoped server-side broker; never forward the Mission Control bearer token. Revalidate grants at tool execution, not just compile. A capability's initialization must report discovered tool schemas and elicitation support before admission.

Hook Script assets carry interpreter, script artifact/version/digest, argv template, event/matcher, timeout, allowed environment refs, output schema and failure policy. Authoring may accept source during capability publication, but mission launch consumes a pin. Run scripts with argv and JSON stdin; do not interpolate tool arguments into shell command text. Persist bounded/redacted results as frames.

Separate host command hooks, in-process SDK callbacks and Mission Control subscription delivery. A native hook mapping also records enforcement/observe-only semantics, timeout behavior, supported tool families and language/version. Existing `_CODEX` copied from `_CLAUDE` and Python-vs-TypeScript Claude hook assumptions must be replaced with evidence-backed entries. A generic `defer` return is not portable.

Kernel controls must live outside agent-writable configuration where possible, backed by restricted credentials and the governed MCP gateway. Native hooks are useful interception points but not a universal security boundary: hosted tools and specialized paths may bypass them. Required write approvals therefore need an enforceable gateway/sandbox boundary or typed rejection.

## Authentication, subscription usage and limits

Record `auth_profile`, billing mode, account identity reference and feature entitlement separately from model configuration. Never copy a user's CLI credential directory into a cloud repo or issue. Local personal CLI sign-in, API credentials, enterprise access tokens and provider cloud accounts are distinct routes.

The owner's Claude subscription is relevant to prioritization, but the SDK documentation directs third-party products to approved authentication methods and does not promise that arbitrary SDK usage consumes Max allowance. Qualify the permitted owner-local route separately; do not silently fall back to billable API usage. [Claude SDK overview](https://code.claude.com/docs/en/agent-sdk/overview). Likewise, Codex's documented CLI authentication modes do not establish every account's model or hosted-product entitlement. [CLI reference](https://learn.chatgpt.com/docs/cli/reference).

Separate token/work/time ceilings from monetary accounting. Track spent, reserved, estimated and unknown; subscription quota is not a dollar cost of zero. Rate-limit resets can become durable waits bounded by mission deadline. If cost cannot be observed, enforce known token/turn/time bounds and disclose the unknown billing dimension. Never claim a hard dollar cap without enforceable provider/account limits.
