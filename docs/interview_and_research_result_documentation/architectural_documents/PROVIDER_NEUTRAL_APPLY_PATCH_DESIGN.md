# Provider-neutral `apply_patch` for Deep Agents

Status: analysis and implementation sketch  
Date: 2026-08-06  
Initial target: `deepagents==0.7.4` in this repository; upstream inspected at `0.7.5`  
OpenAI source inspected: `openai-agents==0.19.4`, commit `f3b6c617853880b6dbad16b58ff9d071d5756afb`

## Decision

Build `apply_patch` as a framework-neutral patch engine with thin model and storage adapters. Do not copy the linked OpenAI tool class wholesale into Deep Agents. That class mixes an OpenAI custom-tool grammar, OpenAI approval callbacks, and an OpenAI sandbox session. The reusable part is the V4A patch language and its text transformation kernel.

Ship three model-facing variants that compile into one canonical `PatchPlan`:

1. `v4a_freeform`: raw `*** Begin Patch` input for models/providers with a free-form custom tool and optional grammar constraints.
2. `v4a_structured`: a normal function tool with `{patch, base_revision?, dry_run?}`. This is the portable Deep Agents default because every supported model already needs ordinary tool calling.
3. `operations_structured`: typed create/update/delete operations for models that perform poorly with a long patch envelope. Keep Deep Agents' exact-string `edit_file` available as the lowest-complexity fallback.

Select a variant from evaluation results, not from provider names. A model profile should record syntax validity, exact-apply rate, stale-context recovery, unnecessary rewrite rate, latency, and token cost.

## What OpenAI's implementation actually contains

There are two related OpenAI surfaces:

- `ApplyPatchTool` in `agents.tool` is an OpenAI Responses hosted-tool type. The SDK receives `apply_patch_call` items and dispatches canonical `ApplyPatchOperation` objects to a host-defined `ApplyPatchEditor`.
- The linked `SandboxApplyPatchTool` is a `CustomTool` bundled by the sandbox `Filesystem` capability. It supplies a Lark grammar and prose prompt, parses raw V4A or JSON input, adapts operation-level approval callbacks to the raw custom-tool callback, and delegates storage to `WorkspaceEditor`.

The linked file is therefore an adapter, not the patch algorithm. The dependency chain is:

```text
model tool call
  -> grammar/envelope parser (sandbox/capabilities/tools/apply_patch_tool.py)
  -> ApplyPatchOperation (editor.py)
  -> WorkspaceEditor (sandbox/apply_patch.py)
  -> apply_diff V4A text transformer (apply_diff.py)
  -> BaseSandboxSession read/write/rm/mkdir + workspace path policy
```

### V4A matching behavior

For updates, the transformer normalizes CRLF to LF for matching, detects the output newline style, parses hunks into delete/insert chunks, and searches forward from a cursor. Context matching tries, in order:

1. exact lines;
2. lines with trailing whitespace removed;
3. lines with leading and trailing whitespace removed.

`@@ <anchor>` advances the cursor to an exact anchor, then to a stripped-whitespace anchor. `*** End of File` first attempts to place context at the actual end, then falls back to a forward search. Chunks are rejected if they overlap. Create-file diffs accept only `+` lines.

Important consequences:

- It chooses the first forward match; it does not reject ambiguous context.
- The accumulated `fuzz` score is not exposed to the caller or used as a policy gate.
- A missing anchor does not fail by itself; following context may still match elsewhere.
- The prose requires relative paths, while `WorkspaceEditor` accepts an absolute path if the sandbox path policy maps it inside the workspace.
- Create writes can overwrite an existing file.
- Multi-operation application is sequential. A failure in operation N leaves operations 1..N-1 committed.
- A move writes the destination and then deletes the source; it is not an atomic rename.
- There is no base revision, compare-and-swap, idempotency key, transaction, rollback journal, or post-write digest receipt.

These are not reasons to discard V4A. They are reasons to place it inside a stronger mutation lifecycle.

## Canonical contracts

Keep parsing, planning, policy, and persistence separate:

```python
from dataclasses import dataclass
from typing import Literal, Protocol

OpKind = Literal["create", "update", "delete", "move"]

@dataclass(frozen=True)
class PatchOp:
    kind: OpKind
    path: str
    diff: str | None = None
    move_to: str | None = None

@dataclass(frozen=True)
class FileVersion:
    path: str
    exists: bool
    content: str | None
    revision: str | None       # preferably backend ETag/version; otherwise sha256(content)

@dataclass(frozen=True)
class PlannedWrite:
    path: str
    before: FileVersion
    after_content: str | None  # None means delete

@dataclass(frozen=True)
class PatchPlan:
    patch_id: str
    operations: tuple[PatchOp, ...]
    writes: tuple[PlannedWrite, ...]
    fuzz_score: int

@dataclass(frozen=True)
class PatchReceipt:
    patch_id: str
    status: Literal["applied", "dry_run", "rejected", "failed", "rolled_back"]
    changed_paths: tuple[str, ...]
    before_revisions: dict[str, str | None]
    after_revisions: dict[str, str | None]
    atomicity: Literal["transactional", "snapshot", "journal_best_effort"]

class PatchStore(Protocol):
    async def read_version(self, path: str) -> FileVersion: ...
    async def commit(self, plan: PatchPlan) -> PatchReceipt: ...
    async def receipt(self, patch_id: str) -> PatchReceipt | None: ...
```

`PatchStore.commit` is the portability boundary. Adapters may use a native transaction, sandbox snapshot, state update, object-store conditional writes, or a local-filesystem journal. A backend that cannot provide rollback must say `journal_best_effort`; the tool must not imply atomicity.

## Planning and commit lifecycle

```text
RECEIVE
  -> PARSE (syntax only)
  -> NORMALIZE (canonical virtual paths and operations)
  -> PREFLIGHT (read every source and destination version)
  -> PLAN (apply all diffs in memory; no writes)
  -> POLICY (path rules, limits, symlinks, fuzz/ambiguity, destructive ops)
  -> APPROVAL (show normalized plan and diff summary)
  -> REVALIDATE (versions/locks immediately before mutation)
  -> COMMIT (native transaction, snapshot, or rollback journal)
  -> VERIFY (re-read and compare expected digests)
  -> RECEIPT + AUDIT
```

Rules:

- Parse and plan the entire patch before the first write.
- Require create targets to be absent unless `overwrite=true` is explicit and policy-approved.
- Require update/delete source revisions to match preflight. Require move destinations to be absent by default.
- Resolve and re-check symlinks/containment at commit time, not only at parse time.
- Serialize mutations per workspace. Use backend CAS or a distributed lock when agents can run in different processes.
- Derive `patch_id` from tenant/workspace identity, normalized patch bytes, and declared base revisions. Store receipts for idempotent replay.
- Bound operation count, patch bytes, files changed, individual file size, aggregate bytes, and allowed encodings.
- Treat generated patches as untrusted input. Never let patch paths select an unrestricted host filesystem.
- Approval occurs after planning, so the reviewer sees actual paths, creates/deletes/moves, fuzz, and size. An edited approval restarts parsing and preflight.

## Deep Agents integration sketch

Deep Agents already routes its filesystem tools through one `BackendProtocol` instance. The first integration should capture that same backend and expose an ordinary structured tool. This works with any LangChain chat model that supports tool calling.

```python
from pydantic import BaseModel, Field
from langchain_core.messages import ToolMessage
from langchain_core.tools import StructuredTool
from langchain.tools import ToolRuntime

class ApplyPatchArgs(BaseModel):
    patch: str = Field(description="V4A patch beginning with *** Begin Patch")
    base_revision: str | None = Field(
        default=None,
        description="Optional workspace/base revision used to reject stale patches",
    )
    dry_run: bool = Field(default=False, description="Plan and validate without committing")

def make_apply_patch_tool(service: "PatchService") -> StructuredTool:
    async def apply_patch(
        patch: str,
        base_revision: str | None,
        dry_run: bool,
        runtime: ToolRuntime,
    ) -> ToolMessage:
        try:
            receipt = await service.apply(
                raw_patch=patch,
                base_revision=base_revision,
                dry_run=dry_run,
                call_id=runtime.tool_call_id,
            )
        except PatchProblem as exc:
            # Return a model-correctable tool error; do not abort the graph.
            return ToolMessage(
                name="apply_patch",
                tool_call_id=runtime.tool_call_id,
                status="error",
                content=exc.model_message(),
                artifact=exc.as_dict(),
            )
        return ToolMessage(
            name="apply_patch",
            tool_call_id=runtime.tool_call_id,
            status="success",
            content=receipt.model_summary(),
            artifact=receipt.as_dict(),
        )

    return StructuredTool.from_function(
        name="apply_patch",
        description=V4A_TOOL_DESCRIPTION,
        coroutine=apply_patch,
        args_schema=ApplyPatchArgs,
        infer_schema=False,
    )

backend = FilesystemBackend(root_dir=workspace, virtual_mode=True)
patch_service = PatchService(store=DeepAgentsPatchStore(backend), policy=policy)

agent = create_deep_agent(
    model=model,
    backend=backend,
    tools=[make_apply_patch_tool(patch_service)],
    interrupt_on={"apply_patch": True},
    checkpointer=checkpointer,
)
```

This is only the wiring. `DeepAgentsPatchStore` must fully page reads, preserve backend path semantics, check `ReadResult.encoding`, map `WriteResult`/`DeleteResult` errors into typed errors, and implement the strongest commit strategy supported by the concrete backend.

Deep Agents' declarative `permissions` protect built-in filesystem tools, not arbitrary custom tools. The patch service must enforce the same effective policy itself, ideally through a public shared policy interface contributed upstream. HITL is a second control and is not a replacement for path authorization.

For OpenAI models, a later provider adapter may expose the same service as a grammar-constrained free-form custom tool. Do not fork the engine or storage semantics. For weaker tool callers, expose `operations_structured` or retain `edit_file`; only the codec changes.

## Recovery matrix

| Failure | Mutation state | Response and recovery |
|---|---|---|
| Invalid envelope/hunk prefix | none | Return line/column, expected token, and one minimal valid example. Model may retry. |
| Path escape, denied route, symlink escape | none | Non-retryable for that path. Return allowed virtual roots without host paths. |
| Source missing/create target exists | none | Return typed precondition failure and current path state. Re-read/re-plan. |
| Invalid or ambiguous context | none | Return hunk index, sanitized nearby candidates, and instruction to read the file. Never guess under strict mode. |
| Fuzzy whitespace match | none before approval | Report score and candidate location. Auto-apply only below configured risk threshold. |
| Revision conflict after approval | none | Return `stale_revision`; refresh files and regenerate. Do not replay old output. |
| Transient backend failure before commit | none | Retry with bounded exponential backoff. |
| Timeout with unknown outcome | unknown | Query receipt by `patch_id`, then reconcile expected digests; never blindly replay. |
| Failure during journal commit | partial | Roll back from captured versions; verify rollback; quarantine workspace if verification fails. |
| Post-write digest mismatch | committed but invalid | Roll back/snapshot-restore, emit high-severity audit event, block further writes. |
| Approval rejection | none | Return rejection as a normal tool outcome; do not disguise it as parser failure. |

Keep framework retries separate from model repair. Transport timeouts and service unavailability may be automatically retried. Syntax, stale context, or a denied path should return structured evidence to the agent for a new decision.

## Test and evaluation gates

Unit/golden corpus:

- add/update/delete/move; multi-file patches; empty files;
- LF/CRLF/mixed-newline and missing-final-newline cases;
- exact, trailing-whitespace, stripped-whitespace, EOF, anchor, repeated/ambiguous contexts;
- malformed envelopes, missing hunks, overlapping hunks, invalid prefixes;
- absolute paths, `..`, Windows drive/UNC paths, Unicode normalization, reserved names, symlinks;
- binary/non-UTF-8, oversized files/patches, destination collision;
- upstream compatibility corpus pinned to the OpenAI commit.

Concurrency/fault injection:

- two agents patch the same revision;
- process death after each commit step;
- timeout after backend accepted a write but before response;
- snapshot/rollback failure;
- duplicate `patch_id` replay;
- composite backend move crossing routes;
- supervisor and subagent writing the same workspace.

Model evaluations:

- first-attempt syntax validity;
- exact task success and unintended-change rate;
- recovery success after a deliberately stale read or context mismatch;
- full-file rewrite frequency;
- tool calls, tokens, latency, and approval burden.

Never route solely by brand. Promote a model/codec pair only after it passes the mutation safety suite.

## Implementation sequence

1. Extract/vendor the V4A codec with attribution and an upstream commit pin. Prefer a small owned codec package over importing OpenAI private parser functions; `agents.apply_diff` is public, but the envelope parser in the linked file is private.
2. Define canonical AST, typed errors, `PatchPlan`, `PatchReceipt`, limits, and strict ambiguity behavior.
3. Build a read-only planner and run the golden corpus against OpenAI's transformer.
4. Implement `DeepAgentsPatchStore` first for `StateBackend` and `FilesystemBackend(virtual_mode=True)`. Mark guarantees explicitly.
5. Add policy, HITL, audit receipts, idempotency, version checks, fault injection, and rollback verification.
6. Wire `v4a_structured` into the existing pinned Deep Agents runtime. Keep current `edit_file` during qualification.
7. Add grammar-constrained OpenAI and structured-operations adapters, then evaluate model/codec pairs.
8. Only after qualification, consider making `apply_patch` a built-in Deep Agents filesystem tool so it can share the public permissions and backend capability machinery.

## Sources

- [OpenAI sandbox custom tool](https://github.com/openai/openai-agents-python/blob/f3b6c617853880b6dbad16b58ff9d071d5756afb/src/agents/sandbox/capabilities/tools/apply_patch_tool.py)
- [OpenAI V4A transformer](https://github.com/openai/openai-agents-python/blob/f3b6c617853880b6dbad16b58ff9d071d5756afb/src/agents/apply_diff.py)
- [OpenAI sandbox workspace editor](https://github.com/openai/openai-agents-python/blob/f3b6c617853880b6dbad16b58ff9d071d5756afb/src/agents/sandbox/apply_patch.py)
- [OpenAI editor protocol](https://github.com/openai/openai-agents-python/blob/f3b6c617853880b6dbad16b58ff9d071d5756afb/src/agents/editor.py)
- [OpenAI standalone example](https://github.com/openai/openai-agents-python/blob/f3b6c617853880b6dbad16b58ff9d071d5756afb/examples/tools/apply_patch.py)
- [Deep Agents backends](https://docs.langchain.com/oss/python/deepagents/backends)
- [Deep Agents customization and custom tools](https://docs.langchain.com/oss/python/deepagents/customization)
- [Deep Agents permissions](https://docs.langchain.com/oss/python/deepagents/permissions)
- [Deep Agents models](https://docs.langchain.com/oss/python/deepagents/models)
- [Deep Agents backend protocol source](https://github.com/langchain-ai/deepagents/blob/ff421f2f316f3819d8ac92225ea032dabfbcefe9/libs/deepagents/deepagents/backends/protocol.py)
- [Deep Agents filesystem middleware source](https://github.com/langchain-ai/deepagents/blob/ff421f2f316f3819d8ac92225ea032dabfbcefe9/libs/deepagents/deepagents/middleware/filesystem.py)

