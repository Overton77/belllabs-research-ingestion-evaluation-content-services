# Control-plane experience, intra-mission conversation, and sandbox seed

> **Historical input — superseded as architecture authority (2026-10-02).** Use the [canonical specification](general-mission-control/SPECIFICATION.md), [new system proposal](general-mission-control/MISSION_CONTROL_SYSTEM_PROPOSAL.md), and [cleanup plan](general-mission-control/DOCUMENTATION-CLEANUP.md). The retained text below records earlier work; conflicting runtime, storage, ownership or sequencing instructions do not govern new implementation.

**Status:** later-phase reminder — not architecture authority  
**Date:** 2026-09-19  
**Does not amend:** [`MISSION_CONTROL_ARCHITECTURE.md`](./MISSION_CONTROL_ARCHITECTURE.md), [`workflow-types/`](./workflow-types/index.md), or ADRs  
**Lands later against:** M7 Live Mission ([architecture §14](./MISSION_CONTROL_ARCHITECTURE.md), [candidate §18](./MISSION_CONTROL_SPEC.md)), M10 capability / environment / sandbox materialization ([sequence M10](./MISSION_CONTROL_IMPLEMENTATION_SEQUENCE.md)), and the communication law ([candidate §11.5](./MISSION_CONTROL_SPEC.md), [supplement §8](./MISSION_CONTROL_SUPPLEMENT.md))

Write this down so it is not lost in chat. It is product bar and an open materialization concern, not a kernel change. When these surfaces are specified, they become dated amendments in the architecture and the relevant `workflow-types/` document.

---

## 1. What the plane is

Mission Control's dashboard is a **business-level agent mission control plane**, not only an operator debugger.

The plane includes **generative UI**. The dashboard is also the **prototyping stomping ground** for generative UI mixed with live business data — Mission state, Artifacts, budgets, Human Tasks, verification dispositions, and later catalog / capability rows.

M7 still ships the lifecycle debugger in architecture §14. This note is the bar the control plane must reach once agent lanes, capabilities, and sandboxes exist: a Coordinator should experience a live Mission the way they experience a first-class agent workspace, while remaining on one Mission contract.

---

## 2. Intra-mission agents talk to each other

Every agent on a Mission — main agents and subagents — can communicate with every other agent on that Mission.

**Main vs subagent is caller-based**, not a different agent kind. The caller relationship is recorded. The conversation and progress surfaces are the same.

Two first-class surfaces to specify later:

| Surface | What it holds |
|---|---|
| **Conversation lifecycle** | Addressable conversations between agents on the Mission: open, park, resume, seal, attach to an Activation / Attempt / Agent Session. Not an unbounded chat dump. |
| **Progress ledger** | Attributable progress the other agents on the Mission can read: what moved, what is blocked, what Artifact or path changed, what the next ask is. |

This is intra-Mission. It does not silently repeal the existing inter-Mission law:

- Child Missions still write Artifacts and status; parents still peek.
- No child→parent token stream.
- Portals still never carry transcripts or Artifact bodies.
- Commands remain the only writes to Mission Control.
- Shared intermediates that must survive a Session are still Artifacts.

When this is specified, decide whether conversation records and progress-ledger entries are Mission Events, Native Event Store rows, a new ledger beside the Journal, or a projection over those. Do not invent a second message bus that bypasses peek, Commands, and Artifacts.

---

## 3. Full agent lifecycle on the dashboard

The dashboard must surface the **full agent lifecycle** inside the agent chat / workspace of a live Mission, including sandbox filesystem state.

Minimum experience to keep in view:

- Tool calls
- File edits
- Reasoning traces
- Sources attached and sources visited
- Sandbox filesystem state in the same chat / workspace the agent is using
- Workflows spawned
- Code editors opened
- Canvases synchronized

M7's Live Mission (topology, canonical event tail, Native Event Store agent tail, inspector, Command composer, output rail, Journal) is the substrate. This note raises the product bar: those streams must compose into a generative-UI workspace, not only a debugger of three-field state.

Generative UI here is mixed with business data. A canvas, editor, or spawned workflow on this plane is bound to Mission identity, Revision, Run, Activation, and Artifact refs — it is not a disposable demo widget.

---

## 4. Open concern — seeding a sandbox from coordinator Artifacts

Coordinators equipped with Mission Control will search capabilities and seed a Mission. They can seed that Mission with Artifacts.

Those seeds have to load into the sandbox. Do not collapse this into "copy files into a workspace." Two load forms:

### 4.1 Metadata load

Bind the seed as **metadata** the sandbox and the agent can address: Artifact identities, digests, capability / profile refs, admission status, paths they will occupy, and the reason they were attached.

The agent can see what was seeded before, or without, hydrating every byte. Peek and generative UI can render the same metadata.

### 4.2 Sandbox Snapshot

A **Sandbox Snapshot** is a sandbox that already carries that metadata — a sandbox instance plus the bound seed metadata (and, when hydrated, the materialized tree).

This is the unit a later Attempt, Continuation Transfer, or spawned agent can attach to: not a bare VM, and not a bag of Artifact IDs with nowhere to run.

Existing language this must stay consistent with:

- Workspace is *what tree and seeds the agent sees*. Sandbox is *where that tree runs and what it may touch* ([supplement §2](./MISSION_CONTROL_SUPPLEMENT.md)). Do not collapse the two.
- Capability / hooks / skill materialization and the sandbox fleet are deferred past M8 ([architecture §16](./MISSION_CONTROL_ARCHITECTURE.md), sequence M10).
- `ContinuationCheckpoint@1` already carries a workspace snapshot / sandbox ref. Decide whether Sandbox Snapshot is that object, a seed-time sibling, or the sandbox-side of a Mission input snapshot.
- Peek still returns pointers and diffs, not file bodies.

Open questions for the later feature specification — do not answer them here:

1. When is Metadata load enough, and when must a Sandbox Snapshot be provisioned before the first Turn?
2. Does a Coordinator seed write Mission inputs, workspace-template seed Artifacts, a Sandbox Snapshot, or all three in order?
3. Is a Sandbox Snapshot immutable (new snapshot per seed / Continuation) or a live instance with a metadata overlay?
4. How do Metadata load and Sandbox Snapshot show up in the dashboard chat / filesystem surface in §3?

---

## 5. Where this sits on the program

| Intent | Do not pull it into | Specify with |
|---|---|---|
| Intra-mission conversation + progress ledger | M0–M6 kernel, three-field state, existing communication law | Later conversation / ledger feature specs; amend architecture and `workflow-types/` only then |
| Full agent lifecycle + generative UI control plane | M7 debugger exit proof | Raise of F7.1 / later dashboard specs; canvases, editors, and spawned workflows as Mission-bound surfaces |
| Capability search and Coordinator seed | M6 `capability_binding: none` | M10 catalog search, admission, `EnvironmentProfile`, `SandboxConfig` |
| Metadata load + Sandbox Snapshot | M5 `snapshot` activity as-is | Later materialization spec; reconcile with workspace vs sandbox and Continuation snapshot |

No kernel change from this note. Keep it next to the program so the control-plane bar and the seed-into-sandbox question are visible when M7 and M10 are specified.
