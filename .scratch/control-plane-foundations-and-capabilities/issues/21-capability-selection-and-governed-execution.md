# 21 — Select, freeze, and execute governed capabilities end to end

**What to build:** Resolve authored capability requirements only against the governed internal catalog, freeze exact approved assets into Effective Run Configuration, verify actual use in Operation Execution Binding, enforce revocation, and migrate the hard-coded bootstrap probe to this replayable path.

**Blocked by:**
- 01 — Implement versioned Workflow Definitions and Effective Run Configuration compilation
- 06 — Implement provider-neutral Operation Execution Bindings and runtime contracts
- 09 — Implement the OpenAI Agents runtime adapter and bounded delegation
- 19 — Implement governed Prompt and Agent Skill catalog intake
- 20 — Implement governed MCP and Plugin catalog intake

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/21


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Resolve and evaluate requirements during authoring/publication, then freeze exact selection decisions before admission; production execution must not search public registries.
- Make deterministic hard gates produce the candidate set before optional model judgment, and preserve component scores without inventing a universal capability score.
- Compare the complete frozen plan with actual rendered, mounted, connected, exposed, read, and executed assets at operation preparation and completion.

## Verification approach

Migrate the diagnostic sandbox probe through import → promotion → search/selection → compile → admit → bind → execute, then move aliases and revoke assets to prove historical replay and policy.

## Explicit non-goals

- Silent substitution, model-selected excluded assets, public marketplace lookup during execution, automatic installation, and secret values in any record.
- Changing the probe's externally visible success condition to hide integration failures.

## Acceptance criteria

- [ ] Requirements support exact revision, stable identity/alias, or governed query plus kind, required capabilities, trust/source constraints, attachment target, class, and explicit failure policy.
- [ ] Internal search runs tenant, authority, promotion, compatibility, license, freshness, health, network, secret, filesystem, sensitivity, and approval gates before ranking or model presentation.
- [ ] Every immutable selection records requirement/context/policy, hard exclusions, candidate revisions and component scores, model-visible set, selected revisions, ambiguity, fallback/degradation, actor, and digest.
- [ ] Publication proves each required requirement resolves to one usable revision; compilation freezes exact digests, components, plans, MCP ceilings, approvals, secret references, and selection decisions.
- [ ] Admission revalidates pinned usability/revocation without alias or search resolution; active revocation follows declared intervention without silent substitution.
- [ ] Operation preparation verifies actual prompts, Skills, MCP schemas/filters, plugin components, model, workspace, approvals, and authority against the frozen plan.
- [ ] MCP exposure is the intersection of probed inventory and every catalog, workflow, configuration, operation, caller, delegation, permission, approval, and revocation ceiling.
- [ ] Outcomes and traces are queryable by exact asset, model, selection, and binding revisions.
- [ ] The existing sandbox Agents probe preserves its success condition through governed exact assets, and bootstrap-only direct execution cannot create domain state.

## Source basis

- Governed Agentic Capability Catalogs and Exact Bindings
- Versioned Workflow Definitions and Effective Run Configuration
- Operation Runtime, Workspaces, Artifacts, and Snapshots

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
