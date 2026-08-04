# Workflow design contracts

An existing Workflow Type is authoritative for:

- purpose and non-goals;
- input admission contract;
- invariants and obligations;
- output contracts;
- allowed blueprints, profiles, and configurations;
- authority ceiling and workspace contract;
- linked-run slots.

Use StageGraph when stages, dependencies, and outputs can be frozen before admission. Use GoalDirected only when bounded iteration is essential. GoalDirected requires an objective, acceptance contract, protected scope, allowed operation classes, iteration budget, convergence policy, session/workspace policy, and independent verifier.

A novel topology remains a `WorkflowDesignDraft`. It cannot be passed to a generic runner. An authorized publisher must create exact definitions and an approved implementation binding first.

Represent known child Workflow Types through declared linked-run slots. Do not disguise them as internal subagents.
