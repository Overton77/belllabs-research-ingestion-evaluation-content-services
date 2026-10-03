# Authority and approvals

Preparation intersects requested authority with all ceilings:

- Workflow Type;
- actor and tenant;
- parent run and delegation;
- data and workspace policy;
- approval state;
- environment and deployment availability.

The narrowest result wins. Skills and prompts are authority-neutral.

Before launch, verify:

- exact definitions are published and not retired or revoked;
- selected schemas and bundle digests still match;
- approval references remain valid;
- required deployment and secret references are available;
- policy and environment snapshot digests still match;
- the ticket is unexpired and belongs to the caller and tenant.

Repeat launch with the same valid idempotency identity returns the same run. A changed proposal, selected goal, or authority request requires a new identity.

Launch is consequential. Present warnings and required approvals before invoking it.
