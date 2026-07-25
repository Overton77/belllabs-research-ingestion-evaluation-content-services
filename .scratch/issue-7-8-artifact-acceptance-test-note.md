# Issue 7/8 live artifact acceptance — test note

---------

This was a **live acceptance test** (not production wiring).

- Ran `uv run python -m app.temporal.run_artifact_acceptance` on branch `issue-7-8-workspaces-artifact-promotion`.
- Used a one-off S3 bucket: `belllabs-artifact-acceptance-298199649527-1784578177` (us-east-1).
- Success marker: `BELL-LABS-ARTIFACT-PROMOTION-OK`
- Last successful run: workflow `fbc4ed0c-fd73-5e38-91df-cd7bd6e62000`, artifact `004d5f9e-86a5-5b70-8d7e-9ff7f0368b65`.

**Follow-up (operational memory):** replace ad-hoc acceptance buckets with **exact S3 bucket layout + object key shapes** we intend to operate on. We will also need **S3 Vector bucket counterparts** (or S3 Vectors integration) so promoted artifacts can be indexed and retrieved from our own operational memory — not just stored as raw bytes.
