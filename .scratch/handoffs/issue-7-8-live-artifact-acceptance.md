# Handoff: run the Issue 7/8 live artifact acceptance

Work in `biotech-research-ingestion-evaluation-system` on branch
`issue-7-8-workspaces-artifact-promotion` (PR #24). Do not change implementation
unless the live run exposes a concrete defect.

## Goal

Run the real API → Temporal → GPT-5 mini Docker sandbox → explicit artifact
promotion acceptance:

```bash
uv run python -m app.temporal.run_artifact_acceptance
```

Success prints:

```text
BELL-LABS-ARTIFACT-PROMOTION-OK
```

The script itself verifies Tavily and `agent-browser` use, create-then-patch
behavior, candidate capture, S3 bytes, Mongo metadata/manifest linkage,
PostgreSQL durable reference/outbox, and the public artifact visibility seam.

## Preparation

1. Confirm Docker is running and build the required image:

   ```bash
   docker build -f infra/sandbox/Dockerfile.agentic-probe \
     -t belllabs-agentic-probe:local .
   docker image inspect belllabs-agentic-probe:local
   ```

2. Create one globally unique S3 bucket (the acceptance script needs only the
   single `S3_BUCKET` setting):

   ```bash
   export AWS_REGION="${AWS_REGION:-us-east-1}"
   export S3_BUCKET="belllabs-artifact-acceptance-$(aws sts get-caller-identity --query Account --output text)-$(date +%s)"

   if [ "$AWS_REGION" = "us-east-1" ]; then
     aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION"
   else
     aws s3api create-bucket \
       --bucket "$S3_BUCKET" \
       --region "$AWS_REGION" \
       --create-bucket-configuration "LocationConstraint=$AWS_REGION"
   fi

   aws s3api put-bucket-versioning \
     --bucket "$S3_BUCKET" \
     --versioning-configuration Status=Enabled
   ```

3. Ensure the shell exports `TAVILY_API_KEY`, `S3_BUCKET`, `AWS_REGION`, and
   `AWS_PROFILE` if required. Ensure application settings provide
   `OPENAI_API_KEY`, MongoDB, application PostgreSQL, and Temporal
   (`localhost:7233` by default). Start the project services if needed:

   ```bash
   docker compose up -d
   docker compose ps
   ```

4. Run the acceptance command. Do not replace unavailable infrastructure with
   mocks and do not report success without the exact success marker.

## Report back

Return:

- whether the success marker was printed;
- the Temporal workflow ID and admitted artifact ID if successful;
- any exact failing command/traceback if unsuccessful;
- confirmation that no credentials or `.env` files were committed.
