# Authoritative schema S3 reference

The first official catalog schema source was published and verified on
2026-07-22. The content-addressed URI below is the governed remote reference;
the version ID binds this record to the exact uploaded S3 object version.

## Current authoritative source

- Repository path: `biotech-kg/src/schema/neo4jbiotechschema.graphql`
- SHA-256: `86b5e0b5d11d203bd75b69b4507b0aad97d5df2495d3897ca64272068ea5f112`
- Content length: `107632` bytes
- Content-addressed object key:
  `schemas/neo4jbiotechschema/sha256/86b5e0b5d11d203bd75b69b4507b0aad97d5df2495d3897ca64272068ea5f112/neo4jbiotechschema.graphql`
- Bucket: `belllabs-biotech-schema-298199649527-us-east-1`
- Region: `us-east-1`
- Version ID: `J6d_lL6g2TEtTL9Imi6hwIMBNSTSahEB`
- S3 URI:
  `s3://belllabs-biotech-schema-298199649527-us-east-1/schemas/neo4jbiotechschema/sha256/86b5e0b5d11d203bd75b69b4507b0aad97d5df2495d3897ca64272068ea5f112/neo4jbiotechschema.graphql`
- S3 SHA-256 checksum (base64): `hrXgtdEdIDvXW2m0UHsKrZfV3ySV04l8pkJyBo6l8RI=`

The digest identifies schema bytes only. Filesystem paths, timestamps, bucket
names, and upload version IDs do not participate in that identity.

## Publish and verify

Configure an AWS CLI credential chain with permission to create and configure an
S3 bucket, then run from the repository root:

```powershell
./biotech-research-ingestion-evaluation-system/scripts/publish_schema_to_s3.ps1
```

The default bucket name is deterministic and scoped to the authenticated account
and region: `belllabs-biotech-schema-<account-id>-<region>`. Pass `-BucketName`
to use a different globally unique private bucket. `-Profile` and `-Region` may
also be supplied explicitly.

The helper:

1. resolves the authenticated AWS account and region;
2. creates the bucket if needed;
3. blocks all four forms of S3 public access and enforces bucket ownership;
4. enables bucket versioning and AES-256 S3-managed default encryption;
5. uploads the schema under the SHA-256-addressed key with checksum and digest
   metadata; and
6. reads back the object metadata and bucket controls, failing unless digest,
   length, checksum, public-access block, versioning, and encryption all match.

Its JSON output is the publication record. A new schema revision gets a new
content-addressed key; never overwrite this reference for different bytes.

## Verified controls

- S3 Block Public Access: all four settings enabled
- Object ownership: `BucketOwnerEnforced`
- Bucket versioning: enabled
- Default encryption: SSE-S3 (`AES256`)
- Remote content length: `107632` bytes
- Remote checksum and `sha256` metadata: match the repository source

The equivalent machine-readable record is
`schema-catalog/source-reference.v1.json`.
