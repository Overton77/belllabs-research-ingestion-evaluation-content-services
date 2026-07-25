[CmdletBinding()]
param(
    [Parameter()]
    [string]$BucketName,

    [Parameter()]
    [string]$Region = $env:AWS_REGION,

    [Parameter()]
    [string]$Profile = $env:AWS_PROFILE,

    [Parameter()]
    [string]$SchemaPath = (Join-Path $PSScriptRoot "..\..\biotech-kg\src\schema\neo4jbiotechschema.graphql")
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    throw "AWS CLI v2 is required."
}

$resolvedSchemaPath = (Resolve-Path -LiteralPath $SchemaPath).Path
if (-not $Region) {
    $Region = aws configure get region
}
if (-not $Region) {
    $Region = "us-east-1"
}

$awsArguments = @()
if ($Profile) {
    $awsArguments += @("--profile", $Profile)
}
$awsArguments += @("--region", $Region)

function Invoke-Aws {
    param([Parameter(Mandatory)][string[]]$Arguments)

    & aws @Arguments @awsArguments
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI command failed: aws $($Arguments -join ' ')"
    }
}

$identityJson = Invoke-Aws -Arguments @("sts", "get-caller-identity", "--output", "json")
$identity = ($identityJson -join "`n") | ConvertFrom-Json
$accountId = [string]$identity.Account
if (-not $accountId) {
    throw "AWS account identity did not contain an account ID."
}

if (-not $BucketName) {
    $BucketName = "belllabs-biotech-schema-$accountId-$Region".ToLowerInvariant()
}
if ($BucketName -notmatch '^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$') {
    throw "Bucket name is not a valid S3 bucket name: $BucketName"
}

$schemaHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $resolvedSchemaPath).Hash.ToLowerInvariant()
$schemaBytes = [IO.File]::ReadAllBytes($resolvedSchemaPath)
$sha256 = [Security.Cryptography.SHA256]::Create()
try {
    $schemaChecksumBase64 = [Convert]::ToBase64String($sha256.ComputeHash($schemaBytes))
}
finally {
    $sha256.Dispose()
}
$objectKey = "schemas/neo4jbiotechschema/sha256/$schemaHash/neo4jbiotechschema.graphql"

$previousErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& aws s3api head-bucket --bucket $BucketName @awsArguments 2>$null
$headBucketExitCode = $LASTEXITCODE
$ErrorActionPreference = $previousErrorActionPreference
if ($headBucketExitCode -ne 0) {
    $createArguments = @("s3api", "create-bucket", "--bucket", $BucketName)
    if ($Region -ne "us-east-1") {
        $createArguments += @("--create-bucket-configuration", "LocationConstraint=$Region")
    }
    Invoke-Aws -Arguments $createArguments | Out-Null
}

Invoke-Aws -Arguments @(
    "s3api", "put-public-access-block",
    "--bucket", $BucketName,
    "--public-access-block-configuration",
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
) | Out-Null

Invoke-Aws -Arguments @(
    "s3api", "put-bucket-ownership-controls",
    "--bucket", $BucketName,
    "--ownership-controls", "Rules=[{ObjectOwnership=BucketOwnerEnforced}]"
) | Out-Null

Invoke-Aws -Arguments @(
    "s3api", "put-bucket-versioning",
    "--bucket", $BucketName,
    "--versioning-configuration", "Status=Enabled"
) | Out-Null

Invoke-Aws -Arguments @(
    "s3api", "put-bucket-encryption",
    "--bucket", $BucketName,
    "--server-side-encryption-configuration",
    "Rules=[{ApplyServerSideEncryptionByDefault={SSEAlgorithm=AES256},BucketKeyEnabled=false}]"
) | Out-Null

$putJson = Invoke-Aws -Arguments @(
    "s3api", "put-object",
    "--bucket", $BucketName,
    "--key", $objectKey,
    "--body", $resolvedSchemaPath,
    "--content-type", "application/graphql; charset=utf-8",
    "--server-side-encryption", "AES256",
    "--checksum-algorithm", "SHA256",
    "--checksum-sha256", $schemaChecksumBase64,
    "--metadata", "sha256=$schemaHash,source=biotech-kg/src/schema/neo4jbiotechschema.graphql",
    "--output", "json"
)
$putResult = ($putJson -join "`n") | ConvertFrom-Json

$headJson = Invoke-Aws -Arguments @(
    "s3api", "head-object",
    "--bucket", $BucketName,
    "--key", $objectKey,
    "--checksum-mode", "ENABLED",
    "--output", "json"
)
$head = ($headJson -join "`n") | ConvertFrom-Json
if ([string]$head.Metadata.sha256 -ne $schemaHash) {
    throw "Remote sha256 metadata does not match the local schema digest."
}
if ([string]$head.ChecksumSHA256 -ne $schemaChecksumBase64) {
    throw "Remote S3 checksum does not match the local schema digest."
}
if ([int64]$head.ContentLength -ne [int64]$schemaBytes.Length) {
    throw "Remote object length does not match the local schema."
}
if ([string]$head.ServerSideEncryption -ne "AES256") {
    throw "Remote object encryption verification failed."
}
if (-not [string]$putResult.VersionId) {
    throw "S3 did not return an object version ID after versioning was enabled."
}

$publicAccessJson = Invoke-Aws -Arguments @(
    "s3api", "get-public-access-block", "--bucket", $BucketName, "--output", "json"
)
$versioningJson = Invoke-Aws -Arguments @(
    "s3api", "get-bucket-versioning", "--bucket", $BucketName, "--output", "json"
)
$encryptionJson = Invoke-Aws -Arguments @(
    "s3api", "get-bucket-encryption", "--bucket", $BucketName, "--output", "json"
)
$ownershipJson = Invoke-Aws -Arguments @(
    "s3api", "get-bucket-ownership-controls", "--bucket", $BucketName, "--output", "json"
)
$publicAccess = ($publicAccessJson -join "`n") | ConvertFrom-Json
$versioning = ($versioningJson -join "`n") | ConvertFrom-Json
$encryption = ($encryptionJson -join "`n") | ConvertFrom-Json
$ownership = ($ownershipJson -join "`n") | ConvertFrom-Json
$block = $publicAccess.PublicAccessBlockConfiguration
if (-not ($block.BlockPublicAcls -and $block.IgnorePublicAcls -and $block.BlockPublicPolicy -and $block.RestrictPublicBuckets)) {
    throw "S3 public-access block verification failed."
}
if ($versioning.Status -ne "Enabled") {
    throw "S3 bucket versioning verification failed."
}
if ($encryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm -ne "AES256") {
    throw "S3 default encryption verification failed."
}
if ($ownership.OwnershipControls.Rules[0].ObjectOwnership -ne "BucketOwnerEnforced") {
    throw "S3 object ownership verification failed."
}

[ordered]@{
    bucket = $BucketName
    region = $Region
    key = $objectKey
    s3_uri = "s3://$BucketName/$objectKey"
    sha256 = $schemaHash
    content_length = [int64]$head.ContentLength
    version_id = [string]$putResult.VersionId
    checksum_sha256_base64 = [string]$head.ChecksumSHA256
    public_access_blocked = $true
    object_ownership = "BucketOwnerEnforced"
    versioning = "Enabled"
    default_encryption = "AES256"
} | ConvertTo-Json
