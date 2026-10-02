# overture_core.cloud

Cloud helpers, split into provider-agnostic code and one subpackage per vendor.

## Modules

| Module | Scope |
| --- | --- |
| `cloud` | Provider-agnostic cloud helpers that don't belong to one specific vendor. |
| `aws` | The home for any AWS-specific helper, built on boto3, one module per service (`core` for STS/IAM, `object` for S3, `ecs`, `ecr`, `datasync`, `secrets`, `codeartifact`). |
| `azure` | The home for any Azure-specific helper, one module per service (`object` for Blob Storage). Imports `azure-storage-blob` lazily; install `overture-core[azure]` to use it. |
| `databricks` | The home for any Databricks-specific helper. Prefers accepting a caller-supplied SDK client over constructing one, keeping `databricks-sdk` out of this package's runtime dependencies. |
