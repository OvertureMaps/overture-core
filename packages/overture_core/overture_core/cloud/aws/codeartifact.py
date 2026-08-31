"""AWS CodeArtifact helpers built on boto3."""

import boto3


def get_codeartifact_token(domain: str, domain_owner: str, region: str) -> str:
    """Mint a short-lived CodeArtifact authorization token.

    The returned token is a bearer credential valid for the domain's default
    TTL (12 hours) usable as the password half of a CodeArtifact repository
    URL (``https://aws:<token>@...``). Callers that mint tokens frequently
    should cache the result themselves; this function always makes a fresh
    STS-backed API call.

    Args:
        domain: CodeArtifact domain name (e.g. "overture-pypi").
        domain_owner: AWS account ID that owns the domain.
        region: Region for the CodeArtifact API endpoint.

    Returns:
        The authorization token string.
    """
    return boto3.client("codeartifact", region_name=region).get_authorization_token(
        domain=domain, domainOwner=domain_owner
    )["authorizationToken"]
