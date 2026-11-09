"""Open a PR in a docs repo to update release-specific content.

Updates an attribution page and a fallback release version string in a
docusaurus config as part of a release publish pipeline.
"""

import logging
import re

import boto3
import botocore.exceptions

log = logging.getLogger(__name__)

# Matches e.g. `const fallback = '2024-07-22.0';` — capture groups preserve the
# quoted wrapper so re.sub can swap just the version string.
FALLBACK_PATTERN = re.compile(r"(const fallback = ')[^']+(')")


def update_docs_for_release(
    s3_bucket: str,
    s3_key: str,
    release_version: str,
    *,
    docs_repo: str,
    base_branch: str,
    attribution_path: str,
    config_path: str,
    app_slug: str,
    app_client_id: str,
    app_installation_id: int,
    pem_secret_name: str,
    secrets_region: str,
) -> str:
    """Update docs repo with new attribution content and release version.

    Overwrites the attribution page with the rendered body from S3 and bumps
    the fallback version in the docusaurus config. Opens a single PR with
    both changes. Authenticates as a GitHub App via a JWT signed with a PEM
    fetched from AWS Secrets Manager.

    Args:
        s3_bucket: Bucket holding the rendered attribution body.
        s3_key: Key of the rendered attribution body within `s3_bucket`.
        release_version: Version string to write into the fallback config.
        docs_repo: The `owner/repo` docs repository to open the PR against
            (e.g. "OvertureMaps/docs").
        base_branch: Branch in `docs_repo` to branch from and target with
            the PR (e.g. "main").
        attribution_path: Path within `docs_repo` to overwrite with the
            rendered attribution body (e.g. "docs/_generated_attribution.mdx").
        config_path: Path within `docs_repo` to a docusaurus config
            containing a `const fallback = '...'` version string to bump
            (e.g. "docusaurus.config.js").
        app_slug: The GitHub App's slug (e.g. "overture-pull-requester"),
            used to resolve its bot user identity for commit authorship.
        app_client_id: The GitHub App's client ID.
        app_installation_id: The App's installation ID on `docs_repo`.
        pem_secret_name: AWS Secrets Manager secret name holding the App's
            private key PEM.
        secrets_region: AWS region `pem_secret_name` lives in.

    Returns the PR URL, or a message if no changes were needed.
    """
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey"):
            raise
        raise RuntimeError(
            f"Attribution artifact missing for release {release_version}: "
            f"s3://{s3_bucket}/{s3_key} does not exist."
        ) from e
    attribution_body = obj["Body"].read().decode("utf-8")
    log.info("Read attribution body from s3://%s/%s", s3_bucket, s3_key)

    # Deferred so importing this module doesn't require PyGithub's transitive
    # deps to be resolvable in every environment that just needs the rest of
    # overture_core.
    from overture_core.apis.github import get_app_bot, get_github_client, open_pr

    gh = get_github_client(
        pem_secret_name,
        app_client_id,
        app_installation_id,
        region=secrets_region,
    )
    repo = gh.get_repo(docs_repo)

    attr_contents = repo.get_contents(attribution_path, ref=base_branch)
    attr_current = attr_contents.decoded_content.decode("utf-8")
    attr_changed = attribution_body.rstrip() != attr_current.rstrip()

    config_contents = repo.get_contents(config_path, ref=base_branch)
    config_current = config_contents.decoded_content.decode("utf-8")

    if not FALLBACK_PATTERN.search(config_current):
        raise ValueError(f"Could not find fallback version pattern in {config_path}.")

    config_updated = FALLBACK_PATTERN.sub(rf"\g<1>{release_version}\2", config_current)
    config_changed = config_updated != config_current

    # Always pass both desired files: open_pr reconciles a branch reused on retry,
    # and returns None if the result matches the base branch.
    files = {
        attribution_path: attribution_body if attr_changed else attr_current,
        config_path: config_updated,
    }

    url = open_pr(
        repo,
        get_app_bot(gh, app_slug),
        branch=f"release-update-{release_version}",
        files=files,
        title=f"[DOCS] Update docs for release {release_version}",
        body=(
            "Automated docs update from the release pipeline.\n\n"
            f"Release version: {release_version}\n\n"
            "Changes:\n"
            + ("- Updated attribution page\n" if attr_changed else "")
            + ("- Updated fallback release version\n" if config_changed else "")
        ),
        base=base_branch,
        draft=False,
    )
    if url is None:
        log.info("No docs changes needed for %s", release_version)
        return "No docs changes detected, skipping PR."
    return f"PR created: {url}"
