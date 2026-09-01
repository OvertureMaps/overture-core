"""Open a PR in overturemaps/docs to update release-specific content.

Updates the attribution page and the fallback release version in
docusaurus.config.js as part of the release publish pipeline.
"""

import logging
import re

import boto3

log = logging.getLogger(__name__)

DOCS_REPO = "OvertureMaps/docs"
BASE_BRANCH = "main"

ATTRIBUTION_PATH = "docs/_generated_attribution.mdx"

CONFIG_PATH = "docusaurus.config.js"
# Matches e.g. `const fallback = '2024-07-22.0';` — capture groups preserve the
# quoted wrapper so re.sub can swap just the version string.
FALLBACK_PATTERN = re.compile(r"(const fallback = ')[^']+(')")


def update_docs_for_release(
    s3_bucket: str,
    s3_key: str,
    release_version: str,
    *,
    app_slug: str,
    app_client_id: str,
    app_installation_id: int,
    pem_secret_name: str,
    secrets_region: str,
) -> str:
    """Update docs repo with new attribution content and release version.

    Overwrites _generated_attribution.mdx with the attribution body from S3 and
    bumps the fallback version in docusaurus.config.js. Opens a single PR with
    both changes. Authenticates as a GitHub App via a JWT signed with a PEM
    fetched from AWS Secrets Manager.

    Args:
        s3_bucket: Bucket holding the rendered attribution body.
        s3_key: Key of the rendered attribution body within `s3_bucket`.
        release_version: Version string to write into the fallback config.
        app_slug: The GitHub App's slug (e.g. "overture-pull-requester"),
            used to resolve its bot user identity for commit authorship.
        app_client_id: The GitHub App's client ID.
        app_installation_id: The App's installation ID on `DOCS_REPO`.
        pem_secret_name: AWS Secrets Manager secret name holding the App's
            private key PEM.
        secrets_region: AWS region `pem_secret_name` lives in.

    Returns the PR URL, or a message if no changes were needed.
    """
    # Deferred so importing this module doesn't require PyGithub's transitive
    # deps to be resolvable in every environment that just needs the rest of
    # overture_core; callers that invoke this function already need PyGithub
    # installed (it's a hard dependency of this package).
    from github import Auth, Github, GithubException, InputGitAuthor

    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    except s3.exceptions.NoSuchKey as e:
        raise RuntimeError(
            f"Attribution artifact missing for release {release_version}: "
            f"s3://{s3_bucket}/{s3_key} does not exist."
        ) from e
    attribution_body = obj["Body"].read().decode("utf-8")
    log.info("Read attribution body from s3://%s/%s", s3_bucket, s3_key)

    # Auth.AppAuth signs a JWT with the PEM; .get_installation_auth exchanges
    # it for a short-lived installation token scoped to this org's install.
    # PyGithub refreshes the token automatically as it nears expiry.
    sm = boto3.client("secretsmanager", region_name=secrets_region)
    pem = sm.get_secret_value(SecretId=pem_secret_name)["SecretString"]
    installation_auth = Auth.AppAuth(app_client_id, pem).get_installation_auth(
        app_installation_id
    )
    gh = Github(auth=installation_auth)
    repo = gh.get_repo(DOCS_REPO)

    # Identity for commit author + DCO Signed-off-by trailer. cncf/dco2 fails
    # the PR if the trailer email doesn't match the commit author email, so we
    # pin both sides to the App's bot user identity.
    bot = gh.get_user(f"{app_slug}[bot]")
    bot_name = bot.name or bot.login
    bot_email = f"{bot.id}+{bot.login}@users.noreply.github.com"
    author = InputGitAuthor(bot_name, bot_email)
    signoff = f"Signed-off-by: {bot_name} <{bot_email}>"

    # Overwrite _generated_attribution.mdx with the S3 body.
    attr_contents = repo.get_contents(ATTRIBUTION_PATH, ref=BASE_BRANCH)
    attr_current = attr_contents.decoded_content.decode("utf-8")
    attr_changed = attribution_body.rstrip() != attr_current.rstrip()

    # edits to docusaurus.config.js
    config_contents = repo.get_contents(CONFIG_PATH, ref=BASE_BRANCH)
    config_current = config_contents.decoded_content.decode("utf-8")

    if not FALLBACK_PATTERN.search(config_current):
        raise ValueError(f"Could not find fallback version pattern in {CONFIG_PATH}.")

    config_updated = FALLBACK_PATTERN.sub(rf"\g<1>{release_version}\2", config_current)
    config_changed = config_updated != config_current

    if not attr_changed and not config_changed:
        log.info("No docs changes needed for %s", release_version)
        return "No docs changes detected, skipping PR."

    branch_name = f"release-update-{release_version}"
    # Make the task idempotent across DAG retries: if a previous run created
    # the branch and then failed before opening the PR, delete it and start
    # fresh from main.
    try:
        existing_ref = repo.get_git_ref(f"heads/{branch_name}")
    except GithubException as e:
        if e.status != 404:
            raise
    else:
        log.info("Deleting stale branch %s from prior run", branch_name)
        existing_ref.delete()

    repo.create_git_ref(
        ref=f"refs/heads/{branch_name}",
        sha=repo.get_branch(BASE_BRANCH).commit.sha,
    )

    if attr_changed:
        repo.update_file(
            path=ATTRIBUTION_PATH,
            message=f"update attribution for {release_version}\n\n{signoff}",
            content=attribution_body,
            sha=attr_contents.sha,
            branch=branch_name,
            author=author,
            committer=author,
        )

    if config_changed:
        repo.update_file(
            path=CONFIG_PATH,
            message=f"update fallback release version to {release_version}\n\n{signoff}",
            content=config_updated,
            sha=config_contents.sha,
            branch=branch_name,
            author=author,
            committer=author,
        )

    pr = repo.create_pull(
        title=f"[DOCS] Update docs for release {release_version}",
        body=(
            "Automated docs update from the release pipeline.\n\n"
            f"Release version: {release_version}\n\n"
            "Changes:\n"
            + ("- Updated attribution page\n" if attr_changed else "")
            + ("- Updated fallback release version\n" if config_changed else "")
        ),
        head=branch_name,
        base=BASE_BRANCH,
    )
    log.info("Created PR: %s", pr.html_url)
    return f"PR created: {pr.html_url}"
