"""URL string utilities."""

import re

# Matches the password segment of a "user:password@" URL so it can be
# redacted before any URL (e.g. a CodeArtifact index URL embedding an auth
# token) is logged or surfaced in an exception message.
_URL_CREDENTIALS_RE = re.compile(r"(://[^/\s:@]+:)([^/\s@]+)(@)")


def mask_url_credentials(text: str) -> str:
    """Replace the password segment of any ``user:password@`` URL in ``text`` with ``***``.

    Works on both bare URLs and free-form text (e.g. subprocess stderr) that may
    contain one or more credential-bearing URLs.
    """
    return _URL_CREDENTIALS_RE.sub(r"\1***\3", text)
