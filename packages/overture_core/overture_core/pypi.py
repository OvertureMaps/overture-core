"""HTTP/PyPI package downloaders.

The CodeArtifact auth token is passed to pip via the ``PIP_INDEX_URL``
environment variable rather than an ``--index-url`` argument, so it never
appears in process arguments or subprocess error messages.
"""

import logging
import os
from urllib.parse import urlparse

import requests

from overture_core.cloud.aws.codeartifact import CodeArtifactPyPiClient
from overture_core.urls import mask_url_credentials


class HttpDownloader:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def download_urls(self, urls: list[str]) -> list[str]:
        return [self.download_url(url) for url in urls if url.strip() != ""]

    def download_url(self, url: str) -> str:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        save_path = os.path.join(self.output_dir, os.path.basename(urlparse(url).path))
        with open(save_path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=2**18):
                if chunk:
                    fh.write(chunk)
        return save_path


class PyPiDownloader:
    def __init__(self, pypi_client: CodeArtifactPyPiClient, output_dir: str):
        self.client = pypi_client
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def download_packages(self, packages: list[str], python_version: str) -> None:
        if not packages:
            return

        # Single pip call: ensures pip resolves a consistent dependency tree
        # (avoids duplicate/conflicting versions when transitive deps overlap
        # with explicitly requested packages).
        pip_command = [
            "download",
            "--python-version",
            python_version,
            "--only-binary",
            ":all:",
            # Required: many packages are published as PEP 440 pre-releases.
            "--pre",
            # We don't care if the package is incompatible with the local Airflow
            # python version; we only want to download it.
            "--ignore-requires-python",
            "--dest",
            self.output_dir,
            *packages,
        ]

        # The index URL embeds the CodeArtifact auth token. Pass it via the
        # environment (PIP_INDEX_URL) instead of an --index-url argument so
        # the token never appears in the process arguments, and therefore
        # cannot leak through any subprocess error message that echoes the
        # command line.
        pip_env = {**os.environ, "PIP_INDEX_URL": self.client.get_url()}

        try:
            import sh  # optional dep (overture-core[codeartifact]); lazy so importing
            # this module doesn't require it on platforms sh doesn't support
            # (e.g. Windows) unless this method is called.

            sh.pip(*pip_command, _env=pip_env)
            logging.info("Successfully downloaded %s and dependencies.", packages)
        except sh.ErrorReturnCode as exc:
            # Mask credentials in case pip echoed the index URL into stderr.
            logging.error(
                "Error downloading pypi packages %s: %s",
                packages,
                mask_url_credentials(exc.stderr.decode()),
            )
            raise
