"""Databricks helpers built on the Databricks SDK."""

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


class DBFSUploader:
    """Uploads a local directory tree to DBFS, preserving its relative layout."""

    def __init__(self, client: "WorkspaceClient"):
        self.client = client

    def upload_directory(self, local_directory: str, dbfs_directory: str) -> list[str]:
        """Upload directory contents to DBFS. Returns list of DBFS paths."""
        result: list[str] = []
        for root, _, files in os.walk(local_directory):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, local_directory)
                dbfs_path = os.path.join(dbfs_directory, relative_path).replace(
                    "\\", "/"
                )
                with open(local_path, "rb") as fh:
                    try:
                        self.client.dbfs.upload(dbfs_path, fh, overwrite=True)
                        logging.info("Uploaded %s to %s", local_path, dbfs_path)
                        result.append(dbfs_path)
                    except Exception:
                        logging.exception(
                            "Failed to upload %s to %s", local_path, dbfs_path
                        )
                        raise
        return result
