"""Unit tests for the DBFSUploader Databricks helper."""

from unittest.mock import MagicMock

import pytest
from databricks.sdk import WorkspaceClient

from overture_core.cloud.databricks import DBFSUploader


@pytest.fixture()
def workspace_client():
    return MagicMock(spec=WorkspaceClient)


class TestDBFSUploader:
    def test_upload_directory_uploads_all_files(self, tmp_path, workspace_client):
        (tmp_path / "sub").mkdir()
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "sub" / "b.txt").write_text("b")

        uploader = DBFSUploader(workspace_client)
        result = uploader.upload_directory(str(tmp_path), "/dbfs/target")

        assert sorted(result) == ["/dbfs/target/a.txt", "/dbfs/target/sub/b.txt"]
        assert workspace_client.dbfs.upload.call_count == 2

    def test_upload_directory_raises_on_failure(self, tmp_path, workspace_client):
        (tmp_path / "a.txt").write_text("a")
        workspace_client.dbfs.upload.side_effect = RuntimeError("boom")

        uploader = DBFSUploader(workspace_client)
        with pytest.raises(RuntimeError, match="boom"):
            uploader.upload_directory(str(tmp_path), "/dbfs/target")

    def test_upload_directory_raises_on_missing_directory(
        self, tmp_path, workspace_client
    ):
        missing = tmp_path / "does-not-exist"

        uploader = DBFSUploader(workspace_client)
        with pytest.raises(NotADirectoryError, match="Not a directory"):
            uploader.upload_directory(str(missing), "/dbfs/target")
        workspace_client.dbfs.upload.assert_not_called()
