"""Unit tests for the DBFSUploader Databricks helper."""

import sys
import types
from unittest.mock import MagicMock

import pytest

from overture_core.cloud.databricks import DBFSUploader


@pytest.fixture()
def workspace_client(monkeypatch):
    mock_client = MagicMock()
    fake_module = types.ModuleType("databricks.sdk")
    fake_module.WorkspaceClient = MagicMock(return_value=mock_client)
    monkeypatch.setitem(sys.modules, "databricks", types.ModuleType("databricks"))
    monkeypatch.setitem(sys.modules, "databricks.sdk", fake_module)
    return mock_client


class TestDBFSUploader:
    def test_upload_directory_uploads_all_files(self, tmp_path, workspace_client):
        (tmp_path / "sub").mkdir()
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "sub" / "b.txt").write_text("b")

        uploader = DBFSUploader()
        result = uploader.upload_directory(str(tmp_path), "/dbfs/target")

        assert sorted(result) == ["/dbfs/target/a.txt", "/dbfs/target/sub/b.txt"]
        assert workspace_client.dbfs.upload.call_count == 2

    def test_upload_directory_raises_on_failure(self, tmp_path, workspace_client):
        (tmp_path / "a.txt").write_text("a")
        workspace_client.dbfs.upload.side_effect = RuntimeError("boom")

        uploader = DBFSUploader()
        with pytest.raises(RuntimeError, match="boom"):
            uploader.upload_directory(str(tmp_path), "/dbfs/target")
