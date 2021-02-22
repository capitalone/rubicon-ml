import os
import uuid
from unittest.mock import patch

import fsspec

from rubicon import domain
from rubicon.repository import LocalRepository
from rubicon.repository.utils import slugify


def test_initialization():
    local_repo = LocalRepository(root_dir="/local/root")

    assert local_repo.PROTOCOL == "file"
    assert type(local_repo.filesystem) == fsspec.implementations.local.LocalFileSystem


@patch("fsspec.implementations.local.LocalFileSystem.open")
@patch("fsspec.implementations.local.LocalFileSystem.mkdirs")
def test_persist_bytes(mock_mkdirs, mock_open):
    bytes_data = b"test data {uuid.uuid4()}"
    bytes_path = "/local/root/path/to/data"

    local_repo = LocalRepository(root_dir="/local/root")
    local_repo._persist_bytes(bytes_data, bytes_path)

    mock_mkdirs.assert_called_once_with(os.path.dirname(bytes_path), exist_ok=True)
    mock_open.assert_called_once_with(bytes_path, "wb")


@patch("fsspec.implementations.local.LocalFileSystem.open")
@patch("fsspec.implementations.local.LocalFileSystem.mkdirs")
def test_persist_domain(mock_mkdirs, mock_open):
    project = domain.Project(f"Test Project {uuid.uuid4()}")
    project_metadata_path = f"/local/root/{slugify(project.name)}/metadata.json"

    local_repo = LocalRepository(root_dir="/local/root")
    local_repo._persist_domain(project, project_metadata_path)

    mock_mkdirs.assert_called_once_with(os.path.dirname(project_metadata_path), exist_ok=True)
    mock_open.assert_called_once_with(project_metadata_path, "w")
