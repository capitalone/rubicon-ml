import uuid
from unittest.mock import patch

import s3fs

from rubicon import domain
from rubicon.repository import S3Repository
from rubicon.repository.utils import slugify


def test_initialization():
    s3_repo = S3Repository(root_dir="s3://bucket/root")

    assert s3_repo.PROTOCOL == "s3"
    assert type(s3_repo.filesystem) == s3fs.core.S3FileSystem


@patch("s3fs.core.S3FileSystem.open")
def test_persist_bytes(mock_open):
    bytes_data = b"test data {uuid.uuid4()}"
    bytes_path = "s3://bucket/root/path/to/data"

    s3_repo = S3Repository(root_dir="s3://bucket/root")
    s3_repo._persist_bytes(bytes_data, bytes_path)

    mock_open.assert_called_once_with(bytes_path, "wb")


@patch("s3fs.core.S3FileSystem.open")
def test_persist_domain(mock_open):
    project = domain.Project(f"Test Project {uuid.uuid4()}")
    project_metadata_path = f"s3://bucket/root/{slugify(project.name)}/metadata.json"

    s3_repo = S3Repository(root_dir="s3://bucket/root")
    s3_repo._persist_domain(project, project_metadata_path)

    mock_open.assert_called_once_with(project_metadata_path, "w")
