import asyncio
import uuid
from unittest.mock import call

import s3fs

from rubicon import domain
from rubicon.repository.asynchronous import S3Repository
from rubicon.repository.utils import json, slugify


def test_initialization():
    s3_repo = S3Repository(root_dir="s3://bucket/root")

    assert s3_repo.PROTOCOL == "s3"
    assert type(s3_repo.filesystem) == s3fs.core.S3FileSystem


def test_connect(asyn_s3_repo_w_mock_filesystem):
    asyncio.run(asyn_s3_repo_w_mock_filesystem._connect())
    expected = [call._connect()]

    assert asyn_s3_repo_w_mock_filesystem.filesystem.mock_calls == expected


def test_persist_bytes(asyn_s3_repo_w_mock_filesystem):
    bytes_data = b"test data {uuid.uuid4()}"
    bytes_path = "s3://bucket/root/path/to/data"

    asyncio.run(asyn_s3_repo_w_mock_filesystem._persist_bytes(bytes_data, bytes_path))

    expected = [call._pipe_file(bytes_path, bytes_data)]

    assert asyn_s3_repo_w_mock_filesystem.filesystem.mock_calls == expected


def test_persist_domain(asyn_s3_repo_w_mock_filesystem):
    project = domain.Project(f"Test Project {uuid.uuid4()}")
    project_metadata_path = f"s3://bucket/root/{slugify(project.name)}/metadata.json"

    asyncio.run(asyn_s3_repo_w_mock_filesystem._persist_domain(project, project_metadata_path))

    expected = [call._pipe_file(project_metadata_path, json.dumps(project))]

    assert asyn_s3_repo_w_mock_filesystem.filesystem.mock_calls == expected
