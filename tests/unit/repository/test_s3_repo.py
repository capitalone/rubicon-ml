import uuid
from unittest.mock import patch

import pandas as pd
import pytest
import s3fs

from rubicon_ml import domain
from rubicon_ml.repository import S3Repository
from rubicon_ml.repository.utils import slugify


def test_initialization():
    s3_repo = S3Repository(root_dir="s3://bucket/root")

    assert isinstance(s3_repo.filesystem, s3fs.core.S3FileSystem)


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


@patch("s3fs.core.S3FileSystem.open")
def test_persist_domain_throws_error(mock_open):
    not_serializable = str

    project = domain.Project(f"Test Project {uuid.uuid4()}", description=not_serializable)
    project_metadata_path = f"s3://bucket/root/{slugify(project.name)}/metadata.json"

    s3_repo = S3Repository(root_dir="s3://bucket/root")
    with pytest.raises(TypeError):
        s3_repo._persist_domain(project, project_metadata_path)

    mock_open.assert_not_called()


@patch("s3fs.core.S3FileSystem.mkdirs")
@patch("pandas.DataFrame.to_parquet")
def test_persist_dataframe(mock_to_parquet, mock_mkdirs):
    s3_repo = S3Repository(root_dir="s3://bucket/root", storage_option_a="test")
    df = pd.DataFrame([[0, 1], [1, 0]], columns=["a", "b"])

    s3_repo._persist_dataframe(df, s3_repo.root_dir)

    mock_to_parquet.assert_called_once_with(
        f"{s3_repo.root_dir}/data.parquet",
        engine="pyarrow",
        storage_options={"storage_option_a": "test"},
    )


@patch("pandas.read_parquet")
def test_read_dataframe(mock_read_parquet):
    s3_repo = S3Repository(root_dir="s3://bucket/root", storage_option_a="test")
    s3_repo._read_dataframe(s3_repo.root_dir)

    mock_read_parquet.assert_called_once_with(
        f"{s3_repo.root_dir}/data.parquet",
        engine="pyarrow",
        storage_options={"storage_option_a": "test"},
    )
