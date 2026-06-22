import json
import sqlite3
from datetime import datetime
import uuid
from unittest.mock import patch

import pandas as pd
import pytest

from rubicon_ml import domain
from rubicon_ml.repository import SqliteRepository
from rubicon_ml.repository.utils import slugify

def json_decode_datetimes(obj):
    """
    Convenience decode to make comparing domain objects possible
    """
    if obj.get('_type') == 'datetime':
        return datetime.fromisoformat(obj['value'])
    return obj

def test_initialization(tmp_path):
    sqlite_repo = SqliteRepository(root_dir=tmp_path)

    assert isinstance(sqlite_repo.con, sqlite3.Connection)


def test_persist_bytes(tmp_path):
    bytes_data = b"test data {uuid.uuid4()}"
    bytes_path = tmp_path / "some" / "other" / "path"

    repo = SqliteRepository(tmp_path)
    repo._persist_bytes(bytes_data, bytes_path)

    # Validate we can read the bytes back
    rows = repo.con.execute(f"SELECT * FROM {repo.BINARY_TABLE}").fetchall()
    assert len(rows) == 1
    row = rows.pop()
    assert row[2] == bytes_data


def test_persist_domain(tmp_path):
    project = domain.Project(f"Test Project {uuid.uuid4()}")
    project_metadata_path = tmp_path / f"{slugify(project.name)}/metadata.json"

    repo = SqliteRepository(root_dir=tmp_path)
    repo._persist_domain(project, project_metadata_path)

    # Validate we can read the bytes back
    rows = repo.con.execute(f"SELECT * FROM {repo.DOMAIN_TABLE}").fetchall()
    assert len(rows) == 1
    row = rows.pop()
    project_metadata = json.loads(row[2], object_hook=json_decode_datetimes)
    new_project = domain.Project(**project_metadata)
    assert new_project == project


#
# @patch("s3fs.core.S3FileSystem.open")
# def test_persist_domain_throws_error(mock_open):
#     not_serializable = str
#
#     project = domain.Project(f"Test Project {uuid.uuid4()}", description=not_serializable)
#     project_metadata_path = f"s3://bucket/root/{slugify(project.name)}/metadata.json"
#
#     s3_repo = S3Repository(root_dir="s3://bucket/root")
#     with pytest.raises(TypeError):
#         s3_repo._persist_domain(project, project_metadata_path)
#
#     mock_open.assert_not_called()
#
#
# @patch("s3fs.core.S3FileSystem.mkdirs")
# @patch("pandas.DataFrame.to_parquet")
# def test_persist_dataframe(mock_to_parquet, mock_mkdirs):
#     s3_repo = S3Repository(root_dir="s3://bucket/root", storage_option_a="test")
#     df = pd.DataFrame([[0, 1], [1, 0]], columns=["a", "b"])
#
#     s3_repo._persist_dataframe(df, s3_repo.root_dir)
#
#     mock_to_parquet.assert_called_once_with(
#         f"{s3_repo.root_dir}/data.parquet",
#         engine="pyarrow",
#         storage_options={"storage_option_a": "test"},
#     )
#
#
# @patch("pandas.read_parquet")
# def test_read_dataframe(mock_read_parquet):
#     s3_repo = S3Repository(root_dir="s3://bucket/root", storage_option_a="test")
#     s3_repo._read_dataframe(s3_repo.root_dir)
#
#     mock_read_parquet.assert_called_once_with(
#         f"{s3_repo.root_dir}/data.parquet",
#         engine="pyarrow",
#         storage_options={"storage_option_a": "test"},
#     )
