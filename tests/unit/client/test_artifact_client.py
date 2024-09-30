import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import h2o
import pandas as pd
import pytest
import xgboost
from h2o import H2OFrame
from h2o.estimators.generic import H2OGenericEstimator
from h2o.estimators.random_forest import H2ORandomForestEstimator

from rubicon_ml import domain
from rubicon_ml.client import Artifact, Rubicon
from rubicon_ml.exceptions import RubiconException


def test_properties(project_client):
    parent = project_client
    domain_artifact = domain.Artifact(
        name="test.txt",
        tags=["x"],
        comments=["this is a comment"],
    )
    artifact = Artifact(domain_artifact, parent)

    assert artifact.id == domain_artifact.id
    assert artifact.name == domain_artifact.name
    assert artifact.description == domain_artifact.description
    assert artifact.tags == domain_artifact.tags
    assert artifact.comments == domain_artifact.comments
    assert artifact.created_at == domain_artifact.created_at
    assert artifact.parent == parent


def test_get_data(project_client):
    project = project_client
    data = b"content"
    artifact = project.log_artifact(name="test.txt", data_bytes=data)
    artifact._get_data()

    assert artifact.data == data


def test_get_json(project_client):
    project = project_client
    data = {"hello": "world", "numbers": [1, 2, 3]}
    artifact = project.log_json(json_object=data, name="test.json")

    assert artifact.get_json() == data


def test_internal_get_data_multiple_backend_error():
    rb = Rubicon(
        composite_config=[
            {"persistence": "memory", "root_dir": "./memory/rootA"},
            {"persistence": "memory", "root_dir": "./memory/rootB"},
        ]
    )
    project = rb.create_project("test")
    data = b"content"
    artifact = project.log_artifact(name="test.txt", data_bytes=data)
    for repo in rb.repositories:
        repo.delete_artifact(project.name, artifact.id)
    with pytest.raises(RubiconException) as e:
        artifact._get_data()
    assert "all configured storage backends failed" in str(e)


def test_get_data_unpickle_false(project_client):
    project = project_client
    data = b"content"
    artifact = project.log_artifact(name="test.txt", data_bytes=data)

    assert artifact.get_data(unpickle=False) == data


def test_get_data_unpickle_true(project_client):
    """Unpickle=True intended for retrieving python objects
    that were logged as artifacts, hence dummy object is needed.
    """

    project = project_client
    global TestObject  # cannot pickle local variable

    class TestObject:
        value = 1

    test_object = TestObject()
    artifact = project.log_artifact(name="test object", data_object=test_object)

    with pytest.deprecated_call():
        assert artifact.get_data(unpickle=True).value == test_object.value


def test_get_data_deserialize_with_pickle(project_client):
    """deserialize="pickle" intended for retrieving python objects
    that were logged as artifacts, hence dummy object is needed.
    """

    project = project_client
    global TestObject  # cannot pickle local variable

    class TestObject:
        value = 1

    test_object = TestObject()
    artifact = project.log_artifact(name="test object", data_object=test_object)

    assert artifact.get_data(deserialize="pickle").value == test_object.value


def test_get_data_multiple_backend_error():
    rb = Rubicon(
        composite_config=[
            {"persistence": "memory", "root_dir": "./memory/rootA"},
            {"persistence": "memory", "root_dir": "./memory/rootB"},
        ]
    )
    project = rb.create_project("test")
    data = b"content"
    artifact = project.log_artifact(name="test.txt", data_bytes=data)
    for repo in rb.repositories:
        repo.delete_artifact(project.name, artifact.id)
    with pytest.raises(RubiconException) as e:
        artifact.get_data()
    assert "all configured storage backends failed" in str(e)


@patch("fsspec.implementations.local.LocalFileSystem.open")
def test_download_cwd(mock_open, project_client):
    project = project_client
    data = b"content"
    artifact = project.log_artifact(name="test.txt", data_bytes=data)
    artifact.data

    mock_file = MagicMock()
    mock_open.side_effect = mock_file

    artifact.download()

    mock_open.assert_called_once_with(os.path.join(os.getcwd(), artifact.name), mode="wb")
    mock_file().write.assert_called_once_with(data)


@patch("fsspec.implementations.local.LocalFileSystem.open")
def test_download_location(mock_open, project_client):
    project = project_client
    data = b"content"
    artifact = project.log_artifact(name="test.txt", data_bytes=data)
    artifact.data

    mock_file = MagicMock()
    mock_open.side_effect = mock_file

    artifact.download(location="/path/to/tests", name="new_name.txt")

    mock_open.assert_called_once_with("/path/to/tests/new_name.txt", mode="wb")
    mock_file().write.assert_called_once_with(data)


@pytest.mark.parametrize(
    ["use_mojo", "deserialization_method"],
    [
        (False, "h2o"),
        (False, "h2o_binary"),
        (True, "h2o_mojo"),
    ],
)
def test_get_data_deserialize_h2o(
    make_classification_df,
    rubicon_local_filesystem_client_with_project,
    use_mojo,
    deserialization_method,
):
    """Test logging `h2o` model data."""
    _, project = rubicon_local_filesystem_client_with_project
    X, y = make_classification_df

    target_name = "target"
    training_frame = pd.concat([X, pd.Series(y)], axis=1)
    training_frame.columns = [*X.columns, target_name]

    h2o.init()

    training_frame_h2o = H2OFrame(training_frame)
    h2o_model = H2ORandomForestEstimator()

    h2o_model.train(
        training_frame=training_frame_h2o,
        x=list(X.columns),
        y=target_name,
    )

    artifact = project.log_h2o_model(h2o_model, use_mojo=use_mojo)
    artifact_data = artifact.get_data(deserialize=deserialization_method)

    if use_mojo:
        assert isinstance(artifact_data, H2OGenericEstimator)
    else:
        assert artifact_data.__class__ == h2o_model.__class__


def test_get_data_deserialize_xgboost(
    make_classification_df, rubicon_local_filesystem_client_with_project
):
    """Test logging `xgboost` model data."""
    _, project = rubicon_local_filesystem_client_with_project
    X, y = make_classification_df

    model = xgboost.XGBClassifier(n_estimators=2)
    model.fit(X, y)
    model = model.get_booster()

    artifact = project.log_xgboost_model(model)
    artifact_data = artifact.get_data(deserialize="xgboost")

    assert artifact_data.__class__ == model.__class__


def test_download_data_unzip(project_client):
    """Test downloading and unzipping artifact data."""
    project = project_client

    with tempfile.TemporaryDirectory() as temp_dir:
        with open(Path(temp_dir, "test_file_a.txt"), "w") as file:
            file.write("testing rubicon")

        with open(Path(temp_dir, "test_file_b.txt"), "w") as file:
            file.write("testing rubicon again")

        artifact = project.log_artifact(
            data_directory=temp_dir,
            name="test.zip",
            tags=["x"],
            comments=["this is a comment"],
        )

        Path(temp_dir, "test_file_a.txt").unlink()
        Path(temp_dir, "test_file_b.txt").unlink()

        artifact.download(location=temp_dir, unzip=True)

        assert Path(temp_dir, "test_file_a.txt").exists()
        assert Path(temp_dir, "test_file_b.txt").exists()


def test_temporary_download(project_client):
    """Test temporarily downloading artifact data."""
    project = project_client
    data = b"content"
    artifact = project.log_artifact(name="test.txt", data_bytes=data)

    with artifact.temporary_download() as temp_artifact_dir:
        assert Path(temp_artifact_dir, "test.txt").exists()

    assert not Path(temp_artifact_dir, "test.txt").exists()
