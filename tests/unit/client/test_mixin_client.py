import subprocess
import tempfile
import warnings
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch

import h2o
import pandas as pd
import pytest
from h2o import H2OFrame
from h2o.estimators.random_forest import H2ORandomForestEstimator

from rubicon_ml.client.mixin import (
    ArtifactMixin,
    CommentMixin,
    DataframeMixin,
    TagMixin,
)
from rubicon_ml.exceptions import RubiconException


def _raise_error():
    raise RubiconException()


# ArtifactMixin
def test_log_artifact_from_bytes(project_client):
    project = project_client
    artifact = ArtifactMixin.log_artifact(
        project, data_bytes=b"content", name="test.txt", tags=["x"], comments=["this is a comment"]
    )

    assert artifact.id in [a.id for a in project.artifacts()]
    assert artifact.name == "test.txt"
    assert artifact.data == b"content"
    assert artifact.tags == ["x"]
    assert artifact.comments == ["this is a comment"]


def test_log_artifact_from_directory(project_client):
    """Test logging artifacts from a directory."""
    project = project_client

    with tempfile.TemporaryDirectory() as temp_dir:
        with open(Path(temp_dir, "test_file_a.txt"), "w") as file:
            file.write("testing rubicon")

        with open(Path(temp_dir, "test_file_b.txt"), "w") as file:
            file.write("testing rubicon again")

        artifact = ArtifactMixin.log_artifact(
            project,
            data_directory=temp_dir,
            name="test.zip",
            tags=["x"],
            comments=["this is a comment"],
        )

    assert artifact.id in [a.id for a in project.artifacts()]
    assert artifact.name == "test.zip"
    assert artifact.tags == ["x"]
    assert artifact.comments == ["this is a comment"]

    with artifact.temporary_download(unzip=True) as temp_artifact_dir:
        assert Path(temp_artifact_dir, "test_file_a.txt").exists()
        assert Path(temp_artifact_dir, "test_file_b.txt").exists()


def test_log_artifact_from_file(project_client):
    project = project_client
    mock_file = MagicMock()
    mock_file.__enter__().read.side_effect = [b"content"]
    artifact = ArtifactMixin.log_artifact(
        project, data_file=mock_file, name="test.txt", tags=["x"], comments=["this is a comment"]
    )

    assert artifact.id in [a.id for a in project.artifacts()]
    assert artifact.name == "test.txt"
    assert artifact.data == b"content"
    assert artifact.tags == ["x"]
    assert artifact.comments == ["this is a comment"]


@patch("fsspec.implementations.local.LocalFileSystem.open")
def test_log_artifact_from_path(mock_open, project_client):
    project = project_client
    mock_file = MagicMock()
    mock_file().read.side_effect = [b"content"]
    mock_open.side_effect = mock_file
    artifact = ArtifactMixin.log_artifact(
        project, data_path="/path/to/test.txt", tags=["x"], comments=["this is a comment"]
    )

    assert artifact.id in [a.id for a in project.artifacts()]
    assert artifact.name == "test.txt"
    assert artifact.data == b"content"
    assert artifact.tags == ["x"]
    assert artifact.comments == ["this is a comment"]


def test_log_artifact_throws_error_if_data_missing(project_client):
    project = project_client
    with pytest.raises(RubiconException) as e:
        ArtifactMixin.log_artifact(project, name="test.txt")

    assert (
        "One of `data_bytes`, `data_directory`, `data_file`, `data_object` "
        "or `data_path` must be provided." in str(e)
    )


def test_log_artifact_throws_error_if_name_missing_data_bytes(project_client):
    project = project_client
    with pytest.raises(RubiconException) as e:
        ArtifactMixin.log_artifact(project, data_bytes=b"content")

    assert "`name` must be provided if not using `data_path`." in str(e)


def test_log_artifact_throws_error_if_name_missing_data_file(project_client):
    project = project_client
    mock_file = MagicMock()
    mock_file.__enter__().read.side_effect = [b"content"]
    with pytest.raises(RubiconException) as e:
        ArtifactMixin.log_artifact(project, data_file=mock_file)

    assert "`name` must be provided if not using `data_path`." in str(e)


def test_log_artifact_throws_error_if_name_missing_data_object(project_client):
    project = project_client

    class TestObject:
        value = "test"

    test_object = TestObject()
    with pytest.raises(RubiconException) as e:
        ArtifactMixin.log_artifact(project, data_object=test_object)

    assert "`name` must be provided if not using `data_path`." in str(e)


def test_get_environment_bytes(project_client, mock_completed_process_empty):
    project = project_client

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = mock_completed_process_empty
        env_bytes = project._get_environment_bytes(["conda", "env", "export"])

    assert env_bytes == b"\n"


def test_get_environment_bytes_error(project_client):
    project = project_client

    with pytest.raises(RubiconException) as e:
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=-1, cmd=b"\n", stderr="yikes"
            )
            project._get_environment_bytes(["conda", "env", "export"])

    assert "yikes" in str(e)


def test_log_conda_env(project_client, mock_completed_process_empty):
    project = project_client

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = mock_completed_process_empty
        artifact = project.log_conda_environment()

    assert artifact.id in [a.id for a in project.artifacts()]
    assert ".yml" in artifact.name
    assert artifact.data == b"\n"


def test_log_pip_requirements(project_client, mock_completed_process_empty):
    project = project_client

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = mock_completed_process_empty
        artifact = project.log_pip_requirements()

    assert artifact.id in [a.id for a in project.artifacts()]
    assert ".txt" in artifact.name
    assert artifact.data == b"\n"


def test_log_json(project_client):
    project = project_client

    data = {"hello": "world", "foo": [1, 2, 3]}
    artifact_a = ArtifactMixin.log_json(project, data, name="test.json")
    artifact_b = ArtifactMixin.log_json(project, data, name="test.txt")

    artifacts = ArtifactMixin.artifacts(project)

    assert len(artifacts) == 2
    assert artifact_a.id in [a.id for a in artifacts]
    assert artifact_b.id in [a.id for a in artifacts]


@pytest.mark.parametrize("use_mojo", [False, True])
def test_log_h2o_model(
    make_classification_df, rubicon_local_filesystem_client_with_project, use_mojo
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

    artifact = project.log_h2o_model(h2o_model, use_mojo=use_mojo, tags=["h2o"])
    read_artifact = project.artifact(name=artifact.name)

    assert artifact.id == read_artifact.id
    assert artifact.name == h2o_model.__class__.__name__
    assert artifact.tags == ["h2o"]


def test_log_h2o_model_raises_error(project_client):
    """Test logging `h2o` model in memory raises error."""
    project = project_client

    h2o_model = H2ORandomForestEstimator()

    with pytest.raises(RubiconException) as error:
        project.log_h2o_model(h2o_model, tags=["h2o"])

    assert "`h2o` models cannot be logged in memory" in str(error)


def test_artifacts(project_client):
    project = project_client
    data = b"content"
    artifact_a = ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")
    artifact_b = ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")

    artifacts = ArtifactMixin.artifacts(project)

    assert len(artifacts) == 2
    assert artifact_a.id in [a.id for a in artifacts]
    assert artifact_b.id in [a.id for a in artifacts]


@mock.patch("rubicon_ml.repository.BaseRepository.get_artifacts_metadata")
def test_artifacts_multiple_backend_error(mock_get_artifacts_metadata, project_composite_client):
    project = project_composite_client

    mock_get_artifacts_metadata.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        ArtifactMixin.artifacts(project)
    assert "all configured storage backends failed" in str(e)


def test_artifacts_by_name(project_client):
    project = project_client
    data = b"content"
    artifact_a = ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")
    artifact_b = ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")
    ArtifactMixin.log_artifact(project, data_bytes=data, name="test2.txt")

    artifacts = ArtifactMixin.artifacts(project, name="test.txt")

    assert len(artifacts) == 2
    assert artifact_a.id in [a.id for a in artifacts]
    assert artifact_b.id in [a.id for a in artifacts]


def test_artifacts_tagged_and(project_client):
    project = project_client

    artifact = ArtifactMixin.log_artifact(project, name="name", data_bytes=b"test", tags=["x", "y"])
    ArtifactMixin.log_artifact(project, name="name", data_bytes=b"test", tags=["x"])
    ArtifactMixin.log_artifact(project, name="name", data_bytes=b"test", tags=["y"])

    artifacts = ArtifactMixin.artifacts(project, tags=["x", "y"], qtype="and")

    assert len(artifacts) == 1
    assert artifact.id in [d.id for d in artifacts]


def test_artifacts_tagged_or(project_client):
    project = project_client

    artifact_a = ArtifactMixin.log_artifact(project, name="name", data_bytes=b"test", tags=["x"])
    artifact_b = ArtifactMixin.log_artifact(project, name="name", data_bytes=b"test", tags=["y"])
    ArtifactMixin.log_artifact(project, name="name", data_bytes=b"test", tags=["z"])

    artifacts = ArtifactMixin.artifacts(project, tags=["x", "y"], qtype="or")

    assert len(artifacts) == 2
    assert artifact_a.id in [d.id for d in artifacts]
    assert artifact_b.id in [d.id for d in artifacts]


def test_artifact_warning(project_client):
    project = project_client
    data = b"content"
    artifact_a = ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")
    artifact_b = ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")

    with warnings.catch_warnings(record=True) as w:
        artifact_c = ArtifactMixin.artifact(project, name="test.txt")
        assert (
            "Multiple artifacts found with name 'test.txt'. Returning most recently logged"
        ) in str(w[0].message)
    assert artifact_c.id != artifact_a.id
    assert artifact_c.id == artifact_b.id


def test_artifact_name_not_found_error(project_client):
    project = project_client
    with pytest.raises(RubiconException) as e:
        ArtifactMixin.artifact(project, name="test.txt")

    assert "No artifact found with name 'test.txt'." in str(e)


def test_artifacts_name_not_found_error(project_client):
    project = project_client
    artifacts = ArtifactMixin.artifacts(project, name="test.txt")

    assert artifacts == []


def test_artifact_by_name(project_client):
    project = project_client
    data = b"content"
    ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")
    artifact = ArtifactMixin.artifact(project, name="test.txt")

    assert artifact.name == "test.txt"


def test_artifact_by_id(project_client):
    project = project_client
    data = b"content"
    ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")
    artifact = ArtifactMixin.artifact(project, name="test.txt")
    artifact_name = ArtifactMixin.artifact(project, id=artifact.id).name

    assert artifact_name == "test.txt"


@mock.patch("rubicon_ml.repository.BaseRepository.get_artifact_metadata")
def test_artifact_multiple_backend_error(mock_get_artifact_metadata, project_composite_client):
    project = project_composite_client
    data = b"content"
    artifact = ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")

    mock_get_artifact_metadata.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        ArtifactMixin.artifact(project, id=artifact.id)
    assert "all configured storage backends failed" in str(e)


def test_delete_artifacts(project_client):
    project = project_client
    artifact = ArtifactMixin.log_artifact(project, data_bytes=b"content", name="test.txt")

    ArtifactMixin.delete_artifacts(project, [artifact.id])

    assert artifact.id not in [a.id for a in project.artifacts()]


# DataframeMixin


def test_log_dataframe(project_client, test_dataframe):
    project = project_client
    df = test_dataframe
    test_df_name = "test_df"
    dataframe = DataframeMixin.log_dataframe(
        project, df, name=test_df_name, tags=["x"], comments=["this is a comment"]
    )
    DataframeMixin.log_dataframe(
        project, df, name="secondary test df", tags=["x"], comments=["this is a comment"]
    )
    assert dataframe.name == test_df_name
    assert dataframe.tags == ["x"]
    assert dataframe.comments == ["this is a comment"]

    assert dataframe.id in [df.id for df in project.dataframes()]


def test_dataframes(project_client, test_dataframe):
    project = project_client
    df = test_dataframe
    dataframe_a = DataframeMixin.log_dataframe(project, df)
    dataframe_b = DataframeMixin.log_dataframe(project, df)

    dataframes = DataframeMixin.dataframes(project)

    assert len(dataframes) == 2
    assert dataframe_a.id in [d.id for d in dataframes]
    assert dataframe_b.id in [d.id for d in dataframes]


@mock.patch("rubicon_ml.repository.BaseRepository.get_dataframes_metadata")
def test_dataframes_multiple_backend_error(mock_get_dataframes_metadata, project_composite_client):
    project = project_composite_client

    mock_get_dataframes_metadata.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        DataframeMixin.dataframes(project)
    assert "all configured storage backends failed" in str(e)


def test_dataframes_by_name(project_client, test_dataframe):
    project = project_client
    df = test_dataframe
    test_df_name = "test_df"
    dataframe_a = DataframeMixin.log_dataframe(project, df, name=test_df_name)
    dataframe_b = DataframeMixin.log_dataframe(project, df, name=test_df_name)

    dataframes = DataframeMixin.dataframes(project, name=test_df_name)

    assert len(dataframes) == 2
    assert dataframe_a.id in [d.id for d in dataframes]
    assert dataframe_b.id in [d.id for d in dataframes]


def test_dataframe_by_name(project_client, test_dataframe):
    project = project_client
    df = test_dataframe
    test_df_name = "test_df"
    dataframe_a = DataframeMixin.log_dataframe(project, df, name=test_df_name)
    dataframe_b = DataframeMixin.dataframe(project, name=test_df_name)
    assert dataframe_a.id == dataframe_b.id


def test_dataframe_by_id(project_client, test_dataframe):
    project = project_client
    df = test_dataframe
    dataframe_a = DataframeMixin.log_dataframe(project, df)
    id = dataframe_a.id
    dataframe_b = DataframeMixin.dataframe(project, id=id)
    assert dataframe_a.id == dataframe_b.id


@mock.patch("rubicon_ml.repository.BaseRepository.get_dataframe_metadata")
def test_dataframe_multiple_backend_error(
    mock_get_dataframe_metadata, project_composite_client, test_dataframe
):
    project = project_composite_client
    dataframe = DataframeMixin.log_dataframe(project, test_dataframe)

    mock_get_dataframe_metadata.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        DataframeMixin.dataframe(project, id=dataframe.id)
    assert "all configured storage backends failed" in str(e)


def test_dataframe_warning(project_client, test_dataframe):
    project = project_client
    df = test_dataframe
    test_df_name = "test_df"
    dataframe_a = DataframeMixin.log_dataframe(project, df, name=test_df_name)
    dataframe_b = DataframeMixin.log_dataframe(project, df, name=test_df_name)

    with warnings.catch_warnings(record=True) as w:
        dataframe_c = DataframeMixin.dataframe(project, name=test_df_name)
        assert (
            "Multiple dataframes found with name 'test_df'. Returning most recently logged"
        ) in str(w[0].message)
    assert dataframe_c.id != dataframe_a.id
    assert dataframe_c.id == dataframe_b.id


def test_dataframe_by_name_not_found(project_client, test_dataframe):
    project = project_client
    test_df_name = "test_df"
    with pytest.raises(RubiconException) as e:
        DataframeMixin.dataframe(project, name=test_df_name)
    assert "No dataframe found with name 'test_df'." in str(e.value)


def test_dataframes_by_name_not_found(project_client, test_dataframe):
    project = project_client
    test_df_name = "test_df"
    dataframes = DataframeMixin.dataframes(project, name=test_df_name)
    assert dataframes == []


def test_get_dataframe_fails_both_set(project_client, test_dataframe):
    project = project_client
    with pytest.raises(ValueError) as e:
        DataframeMixin.dataframe(project, name="foo", id=123)

    assert "`name` OR `id` required." in str(e.value)


def test_get_dataframe_fails_neither_set(project_client, test_dataframe):
    project = project_client

    with pytest.raises(ValueError) as e:
        DataframeMixin.dataframe(project, name=None, id=None)

    assert "`name` OR `id` required." in str(e.value)


def test_dataframes_tagged_and(project_client, test_dataframe):
    project = project_client
    df = test_dataframe
    dataframe = DataframeMixin.log_dataframe(project, df, tags=["x", "y"])
    DataframeMixin.log_dataframe(project, df, tags=["x"])
    DataframeMixin.log_dataframe(project, df, tags=["y"])

    dataframes = DataframeMixin.dataframes(project, tags=["x", "y"], qtype="and")

    assert len(dataframes) == 1
    assert dataframe.id in [d.id for d in dataframes]


def test_dataframes_tagged_or(project_client, test_dataframe):
    project = project_client
    df = test_dataframe
    dataframe_a = DataframeMixin.log_dataframe(project, df, tags=["x"])
    dataframe_b = DataframeMixin.log_dataframe(project, df, tags=["y"])
    DataframeMixin.log_dataframe(project, df, tags=["z"])

    dataframes = DataframeMixin.dataframes(project, tags=["x", "y"], qtype="or")

    assert len(dataframes) == 2
    assert dataframe_a.id in [d.id for d in dataframes]
    assert dataframe_b.id in [d.id for d in dataframes]


def test_delete_dataframes(project_client, test_dataframe):
    project = project_client
    df = test_dataframe
    dataframe = DataframeMixin.log_dataframe(project, df, tags=["x"])

    DataframeMixin.delete_dataframes(project, [dataframe.id])

    assert dataframe.id not in [df.id for df in project.dataframes()]


# TagMixin


def test_get_taggable_experiment_identifiers(project_client):
    project = project_client
    experiment = project.log_experiment()

    project_name, experiment_id, dataframe_id = TagMixin._get_taggable_identifiers(experiment)

    assert project_name == project.name
    assert experiment_id == experiment.id
    assert dataframe_id is None


def test_get_taggable_dataframe_identifiers(project_client, test_dataframe):
    project = project_client
    experiment = project.log_experiment()

    df = test_dataframe
    project_df = project.log_dataframe(df)
    experiment_df = experiment.log_dataframe(df)

    project_name, experiment_id, dataframe_id = TagMixin._get_taggable_identifiers(project_df)

    assert project_name == project.name
    assert experiment_id is None
    assert dataframe_id == project_df.id

    project_name, experiment_id, dataframe_id = TagMixin._get_taggable_identifiers(experiment_df)

    assert project_name == project.name
    assert experiment_id is experiment.id
    assert dataframe_id == experiment_df.id


def test_get_taggable_artifact_identifiers(project_client):
    project = project_client
    experiment = project.log_experiment()

    project_artifact = project.log_artifact(data_bytes=b"test", name="test")
    experiment_artifact = experiment.log_artifact(data_bytes=b"test", name="test")

    project_name, experiment_id, artifact_id = TagMixin._get_taggable_identifiers(project_artifact)

    assert project_name == project.name
    assert experiment_id is None
    assert artifact_id == project_artifact.id

    project_name, experiment_id, artifact_id = TagMixin._get_taggable_identifiers(
        experiment_artifact
    )

    assert project_name == project.name
    assert experiment_id is experiment.id
    assert artifact_id == experiment_artifact.id


def test_add_tags(project_client):
    project = project_client
    experiment = project.log_experiment()

    TagMixin.add_tags(experiment, ["x"])

    assert experiment.tags == ["x"]


def test_remove_tags(project_client):
    project = project_client
    experiment = project.log_experiment(tags=["x", "y"])

    TagMixin.remove_tags(experiment, ["x", "y"])

    assert experiment.tags == []


@mock.patch("rubicon_ml.repository.BaseRepository.get_tags")
def test_tags_multiple_backend_error(mock_get_tags, project_composite_client):
    project = project_composite_client
    experiment = project.log_experiment(tags=["x", "y"])

    def raise_error():
        raise RubiconException()

    mock_get_tags.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        experiment.tags()
    assert "all configured storage backends failed" in str(e)


def test_add_comments(project_client):
    project = project_client
    experiment = project.log_experiment()

    CommentMixin.add_comments(experiment, ["this is a comment"])

    assert experiment.comments == ["this is a comment"]


def test_remove_comments(project_client):
    project = project_client
    experiment = project.log_experiment(comments=["comment 1", "comment 2"])

    CommentMixin.remove_comments(experiment, ["comment 1", "comment 2"])

    assert experiment.comments == []


@mock.patch("rubicon_ml.repository.BaseRepository.get_comments")
def test_comments_multiple_backend_error(mock_get_comments, project_composite_client):
    project = project_composite_client
    experiment = project.log_experiment(comments=["comment 1", "comment 2"])

    def raise_error():
        raise RubiconException()

    mock_get_comments.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        experiment.comments()
    assert "all configured storage backends failed" in str(e)
