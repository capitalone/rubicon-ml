import subprocess
from unittest.mock import MagicMock, patch

import pytest

from rubicon.client.mixin import (
    ArtifactMixin,
    DataframeMixin,
    MultiParentMixin,
    TagMixin,
)
from rubicon.exceptions import RubiconException


def test_get_project_identifiers(project_client):
    project = project_client
    project_name, experiment_id = MultiParentMixin._get_parent_identifiers(project)

    assert project_name == project.name
    assert experiment_id is None


def test_get_experiment_identifiers(project_client):
    project = project_client
    experiment = project.log_experiment()
    project_name, experiment_id = MultiParentMixin._get_parent_identifiers(experiment)

    assert project_name == project.name
    assert experiment_id == experiment.id


# ArtifactMixin
def test_log_artifact_from_bytes(project_client):
    project = project_client
    artifact = ArtifactMixin.log_artifact(project, data_bytes=b"content", name="test.txt")

    assert artifact.id in [a.id for a in project.artifacts()]
    assert artifact.name == "test.txt"
    assert artifact.data == b"content"


def test_log_artifact_from_file(project_client):
    project = project_client
    mock_file = MagicMock()
    mock_file.__enter__().read.side_effect = [b"content"]
    artifact = ArtifactMixin.log_artifact(project, data_file=mock_file, name="test.txt")

    assert artifact.id in [a.id for a in project.artifacts()]
    assert artifact.name == "test.txt"
    assert artifact.data == b"content"


@patch("fsspec.implementations.local.LocalFileSystem.open")
def test_log_artifact_from_path(mock_open, project_client):
    project = project_client
    mock_file = MagicMock()
    mock_file().read.side_effect = [b"content"]
    mock_open.side_effect = mock_file
    artifact = ArtifactMixin.log_artifact(project, data_path="/path/to/test.txt")

    assert artifact.id in [a.id for a in project.artifacts()]
    assert artifact.name == "test.txt"
    assert artifact.data == b"content"


def test_log_artifact_throws_error_if_data_missing(project_client):
    project = project_client
    with pytest.raises(RubiconException) as e:
        ArtifactMixin.log_artifact(project, name="test.txt")

    assert "`data_bytes`, `data_file` or `data_path` must be provided" in str(e)


def test_log_artifact_throws_error_if_name_missing(project_client):
    project = project_client
    with pytest.raises(RubiconException) as e:
        ArtifactMixin.log_artifact(project, data_bytes=b"content")

    assert "`name` must be provided" in str(e)


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


def test_artifacts(project_client):
    project = project_client
    data = b"content"
    artifact_a = ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")
    artifact_b = ArtifactMixin.log_artifact(project, data_bytes=data, name="test.txt")

    artifacts = ArtifactMixin.artifacts(project)

    assert len(artifacts) == 2
    assert artifact_a.id in [a.id for a in artifacts]
    assert artifact_b.id in [a.id for a in artifacts]


def test_delete_artifacts(project_client):
    project = project_client
    artifact = ArtifactMixin.log_artifact(project, data_bytes=b"content", name="test.txt")

    ArtifactMixin.delete_artifacts(project, [artifact.id])

    assert artifact.id not in [a.id for a in project.artifacts()]


# DataframeMixin


def test_log_dataframe(project_client, test_dataframe):
    project = project_client
    df = test_dataframe
    dataframe = DataframeMixin.log_dataframe(project, df, tags=["x"])

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
    df = test_dataframe
    logged_df = project.log_dataframe(df)

    project_name, experiment_id, dataframe_id = TagMixin._get_taggable_identifiers(logged_df)

    assert project_name == project.name
    assert experiment_id is None
    assert dataframe_id == logged_df.id


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
