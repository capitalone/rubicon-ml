import asyncio
import uuid
from unittest.mock import MagicMock, call, patch

from dask import dataframe as dd

from rubicon import domain
from rubicon.client.asynchronous import ArtifactMixin, DataframeMixin, TagMixin


def test_log_dataframe(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))

    mock_dataframe = MagicMock(spec=dd.DataFrame)
    dataframe = asyncio.run(DataframeMixin.log_dataframe(project, mock_dataframe))

    expected = [
        call.create_dataframe(dataframe._domain, mock_dataframe, project.name, experiment_id=None)
    ]

    assert dataframe.parent.id == project.id
    assert rubicon.repository.mock_calls[1:] == expected


def test_get_dataframes(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    dataframe_domains = [domain.Dataframe(parent_id=project.id) for _ in range(0, 3)]

    rubicon.repository.get_dataframes_metadata.return_value = dataframe_domains

    dataframes = asyncio.run(DataframeMixin.dataframes(project))

    expected = [call.get_dataframes_metadata(project.name, experiment_id=None)]

    dataframe_ids = [d.id for d in dataframes]
    for dataframe_id in [d.id for d in dataframe_domains]:
        assert dataframe_id in dataframe_ids
        dataframe_ids.remove(dataframe_id)

    assert len(dataframe_ids) == 0
    assert rubicon.repository.mock_calls[1:] == expected


def test_delete_dataframes(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    dataframe_domains = [domain.Dataframe(parent_id=project.id) for _ in range(0, 3)]

    dataframe_ids = [d.id for d in dataframe_domains]
    asyncio.run(DataframeMixin.delete_dataframes(project, dataframe_ids))

    expected = [
        call.delete_dataframe(project.name, dataframe_id, experiment_id=None)
        for dataframe_id in dataframe_ids
    ]

    assert rubicon.repository.mock_calls[1:] == expected


def test_filter_dataframes(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))

    mock_dataframe = MagicMock(spec=dd.DataFrame)
    dataframe_a = asyncio.run(DataframeMixin.log_dataframe(project, mock_dataframe, tags=["a"]))
    dataframe_b = asyncio.run(DataframeMixin.log_dataframe(project, mock_dataframe, tags=["b"]))

    rubicon.repository.get_tags.side_effect = [[{"added_tags": "a"}], [{"added_tags": "b"}]]
    asyncio.run(project._filter_dataframes([dataframe_a, dataframe_b], ["a"], "and"))

    assert project._dataframes == [dataframe_a]


def test_log_artifact(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))

    artifact_data = b"test artifact data"
    artifact_name = f"Test Artifact {uuid.uuid4()}"
    artifact = asyncio.run(
        ArtifactMixin.log_artifact(project, data_bytes=artifact_data, name=artifact_name)
    )

    expected = [
        call.create_artifact(artifact._domain, artifact_data, project.name, experiment_id=None)
    ]

    assert artifact.parent.id == project.id
    assert rubicon.repository.mock_calls[1:] == expected


def test_log_conda_environment(asyn_client_w_mock_repo, mock_completed_process_empty):
    rubicon = asyn_client_w_mock_repo
    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = mock_completed_process_empty
        artifact = asyncio.run(project.log_conda_environment())

    expected = [call("conda env export", capture_output=True, check=True)]

    assert mock_run.mock_calls == expected
    assert ".yml" in artifact.name


def test_log_pip_requirements(asyn_client_w_mock_repo, mock_completed_process_empty):
    rubicon = asyn_client_w_mock_repo
    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = mock_completed_process_empty
        artifact = asyncio.run(project.log_pip_requirements())

    expected = [call("pip freeze", capture_output=True, check=True)]

    assert mock_run.mock_calls == expected
    assert ".txt" in artifact.name


def test_get_artifacts(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    artifact_domains = [
        domain.Artifact(parent_id=project.id, name=f"Test Artifact {uuid.uuid4()}")
        for _ in range(0, 3)
    ]

    rubicon.repository.get_artifacts_metadata.return_value = artifact_domains

    artifacts = asyncio.run(ArtifactMixin.artifacts(project))

    expected = [call.get_artifacts_metadata(project.name, experiment_id=None)]

    artifact_ids = [a.id for a in artifacts]
    for artifact_id in [a.id for a in artifact_domains]:
        assert artifact_id in artifact_ids
        artifact_ids.remove(artifact_id)

    assert len(artifact_ids) == 0
    assert rubicon.repository.mock_calls[1:] == expected


def test_delete_artifacts(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    artifact_domains = [
        domain.Artifact(parent_id=project.id, name=f"Test Artifact {uuid.uuid4()}")
        for _ in range(0, 3)
    ]

    artifact_ids = [a.id for a in artifact_domains]
    asyncio.run(ArtifactMixin.delete_artifacts(project, artifact_ids))

    expected = [
        call.delete_artifact(project.name, artifact_id, experiment_id=None)
        for artifact_id in artifact_ids
    ]

    assert rubicon.repository.mock_calls[1:] == expected


def test_add_tags(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment = asyncio.run(project.log_experiment())

    tags = ["x"]
    asyncio.run(TagMixin.add_tags(experiment, tags))

    expected = [call.add_tags(project_name, tags, experiment_id=experiment.id, dataframe_id=None)]

    rubicon.repository.get_tags.return_value = [{"added_tags": tag} for tag in tags]

    assert rubicon.repository.mock_calls[2:] == expected
    assert asyncio.run(experiment.tags) == tags


def test_remove_tags(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment = asyncio.run(project.log_experiment(tags=["x", "y"]))

    tags = ["x"]
    asyncio.run(TagMixin.remove_tags(experiment, tags))

    expected = [
        call.remove_tags(project_name, tags, experiment_id=experiment.id, dataframe_id=None)
    ]

    rubicon.repository.get_tags.return_value = [{"removed_tags": tag} for tag in tags]

    assert rubicon.repository.mock_calls[2:] == expected
    assert asyncio.run(experiment.tags) == ["y"]
