import asyncio
import uuid
from unittest.mock import MagicMock, call, patch


def test_get_artifact_data(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))

    data_bytes = b"test artifact data"
    artifact_name = f"Test Artifact {uuid.uuid4()}"
    artifact = asyncio.run(project.log_artifact(data_bytes=data_bytes, name=artifact_name))

    rubicon.repository.get_artifact_data.return_value = data_bytes

    data = asyncio.run(artifact.data)

    expected = [call.get_artifact_data(project.name, artifact.id, experiment_id=None)]

    assert data == data_bytes
    assert rubicon.repository.mock_calls[2:] == expected


def test_download(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))

    data_bytes = b"test artifact data"
    artifact_name = f"Test Artifact {uuid.uuid4()}"
    artifact = asyncio.run(project.log_artifact(data_bytes=data_bytes, name=artifact_name))

    rubicon.repository.get_artifact_data.return_value = data_bytes

    with patch("fsspec.open") as mock_fsspec:
        mock_file = MagicMock()
        mock_fsspec.return_value = mock_file

        asyncio.run(artifact.download())

        expected = [call.__enter__(), call.__enter__().write(data_bytes)]

        assert mock_file.mock_calls[:2] == expected
