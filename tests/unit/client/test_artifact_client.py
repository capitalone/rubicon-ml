import os
from unittest.mock import MagicMock, patch

from rubicon_ml import domain
from rubicon_ml.client import Artifact


def test_properties(project_client):
    parent = project_client
    domain_artifact = domain.Artifact(name="test.txt")
    artifact = Artifact(domain_artifact, parent)

    assert artifact.id == domain_artifact.id
    assert artifact.name == domain_artifact.name
    assert artifact.description == domain_artifact.description
    assert artifact.created_at == domain_artifact.created_at
    assert artifact.parent == parent


def test_get_data(project_client):
    project = project_client
    data = b"content"
    artifact = project.log_artifact(name="test.txt", data_bytes=data)
    artifact._get_data()

    assert artifact.data == data


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

    assert artifact.get_data(unpickle=True).value == test_object.value


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
