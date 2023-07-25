import subprocess
from unittest import mock

import dask.dataframe as dd
import pandas as pd
import pytest

from rubicon_ml import client, domain
from rubicon_ml.client import Rubicon
from rubicon_ml.exceptions import RubiconException


class TestRepository:
    root_dir = ""

    @property
    def filesystem(self):
        class TestFilesystem:
            def rm(self, path, recursive):
                pass

        return TestFilesystem()


def test_get_repository(rubicon_client):
    rubicon = rubicon_client
    assert rubicon.repository == rubicon.config.repository


def test_set_repository(rubicon_client):
    rubicon = rubicon_client

    test_repo = TestRepository()
    rubicon.repository = test_repo

    assert rubicon.config.repository == test_repo


def test_repository_storage_options():
    storage_options = {"key": "secret"}
    rubicon_memory = Rubicon(persistence="memory", root_dir="./", **storage_options)
    rubicon_s3 = Rubicon(persistence="filesystem", root_dir="s3://nothing", **storage_options)

    assert rubicon_memory.config.repository.filesystem.storage_options["key"] == "secret"
    assert rubicon_s3.config.repository.filesystem.storage_options["key"] == "secret"


def test_get_github_url(rubicon_client, mock_completed_process_git):
    rubicon = rubicon_client

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock_completed_process_git

        expected = [
            mock.call(["git", "remote", "-v"], capture_output=True),
        ]

        assert rubicon._get_github_url() == "github.com"
        assert mock_run.mock_calls == expected


def test_get_github_url_no_remotes(rubicon_client, mock_completed_process_empty):
    rubicon = rubicon_client

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock_completed_process_empty

        assert rubicon._get_github_url() is None


def test_create_project(rubicon_client):
    rubicon = rubicon_client
    project = rubicon.create_project(
        "Test Project A", training_metadata=[("test/path", "SELECT * FROM test")]
    )

    assert project._domain.name == "Test Project A"


def test_create_project_with_auto_git(mock_completed_process_git):
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock_completed_process_git

        rubicon = Rubicon("memory", "test-root", auto_git_enabled=True)
        rubicon.create_project("Test Project A")

        expected = [
            mock.call(["git", "rev-parse", "--git-dir"], capture_output=True),
            mock.call(["git", "remote", "-v"], capture_output=True),
        ]

    assert mock_run.mock_calls == expected

    rubicon.repository.filesystem.store = {}


def test_get_project_by_name(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client

    assert "Test Project" == rubicon.get_project("Test Project").name


def test_get_project_by_id(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client
    project_id = project.id

    assert project_id == rubicon.get_project(id=project_id).id


def test_get_project_fails_both_set(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client
    with pytest.raises(ValueError) as e:
        rubicon.get_project(name="foo", id=123)

    assert "`name` OR `id` required." in str(e.value)


def test_get_project_fails_neither_set(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client
    with pytest.raises(ValueError) as e:
        rubicon.get_project(name=None, id=None)

    assert "`name` OR `id` required." in str(e.value)


<<<<<<< HEAD
=======
@mock.patch("rubicon_ml.repository.BaseRepository.get_project")
def test_get_project_multiple_backend_error(mock_get_project, rubicon_client):
    rubicon = rubicon_client

    def raise_error():
        raise RubiconException()

    mock_get_project.side_effect = raise_error
    with pytest.raises(RubiconException) as e:
        rubicon.get_project(name="Test Project")
    assert "all configured storage backends failed" in str(e)


>>>>>>> 34d3bcbccd2fc9079f3f5c9dd0171c7cf04a51c3
def test_get_projects(rubicon_client):
    rubicon = rubicon_client
    rubicon.create_project("Project A")
    rubicon.create_project("Project B")

    projects = rubicon.projects()

    assert len(projects) == 2
    assert projects[0].name == "Project A"
    assert projects[1].name == "Project B"


@mock.patch("rubicon_ml.repository.BaseRepository.get_projects")
def test_get_projects_multiple_backend_error(mock_get_projects, rubicon_client):
    rubicon = rubicon_client

    def raise_error():
        raise RubiconException()

    mock_get_projects.side_effect = raise_error
    with pytest.raises(RubiconException) as e:
        rubicon.projects()
    assert "all configured storage backends failed" in str(e)


def test_get_or_create_project(rubicon_client):
    rubicon = rubicon_client
    created_project = rubicon.get_or_create_project("Test Project A")
    assert created_project._domain.name == "Test Project A"

    fetched_project = rubicon.get_or_create_project("Test Project A")
    assert fetched_project._domain.name == "Test Project A"
    assert created_project.id == fetched_project.id


def test_sync_from_memory(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client

    with pytest.raises(RubiconException) as e:
        rubicon.sync("Test Project", "s3://test/path")

    assert "can't sync projects written to memory" in str(e)


@mock.patch("subprocess.run")
@mock.patch("rubicon_ml.client.Rubicon.get_project")
def test_sync_from_local(mock_get_project, mock_run):
    rubicon = Rubicon(persistence="filesystem", root_dir="./local/path")
    project_name = "Sync Test Project"
    mock_get_project.return_value = client.Project(domain.Project(project_name))

    rubicon.sync(project_name, "s3://test/path")

    assert "aws s3 sync ./local/path/sync-test-project s3://test/path" in str(
        mock_run._mock_call_args_list
    )


@mock.patch("subprocess.run")
@mock.patch("rubicon_ml.client.Rubicon.get_project")
def test_sync_from_local_error(mock_get_project, mock_run):
    rubicon = Rubicon(persistence="filesystem", root_dir="./local/path")
    project_name = "Sync Test Project"
    mock_get_project.return_value = client.Project(domain.Project(project_name))
    mock_run.side_effect = subprocess.CalledProcessError(
        cmd="aws cli sync", stderr="Some error. I bet it was proxy tho.", returncode=1
    )

    with pytest.raises(RubiconException) as e:
        rubicon.sync(project_name, "s3://test/path")

    assert "Some error. I bet it was proxy tho." in str(e)


def test_get_project_as_dask_df(rubicon_and_project_client_with_experiments):
    rubicon, project = rubicon_and_project_client_with_experiments
    ddf = rubicon.get_project_as_df(name="Test Project", df_type="dask")

    assert isinstance(ddf, dd.core.DataFrame)


def test_get_project_as_pandas_df(rubicon_and_project_client_with_experiments):
    rubicon, project = rubicon_and_project_client_with_experiments
    ddf = rubicon.get_project_as_df(name="Test Project", df_type="pandas")

    assert isinstance(ddf, pd.DataFrame)
