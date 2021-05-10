import subprocess
from unittest import mock

import dask.dataframe as dd
import fsspec
import pytest
import yaml

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


def test_get_project(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client

    assert "Test Project" == rubicon.get_project("Test Project").name


def test_get_projects(rubicon_client):
    rubicon = rubicon_client
    rubicon.create_project("Project A")
    rubicon.create_project("Project B")

    projects = rubicon.projects()

    assert len(projects) == 2
    assert projects[0].name == "Project A"
    assert projects[1].name == "Project B"


def test_get_or_create_project(rubicon_client):
    rubicon = rubicon_client
    created_project = rubicon.get_or_create_project("Test Project A")
    assert created_project._domain.name == "Test Project A"

    fetched_project = rubicon.get_or_create_project("Test Project A")
    assert fetched_project._domain.name == "Test Project A"
    assert created_project.id == fetched_project.id


def test_publish(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client
    experiment = project.log_experiment()

    catalog_yaml = rubicon.publish(project.name)
    catalog = yaml.safe_load(catalog_yaml)

    assert f"project_{project.id.replace('-', '_')}" in catalog["sources"]
    assert (
        "rubicon_project" == catalog["sources"][f"project_{project.id.replace('-', '_')}"]["driver"]
    )
    assert (
        project.repository.root_dir
        == catalog["sources"][f"project_{project.id.replace('-', '_')}"]["args"]["urlpath"]
    )
    assert (
        project.name
        == catalog["sources"][f"project_{project.id.replace('-', '_')}"]["args"]["project_name"]
    )
    assert f"experiment_{experiment.id.replace('-', '_')}" in catalog["sources"]
    assert (
        "rubicon_experiment"
        == catalog["sources"][f"experiment_{experiment.id.replace('-', '_')}"]["driver"]
    )
    assert (
        experiment.repository.root_dir
        == catalog["sources"][f"experiment_{experiment.id.replace('-', '_')}"]["args"]["urlpath"]
    )
    assert (
        experiment.id
        == catalog["sources"][f"experiment_{experiment.id.replace('-', '_')}"]["args"][
            "experiment_id"
        ]
    )
    assert (
        project.name
        == catalog["sources"][f"experiment_{experiment.id.replace('-', '_')}"]["args"][
            "project_name"
        ]
    )


def test_publish_by_ids(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client
    experiment_a = project.log_experiment()
    experiment_b = project.log_experiment()

    catalog_yaml = rubicon.publish(project.name, experiment_ids=[experiment_a.id])
    catalog = yaml.safe_load(catalog_yaml)

    assert f"project_{project.id.replace('-', '_')}" in catalog["sources"]
    assert f"experiment_{experiment_a.id.replace('-', '_')}" in catalog["sources"]
    assert f"experiment_{experiment_b.id.replace('-', '_')}" not in catalog["sources"]


def test_publish_by_tags(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client
    experiment_a = project.log_experiment(tags=["a"])
    experiment_b = project.log_experiment(tags=["b"])

    catalog_yaml = rubicon.publish(project.name, experiment_tags=["a"])
    catalog = yaml.safe_load(catalog_yaml)

    assert f"project_{project.id.replace('-', '_')}" in catalog["sources"]
    assert f"experiment_{experiment_a.id.replace('-', '_')}" in catalog["sources"]
    assert f"experiment_{experiment_b.id.replace('-', '_')}" not in catalog["sources"]


def test_publish_to_file(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client
    project.log_experiment()
    project.log_experiment()

    catalog_yaml = rubicon.publish(project.name, output_filepath="memory://catalog.yml")

    with fsspec.open("memory://catalog.yml", "r") as f:
        written_catalog = f.read()

    assert catalog_yaml == written_catalog


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
    ddf = rubicon.get_project_as_dask_df(name="Test Project")

    assert isinstance(ddf, dd.core.DataFrame)
