import asyncio
import uuid
from unittest.mock import call

import dask.dataframe as dd

from rubicon_ml import domain
from rubicon_ml.client.asynchronous import Rubicon
from rubicon_ml.exceptions import RubiconException


def test_repository_storage_options():
    storage_options = {"key": "secret"}
    rubicon_s3 = Rubicon(persistence="filesystem", root_dir="s3://nothing", **storage_options)

    assert rubicon_s3.config.repository.filesystem.storage_options["key"] == "secret"


def test_create_project(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))

    expected = [call.create_project(project._domain)]

    assert project.name == project_name
    assert rubicon.repository.mock_calls == expected


def test_get_project(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project_domain = domain.Project(name=project_name)

    rubicon.repository.get_project.return_value = project_domain

    project = asyncio.run(rubicon.get_project(project_name))

    expected = [call.get_project(project_name)]

    assert project.name == project_name
    assert rubicon.repository.mock_calls == expected


def test_get_or_create_project_create(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo
    rubicon.repository.get_project.side_effect = RubiconException

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.get_or_create_project(project_name))

    expected = [
        call.get_project(project.name),
        call.create_project(project._domain),
    ]

    assert project.name == project_name
    assert rubicon.repository.mock_calls == expected


def test_get_or_create_project_get(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project_domain = domain.Project(name=project_name)

    rubicon.repository.get_project.return_value = project_domain

    project = asyncio.run(rubicon.get_or_create_project(project_name))

    expected = [call.get_project(project.name)]

    assert project.name == project_name
    assert rubicon.repository.mock_calls == expected


def test_get_projects(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_domains = [domain.Project(name=f"Test Project {uuid.uuid4()}") for _ in range(0, 3)]

    rubicon.repository.get_projects.return_value = project_domains

    projects = asyncio.run(rubicon.projects())

    expected = [call.get_projects()]

    project_ids = [p.id for p in projects]
    for pid in [p.id for p in project_domains]:
        assert pid in project_ids
        project_ids.remove(pid)

    assert len(project_ids) == 0
    assert rubicon.repository.mock_calls == expected


def test_get_projects_as_dask_df(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project_domain = domain.Project(name=project_name)
    experiment_domains = [
        domain.Experiment(project_name=project_name, name=f"Test Experiment {uuid.uuid4()}")
        for _ in range(0, 2)
    ]

    rubicon.repository.get_project.return_value = project_domain
    rubicon.repository.get_experiments.return_value = experiment_domains
    rubicon.repository.get_tags.return_value = [{"added_tags": [], "removed_tags": []}]
    rubicon.repository.get_parameters.return_value = []
    rubicon.repository.get_metrics.return_value = []

    ddf = asyncio.run(rubicon.get_project_as_dask_df(project_name))

    assert isinstance(ddf, dd.core.DataFrame)
    assert len(ddf.compute()) == 2
