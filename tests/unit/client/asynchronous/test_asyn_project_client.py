import asyncio
import uuid
from unittest.mock import call

from rubicon_ml import domain


def test_log_experiment(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment = asyncio.run(project.log_experiment())

    expected = [call.create_experiment(experiment._domain)]

    assert experiment.project.name == project_name
    assert rubicon.repository.mock_calls[1:] == expected


def test_get_experiment(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment_domain = domain.Experiment(project_name=project.name)

    rubicon.repository.get_experiment.return_value = experiment_domain

    experiment = asyncio.run(project.experiment(experiment_domain.id))

    expected = [call.get_experiment(project.name, experiment_domain.id)]

    assert experiment.id == experiment_domain.id
    assert rubicon.repository.mock_calls[1:] == expected


def test_get_experiments(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment_domains = [domain.Experiment(project_name=project.name) for _ in range(0, 3)]

    rubicon.repository.get_experiments.return_value = experiment_domains

    experiments = asyncio.run(project.experiments())

    expected = [call.get_experiments(project.name)]

    experiment_ids = [e.id for e in experiments]
    for eid in [e.id for e in experiment_domains]:
        assert eid in experiment_ids
        experiment_ids.remove(eid)

    assert len(experiment_ids) == 0
    assert rubicon.repository.mock_calls[1:] == expected


def test_filter_experiments(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))

    experiment_a = asyncio.run(project.log_experiment(tags=["a"]))
    experiment_b = asyncio.run(project.log_experiment(tags=["b"]))

    rubicon.repository.get_tags.side_effect = [[{"added_tags": "a"}], [{"added_tags": "b"}]]
    asyncio.run(project._filter_experiments([experiment_a, experiment_b], ["a"], "and"))

    assert project._experiments == [experiment_a]


def test_to_dask_df(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))

    experiment_domains = [
        domain.Experiment(project_name=project_name, name=f"Test Experiment {uuid.uuid4()}")
        for _ in range(0, 2)
    ]

    parameter_domains = [domain.Parameter("n_components")]
    metric_domains = [domain.Metric("accuracy", 90)]

    rubicon.repository.get_experiments.return_value = experiment_domains
    rubicon.repository.get_tags.return_value = [{"added_tags": [], "removed_tags": []}]
    rubicon.repository.get_parameters.return_value = parameter_domains
    rubicon.repository.get_metrics.return_value = metric_domains

    ddf = asyncio.run(project.to_dask_df())
    df = ddf.compute()

    # check that all experiments made it into df
    assert len(df) == 2

    # check the cols within the df
    exp_details = ["id", "name", "description", "model_name", "commit_hash", "tags", "created_at"]
    for detail in exp_details:
        assert detail in df.columns
