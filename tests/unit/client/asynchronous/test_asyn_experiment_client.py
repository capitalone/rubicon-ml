import asyncio
import uuid
from unittest.mock import call

from rubicon_ml import domain


def test_log_feature(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment = asyncio.run(project.log_experiment())

    feature_name = f"test feature {uuid.uuid4()}"
    feature = asyncio.run(experiment.log_feature(name=feature_name))

    expected = [call.create_feature(feature._domain, project_name, experiment.id)]

    assert feature.name == feature_name
    assert rubicon.repository.mock_calls[2:] == expected


def test_get_features(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment = asyncio.run(project.log_experiment())

    feature_name = f"test feature {uuid.uuid4()}"
    feature_domains = [domain.Feature(name=f"{feature_name} {i}") for i in range(0, 3)]

    rubicon.repository.get_features.return_value = feature_domains

    features = asyncio.run(experiment.features())

    expected = [call.get_features(project_name, experiment.id)]

    feature_ids = [f.id for f in features]
    for feature_id in [f.id for f in feature_domains]:
        assert feature_id in feature_ids
        feature_ids.remove(feature_id)

    assert len(feature_ids) == 0
    assert rubicon.repository.mock_calls[2:] == expected


def test_log_parameter(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment = asyncio.run(project.log_experiment())

    parameter_name = f"test parameter {uuid.uuid4()}"
    parameter = asyncio.run(experiment.log_parameter(name=parameter_name, value=0))

    expected = [call.create_parameter(parameter._domain, project_name, experiment.id)]

    assert parameter.name == parameter_name
    assert rubicon.repository.mock_calls[2:] == expected


def test_get_parameters(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment = asyncio.run(project.log_experiment())

    parameter_name = f"test parameter {uuid.uuid4()}"
    parameter_domains = [domain.Feature(name=f"{parameter_name} {i}") for i in range(0, 3)]

    rubicon.repository.get_parameters.return_value = parameter_domains

    parameters = asyncio.run(experiment.parameters())

    expected = [call.get_parameters(project_name, experiment.id)]

    parameter_ids = [p.id for p in parameters]
    for parameter_id in [p.id for p in parameter_domains]:
        assert parameter_id in parameter_ids
        parameter_ids.remove(parameter_id)

    assert len(parameter_ids) == 0
    assert rubicon.repository.mock_calls[2:] == expected


def test_log_metric(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment = asyncio.run(project.log_experiment())

    metric_name = f"test metric {uuid.uuid4()}"
    metric = asyncio.run(experiment.log_metric(name=metric_name, value=0))

    expected = [call.create_metric(metric._domain, project_name, experiment.id)]

    assert metric.name == metric_name
    assert rubicon.repository.mock_calls[2:] == expected


def test_get_metrics(asyn_client_w_mock_repo):
    rubicon = asyn_client_w_mock_repo

    project_name = f"Test Project {uuid.uuid4()}"
    project = asyncio.run(rubicon.create_project(project_name))
    experiment = asyncio.run(project.log_experiment())

    metric_name = f"test metric {uuid.uuid4()}"
    metric_domains = [domain.Feature(name=f"{metric_name} {i}") for i in range(0, 3)]

    rubicon.repository.get_metrics.return_value = metric_domains

    metrics = asyncio.run(experiment.metrics())

    expected = [call.get_metrics(project_name, experiment.id)]

    metric_ids = [m.id for m in metrics]
    for metric_id in [m.id for m in metric_domains]:
        assert metric_id in metric_ids
        metric_ids.remove(metric_id)

    assert len(metric_ids) == 0
    assert rubicon.repository.mock_calls[2:] == expected
