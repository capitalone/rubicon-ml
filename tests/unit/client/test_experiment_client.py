from unittest import mock

import pytest

from rubicon_ml import domain
from rubicon_ml.client import Experiment
from rubicon_ml.exceptions import RubiconException


def _raise_error():
    raise RubiconException()


def test_properties(project_client):
    project = project_client

    domain_experiment = domain.Experiment(
        project_name=project.name,
        description="some description",
        name="exp-1",
        model_name="ModelOne model",
        branch_name="branch",
        commit_hash="a-commit-hash",
        training_metadata=domain.utils.TrainingMetadata([("test/path", "SELECT * FROM test")]),
        tags=["x"],
        comments=["this is a comment"],
    )
    experiment = Experiment(domain_experiment, project)

    assert experiment.name == "exp-1"
    assert experiment.description == "some description"
    assert experiment.model_name == "ModelOne model"
    assert experiment.branch_name == "branch"
    assert experiment.commit_hash == "a-commit-hash"
    assert experiment.name == domain_experiment.name
    assert experiment.commit_hash == domain_experiment.commit_hash
    assert experiment.training_metadata == domain_experiment.training_metadata.training_metadata[0]
    assert experiment.tags == domain_experiment.tags
    assert experiment.comments == domain_experiment.comments
    assert experiment.created_at == domain_experiment.created_at
    assert experiment.id == domain_experiment.id
    assert experiment.project == project


def test_get_identifiers(project_client):
    project = project_client
    experiment = project.log_experiment()
    project_name, experiment_id = experiment._get_identifiers()

    assert project_name == project.name
    assert experiment_id == experiment.id


def test_log_metric(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")

    experiment.log_metric("Accuracy", 99)
    experiment.log_metric("AUC", 0.825)

    assert "Accuracy" in [m.name for m in experiment.metrics()]
    assert "AUC" in [m.name for m in experiment.metrics()]


def test_get_metrics(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")

    metric = {"name": "Accuracy", "value": 99}
    experiment.log_metric(metric["name"], metric["value"])

    metrics = experiment.metrics()

    assert len(metrics) == 1
    assert metrics[0].name == metric["name"]
    assert metrics[0].value == metric["value"]


@mock.patch("rubicon_ml.repository.BaseRepository.get_metrics")
def test_get_metrics_multiple_backend_error(mock_get_metrics, project_composite_client):
    project = project_composite_client
    experiment = project.log_experiment(name="exp1")

    mock_get_metrics.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        experiment.metrics()
    assert "all configured storage backends failed" in str(e)


def test_get_metric_by_name(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_metric("accuracy", 100)

    metric = experiment.metric(name="accuracy").name
    assert metric == "accuracy"


def test_get_metric_fails_neither_set(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_metric("accuracy", 100)

    with pytest.raises(ValueError) as e:
        experiment.metric(name=None, id=None)

    assert "`name` OR `id` required." in str(e)


def test_get_metric_fails_both_set(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_metric("accuracy", 100)

    with pytest.raises(ValueError) as e:
        experiment.metric(name="foo", id=123)

    assert "`name` OR `id` required." in str(e)


def test_metrics_tagged_and(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")

    metric = experiment.log_metric(name="name", value=0, tags=["x", "y"])
    experiment.log_metric(name="name_a", value=0, tags=["x"])
    experiment.log_metric(name="name_b", value=0, tags=["y"])

    metrics = experiment.metrics(tags=["x", "y"], qtype="and")

    assert len(metrics) == 1
    assert metric.id in [d.id for d in metrics]


def test_metrics_tagged_or(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")

    metric_a = experiment.log_metric(name="name_a", value=0, tags=["x"])
    metric_b = experiment.log_metric(name="name_b", value=0, tags=["y"])
    experiment.log_metric(name="name_c", value=0, tags=["z"])

    metrics = experiment.metrics(tags=["x", "y"], qtype="or")

    assert len(metrics) == 2
    assert metric_a.id in [d.id for d in metrics]
    assert metric_b.id in [d.id for d in metrics]


def test_get_metric_by_id(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_metric("accuracy", 100)
    metric_id = experiment.metric("accuracy").id

    metric = experiment.metric(id=metric_id).name
    assert metric == "accuracy"


@mock.patch("rubicon_ml.repository.BaseRepository.get_metric")
def test_get_metric_multiple_backend_error(mock_get_metric, project_composite_client):
    project = project_composite_client
    experiment = project.log_experiment(name="exp1")

    mock_get_metric.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        experiment.metric("accuracy")
    assert "all configured storage backends failed" in str(e)


def test_log_feature(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")

    experiment.log_feature("year")

    assert "year" in [f.name for f in experiment.features()]


def test_get_features(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_feature("year")
    experiment.log_feature("credit score")

    features = experiment.features()

    assert len(features) == 2
    assert features[0].name == "year"
    assert features[1].name == "credit score"


@mock.patch("rubicon_ml.repository.BaseRepository.get_features")
def test_get_features_multiple_backend_error(mock_get_features, project_composite_client):
    project = project_composite_client
    experiment = project.log_experiment(name="exp1")

    mock_get_features.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        experiment.features()
    assert "all configured storage backends failed" in str(e)


def test_get_feature_by_name(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_feature("year")

    feature = experiment.feature(name="year").name
    assert feature == "year"


def test_get_feature_by_id(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_feature("year")
    feature_id = experiment.feature("year").id

    feature = experiment.feature(id=feature_id).name
    assert feature == "year"


def test_get_feature_fails_neither_set(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_feature("year")

    with pytest.raises(ValueError) as e:
        experiment.feature(name=None, id=None)

    assert "`name` OR `id` required." in str(e)


def test_get_feature_fails_both_set(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_feature("year")

    with pytest.raises(ValueError) as e:
        experiment.feature(name="foo", id=123)

    assert "`name` OR `id` required." in str(e)


@mock.patch("rubicon_ml.repository.BaseRepository.get_feature")
def test_get_feature_multiple_backend_error(mock_get_feature, project_composite_client):
    project = project_composite_client
    experiment = project.log_experiment(name="exp1")

    mock_get_feature.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        experiment.feature("year")
    assert "all configured storage backends failed" in str(e)


def test_features_tagged_and(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")

    feature = experiment.log_feature(name="name", tags=["x", "y"])
    experiment.log_feature(name="name_a", tags=["x"])
    experiment.log_feature(name="name_b", tags=["y"])

    features = experiment.features(tags=["x", "y"], qtype="and")

    assert len(features) == 1
    assert feature.id in [d.id for d in features]


def test_features_tagged_or(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")

    feature_a = experiment.log_feature(name="name_a", tags=["x"])
    feature_b = experiment.log_feature(name="name_b", tags=["y"])
    experiment.log_feature(name="name_c", tags=["z"])

    features = experiment.features(tags=["x", "y"], qtype="or")

    assert len(features) == 2
    assert feature_a.id in [d.id for d in features]
    assert feature_b.id in [d.id for d in features]


def test_log_parameter(project_client):
    project = project_client
    experiment = project.log_experiment()

    experiment.log_parameter("test", value="value")

    assert "test" in [p.name for p in experiment.parameters()]
    assert "value" in [p.value for p in experiment.parameters()]


def test_parameters(project_client):
    project = project_client
    experiment = project.log_experiment()

    parameter_a = experiment.log_parameter("test_a", value="value_a")
    parameter_b = experiment.log_parameter("test_b", value="value_b")

    parameters = experiment.parameters()

    assert len(parameters) == 2
    assert parameter_a.id in [p.id for p in parameters]
    assert parameter_b.id in [p.id for p in parameters]


@mock.patch("rubicon_ml.repository.BaseRepository.get_parameters")
def test_parameters_multiple_backend_error(mock_get_parameters, project_composite_client):
    project = project_composite_client
    experiment = project.log_experiment(name="exp1")

    mock_get_parameters.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        experiment.parameters()
    assert "all configured storage backends failed" in str(e)


def test_get_parameter_by_name(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_parameter("n_estimators", "estimator")

    parameter = experiment.parameter(name="n_estimators").name
    assert parameter == "n_estimators"


def test_get_parameter_by_id(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_parameter("n_estimators", "estimator")
    parameter_id = experiment.parameter("n_estimators").id

    parameter = experiment.parameter(id=parameter_id).name
    assert parameter == "n_estimators"


def test_get_parameter_fails_neither_set(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_parameter("n_estimators", "estimator")

    with pytest.raises(ValueError) as e:
        experiment.parameter(name=None, id=None)

    assert "`name` OR `id` required." in str(e)


def test_get_parameter_fails_both_set(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_parameter("n_estimators", "estimator")

    with pytest.raises(ValueError) as e:
        experiment.parameter(name="foo", id=123)

    assert "`name` OR `id` required." in str(e)


@mock.patch("rubicon_ml.repository.BaseRepository.get_parameter")
def test_get_parameter_multiple_backend_error(mock_get_parameter, project_composite_client):
    project = project_composite_client
    experiment = project.log_experiment(name="exp1")

    mock_get_parameter.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        experiment.parameter("n_estimators")
    assert "all configured storage backends failed" in str(e)


def test_parameters_tagged_and(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")

    parameter = experiment.log_parameter(name="param_1", tags=["x", "y"])
    experiment.log_parameter(name="param_2", tags=["x"])
    experiment.log_parameter(name="param_3", tags=["y"])

    parameters = experiment.parameters(tags=["x", "y"], qtype="and")

    assert len(parameters) == 1
    assert parameter.id in [d.id for d in parameters]


def test_parameters_tagged_or(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")

    param_a = experiment.log_parameter(name="param_a", tags=["x"])
    param_b = experiment.log_parameter(name="param_b", tags=["y"])
    experiment.log_parameter(name="param_c", tags=["z"])

    parameters = experiment.parameters(tags=["x", "y"], qtype="or")

    assert len(parameters) == 2
    assert param_a.id in [d.id for d in parameters]
    assert param_b.id in [d.id for d in parameters]


def test_add_child_experiment(project_client):
    project = project_client
    parent = project.log_experiment(name="parent")
    child = project.log_experiment(name="child")

    parent.add_child_experiment(child)

    assert f"parent:{parent.id}" in child.tags
    assert f"child:{child.id}" in parent.tags


def test_add_child_experiment_error(rubicon_and_project_client):
    rubicon, project = rubicon_and_project_client
    another_project = rubicon.create_project(name="another one")

    parent = project.log_experiment(name="parent")
    child = another_project.log_experiment(name="child")

    with pytest.raises(RubiconException) as error:
        parent.add_child_experiment(child)

    assert "Descendents must be logged to the same project." in str(error)


def test_get_child_experiments(project_client):
    project = project_client
    parent = project.log_experiment(name="parent")
    child = project.log_experiment(name="child")

    parent.add_child_experiment(child)

    assert parent.get_child_experiments()[0].id == child.id


def test_get_parent_experiments(project_client):
    project = project_client
    parent = project.log_experiment(name="parent")
    child = project.log_experiment(name="child")

    parent.add_child_experiment(child)

    assert child.get_parent_experiments()[0].id == parent.id


def test_get_relative_experiments_none(project_client):
    project = project_client
    experiment = project.log_experiment()

    assert experiment.get_child_experiments() == []
    assert experiment.get_parent_experiments() == []
