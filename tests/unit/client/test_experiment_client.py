from rubicon_ml import domain
from rubicon_ml.client import Experiment


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


def test_get_metric_by_name(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_metric("accuracy", 100)

    metric = experiment.metric(name="accuracy").name
    assert metric == "accuracy"


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
