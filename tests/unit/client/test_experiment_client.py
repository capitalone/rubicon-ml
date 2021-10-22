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


def test_log_feature(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")

    experiment.log_feature("age")

    assert "age" in [f.name for f in experiment.features()]


def test_get_features(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_feature("age")
    experiment.log_feature("credit score")

    features = experiment.features()

    assert len(features) == 2
    assert features[0].name == "age"
    assert features[1].name == "credit score"


def test_get_feature_by_name(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_feature("age")

    feature = experiment.feature(name="age").name
    assert feature.name == "age"


def test_get_feature_by_id(project_client):
    project = project_client
    experiment = project.log_experiment(name="exp1")
    experiment.log_feature("age")
    feature_id = experiment.feature("age").id

    feature = experiment.feature(feature_id).name
    assert feature.name == "age"


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
