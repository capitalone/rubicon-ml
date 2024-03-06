from rubicon_ml import domain
from rubicon_ml.client import Metric


def test_properties(project_client):
    parent = project_client
    domain_metric = domain.Metric(
        "Accuracy",
        99,
        description="some description",
        tags=["x"],
        comments=["this is a comment"],
    )
    metric = Metric(domain_metric, parent)

    assert metric.name == "Accuracy"
    assert metric.value == 99
    assert metric.directionality == "score"
    assert metric.description == "some description"
    assert metric.id == domain_metric.id
    assert metric.tags == domain_metric.tags
    assert metric.comments == domain_metric.comments
    assert metric.created_at == domain_metric.created_at
    assert hasattr(metric, "parent")
