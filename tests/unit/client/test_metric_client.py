from rubicon_ml import domain
from rubicon_ml.client import Metric


def test_properties():
    domain_metric = domain.Metric("Accuracy", 99, description="some description")
    metric = Metric(domain_metric)

    assert metric.name == "Accuracy"
    assert metric.value == 99
    assert metric.directionality == "score"
    assert metric.description == "some description"
    assert metric.id == domain_metric.id
    assert metric.created_at == domain_metric.created_at
