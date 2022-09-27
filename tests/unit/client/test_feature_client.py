from unittest.mock import MagicMock

from rubicon_ml import domain
from rubicon_ml.client import Feature


def test_properties():
    domain_feature = domain.Feature("year", description="year feature", importance=0.5)
    feature = Feature(domain_feature, MagicMock())

    assert feature.name == "year"
    assert feature.description == "year feature"
    assert feature.importance == 0.5
    assert feature.id == domain_feature.id
    assert feature.created_at == domain_feature.created_at
    assert hasattr(feature, "parent")
