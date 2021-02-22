from rubicon import domain
from rubicon.client import Feature


def test_properties():
    domain_feature = domain.Feature("age", description="That age tho", importance=0.5)
    feature = Feature(domain_feature)

    assert feature.name == "age"
    assert feature.description == "That age tho"
    assert feature.importance == 0.5
    assert feature.id == domain_feature.id
    assert feature.created_at == domain_feature.created_at
