from rubicon_ml import domain
from rubicon_ml.client import Feature


def test_properties(project_client):
    parent = project_client
    domain_feature = domain.Feature(
        name="year",
        description="year feature",
        importance=0.5,
        tags=["x"],
        comments=["this is a comment"],
    )
    feature = Feature(domain_feature, parent)
    print(feature.tags)

    assert feature.name == "year"
    assert feature.description == "year feature"
    assert feature.importance == 0.5
    assert feature.id == domain_feature.id
    assert feature.tags == domain_feature.tags
    assert feature.comments == domain_feature.comments
    assert feature.created_at == domain_feature.created_at
    assert hasattr(feature, "parent")
