from rubicon_ml import domain
from rubicon_ml.client import Parameter


def test_properties(project_client):
    parent = project_client
    domain_parameter = domain.Parameter(
        "name",
        value="value",
        description="description",
        tags=["x"],
        comments=["this is a comment"],
    )
    parameter = Parameter(domain_parameter, parent)

    assert parameter.id == domain_parameter.id
    assert parameter.name == domain_parameter.name
    assert parameter.value == domain_parameter.value
    assert parameter.description == domain_parameter.description
    assert parameter.tags == domain_parameter.tags
    assert parameter.comments == domain_parameter.comments
    assert parameter.created_at == domain_parameter.created_at
    assert hasattr(parameter, "parent")
