from unittest.mock import MagicMock

from rubicon_ml import domain
from rubicon_ml.client import Parameter


def test_properties():
    domain_parameter = domain.Parameter("name", value="value", description="description")
    parameter = Parameter(domain_parameter, MagicMock())

    assert parameter.id == domain_parameter.id
    assert parameter.name == domain_parameter.name
    assert parameter.value == domain_parameter.value
    assert parameter.description == domain_parameter.description
    assert parameter.created_at == domain_parameter.created_at
    assert hasattr(parameter, "parent")
