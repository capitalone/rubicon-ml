from unittest import mock

import pytest

from rubicon_ml.domain import Experiment, Project


@pytest.mark.parametrize(
    ["domain_cls", "required_kwargs"],
    [
        (Project, {"name": "test_domain_extra_kwargs"}),
        (Experiment, {"project_name": "test_domain_extra_kwargs"}),
    ],
)
def test_domain_extra_kwargs(domain_cls, required_kwargs):
    with mock.patch("rubicon_ml.domain.mixin.LOGGER.warning") as mock_logger_warning:
        domain = domain_cls(extra="extra", **required_kwargs)

    mock_logger_warning.assert_called_once_with(
        f"{domain_cls.__name__}.__init__() got an unexpected keyword argument(s): `extra`",
    )

    assert "extra" not in domain.__dict__
    for key, value in required_kwargs.items():
        assert getattr(domain, key) == value
