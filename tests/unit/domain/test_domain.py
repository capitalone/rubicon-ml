import pytest

from rubicon_ml.domain.project import Project


@pytest.mark.parametrize(
    ["domain_cls", "required_kwargs"],
    [(Project, {"name": "test_domain_extra_kwargs"})],
)
def test_domain_extra_kwargs(domain_cls, required_kwargs):
    domain = domain_cls(extra="extra", **required_kwargs)

    assert "extra" not in domain.__dict__

    for key, value in required_kwargs.items():
        assert getattr(domain, key) == value
