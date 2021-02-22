import pytest

from rubicon.domain import Metric


def test_metric_throws_error_if_invalid_directionality():
    with pytest.raises(ValueError) as e:
        Metric("accuracy", 99, directionality="invalid")

    assert str(["score", "loss"]) in str(e)
