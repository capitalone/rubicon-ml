import pytest

from rubicon_ml.domain.utils import TrainingMetadata
from rubicon_ml.exceptions import RubiconException


def test_init_throws_validation_error():
    with pytest.raises(RubiconException) as e:
        training_metadata = ["not", "a", "list", "of", "tuples"]
        TrainingMetadata(training_metadata)

    assert "must be a list of tuples" in str(e)


def test_init_convert_to_list():
    training_metadata = TrainingMetadata(("test/path", "SELECT * FROM test"))

    assert isinstance(training_metadata.training_metadata, list)


def test_repr():
    training_metadata = TrainingMetadata(("test/path", "SELECT * FROM test"))

    assert str(training_metadata) == str(training_metadata.training_metadata)
