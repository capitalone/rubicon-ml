from datetime import date, datetime

from rubicon_ml.domain.utils import TrainingMetadata
from rubicon_ml.repository.utils import json


def test_can_serialize_datetime():
    now = datetime.utcnow()
    to_serialize = {"date": now, "other": None}
    serialized = json.dumps(to_serialize)

    assert "datetime" in serialized
    assert str(now) in serialized


def test_can_deserialize_datetime():
    now = datetime.utcnow()
    to_deserialize = '{"date": {"_type": "datetime", "value": "' + str(now) + '"}}'
    deserialized = json.loads(to_deserialize)

    assert deserialized["date"] == now


def test_can_serialize_date():
    a_date = date(2021, 1, 1)
    to_serialize = {"date": a_date, "other": None}
    serialized = json.dumps(to_serialize)

    assert "date" in serialized
    assert str(a_date) in serialized


def test_can_deserialize_date():
    a_date = date(2021, 1, 1)
    to_deserialize = '{"date": {"_type": "date", "value": "' + str(a_date) + '"}}'
    deserialized = json.loads(to_deserialize)

    assert deserialized["date"] == a_date


def test_can_serialize_set():
    tags = ["tag-a", "tag-b"]
    to_serialize = {"tags": set(tags), "other": None}
    serialized = json.dumps(to_serialize)

    assert "tags" in serialized
    assert "tag-a" in serialized
    assert "tag-b" in serialized


def test_can_deserialize_set():
    to_deserialize = '{"tags": {"_type": "set", "value": ["tag-b", "tag-a"]}}'
    deserialized = json.loads(to_deserialize)

    assert deserialized["tags"] == set(["tag-a", "tag-b"])


def test_can_serialize_training_metadata():
    training_metadata = TrainingMetadata(
        [("test/path", "SELECT * FROM test"), ("test/other/path", "SELECT * FROM test")]
    )
    to_serialize = {"training_metadata": training_metadata}
    serialized = json.dumps(to_serialize)

    assert "training_metadata" in serialized
    assert (
        '[["test/path", "SELECT * FROM test"], ["test/other/path", "SELECT * FROM test"]]'
        in serialized
    )


def test_can_deserialize_training_metadata():
    to_deserialize = '{"training_metadata": {"_type": "training_metadata", "value": [["test/path", "SELECT * FROM test"], ["test/other/path", "SELECT * FROM test"]]}}'
    deserialized = json.loads(to_deserialize)

    assert isinstance(deserialized["training_metadata"], TrainingMetadata)
    assert deserialized["training_metadata"].training_metadata == [
        ("test/path", "SELECT * FROM test"),
        ("test/other/path", "SELECT * FROM test"),
    ]
