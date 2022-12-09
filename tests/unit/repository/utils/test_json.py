from base64 import b64encode
from datetime import date, datetime

import numpy as np

from rubicon_ml.domain.utils import TrainingMetadata
from rubicon_ml.repository.utils import json


def test_can_serialize_datetime():
    now = datetime.utcnow()
    to_serialize = {"date": now, "other": None}
    serialized = json.dumps(to_serialize)

    assert "datetime" in serialized
    assert now.strftime("%Y-%m-%d %H:%M:%S.%f") in serialized


def test_can_deserialize_datetime():
    now = datetime.utcnow()
    to_deserialize = (
        '{"date": {"_type": "datetime", "value": "' + now.strftime("%Y-%m-%d %H:%M:%S.%f") + '"}}'
    )
    deserialized = json.loads(to_deserialize)

    assert deserialized["date"] == now


def test_can_serialize_date():
    a_date = date(2021, 1, 1)
    to_serialize = {"date": a_date, "other": None}
    serialized = json.dumps(to_serialize)

    assert "date" in serialized
    assert a_date.isoformat() in serialized


def test_can_deserialize_date():
    a_date = date(2021, 1, 1)
    to_deserialize = '{"date": {"_type": "date", "value": "' + a_date.isoformat() + '"}}'
    deserialized = json.loads(to_deserialize)

    assert deserialized["date"] == a_date


def test_can_serialize_set():
    tags = ["tag-a", "tag-b"]
    to_serialize = {"tags": set(tags), "other": None}
    serialized = json.dumps(to_serialize)

    assert "set" in serialized
    assert "tag-a" in serialized
    assert "tag-b" in serialized


def test_can_deserialize_set():
    to_deserialize = '{"tags": {"_type": "set", "value": ["tag-b", "tag-a"]}}'
    deserialized = json.loads(to_deserialize)

    assert deserialized["tags"] == set(["tag-a", "tag-b"])


def test_can_serialize_numpy_scalar():
    int32 = np.int32(5)
    to_serialize = {"int32": int32}
    serialized = json.dumps(to_serialize)

    assert "numpy" in serialized
    assert "[]" in serialized
    assert np.lib.format.dtype_to_descr(int32.dtype) in serialized
    assert b64encode(int32.tobytes()).decode() in serialized


def test_can_deserialize_numpy_scalar():
    to_deserialize = (
        '{"int32": {"_type": "numpy", "_dtype": "<i4", "_shape": [], "value": "BQAAAA=="}}'
    )
    deserialized = json.loads(to_deserialize)

    assert deserialized["int32"] == np.int32(5)


def test_can_serialize_numpy_ndarray():
    float32_arr = np.array([np.float32(0.5)] * 4).reshape((2, 2))
    to_serialize = {"float32_arr": float32_arr}
    serialized = json.dumps(to_serialize)

    assert "numpy" in serialized
    assert "[2, 2]" in serialized
    assert np.lib.format.dtype_to_descr(float32_arr.dtype) in serialized
    assert b64encode(float32_arr.tobytes()).decode() in serialized


def test_can_deserialize_numpy_ndarray():
    to_deserialize = (
        '{"float32_arr": {"_type": "numpy", "_dtype": "<f4", "_shape": [2, 2], "value": '
        '"AAAAPwAAAD8AAAA/AAAAPw=="}}'
    )
    deserialized = json.loads(to_deserialize)

    assert (deserialized["float32_arr"] == np.array([np.float32(0.5)] * 4).reshape((2, 2))).all()


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
    to_deserialize = (
        '{"training_metadata": {"_type": "training_metadata", "value": '
        '[["test/path", "SELECT * FROM test"], ["test/other/path", "SELECT * FROM test"]]}}'
    )
    deserialized = json.loads(to_deserialize)

    assert isinstance(deserialized["training_metadata"], TrainingMetadata)
    assert deserialized["training_metadata"].training_metadata == [
        ("test/path", "SELECT * FROM test"),
        ("test/other/path", "SELECT * FROM test"),
    ]
