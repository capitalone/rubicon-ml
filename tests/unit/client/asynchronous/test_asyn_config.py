import pytest

from rubicon_ml.client.asynchronous import Config
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.asynchronous import S3Repository


def test_get_asyn_s3_repository():
    config = Config("filesystem", "s3://rubicon-file-system")

    assert isinstance(config.repository, S3Repository)
    assert config.root_dir == config.repository.root_dir


def test_get_asyn_local_repository_throws_error():
    with pytest.raises(RubiconException) as e:
        Config("filesystem", "/local/rubicon-file-system")

    cls_name = f"{Config.__module__}.{Config.__name__}"
    assert f"{cls_name} has no persistence layer for the provided configuration" in str(e)
