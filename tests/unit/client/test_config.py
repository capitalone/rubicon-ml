from unittest.mock import patch

import pytest

from rubicon_ml.client import Config
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import LocalRepository, MemoryRepository, S3Repository


def test_parameters():
    config = Config("memory", "./rubicon-file-system")

    assert config.persistence == "memory"
    assert config.root_dir == "./rubicon-file-system"


def test_enviroment():
    with patch.dict("os.environ", {"PERSISTENCE": "memory", "ROOT_DIR": "./rubicon-file-system"}):
        config = Config()

        assert config.persistence == "memory"
        assert config.root_dir == "./rubicon-file-system"


def test_init_local_repository():
    config = Config("filesystem", "/local/rubicon-file-system")

    assert isinstance(config.repository, LocalRepository)
    assert config.root_dir == config.repository.root_dir


def test_init_s3_repository():
    config = Config("filesystem", "s3://rubicon-file-system")

    assert isinstance(config.repository, S3Repository)
    assert config.root_dir == config.repository.root_dir


def test_init_memory_repository():
    config = Config("memory", "/memory/rubicon-file-system")

    assert isinstance(config.repository, MemoryRepository)
    assert config.root_dir == config.repository.root_dir


def test_invalid_persistence():
    with pytest.raises(ValueError) as e:
        Config("invalid")

    assert str(Config.PERSISTENCE_TYPES) in str(e)


def test_invalid_root_dir():
    with pytest.raises(ValueError) as e:
        Config("filesystem")

    assert "root_dir cannot be None" in str(e)


def test_not_in_git_repo():
    with patch("subprocess.run") as mock_run:

        class MockCompletedProcess:
            returncode = 1

        mock_run.return_value = MockCompletedProcess()

        with pytest.raises(RubiconException) as e:
            Config("memory", is_auto_git_enabled=True)

    assert "Not a `git` repo" in str(e)
