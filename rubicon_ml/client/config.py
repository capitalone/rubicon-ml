import os
import subprocess
from typing import Dict, Optional, Tuple, Type

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import (
    BaseRepository,
    LocalRepository,
    MemoryRepository,
    S3Repository,
)


class Config:
    """Used to configure `rubicon` client objects.

    Configuration can be specified (in order of precedence) by:
        1. environment variables 'PERSISTENCE' and 'ROOT_DIR'
        2. arguments to `__init__`

    Parameters
    ----------
    persistence : str, optional
        The persistence type. Can be one of ["filesystem", "memory"].
    root_dir : str, optional
        Absolute or relative filepath. Defaults to using the local
        filesystem. Prefix with s3:// to use s3 instead.
    is_auto_git_enabled : bool, optional
        True to use the `git` command to automatically log relevant repository
        information to projects and experiments logged with this client instance,
        False otherwise. Defaults to False.
    storage_options : dict, optional
        Additional keyword arguments specific to the protocol being chosen. They
        are passed directly to the underlying filesystem class.
    """

    PERSISTENCE_TYPES = ["filesystem", "memory"]
    REPOSITORIES: Dict[str, Type[BaseRepository]] = {
        "memory-memory": MemoryRepository,
        "filesystem-local": LocalRepository,
        "filesystem-s3": S3Repository,
    }

    def __init__(
        self,
        persistence: Optional[str] = None,
        root_dir: Optional[str] = None,
        is_auto_git_enabled: bool = False,
        **storage_options,
    ):
        self.storage_options = storage_options

        self.persistence, self.root_dir, self.is_auto_git_enabled = self._load_config(
            persistence, root_dir, is_auto_git_enabled
        )
        self.repository = self._get_repository()

    @staticmethod
    def _check_is_in_git_repo():
        """Raise a `RubiconException` if not called from within a `git` repository."""
        if subprocess.run(["git", "rev-parse", "--git-dir"], capture_output=True).returncode != 0:
            raise RubiconException(
                "Not a `git` repo: Falied to locate the '.git' directory in this or any parent directories."
            )

    @classmethod
    def _load_config(
        cls, persistence: Optional[str], root_dir: Optional[str], is_auto_git_enabled: bool
    ) -> Tuple[str, Optional[str], bool]:
        """Get the configuration values."""
        persistence = os.environ.get("PERSISTENCE", persistence)
        if persistence not in cls.PERSISTENCE_TYPES:
            raise ValueError(f"PERSISTENCE must be one of {cls.PERSISTENCE_TYPES}.")

        root_dir = os.environ.get("ROOT_DIR", root_dir)
        if root_dir is None and persistence == "filesystem":
            raise ValueError("root_dir cannot be None.")

        if is_auto_git_enabled:
            cls._check_is_in_git_repo()

        return (persistence, root_dir, is_auto_git_enabled)

    def _get_protocol(self) -> str:
        """Get the file protocol of the configured root directory."""
        if self.persistence == "memory":
            return "memory"
        elif self.persistence == "filesystem":
            if self.root_dir.startswith("s3://"):
                return "s3"
            else:
                return "local"

        return "custom"  # catch-all for external backends

    def _get_repository(self) -> BaseRepository:
        """Get the repository for the configured persistence type."""
        protocol = self._get_protocol()

        repository_key = f"{self.persistence}-{protocol}"
        repository = self.REPOSITORIES.get(repository_key)

        if repository is None:
            raise RubiconException(
                f"{self.__class__.__module__}.{self.__class__.__name__} has no persistence "
                + f"layer for the provided configuration: `persistence`: {self.persistence}, "
                + f"`protocol` (from `root_dir`): {protocol}"
            )

        return repository(root_dir=self.root_dir, **self.storage_options)
