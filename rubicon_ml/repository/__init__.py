import warnings

from rubicon_ml.repository.base import RepositoryBase
from rubicon_ml.repository.fsspec import FsspecRepository
from rubicon_ml.repository.local import LocalRepository
from rubicon_ml.repository.memory import MemoryRepository
from rubicon_ml.repository.s3 import S3Repository
from rubicon_ml.repository.wandb import WandBRepository


class BaseRepository(FsspecRepository):
    """Deprecated. Use ``FsspecRepository`` for filesystem backends
    or ``RepositoryBase`` for the abstract interface.
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        warnings.warn(
            f"{cls.__name__} inherits from `BaseRepository`, which is deprecated. "
            "Inherit from `FsspecRepository` (for filesystem backends) or "
            "`RepositoryBase` (for custom backends) instead.",
            DeprecationWarning,
            stacklevel=2,
        )


__all__ = [
    "BaseRepository",
    "FsspecRepository",
    "LocalRepository",
    "MemoryRepository",
    "RepositoryBase",
    "S3Repository",
    "WandBRepository",
]
