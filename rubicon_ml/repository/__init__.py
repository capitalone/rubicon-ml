from rubicon_ml.repository.base import RepositoryBase
from rubicon_ml.repository.fsspec import FsspecRepository
from rubicon_ml.repository.local import LocalRepository
from rubicon_ml.repository.memory import MemoryRepository
from rubicon_ml.repository.s3 import S3Repository
from rubicon_ml.repository.wandb import WandBRepository


class BaseRepository(FsspecRepository):
    """Deprecated. Use ``FsspecRepository`` directly for filesystem backends
    or ``RepositoryBase`` for the abstract interface.

    This shim exists for backwards compatibility with code that subclasses
    ``BaseRepository``. It will be removed in a future release.
    """

    pass


__all__ = [
    "BaseRepository",
    "FsspecRepository",
    "LocalRepository",
    "MemoryRepository",
    "RepositoryBase",
    "S3Repository",
    "WandBRepository",
]
