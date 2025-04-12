from rubicon_ml.repository.v2.base import BaseRepository
from rubicon_ml.repository.v2.fsspec import (
    LocalRepository,
    MemoryRepository,
    S3Repository,
)
from rubicon_ml.repository.v2.logger import LoggerRepository


class V1CompatibilityMixin:
    """Mixin to make V2 repositories compaitble with the client.

    For use in integration testing. Will be removed later after
    any necessary client updates.
    """

    pass


class BaseRepositoryV2(BaseRepository, V1CompatibilityMixin):
    """`BaseRepository` alias for testing and integration."""

    pass


class LocalRepositoryV2(LocalRepository, V1CompatibilityMixin):
    """`LocalRepository` alias for testing and integration."""

    pass


class MemoryRepositoryV2(MemoryRepository, V1CompatibilityMixin):
    """`MemoryRepository` alias for testing and integration."""

    pass


class S3RepositoryV2(S3Repository, V1CompatibilityMixin):
    """`S3Repository` alias for testing and integration."""

    pass


class LoggerRepositoryV2(LoggerRepository, V1CompatibilityMixin):
    """`LoggerRepository` alias for testing and integration."""

    pass
