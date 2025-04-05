from rubicon_ml.repository.v2.base import BaseRepository


class FsspecRepository(BaseRepository):
    """Base repository for `fsspec`-based backends."""

    ...


class LocalRepository(FsspecRepository):
    """Local filesystem repository leveraging `fsspec`."""

    ...


class MemoryRepository(FsspecRepository):
    """In-memory filesystem repository leveraging `fsspec`."""

    ...


class S3Repository(FsspecRepository):
    """S3 filesystem repository leveraging `fsspec`."""

    ...


class LocalRepositoryV2(LocalRepository):
    """`LocalRepository` alias for testing and integration."""

    pass


class MemoryRepositoryV2(MemoryRepository):
    """`MemoryRepository` alias for testing and integration."""

    pass


class S3RepositoryV2(S3Repository):
    """`S3Repository` alias for testing and integration."""

    pass
