from abc import abstractmethod

from rubicon_ml.repository.v2.base import BaseRepository


class FsspecRepository(BaseRepository):
    """Base repository for `fsspec`-based backends."""

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def _get_filesystem(self, *args, **kwargs):
        ...

    # core read/writes

    def read_domain(self, *args, **kwargs):
        return

    def read_domains(self, *args, **kwargs):
        return

    def write_domain(self, *args, **kwargs):
        return

    # binary read/writes

    def read_artifact_data(self, *args, **kwargs):
        return

    def write_artifact_data(self, *args, **kwargs):
        return

    def read_dataframe_data(self, *args, **kwargs):
        return

    def write_dataframe_data(self, *args, **kwargs):
        return


class LocalRepository(FsspecRepository):
    """Local filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self, *args, **kwargs):
        return "file"


class MemoryRepository(FsspecRepository):
    """In-memory filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self, *args, **kwargs):
        return "memory"


class S3Repository(FsspecRepository):
    """S3 filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self, *args, **kwargs):
        return "s3"


class LocalRepositoryV2(LocalRepository):
    """`LocalRepository` alias for testing and integration."""

    pass


class MemoryRepositoryV2(MemoryRepository):
    """`MemoryRepository` alias for testing and integration."""

    pass


class S3RepositoryV2(S3Repository):
    """`S3Repository` alias for testing and integration."""

    pass
