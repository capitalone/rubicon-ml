from abc import abstractmethod
from typing import Any

from rubicon_ml.repository.v2.base import BaseRepository


class FsspecRepository(BaseRepository):
    """Base repository for `fsspec`-based backends."""

    def __init__(self, *args: Any, **kwargs: Any):
        pass

    @abstractmethod
    def _get_filesystem(self, *args: Any, **kwargs: Any):
        ...

    # core read/writes

    def read_domain(self, *args: Any, **kwargs: Any):
        return

    def read_domains(self, *args: Any, **kwargs: Any):
        return

    def write_domain(self, *args: Any, **kwargs: Any):
        return

    # binary read/writes

    def read_artifact_data(self, *args: Any, **kwargs: Any):
        return

    def write_artifact_data(self, *args: Any, **kwargs: Any):
        return

    def stream_artifact_data(self, *args: Any, **kwargs: Any):
        return

    def read_dataframe_data(self, *args: Any, **kwargs: Any):
        return

    def write_dataframe_data(self, *args: Any, **kwargs: Any):
        return


class LocalRepository(FsspecRepository):
    """Local filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self, *args: Any, **kwargs: Any):
        return "file"


class MemoryRepository(FsspecRepository):
    """In-memory filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self, *args: Any, **kwargs: Any):
        return "memory"


class S3Repository(FsspecRepository):
    """S3 filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self, *args: Any, **kwargs: Any):
        return "s3"
