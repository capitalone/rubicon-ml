from typing import TYPE_CHECKING, List

from rubicon_ml.repository.v2.base import BaseRepository
from rubicon_ml.repository.v2.fsspec import (
    LocalRepository,
    MemoryRepository,
    S3Repository,
)
from rubicon_ml.repository.v2.logger import LoggerRepository

if TYPE_CHECKING:
    from rubicon_ml import domain


class V1CompatibilityMixin:
    """Mixin to make V2 repositories compaitble with the client.

    For use in integration testing. Will be removed later after any necessary
    client updates.
    """

    def create_project(self, project: "domain.Project"):
        self.write_project_metadata(project)

    def get_project(self, project_name: str) -> "domain.Project":
        return self.read_project_metadata(project_name)

    def get_projects(self) -> List["domain.Project"]:
        return self.read_projects_metadata()


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
