from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Literal, Optional, Union

import pandas as pd

from rubicon_ml import domain

if TYPE_CHECKING:
    import dask.dataframe as dd
    import polars as pl


class RepositoryBase(ABC):
    """Abstract base class defining the repository interface for rubicon-ml.

    All repository backends must implement the 8 abstract methods defined here.
    The ~30 concrete convenience methods (with V1 names like ``create_project``,
    ``get_experiment``, etc.) delegate to these abstracts and can be selectively
    overridden by subclasses when the default composition doesn't fit.

    Abstract methods
    ----------------
    write_domain, read_domain, read_domains, remove_domain
        Core CRUD for any domain object (including TagUpdate/CommentUpdate).
    write_artifact_data, read_artifact_data
        Binary data persistence for artifacts.
    write_dataframe_data, read_dataframe_data
        Dataframe persistence (pandas/dask/polars).
    """

    PROTOCOL: Optional[str] = None

    # -------- Abstract Methods --------

    @abstractmethod
    def write_domain(
        self,
        domain_obj,
        project_name: str,
        *,
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        """Persist a domain object."""
        ...

    @abstractmethod
    def read_domain(
        self,
        domain_cls,
        project_name: str,
        *,
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        """Read a single domain object."""
        ...

    @abstractmethod
    def read_domains(
        self,
        domain_cls,
        project_name: Optional[str] = None,
        *,
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        """Read all domain objects of a given type."""
        ...

    @abstractmethod
    def remove_domain(
        self,
        domain_cls,
        project_name: str,
        *,
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        """Remove a domain object."""
        ...

    @abstractmethod
    def write_artifact_data(
        self,
        data: bytes,
        project_name: str,
        artifact_id: str,
        *,
        experiment_id: Optional[str] = None,
    ):
        """Persist artifact binary data."""
        ...

    @abstractmethod
    def read_artifact_data(
        self,
        project_name: str,
        artifact_id: str,
        *,
        experiment_id: Optional[str] = None,
    ) -> bytes:
        """Read artifact binary data."""
        ...

    @abstractmethod
    def write_dataframe_data(
        self,
        df: Union[pd.DataFrame, "dd.DataFrame", "pl.DataFrame"],
        project_name: str,
        dataframe_id: str,
        *,
        experiment_id: Optional[str] = None,
    ):
        """Persist dataframe data."""
        ...

    @abstractmethod
    def read_dataframe_data(
        self,
        project_name: str,
        dataframe_id: str,
        *,
        experiment_id: Optional[str] = None,
        df_type: Literal["pandas", "dask", "polars"] = "pandas",
    ):
        """Read dataframe data."""
        ...

    # -------- Convenience Methods --------
    # These methods have genuinely different per-backend semantics and
    # must be overridden by subclasses. Simple CRUD (experiments, metrics,
    # features, parameters, tags, comments) should be handled by calling
    # the abstract methods directly from the client layer.

    def create_project(self, project: domain.Project):
        """Persist a project to the configured backend."""
        self.write_domain(project, project.name)

    def get_project(self, project_name: str) -> domain.Project:
        """Retrieve a project from the configured backend."""
        return self.read_domain(domain.Project, project_name)

    def get_projects(self) -> List[domain.Project]:
        """Retrieve all projects from the configured backend."""
        return self.read_domains(domain.Project)

    def create_artifact(self, artifact, data, project_name, experiment_id=None):
        """Persist an artifact (metadata + data) to the configured backend."""
        self.write_domain(
            artifact,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=artifact.id,
            entity_type="Artifact",
        )
        self.write_artifact_data(data, project_name, artifact.id, experiment_id=experiment_id)

    def create_dataframe(self, dataframe, data, project_name, experiment_id=None):
        """Persist a dataframe (metadata + data) to the configured backend."""
        self.write_domain(
            dataframe,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=dataframe.id,
            entity_type="Dataframe",
        )
        self.write_dataframe_data(data, project_name, dataframe.id, experiment_id=experiment_id)
