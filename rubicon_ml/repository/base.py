from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Literal, Optional, Union

import pandas as pd

from rubicon_ml import domain
from rubicon_ml.domain.comment_update import CommentUpdate
from rubicon_ml.domain.tag_update import TagUpdate

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

    # -------- Convenience Methods: Projects --------

    def create_project(self, project: domain.Project):
        """Persist a project to the configured backend."""
        self.write_domain(project, project.name)

    def get_project(self, project_name: str) -> domain.Project:
        """Retrieve a project from the configured backend."""
        return self.read_domain(domain.Project, project_name)

    def get_projects(self) -> List[domain.Project]:
        """Retrieve all projects from the configured backend."""
        return self.read_domains(domain.Project)

    # -------- Convenience Methods: Experiments --------

    def create_experiment(self, experiment: domain.Experiment):
        """Persist an experiment to the configured backend."""
        self.write_domain(experiment, experiment.project_name, experiment_id=experiment.id)

    def get_experiment(self, project_name: str, experiment_id: str) -> domain.Experiment:
        """Retrieve an experiment from the configured backend."""
        return self.read_domain(domain.Experiment, project_name, experiment_id=experiment_id)

    def get_experiments(self, project_name: str) -> List[domain.Experiment]:
        """Retrieve all experiments for a project."""
        return self.read_domains(domain.Experiment, project_name)

    # -------- Convenience Methods: Artifacts --------

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

    def get_artifact_metadata(self, project_name, artifact_id, experiment_id=None):
        """Retrieve an artifact's metadata."""
        return self.read_domain(
            domain.Artifact,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=artifact_id,
            entity_type="Artifact",
        )

    def get_artifacts_metadata(self, project_name, experiment_id=None):
        """Retrieve all artifacts' metadata."""
        return self.read_domains(domain.Artifact, project_name, experiment_id=experiment_id)

    def get_artifact_data(self, project_name, artifact_id, experiment_id=None):
        """Retrieve an artifact's raw data."""
        return self.read_artifact_data(project_name, artifact_id, experiment_id=experiment_id)

    def delete_artifact(self, project_name, artifact_id, experiment_id=None):
        """Delete an artifact."""
        self.remove_domain(
            domain.Artifact,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=artifact_id,
            entity_type="Artifact",
        )

    # -------- Convenience Methods: Dataframes --------

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

    def get_dataframe_metadata(self, project_name, dataframe_id, experiment_id=None):
        """Retrieve a dataframe's metadata."""
        return self.read_domain(
            domain.Dataframe,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=dataframe_id,
            entity_type="Dataframe",
        )

    def get_dataframes_metadata(self, project_name, experiment_id=None):
        """Retrieve all dataframes' metadata."""
        return self.read_domains(domain.Dataframe, project_name, experiment_id=experiment_id)

    def get_dataframe_data(self, project_name, dataframe_id, experiment_id=None, df_type="pandas"):
        """Retrieve a dataframe's raw data."""
        return self.read_dataframe_data(
            project_name,
            dataframe_id,
            experiment_id=experiment_id,
            df_type=df_type,
        )

    def delete_dataframe(self, project_name, dataframe_id, experiment_id=None):
        """Delete a dataframe."""
        self.remove_domain(
            domain.Dataframe,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=dataframe_id,
            entity_type="Dataframe",
        )

    # -------- Convenience Methods: Features --------

    def create_feature(self, feature, project_name, experiment_id):
        """Persist a feature to the configured backend."""
        self.write_domain(
            feature,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=feature.name,
            entity_type="Feature",
        )

    def get_feature(self, project_name, experiment_id, feature_name):
        """Retrieve a feature."""
        return self.read_domain(
            domain.Feature,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=feature_name,
            entity_type="Feature",
        )

    def get_features(self, project_name, experiment_id):
        """Retrieve all features for an experiment."""
        return self.read_domains(domain.Feature, project_name, experiment_id=experiment_id)

    # -------- Convenience Methods: Metrics --------

    def create_metric(self, metric, project_name, experiment_id):
        """Persist a metric to the configured backend."""
        self.write_domain(
            metric,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=metric.name,
            entity_type="Metric",
        )

    def get_metric(self, project_name, experiment_id, metric_name):
        """Retrieve a metric."""
        return self.read_domain(
            domain.Metric,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=metric_name,
            entity_type="Metric",
        )

    def get_metrics(self, project_name, experiment_id):
        """Retrieve all metrics for an experiment."""
        return self.read_domains(domain.Metric, project_name, experiment_id=experiment_id)

    # -------- Convenience Methods: Parameters --------

    def create_parameter(self, parameter, project_name, experiment_id):
        """Persist a parameter to the configured backend."""
        self.write_domain(
            parameter,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=parameter.name,
            entity_type="Parameter",
        )

    def get_parameter(self, project_name, experiment_id, parameter_name):
        """Retrieve a parameter."""
        return self.read_domain(
            domain.Parameter,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=parameter_name,
            entity_type="Parameter",
        )

    def get_parameters(self, project_name, experiment_id):
        """Retrieve all parameters for an experiment."""
        return self.read_domains(domain.Parameter, project_name, experiment_id=experiment_id)

    # -------- Convenience Methods: Tags --------

    def add_tags(
        self, project_name, tags, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Persist tags to the configured backend."""
        self.write_domain(
            TagUpdate(added_tags=tags),
            project_name,
            experiment_id=experiment_id,
            entity_identifier=entity_identifier,
            entity_type=entity_type,
        )

    def remove_tags(
        self, project_name, tags, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Remove tags from the configured backend."""
        self.write_domain(
            TagUpdate(removed_tags=tags),
            project_name,
            experiment_id=experiment_id,
            entity_identifier=entity_identifier,
            entity_type=entity_type,
        )

    def get_tags(self, project_name, experiment_id=None, entity_identifier=None, entity_type=None):
        """Retrieve tags from the configured backend.

        Returns
        -------
        list of dict
            A list of dictionaries with keys ``added_tags`` and/or
            ``removed_tags``.
        """
        updates = self.read_domains(
            TagUpdate,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=entity_identifier,
            entity_type=entity_type,
        )
        return [u.__dict__ for u in updates]

    # -------- Convenience Methods: Comments --------

    def add_comments(
        self, project_name, comments, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Persist comments to the configured backend."""
        self.write_domain(
            CommentUpdate(added_comments=comments),
            project_name,
            experiment_id=experiment_id,
            entity_identifier=entity_identifier,
            entity_type=entity_type,
        )

    def remove_comments(
        self, project_name, comments, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Remove comments from the configured backend."""
        self.write_domain(
            CommentUpdate(removed_comments=comments),
            project_name,
            experiment_id=experiment_id,
            entity_identifier=entity_identifier,
            entity_type=entity_type,
        )

    def get_comments(
        self, project_name, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Retrieve comments from the configured backend.

        Returns
        -------
        list of dict
            A list of dictionaries with keys ``added_comments`` and/or
            ``removed_comments``.
        """
        updates = self.read_domains(
            CommentUpdate,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=entity_identifier,
            entity_type=entity_type,
        )
        return [u.__dict__ for u in updates]
