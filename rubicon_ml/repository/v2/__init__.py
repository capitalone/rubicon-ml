from typing import TYPE_CHECKING, List, Optional

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

    def create_experiment(self, experiment: "domain.Experiment"):
        self.write_experiment_metadata(experiment)

    def get_experiment(self, project_name: str, experiment_id: str) -> "domain.Experiment":
        return self.read_experiment_metadata(project_name, experiment_id)

    def get_experiments(self, project_name: str) -> List["domain.Experiment"]:
        return self.read_experiments_metadata(project_name)

    def create_feature(self, feature: "domain.Feature", project_name: str, experiment_id: str):
        self.write_feature_metadata(feature, project_name, experiment_id)

    def create_artifact(
        self,
        artifact: "domain.Artifact",
        data: bytes,
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        self.write_artifact_metadata(artifact, project_name, experiment_id)
        self.write_artifact_data(data, artifact.id, project_name, experiment_id)

    def get_artifact_metadata(
        self, project_name: str, artifact_id: str, experiment_id: Optional[str] = None
    ) -> "domain.Artifact":
        return self.read_artifact_metadata(project_name, artifact_id, experiment_id)

    def get_artifacts_metadata(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> List["domain.Artifact"]:
        return self.read_artifacts_metadata(project_name, experiment_id)

    def create_dataframe(
        self,
        dataframe: "domain.Dataframe",
        data,
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        self.write_dataframe_metadata(dataframe, project_name, experiment_id)
        self.write_dataframe_data(data, dataframe.id, project_name, experiment_id)

    def get_dataframe_metadata(
        self, project_name: str, dataframe_id: str, experiment_id: Optional[str] = None
    ) -> "domain.Dataframe":
        return self.read_dataframe_metadata(project_name, dataframe_id, experiment_id)

    def get_dataframes_metadata(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> List["domain.Dataframe"]:
        return self.read_dataframes_metadata(project_name, experiment_id)

    def get_feature(
        self, project_name: str, experiment_id: str, feature_name: str
    ) -> "domain.Feature":
        return self.read_feature_metadata(project_name, experiment_id, feature_name)

    def get_features(self, project_name: str, experiment_id: str) -> List["domain.Feature"]:
        return self.read_features_metadata(project_name, experiment_id)

    def create_metric(self, metric: "domain.Metric", project_name: str, experiment_id: str):
        self.write_metric_metadata(metric, project_name, experiment_id)

    def get_metric(
        self, project_name: str, experiment_id: str, metric_name: str
    ) -> "domain.Metric":
        return self.read_metric_metadata(project_name, experiment_id, metric_name)

    def get_metrics(self, project_name: str, experiment_id: str) -> List["domain.Metric"]:
        return self.read_metrics_metadata(project_name, experiment_id)

    def create_parameter(
        self, parameter: "domain.Parameter", project_name: str, experiment_id: str
    ):
        self.write_parameter_metadata(parameter, project_name, experiment_id)

    def get_parameter(
        self, project_name: str, experiment_id: str, parameter_name: str
    ) -> "domain.Parameter":
        return self.read_parameter_metadata(project_name, experiment_id, parameter_name)

    def get_parameters(self, project_name: str, experiment_id: str) -> List["domain.Parameter"]:
        return self.read_parameters_metadata(project_name, experiment_id)


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
