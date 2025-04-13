from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional

from rubicon_ml import domain

if TYPE_CHECKING:
    from rubicon_ml.domain import DOMAIN_TYPES


class BaseRepository(ABC):
    """Abstract base for rubicon-ml backend repositories."""

    # core domain read/writes

    @abstractmethod
    def read_domain(self, *args: Any, **kwargs: Any):
        ...

    @abstractmethod
    def read_domains(self, *args: Any, **kwargs: Any):
        ...

    @abstractmethod
    def write_domain(
        self,
        domain: "DOMAIN_TYPES",
        project_name: Optional[str] = None,
        experiment_id: Optional[str] = None,
    ):
        ...

    # binary read/writes

    @abstractmethod
    def read_artifact_data(self, *args: Any, **kwargs: Any):
        ...

    @abstractmethod
    def stream_artifact_data(self, *args: Any, **kwargs: Any):
        ...

    @abstractmethod
    def write_artifact_data(self, *args: Any, **kwargs: Any):
        ...

    @abstractmethod
    def read_dataframe_data(self, *args: Any, **kwargs: Any):
        ...

    @abstractmethod
    def write_dataframe_data(self, *args: Any, **kwargs: Any):
        ...

    # domain entity read/writes

    def read_project_metadata(self, project_name: str):
        project = self.read_domain(project_name)

        return project

    def read_projects_metadata(self):
        projects = self.read_domains()

        return projects

    def write_project_metadata(self, project: domain.Project):
        self.write_domain(project)

    def read_experiment_metadata(self, project_name: str, experiment_id: str):
        experiment = self.read_domain(project_name, experiment_id)

        return experiment

    def read_experiments_metadata(self, project_name: str):
        experiments = self.read_domains(project_name)

        return experiments

    def write_experiment_metadata(self, experiment: domain.Experiment):
        self.write_domain(experiment)

    def read_artifact_metadata(self, *args: Any, **kwargs: Any):
        artifact = self.read_domain(*args, **kwargs)

        return artifact

    def read_artifacts_metadata(self, *args: Any, **kwargs: Any):
        artifacts = self.read_domains(*args, **kwargs)

        return artifacts

    def write_artifact_metadata(self, *args: Any, **kwargs: Any):
        self.write_domain(*args, **kwargs)

    def read_dataframe_metadata(self, *args: Any, **kwargs: Any):
        dataframe = self.read_domain(*args, **kwargs)

        return dataframe

    def read_dataframes_metadata(self, *args: Any, **kwargs: Any):
        dataframes = self.read_domains(*args, **kwargs)

        return dataframes

    def write_dataframe_metadata(self, *args: Any, **kwargs: Any):
        self.write_domain(*args, **kwargs)

    def read_feature_metadata(self, project_name: str, experiment_id: str, feature_name: str):
        feature = self.read_domain(project_name, experiment_id, feature_name)

        return feature

    def read_features_metadata(self, project_name: str, experiment_id: str):
        features = self.read_domains(project_name, experiment_id, domain_type=domain.Feature)

        return features

    def write_feature_metadata(
        self, feature: domain.Feature, project_name: str, experiment_id: str
    ):
        self.write_domain(feature, project_name, experiment_id)

    def read_metric_metadata(self, project_name: str, experiment_id: str, metric_name: str):
        metric = self.read_domain(project_name, experiment_id, metric_name)

        return metric

    def read_metrics_metadata(self, project_name: str, experiment_id: str):
        metrics = self.read_domains(project_name, experiment_id, domain_type=domain.Metric)

        return metrics

    def write_metric_metadata(self, metric: domain.Metric, project_name: str, experiment_id: str):
        self.write_domain(metric, project_name, experiment_id)

    def read_parameter_metadata(self, project_name: str, experiment_id: str, parameter_name: str):
        parameter = self.read_domain(project_name, experiment_id, parameter_name)

        return parameter

    def read_parameters_metadata(self, project_name: str, experiment_id: str):
        parameters = self.read_domains(project_name, experiment_id, domain_type=domain.Parameter)

        return parameters

    def write_parameter_metadata(
        self, parameter: domain.Parameter, project_name: str, experiment_id: str
    ):
        self.write_domain(parameter, project_name, experiment_id)
