from abc import ABC, abstractmethod
from typing import Any

from rubicon_ml import domain


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
    def write_domain(self, *args: Any, **kwargs: Any):
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
        projects = self.read_domains(domain_type=domain.Project)

        return projects

    def write_project_metadata(self, project: domain.Project):
        self.write_domain(project)

    def read_experiment_metadata(self, *args: Any, **kwargs: Any):
        experiment = self.read_domain(*args, **kwargs)

        return experiment

    def read_experiments_metadata(self, *args: Any, **kwargs: Any):
        experiments = self.read_domains(*args, **kwargs)

        return experiments

    def write_experiment_metadata(self, *args: Any, **kwargs: Any):
        self.write_domain(*args, **kwargs)

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

    def read_feature_metadata(self, *args: Any, **kwargs: Any):
        feature = self.read_domain(*args, **kwargs)

        return feature

    def read_features_metadata(self, *args: Any, **kwargs: Any):
        features = self.read_domains(*args, **kwargs)

        return features

    def write_feature_metadata(self, *args: Any, **kwargs: Any):
        self.write_domain(*args, **kwargs)

    def read_metric_metadata(self, *args: Any, **kwargs: Any):
        metric = self.read_domain(*args, **kwargs)

        return metric

    def read_metrics_metadata(self, *args: Any, **kwargs: Any):
        metrics = self.read_domains(*args, **kwargs)

        return metrics

    def write_metric_metadata(self, *args: Any, **kwargs: Any):
        self.write_domain(*args, **kwargs)

    def read_parameter_metadata(self, *args: Any, **kwargs: Any):
        parameter = self.read_domain(*args, **kwargs)

        return parameter

    def read_parameters_metadata(self, *args: Any, **kwargs: Any):
        parameters = self.read_domains(*args, **kwargs)

        return parameters

    def write_parameter_metadata(self, *args: Any, **kwargs: Any):
        self.write_domain(*args, **kwargs)
