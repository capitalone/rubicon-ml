from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, List, Optional

from rubicon_ml import domain

if TYPE_CHECKING:
    from rubicon_ml.domain import DOMAIN_CLASS_TYPES, DOMAIN_TYPES


class BaseRepository(ABC):
    """Abstract base for rubicon-ml backend repositories."""

    # core domain read/writes

    @abstractmethod
    def read_domain(
        self,
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ) -> "DOMAIN_TYPES": ...

    @abstractmethod
    def read_domains(
        self,
        domain_cls: "DOMAIN_CLASS_TYPES",
        project_name: str,
        experiment_id: Optional[str] = None,
    ) -> List["DOMAIN_TYPES"]: ...

    @abstractmethod
    def write_domain(
        self,
        domain: "DOMAIN_TYPES",
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ): ...

    # binary read/writes

    @abstractmethod
    def read_artifact_data(
        self,
        artifact_id: str,
        project_name: str,
        experiment_id: Optional[str] = None,
    ) -> bytes: ...

    @abstractmethod
    def write_artifact_data(
        self,
        artifact_domain: domain.Artifact,
        project_name: str,
        experiment_id: Optional[str] = None,
    ): ...

    @abstractmethod
    def read_dataframe_data(self, *args: Any, **kwargs: Any): ...

    @abstractmethod
    def write_dataframe_data(self, *args: Any, **kwargs: Any): ...

    # domain entity read/writes

    def read_project_metadata(self, project_name: str) -> domain.Project:
        project = self.read_domain(domain.Project, project_name)

        return project

    def read_projects_metadata(self) -> List[domain.Project]:
        projects = self.read_domains(domain.Project)

        return projects

    def write_project_metadata(self, project: domain.Project):
        self.write_domain(project, project.name)

    def read_experiment_metadata(self, project_name: str, experiment_id: str) -> domain.Experiment:
        experiment = self.read_domain(domain.Experiment, project_name, experiment_id=experiment_id)

        return experiment

    def read_experiments_metadata(self, project_name: str) -> List[domain.Experiment]:
        experiments = self.read_domains(domain.Experiment, project_name)

        return experiments

    def write_experiment_metadata(self, experiment: domain.Experiment):
        self.write_domain(experiment, experiment.project_name, experiment_id=experiment.id)

    def read_artifact_metadata(
        self, project_name: str, artifact_id: str, experiment_id: Optional[str] = None
    ) -> domain.Artifact:
        artifact = self.read_domain(
            domain.Artifact, project_name, artifact_id=artifact_id, experiment_id=experiment_id
        )

        return artifact

    def read_artifacts_metadata(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> List[domain.Artifact]:
        artifacts = self.read_domains(domain.Artifact, project_name, experiment_id=experiment_id)

        return artifacts

    def write_artifact_metadata(
        self, artifact: domain.Artifact, project_name: str, experiment_id: Optional[str] = None
    ):
        self.write_domain(
            artifact, project_name, artifact_id=artifact.id, experiment_id=experiment_id
        )

    def read_dataframe_metadata(
        self, project_name: str, dataframe_id: str, experiment_id: Optional[str] = None
    ) -> domain.Dataframe:
        dataframe = self.read_domain(
            domain.Dataframe, project_name, dataframe_id=dataframe_id, experiment_id=experiment_id
        )

        return dataframe

    def read_dataframes_metadata(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> List[domain.Dataframe]:
        dataframes = self.read_domains(domain.Dataframe, project_name, experiment_id=experiment_id)

        return dataframes

    def write_dataframe_metadata(
        self, dataframe: domain.Dataframe, project_name: str, experiment_id: Optional[str] = None
    ):
        self.write_domain(
            dataframe, project_name, dataframe_id=dataframe.id, experiment_id=experiment_id
        )

    def read_feature_metadata(
        self, project_name: str, experiment_id: str, feature_name: str
    ) -> domain.Feature:
        feature = self.read_domain(
            domain.Feature, project_name, experiment_id=experiment_id, feature_name=feature_name
        )

        return feature

    def read_features_metadata(self, project_name: str, experiment_id: str) -> List[domain.Feature]:
        features = self.read_domains(domain.Feature, project_name, experiment_id=experiment_id)

        return features

    def write_feature_metadata(
        self, feature: domain.Feature, project_name: str, experiment_id: str
    ):
        self.write_domain(
            feature, project_name, experiment_id=experiment_id, feature_name=feature.name
        )

    def read_metric_metadata(
        self, project_name: str, experiment_id: str, metric_name: str
    ) -> domain.Metric:
        metric = self.read_domain(
            domain.Metric, project_name, experiment_id=experiment_id, metric_name=metric_name
        )

        return metric

    def read_metrics_metadata(self, project_name: str, experiment_id: str) -> List[domain.Metric]:
        metrics = self.read_domains(domain.Metric, project_name, experiment_id=experiment_id)

        return metrics

    def write_metric_metadata(self, metric: domain.Metric, project_name: str, experiment_id: str):
        self.write_domain(
            metric, project_name, experiment_id=experiment_id, metric_name=metric.name
        )

    def read_parameter_metadata(
        self, project_name: str, experiment_id: str, parameter_name: str
    ) -> domain.Parameter:
        parameter = self.read_domain(
            domain.Parameter,
            project_name,
            experiment_id=experiment_id,
            parameter_name=parameter_name,
        )

        return parameter

    def read_parameters_metadata(
        self, project_name: str, experiment_id: str
    ) -> List[domain.Parameter]:
        parameters = self.read_domains(domain.Parameter, project_name, experiment_id=experiment_id)

        return parameters

    def write_parameter_metadata(
        self, parameter: domain.Parameter, project_name: str, experiment_id: str
    ):
        self.write_domain(
            parameter, project_name, experiment_id=experiment_id, parameter_name=parameter.name
        )
