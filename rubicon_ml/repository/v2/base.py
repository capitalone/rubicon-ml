from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from rubicon_ml import domain

if TYPE_CHECKING:
    import dask.dataframe as dd
    import pandas as pd
    import polars as pl

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
        domain_cls: Union["DOMAIN_CLASS_TYPES", Literal["CommentUpdate", "TagUpdate"]],
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
        project_name: Optional[str] = None,
    ) -> List[Union["DOMAIN_TYPES", Dict]]: ...

    @abstractmethod
    def write_domain(
        self,
        domain: Union["DOMAIN_TYPES", Dict],
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
        artifact_data: bytes,
        project_name: str,
        experiment_id: Optional[str] = None,
    ): ...

    @abstractmethod
    def read_dataframe_data(
        self,
        dataframe_id: str,
        dataframe_type: Literal["dask", "dd", "pandas", "pd", "polars", "pl"],
        project_name: str,
        experiment_id: Optional[str] = None,
    ) -> Union["dd.DataFrame", "pd.DataFrame", "pl.DataFrame"]: ...

    @abstractmethod
    def write_dataframe_data(
        self,
        dataframe_data: Union["dd.DataFrame", "pd.DataFrame", "pl.DataFrame"],
        dataframe_id: str,
        project_name: str,
        experiment_id: Optional[str] = None,
    ): ...

    # domain entity read/writes

    def read_project_metadata(self, project_name: str) -> domain.Project:
        return self.read_domain(domain.Project, project_name)

    def read_projects_metadata(self) -> List[domain.Project]:
        return self.read_domains(domain.Project)

    def write_project_metadata(self, project: domain.Project):
        self.write_domain(project, project.name)

    def read_experiment_metadata(self, project_name: str, experiment_id: str) -> domain.Experiment:
        return self.read_domain(domain.Experiment, project_name, experiment_id=experiment_id)

    def read_experiments_metadata(self, project_name: str) -> List[domain.Experiment]:
        return self.read_domains(domain.Experiment, project_name)

    def write_experiment_metadata(self, experiment: domain.Experiment):
        self.write_domain(experiment, experiment.project_name, experiment_id=experiment.id)

    def read_artifact_metadata(
        self, project_name: str, artifact_id: str, experiment_id: Optional[str] = None
    ) -> domain.Artifact:
        return self.read_domain(
            domain.Artifact, project_name, artifact_id=artifact_id, experiment_id=experiment_id
        )

    def read_artifacts_metadata(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> List[domain.Artifact]:
        return self.read_domains(domain.Artifact, project_name, experiment_id=experiment_id)

    def write_artifact_metadata(
        self, artifact: domain.Artifact, project_name: str, experiment_id: Optional[str] = None
    ):
        self.write_domain(
            artifact, project_name, artifact_id=artifact.id, experiment_id=experiment_id
        )

    def read_dataframe_metadata(
        self, project_name: str, dataframe_id: str, experiment_id: Optional[str] = None
    ) -> domain.Dataframe:
        return self.read_domain(
            domain.Dataframe, project_name, dataframe_id=dataframe_id, experiment_id=experiment_id
        )

    def read_dataframes_metadata(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> List[domain.Dataframe]:
        return self.read_domains(domain.Dataframe, project_name, experiment_id=experiment_id)

    def write_dataframe_metadata(
        self, dataframe: domain.Dataframe, project_name: str, experiment_id: Optional[str] = None
    ):
        self.write_domain(
            dataframe, project_name, dataframe_id=dataframe.id, experiment_id=experiment_id
        )

    def read_feature_metadata(
        self, project_name: str, experiment_id: str, feature_name: str
    ) -> domain.Feature:
        return self.read_domain(
            domain.Feature, project_name, experiment_id=experiment_id, feature_name=feature_name
        )

    def read_features_metadata(self, project_name: str, experiment_id: str) -> List[domain.Feature]:
        return self.read_domains(domain.Feature, project_name, experiment_id=experiment_id)

    def write_feature_metadata(
        self, feature: domain.Feature, project_name: str, experiment_id: str
    ):
        self.write_domain(
            feature, project_name, experiment_id=experiment_id, feature_name=feature.name
        )

    def read_metric_metadata(
        self, project_name: str, experiment_id: str, metric_name: str
    ) -> domain.Metric:
        return self.read_domain(
            domain.Metric, project_name, experiment_id=experiment_id, metric_name=metric_name
        )

    def read_metrics_metadata(self, project_name: str, experiment_id: str) -> List[domain.Metric]:
        return self.read_domains(domain.Metric, project_name, experiment_id=experiment_id)

    def write_metric_metadata(self, metric: domain.Metric, project_name: str, experiment_id: str):
        self.write_domain(
            metric, project_name, experiment_id=experiment_id, metric_name=metric.name
        )

    def read_parameter_metadata(
        self, project_name: str, experiment_id: str, parameter_name: str
    ) -> domain.Parameter:
        return self.read_domain(
            domain.Parameter,
            project_name,
            experiment_id=experiment_id,
            parameter_name=parameter_name,
        )

    def read_parameters_metadata(
        self, project_name: str, experiment_id: str
    ) -> List[domain.Parameter]:
        return self.read_domains(domain.Parameter, project_name, experiment_id=experiment_id)

    def write_parameter_metadata(
        self, parameter: domain.Parameter, project_name: str, experiment_id: str
    ):
        self.write_domain(
            parameter, project_name, experiment_id=experiment_id, parameter_name=parameter.name
        )

    def read_comment_updates_metadata(
        self,
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ):
        return self.read_domains(
            "CommentUpdate",
            artifact_id=artifact_id,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
            feature_name=feature_name,
            metric_name=metric_name,
            parameter_name=parameter_name,
            project_name=project_name,
        )

    def write_comment_update_metadata(
        self,
        comment_update: Dict,
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ):
        self.write_domain(
            comment_update,
            project_name,
            artifact_id=artifact_id,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
            feature_name=feature_name,
            metric_name=metric_name,
            parameter_name=parameter_name,
        )

    def read_tag_updates_metadata(
        self,
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ):
        return self.read_domains(
            "TagUpdate",
            artifact_id=artifact_id,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
            feature_name=feature_name,
            metric_name=metric_name,
            parameter_name=parameter_name,
            project_name=project_name,
        )

    def write_tag_update_metadata(
        self,
        tag_update: Dict,
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ):
        self.write_domain(
            tag_update,
            project_name,
            artifact_id=artifact_id,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
            feature_name=feature_name,
            metric_name=metric_name,
            parameter_name=parameter_name,
        )
