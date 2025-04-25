from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from rubicon_ml.repository.v2.base import BaseRepository
from rubicon_ml.repository.v2.deprecated import ArchiveMixin
from rubicon_ml.repository.v2.fsspec import (
    LocalRepository,
    MemoryRepository,
    S3Repository,
)
from rubicon_ml.repository.v2.logger import LoggerRepository

if TYPE_CHECKING:
    import dask.dataframe as dd
    import pandas as pd
    import polars as pl

    from rubicon_ml import domain


class V1CompatibilityMixin(ArchiveMixin):
    """Mixin to make V2 repositories compaitble with the client.

    For use in integration testing. Will be removed later after any necessary
    client updates.
    """

    # helpers

    def _get_identifier_kwargs_from_path(self, path: str) -> Dict:
        entity_identifier_kwargs = {}
        split_path = path.split("/")[:-1]

        for i in range(len(split_path) - 1, 0, -2):
            path_parts = split_path[i - 1 : i + 1]

            if len(path_parts) == 2 and path_parts[0]:
                entity_type, entity_identifier = path_parts

                if entity_type in ["artifacts", "dataframes", "experiments"]:
                    entity_identifier_kwargs[f"{entity_type[:-1]}_id"] = entity_identifier
                else:
                    entity_identifier = entity_identifier.replace("-", " ")

                    if entity_type in ["features", "metrics", "parameters"]:
                        entity_identifier_kwargs[f"{entity_type[:-1]}_name"] = entity_identifier
                    else:
                        entity_identifier_kwargs["project_name"] = entity_identifier

        return entity_identifier_kwargs

    def _get_tag_and_comment_identifier_kwargs(
        self, entity_identifier: str, entity_type: str, experiment_id: Optional[str]
    ) -> Dict:
        entity_identifier_kwargs = {"experiment_id": experiment_id}

        if entity_type in ["Artifact", "Dataframe"]:
            entity_identifier_kwargs[f"{entity_type.lower()}_id"] = entity_identifier
        elif entity_type in ["Feature", "Metric", "Parameter"]:
            entity_identifier_kwargs[f"{entity_type.lower()}_name"] = entity_identifier

        return entity_identifier_kwargs

    # method forwarders for compatibility

    def _get_artifact_data_path(
        self, project_name: str, experiment_id: Optional[str], artifact_id: str
    ) -> str:
        path_root, _ = self._make_path(
            artifact_id=artifact_id, experiment_id=experiment_id, project_name=project_name
        )
        return f"{path_root}/data"

    def _get_artifact_metadata_root(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> str:
        path_root, _ = self._make_path(experiment_id=experiment_id, project_name=project_name)
        return f"{path_root}/artifacts"

    def _get_dataframe_metadata_root(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> str:
        path_root, _ = self._make_path(experiment_id=experiment_id, project_name=project_name)
        return f"{path_root}/dataframes"

    def _get_experiment_metadata_root(self, project_name: str) -> str:
        path_root, _ = self._make_path(project_name=project_name)
        return f"{path_root}/experiments"

    def _get_tag_metadata_root(
        self,
        project_name,
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ) -> str:
        if experiment_id is None and entity_identifier is None:
            raise ValueError("`experiment_id` and `entity_identifier` can not both be `None`.")

        tag_identifier_kwargs = self._get_tag_and_comment_identifier_kwargs(
            entity_identifier,
            entity_type,
            experiment_id,
        )
        path_root, _ = self._make_path(project_name=project_name, **tag_identifier_kwargs)

        return path_root

    def _persist_bytes(self, bytes_data: bytes, path: str):
        domain_identifier_kwargs = self._get_identifier_kwargs_from_path(path)
        artifact_id = domain_identifier_kwargs.pop("artifact_id")
        project_name = domain_identifier_kwargs.pop("project_name")

        self.write_artifact_data(bytes_data, artifact_id, project_name, **domain_identifier_kwargs)

    def _persist_dataframe(
        self, df: Union["pd.DataFrame", "dd.DataFrame", "pl.DataFrame"], path: str
    ):
        domain_identifier_kwargs = self._get_identifier_kwargs_from_path(path)
        dataframe_id = domain_identifier_kwargs.pop("dataframe_id")
        project_name = domain_identifier_kwargs.pop("project_name")

        self.write_dataframe_data(df, dataframe_id, project_name, **domain_identifier_kwargs)

    def _persist_domain(self, domain: "domain.DOMAIN_TYPES", path: str):
        domain_identifier_kwargs = self._get_identifier_kwargs_from_path(path)
        project_name = domain_identifier_kwargs.pop("project_name")

        self.write_domain(domain, project_name, **domain_identifier_kwargs)

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

    def delete_artifact(
        self, project_name: str, artifact_id: str, experiment_id: Optional[str] = None
    ):
        self.remove_artifact_metadata(project_name, artifact_id, experiment_id)

    def get_artifact_metadata(
        self, project_name: str, artifact_id: str, experiment_id: Optional[str] = None
    ) -> "domain.Artifact":
        return self.read_artifact_metadata(project_name, artifact_id, experiment_id)

    def get_artifacts_metadata(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> List["domain.Artifact"]:
        return self.read_artifacts_metadata(project_name, experiment_id)

    def get_artifact_data(
        self, project_name: str, artifact_id: str, experiment_id: Optional[str] = None
    ) -> bytes:
        return self.read_artifact_data(artifact_id, project_name, experiment_id)

    def create_dataframe(
        self,
        dataframe: "domain.Dataframe",
        data,
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        self.write_dataframe_metadata(dataframe, project_name, experiment_id)
        self.write_dataframe_data(data, dataframe.id, project_name, experiment_id)

    def delete_dataframe(
        self, project_name: str, dataframe_id: str, experiment_id: Optional[str] = None
    ):
        self.remove_dataframe_metadata(project_name, dataframe_id, experiment_id)

    def get_dataframe_metadata(
        self, project_name: str, dataframe_id: str, experiment_id: Optional[str] = None
    ) -> "domain.Dataframe":
        return self.read_dataframe_metadata(project_name, dataframe_id, experiment_id)

    def get_dataframes_metadata(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> List["domain.Dataframe"]:
        return self.read_dataframes_metadata(project_name, experiment_id)

    def get_dataframe_data(
        self,
        project_name: str,
        dataframe_id: str,
        experiment_id: Optional[str] = None,
        df_type: Literal["pandas", "dask", "polars"] = "pandas",
    ) -> Union["dd.DataFrame", "pd.DataFrame", "pl.DataFrame"]:
        return self.read_dataframe_data(dataframe_id, df_type, project_name, experiment_id)

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

    def add_comments(
        self,
        project_name: str,
        comments: List[str],
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        self.write_comment_update_metadata(
            {"added_comments": comments},
            project_name,
            **self._get_tag_and_comment_identifier_kwargs(
                entity_identifier,
                entity_type,
                experiment_id,
            ),
        )

    def get_comments(
        self,
        project_name: str,
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ) -> List[Dict]:
        return self.read_comment_updates_metadata(
            project_name,
            **self._get_tag_and_comment_identifier_kwargs(
                entity_identifier,
                entity_type,
                experiment_id,
            ),
        )

    def remove_comments(
        self,
        project_name: str,
        comments: List[str],
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        self.write_comment_update_metadata(
            {"removed_comments": comments},
            project_name,
            **self._get_tag_and_comment_identifier_kwargs(
                entity_identifier,
                entity_type,
                experiment_id,
            ),
        )

    def add_tags(
        self,
        project_name: str,
        tags: List[str],
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        self.write_tag_update_metadata(
            {"added_tags": tags},
            project_name,
            **self._get_tag_and_comment_identifier_kwargs(
                entity_identifier,
                entity_type,
                experiment_id,
            ),
        )

    def get_tags(
        self,
        project_name: str,
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ) -> List[Dict]:
        return self.read_tag_updates_metadata(
            project_name,
            **self._get_tag_and_comment_identifier_kwargs(
                entity_identifier,
                entity_type,
                experiment_id,
            ),
        )

    def remove_tags(
        self,
        project_name: str,
        tags: List[str],
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        self.write_tag_update_metadata(
            {"removed_tags": tags},
            project_name,
            **self._get_tag_and_comment_identifier_kwargs(
                entity_identifier,
                entity_type,
                experiment_id,
            ),
        )

    @property
    def PROTOCOL(self):
        return self.protocol


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
