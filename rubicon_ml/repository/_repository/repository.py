from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Literal

from rubicon_ml.domain.artifact import Artifact
from rubicon_ml.domain.dataframe import Dataframe
from rubicon_ml.domain.experiment import Experiment
from rubicon_ml.domain.feature import Feature
from rubicon_ml.domain.metric import Metric
from rubicon_ml.domain.mixin import CommentUpdate
from rubicon_ml.domain.parameter import Parameter
from rubicon_ml.domain.project import Project

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES, DOMAIN_CLASS_TYPES, DOMAIN_TYPES


class RepositoryABC(ABC):
    """Abstract base for rubicon-ml backend repositories."""

    @abstractmethod
    def _get_artifact_data_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_artifact_metadata_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_comment_metadata_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_dataframe_data_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_dataframe_metadata_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_experiment_metadata_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_feature_metadata_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_metric_metadata_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_parameter_metadata_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_project_metadata_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_tag_metadata_location(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _read_bytes(self, location: str, *args) -> bytes:
        """"""
        ...

    @abstractmethod
    def _read_dataframe(
        self, location: str, df_type: Literal["dask", "pandas", "polars"], *args
    ) -> "DATAFRAME_TYPES":
        """"""
        ...

    @abstractmethod
    def _read_json(self, location: str, domain_cls: "DOMAIN_CLASS_TYPES", *args) -> "DOMAIN_TYPES":
        """"""
        ...

    @abstractmethod
    def _read_jsons(
        self, location: str, domain_cls: "DOMAIN_CLASS_TYPES", *args
    ) -> List["DOMAIN_TYPES"]:
        """"""
        ...

    @abstractmethod
    def _write_bytes(self, data: bytes, location: str, *args):
        """"""
        ...

    @abstractmethod
    def _write_dataframe(self, data: "DATAFRAME_TYPES", location: str, *args):
        """"""
        ...

    @abstractmethod
    def _write_json(self, data: "DOMAIN_TYPES", location: str, *args):
        """"""
        ...

    def _get_location(self, domain_cls: "DOMAIN_CLASS_TYPES", *args) -> str:
        if domain_cls == Artifact:
            return self._get_artifact_metadata_location(*args)
        elif domain_cls == CommentUpdate:
            return self._get_comment_metadata_location(*args)
        elif domain_cls == Dataframe:
            return self._get_dataframe_metadata_location(*args)
        elif domain_cls == Experiment:
            return self._get_experiment_metadata_location(*args)
        elif domain_cls == Feature:
            return self._get_feature_metadata_location(*args)
        elif domain_cls == Metric:
            return self._get_metric_metadata_location(*args)
        elif domain_cls == Parameter:
            return self._get_parameter_metadata_location(*args)
        elif domain_cls == Project:
            return self._get_project_metadata_location(*args)
        else:
            return self._get_tag_metadata_location(*args)

    def read_bytes(self, *args) -> bytes:
        """"""
        location = self._get_artifact_data_location(*args)

        return self._read_bytes(location, *args)

    def read_dataframe(
        self, df_type: Literal["dask", "pandas", "polars"], *args
    ) -> "DATAFRAME_TYPES":
        """"""
        location = self._get_dataframe_data_location(*args)

        return self._read_dataframe(location, df_type, *args)

    def read_json(self, domain_cls: "DOMAIN_CLASS_TYPES", *args) -> "DOMAIN_TYPES":
        """"""
        location = self._get_location(domain_cls, *args)

        return self._read_json(location, domain_cls, *args)

    def read_jsons(self, domain_cls: "DOMAIN_CLASS_TYPES", *args) -> List["DOMAIN_TYPES"]:
        """"""
        location = self._get_location(domain_cls, *args)

        return self._read_jsons(location, domain_cls, *args)

    def write_bytes(self, data: bytes, *args):
        """"""
        location = self._get_artifact_data_location(*args)

        return self._write_bytes(data, location, *args)

    def write_dataframe(self, data: "DATAFRAME_TYPES", *args):
        """"""
        location = self._get_dataframe_data_location(*args)

        return self._write_dataframe(data, location, *args)

    def write_json(self, data: "DOMAIN_TYPES", *args):
        """"""
        location = self._get_location(type(data), *args)

        return self._write_json(data, location, *args)
