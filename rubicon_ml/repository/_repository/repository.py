from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Union

from rubicon_ml.domain.artifact import Artifact
from rubicon_ml.domain.dataframe import Dataframe
from rubicon_ml.domain.experiment import Experiment
from rubicon_ml.domain.feature import Feature
from rubicon_ml.domain.metric import Metric
from rubicon_ml.domain.parameter import Parameter
from rubicon_ml.domain.project import Project

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES, DOMAIN_TYPES


class RepositoryABC(ABC):
    """Abstract base for rubicon-ml backend repositories."""

    @abstractmethod
    def _get_artifact_data_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_artifact_metadata_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_comment_metadata_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_dataframe_data_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_dataframe_metadata_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_experiment_metadata_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_feature_metadata_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_metric_metadata_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_parameter_metadata_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_project_metadata_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _get_tag_metadata_path(self, *args) -> str:
        """"""
        ...

    @abstractmethod
    def _read_bytes(self, path: str, *args) -> bytes:
        """"""
        ...

    @abstractmethod
    def _read_dataframe(self, path: str, *args) -> "DATAFRAME_TYPES":
        """"""
        ...

    @abstractmethod
    def _read_domain(self, path: str, *args) -> "DOMAIN_TYPES":
        """"""
        ...

    @abstractmethod
    def _write_bytes(self, data: bytes, path: str, *args):
        """"""
        ...

    @abstractmethod
    def _write_dataframe(self, data: "DATAFRAME_TYPES", path: str, *args):
        """"""
        ...

    @abstractmethod
    def _write_domain(self, data: "DOMAIN_TYPES", path: str, *args):
        """"""
        ...

    def _get_path(self, data: Union[bytes, "DOMAIN_TYPES", "DATAFRAME_TYPES"], *args) -> str:
        """TODO: HANDLE TAGS AND COMMENTS"""
        if isinstance(data, Artifact):
            return self._get_artifact_metadata_path(*args)
        elif isinstance(data, Dataframe):
            return self._get_dataframe_metadata_path(*args)
        elif isinstance(data, Experiment):
            return self._get_experiment_metadata_path(*args)
        elif isinstance(data, Feature):
            return self._get_feature_metadata_path(*args)
        elif isinstance(data, Metric):
            return self._get_metric_metadata_path(*args)
        elif isinstance(data, Parameter):
            return self._get_parameter_metadata_path(*args)
        elif isinstance(data, Project):
            return self._get_project_metadata_path(*args)
        elif isinstance(data, bytes):
            return self._get_artifact_data_path(*args)
        else:
            return self._get_dataframe_data_path(*args)

    def read_bytes(self, *args) -> bytes:
        """"""
        path = self._get_path(*args)

        return self._read_bytes(path, *args)

    def read_dataframe(self, *args) -> "DATAFRAME_TYPES":
        """"""
        path = self._get_path(*args)

        return self._read_dataframe(path, *args)

    def read_domain(self, *args) -> "DOMAIN_TYPES":
        """"""
        path = self._get_path(*args)

        return self._read_domain(path, *args)

    def write_bytes(self, data: bytes, *args):
        """"""
        path = self._get_path(*args)

        self._write_bytes(data, path, *args)

    def write_dataframe(self, data: "DATAFRAME_TYPES", *args):
        """"""
        path = self._get_path(*args)

        self._write_dataframe(data, path, *args)

    def write_domain(self, data: "DOMAIN_TYPES", *args):
        """"""
        path = self._get_path(*args)

        self._write_domain(data, path, *args)
