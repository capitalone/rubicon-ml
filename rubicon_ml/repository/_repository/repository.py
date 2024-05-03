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
    def _read_dataframe(self, location: str, *args) -> "DATAFRAME_TYPES":
        """"""
        ...

    @abstractmethod
    def _read_domain(self, location: str, *args) -> "DOMAIN_TYPES":
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
    def _write_domain(self, data: "DOMAIN_TYPES", location: str, *args):
        """"""
        ...

    def _get_location(self, data: Union[bytes, "DOMAIN_TYPES", "DATAFRAME_TYPES"], *args) -> str:
        """TODO: HANDLE TAGS AND COMMENTS"""
        if isinstance(data, Artifact):
            return self._get_artifact_metadata_location(*args)
        elif isinstance(data, Dataframe):
            return self._get_dataframe_metadata_location(*args)
        elif isinstance(data, Experiment):
            return self._get_experiment_metadata_location(*args)
        elif isinstance(data, Feature):
            return self._get_feature_metadata_location(*args)
        elif isinstance(data, Metric):
            return self._get_metric_metadata_location(*args)
        elif isinstance(data, Parameter):
            return self._get_parameter_metadata_location(*args)
        elif isinstance(data, Project):
            return self._get_project_metadata_location(*args)
        elif isinstance(data, bytes):
            return self._get_artifact_data_location(*args)
        else:
            return self._get_dataframe_data_location(*args)

    def read_bytes(self, *args) -> bytes:
        """"""
        location = self._get_location(*args)

        return self._read_bytes(location, *args)

    def read_dataframe(self, *args) -> "DATAFRAME_TYPES":
        """"""
        location = self._get_location(*args)

        return self._read_dataframe(location, *args)

    def read_domain(self, *args) -> "DOMAIN_TYPES":
        """"""
        location = self._get_location(*args)

        return self._read_domain(location, *args)

    def write_bytes(self, data: bytes, *args):
        """"""
        location = self._get_location(*args)

        self._write_bytes(data, location, *args)

    def write_dataframe(self, data: "DATAFRAME_TYPES", *args):
        """"""
        location = self._get_location(*args)

        self._write_dataframe(data, location, *args)

    def write_domain(self, data: "DOMAIN_TYPES", *args):
        """"""
        location = self._get_location(*args)

        self._write_domain(data, location, *args)
