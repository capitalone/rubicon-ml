import logging
from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import fsspec

from rubicon_ml.repository.utils import json, slugify
from rubicon_ml.repository.v2.base import BaseRepository

if TYPE_CHECKING:
    from rubicon_ml.domain import DOMAIN_TYPES, DomainsVar

LOGGER = logging.Logger(__name__)


class FsspecRepository(BaseRepository):
    """Base repository for `fsspec`-based backends."""

    def __init__(self, root_dir: Optional[str] = None, **storage_options: Any):
        self.root_dir = root_dir
        self.storage_options = storage_options

        self._filesystem = None

    @abstractmethod
    def _get_filesystem(self):
        ...

    def _make_path(
        self,
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ):
        path = Path(self.root_dir, slugify(project_name))

        if experiment_id is not None:
            path = Path(path, "experiments", experiment_id)

        if artifact_id is not None:
            path = Path(path, "artifacts", artifact_id)

        if dataframe_id is not None:
            path = Path(path, "dataframes", dataframe_id)

        if feature_name is not None:
            path = Path(path, "features", slugify(feature_name))

        if metric_name is not None:
            path = Path(path, "metrics", slugify(metric_name))

        if parameter_name is not None:
            path = Path(path, "parameters", slugify(parameter_name))

        return Path(path, "metadata.json")

    # core read/writes

    def read_domain(
        self,
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ):
        return

    def read_domains(
        self,
        domain_cls: "DomainsVar",
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        return

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
    ):
        path = self._make_path(
            project_name,
            artifact_id=artifact_id,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
            feature_name=feature_name,
            metric_name=metric_name,
            parameter_name=parameter_name,
        )

        LOGGER.warn(path)  # TODO: REMOVE

        self.filesystem.mkdirs(path.parent, exist_ok=True)

        with self.filesystem.open(path, "w") as domain_file:
            domain_file.write(json.dumps(domain))

    # binary read/writes

    def read_artifact_data(self, *args: Any, **kwargs: Any):
        return

    def write_artifact_data(self, *args: Any, **kwargs: Any):
        return

    def stream_artifact_data(self, *args: Any, **kwargs: Any):
        return

    def read_dataframe_data(self, *args: Any, **kwargs: Any):
        return

    def write_dataframe_data(self, *args: Any, **kwargs: Any):
        return

    @property
    def filesystem(self):
        if self._filesystem is None:
            self._filesystem = self._get_filesystem()

        return self._filesystem


class LocalRepository(FsspecRepository):
    """Local filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self):
        return fsspec.filesystem("local", **self.storage_options)


class MemoryRepository(FsspecRepository):
    """In-memory filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self):
        return fsspec.filesystem("memory", **self.storage_options)


class S3Repository(FsspecRepository):
    """S3 filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self):
        return fsspec.filesystem("s3", **self.storage_options)
