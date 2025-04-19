import logging
from abc import abstractmethod
from json import JSONDecodeError
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Optional

import fsspec

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.utils import json, slugify
from rubicon_ml.repository.v2.base import BaseRepository

if TYPE_CHECKING:
    from rubicon_ml.domain import DOMAIN_CLASS_TYPES, DOMAIN_TYPES

LOGGER = logging.Logger(__name__)


class FsspecRepository(BaseRepository):
    """Base repository for `fsspec`-based backends."""

    def __init__(self, root_dir: Optional[str] = None, **storage_options: Any):
        self.root_dir = root_dir
        self.storage_options = storage_options

        self._filesystem = None

    @abstractmethod
    def _get_filesystem(self) -> fsspec.spec.AbstractFileSystem: ...

    def _make_path(
        self,
        project_name: Optional[str] = None,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ) -> (Path, Optional[str]):
        path = Path(self.root_dir)
        domain_identifier = None

        if project_name is not None:
            path = Path(path, slugify(project_name))
            domain_identifier = project_name

        if experiment_id is not None:
            path = Path(path, "experiments", experiment_id)
            domain_identifier = experiment_id

        if artifact_id is not None:
            path = Path(path, "artifacts", artifact_id)
            domain_identifier = artifact_id

        if dataframe_id is not None:
            path = Path(path, "dataframes", dataframe_id)
            domain_identifier = dataframe_id

        if feature_name is not None:
            path = Path(path, "features", slugify(feature_name))
            domain_identifier = feature_name

        if metric_name is not None:
            path = Path(path, "metrics", slugify(metric_name))
            domain_identifier = metric_name

        if parameter_name is not None:
            path = Path(path, "parameters", slugify(parameter_name))
            domain_identifier = parameter_name

        return path, domain_identifier

    # core read/writes

    def read_domain(
        self,
        domain_cls: "DOMAIN_CLASS_TYPES",
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ) -> "DOMAIN_TYPES":
        path_root, domain_identifier = self._make_path(
            project_name,
            artifact_id=artifact_id,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
            feature_name=feature_name,
            metric_name=metric_name,
            parameter_name=parameter_name,
        )
        path = Path(path_root, "metadata.json")

        try:
            domain_file = self.filesystem.open(path)
        except FileNotFoundError:
            raise RubiconException(f"{domain_cls.__name__} '{domain_identifier}' was not found.")

        return domain_cls(**json.load(domain_file))

    def read_domains(
        self,
        domain_cls: "DOMAIN_CLASS_TYPES",
        project_name: Optional[str] = None,
        experiment_id: Optional[str] = None,
    ) -> List["DOMAIN_TYPES"]:
        domains = []
        path_root, _ = self._make_path(project_name, experiment_id=experiment_id)

        if project_name:
            path_root = Path(path_root, f"{domain_cls.__name__.lower()}s")

        try:
            metadata_file_paths = [
                Path(metadata_path.get("name"), "metadata.json")
                for metadata_path in self.filesystem.ls(path_root, detail=True)
                if metadata_path.get("type", metadata_path.get("StorageClass")).lower()
                == "directory"
            ]
        except FileNotFoundError:
            return []

        for path, metadata in self.filesystem.cat(metadata_file_paths, on_error="return").items():
            if isinstance(metadata, FileNotFoundError):
                LOGGER.warn(f"Ignoring non-rubicon-ml-metadata at {path}.")
            else:
                try:
                    domain = domain_cls(**json.loads(metadata))
                    domains.append(domain)
                except (JSONDecodeError, TypeError):
                    LOGGER.warn(f"Failed to load {domain_cls.__name__.lower()} at {path}.")

        domains.sort(key=lambda d: d.created_at)

        return domains

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
        path_root, domain_identifier = self._make_path(
            project_name,
            artifact_id=artifact_id,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
            feature_name=feature_name,
            metric_name=metric_name,
            parameter_name=parameter_name,
        )
        self.filesystem.mkdirs(path_root, exist_ok=True)

        path = Path(path_root, "metadata.json")

        if self.filesystem.exists(path):
            raise RubiconException(
                f"{domain.__class__.__name__} '{domain_identifier}' already exists."
            )

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
    def filesystem(self) -> fsspec.spec.AbstractFileSystem:
        if self._filesystem is None:
            self._filesystem = self._get_filesystem()

        return self._filesystem


class LocalRepository(FsspecRepository):
    """Local filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self) -> fsspec.spec.AbstractFileSystem:
        return fsspec.filesystem("local", **self.storage_options)


class MemoryRepository(FsspecRepository):
    """In-memory filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self) -> fsspec.spec.AbstractFileSystem:
        return fsspec.filesystem("memory", **self.storage_options)


class S3Repository(FsspecRepository):
    """S3 filesystem repository leveraging `fsspec`."""

    def _get_filesystem(self) -> fsspec.spec.AbstractFileSystem:
        return fsspec.filesystem("s3", **self.storage_options)
