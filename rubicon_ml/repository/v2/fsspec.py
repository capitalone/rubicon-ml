import logging
from abc import abstractmethod
from json import JSONDecodeError
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Literal, Optional, Union

import fsspec

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.utils import json, slugify
from rubicon_ml.repository.v2.base import BaseRepository

if TYPE_CHECKING:
    import dask.dataframe as dd
    import pandas as pd
    import polars as pl

    from rubicon_ml.domain import DOMAIN_CLASS_TYPES, DOMAIN_TYPES

LOGGER = logging.Logger(__name__)


class FsspecRepository(BaseRepository):
    """Base repository for `fsspec`-based backends."""

    def __init__(self, root_dir: Optional[str] = None, **storage_options: Any):
        self.root_dir = root_dir
        self.storage_options = storage_options

        self._filesystem = None

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

        domains.sort(key=lambda domain: domain.created_at)

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

    def read_artifact_data(
        self,
        artifact_id: str,
        project_name: str,
        experiment_id: Optional[str] = None,
    ) -> bytes:
        path_root, _ = self._make_path(
            project_name,
            artifact_id=artifact_id,
            experiment_id=experiment_id,
        )
        path = Path(path_root, "data")

        try:
            artifact_data_file = self.filesystem.open(path, "rb")
        except FileNotFoundError:
            raise RubiconException(f"Artifact '{artifact_id}' data was not found.")

        return artifact_data_file.read()

    def write_artifact_data(
        self,
        artifact_data: bytes,
        artifact_id: str,
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        path_root, _ = self._make_path(
            project_name,
            artifact_id=artifact_id,
            experiment_id=experiment_id,
        )
        self.filesystem.mkdirs(path_root, exist_ok=True)

        path = Path(path_root, "data")

        if self.filesystem.exists(path):
            raise RubiconException(f"Artifact '{artifact_id}' data already exists.")

        with self.filesystem.open(path, "wb") as artifact_data_file:
            artifact_data_file.write(artifact_data)

    def read_dataframe_data(
        self,
        dataframe_id: str,
        dataframe_type: Literal["dask", "dd", "pandas", "pd", "polars", "pl"],
        project_name: str,
        experiment_id: Optional[str] = None,
    ) -> Union["dd.DataFrame", "pd.DataFrame", "pl.DataFrame"]:
        path_root, _ = self._make_path(
            project_name,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
        )
        path = Path(path_root, "data")

        import_error_message = (
            f"`rubicon_ml` requires `{dataframe_type}` to read dataframes with "
            f"`dataframe_type`='{dataframe_type}'. `pip install {{}}` and try again."
        )

        if dataframe_type in ["dask", "dd"]:
            try:
                import dask.dataframe as dd
            except ImportError:
                raise RubiconException(import_error_message.format("dask[dataframe]"))

            dataframe_data = dd.read_parquet(
                path, engine="pyarrow", storage_options=self.storage_options
            )
        elif dataframe_type in ["pandas", "pd"]:
            try:
                import pandas as pd
            except ImportError:
                raise RubiconException(import_error_message.format("pandas"))

            path = Path(path, "data.parquet")
            dataframe_data = pd.read_parquet(
                path, engine="pyarrow", storage_options=self.storage_options
            )
        elif dataframe_type in ["polars", "pl"]:
            try:
                import polars as pl
            except ImportError:
                raise RubiconException(import_error_message.format("polars"))

            dataframe_data = pl.read_parquet(path, storage_options=self.storage_options)
        else:
            raise ValueError(
                "`dataframe_type` must be one of 'dask' (or 'dd'), 'pandas' (or 'pd') or "
                "'polars' (or 'pl')."
            )

        return dataframe_data

    def write_dataframe_data(
        self,
        dataframe_data: Union["dd.DataFrame", "pd.DataFrame", "pl.DataFrame"],
        dataframe_id: str,
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        path_root, _ = self._make_path(
            project_name,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
        )
        self.filesystem.mkdirs(path_root, exist_ok=True)

        path = Path(path_root, "data")

        if not hasattr(dataframe_data, "compute"):
            self.filesystem.mkdirs(path, exist_ok=True)
            path = Path(path, "data.parquet")

        if hasattr(dataframe_data, "write_parquet"):
            dataframe_data.write_parquet(path, storage_options=self.storage_options)
        else:
            dataframe_data.to_parquet(path, engine="pyarrow", storage_options=self.storage_options)

    @property
    def filesystem(self) -> fsspec.spec.AbstractFileSystem:
        if self._filesystem is None:
            self._filesystem = fsspec.filesystem(self.protocol, **self.storage_options)

        return self._filesystem

    @property
    @abstractmethod
    def protocol(self) -> str: ...


class LocalRepository(FsspecRepository):
    """Local filesystem repository leveraging `fsspec`."""

    @property
    def protocol(self) -> str:
        return "local"


class MemoryRepository(FsspecRepository):
    """In-memory filesystem repository leveraging `fsspec`."""

    @property
    def protocol(self) -> str:
        return "memory"


class S3Repository(FsspecRepository):
    """S3 filesystem repository leveraging `fsspec`."""

    @property
    def protocol(self) -> str:
        return "s3"
