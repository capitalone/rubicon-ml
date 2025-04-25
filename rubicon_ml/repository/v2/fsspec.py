import logging
import pickle
import warnings
from abc import abstractmethod
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

import fsspec

from rubicon_ml.domain.utils.uuid import uuid, uuid4
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.utils import json, slugify
from rubicon_ml.repository.v2.base import BaseRepository

if TYPE_CHECKING:
    import dask.dataframe as dd
    import pandas as pd
    import polars as pl

    from rubicon_ml.domain import DOMAIN_CLASS_TYPES, DOMAIN_TYPES, Artifact, Dataframe

LOGGER = logging.Logger(__name__)
logging.captureWarnings(True)


class FsspecRepository(BaseRepository):
    """Base repository for `fsspec`-based backends."""

    def __init__(self, root_dir: Optional[str] = None, **storage_options: Any):
        if root_dir is None:
            raise ValueError(f"`root_dir` for `{self.__class__.__name__}` can not be None.")

        self.root_dir = root_dir.rstrip("/")
        self.storage_options = storage_options

        self._filesystem = None

    def _make_directories(self, path: str):
        self.filesystem.mkdirs(path, exist_ok=True)

    def _make_not_found_exception(
        self, domain_cls: "DOMAIN_CLASS_TYPES", domain_identifier: str
    ) -> RubiconException:
        cls_name = domain_cls.__name__.lower()

        try:
            _ = uuid.UUID(str(domain_identifier))
        except ValueError:
            domain_identifier_type = "name"
            quote_char = "'"
        else:
            domain_identifier_type = "id"
            quote_char = "`"

        return RubiconException(
            f"No {cls_name} with {domain_identifier_type} "
            f"{quote_char}{domain_identifier}{quote_char} found."
        )

    def _make_path(
        self,
        project_name: Optional[str] = None,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ) -> (str, Optional[str]):
        domain_identifier = None
        path = self.root_dir

        if project_name is not None:
            path += f"/{slugify(project_name)}"
            domain_identifier = project_name

        if experiment_id is not None:
            path += f"/experiments/{experiment_id}"
            domain_identifier = experiment_id

        if artifact_id is not None:
            path += f"/artifacts/{artifact_id}"
            domain_identifier = artifact_id

        if dataframe_id is not None:
            path += f"/dataframes/{dataframe_id}"
            domain_identifier = dataframe_id

        if feature_name is not None:
            path += f"/features/{slugify(feature_name)}"
            domain_identifier = feature_name

        if metric_name is not None:
            path += f"/metrics/{slugify(metric_name)}"
            domain_identifier = metric_name

        if parameter_name is not None:
            path += f"/parameters/{slugify(parameter_name)}"
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
        path = f"{path_root}/metadata.json"

        try:
            domain_file = self.filesystem.open(path)
        except FileNotFoundError:
            raise self._make_not_found_exception(domain_cls, domain_identifier)

        return domain_cls(**json.load(domain_file))

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
    ) -> List[Union["DOMAIN_TYPES", Dict]]:
        domains = []
        path_root, _ = self._make_path(
            project_name,
            artifact_id=artifact_id,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
            feature_name=feature_name,
            metric_name=metric_name,
            parameter_name=parameter_name,
        )

        if not isinstance(domain_cls, str) and project_name:
            path_root += f"/{domain_cls.__name__.lower()}s"

        if domain_cls == "CommentUpdate":
            path = f"{path_root}/comments_*.json"
        elif domain_cls == "TagUpdate":
            path = f"{path_root}/tags_*.json"

        try:
            if isinstance(domain_cls, str):
                metadata_file_paths = self.filesystem.glob(path, detail=True)
            else:
                metadata_file_paths = [
                    f"{metadata_path.get('name')}/metadata.json"
                    for metadata_path in self.filesystem.ls(path_root, detail=True)
                    if metadata_path.get("type", metadata_path.get("StorageClass")).lower()
                    == "directory"
                ]
        except FileNotFoundError:
            return []

        if not metadata_file_paths:
            return []

        if isinstance(domain_cls, str):
            metadata_file_paths = metadata_file_paths.values()
            metadata_file_paths_with_timestamps = [
                (p.get("created", p.get("LastModified")), p.get("name"))
                for p in metadata_file_paths
            ]
            metadata_file_paths_with_timestamps.sort()
            metadata_file_paths = [p[1] for p in metadata_file_paths_with_timestamps]

        for path, metadata in self.filesystem.cat(metadata_file_paths, on_error="return").items():
            if isinstance(metadata, FileNotFoundError):
                warnings.warn(f"Ignoring {path}. `rubicon-ml` metadata not found.")
            else:
                try:
                    if isinstance(domain_cls, str):
                        domain = json.loads(metadata)
                    else:
                        domain = domain_cls(**json.loads(metadata))
                except (JSONDecodeError, TypeError):
                    warnings.warn(f"Failed to load {domain_cls} at {path}.")

                domains.append(domain)

        if not isinstance(domain_cls, str):
            domains.sort(key=lambda domain: domain.created_at)

        return domains

    def remove_domain(
        self,
        domain_cls: Union["Artifact", "Dataframe"],
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
    ):
        path_root, domain_identifier = self._make_path(
            project_name,
            artifact_id=artifact_id,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
        )

        try:
            self.filesystem.rm(path_root, recursive=True)
        except FileNotFoundError:
            raise self._make_not_found_exception(domain_cls, domain_identifier)

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
    ):
        domain_dump = json.dumps(domain)
        path_root, domain_identifier = self._make_path(
            project_name,
            artifact_id=artifact_id,
            dataframe_id=dataframe_id,
            experiment_id=experiment_id,
            feature_name=feature_name,
            metric_name=metric_name,
            parameter_name=parameter_name,
        )

        if isinstance(domain, dict):
            if "added_comments" in domain or "removed_comments" in domain:
                path = f"{path_root}/comments_{uuid4()}.json"
            elif "added_tags" in domain or "removed_tags" in domain:
                path = f"{path_root}/tags_{uuid4()}.json"
            else:
                raise ValueError(f"Unknown domain entity: {domain}.")
        else:
            path = f"{path_root}/metadata.json"

        if self.filesystem.exists(path):
            try:
                _ = uuid.UUID(str(domain_identifier))
            except ValueError:
                domain_identifier_type = "name"
                quote_char = "'"
            else:
                domain_identifier_type = "id"
                quote_char = "`"

            raise RubiconException(
                f"{domain.__class__.__name__} with {domain_identifier_type} "
                f"{quote_char}{domain_identifier}{quote_char} already exists."
            )

        self._make_directories(path_root)

        with self.filesystem.open(path, "w") as domain_file:
            domain_file.write(domain_dump)

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
        path = f"{path_root}/data"

        try:
            artifact_data_file = self.filesystem.open(path, "rb")
        except FileNotFoundError:
            raise RubiconException(f"No data for artifact with id `{artifact_id}` found.")

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
        self._make_directories(path_root)

        path = f"{path_root}/data"

        if self.filesystem.exists(path):
            raise RubiconException(f"Data for artifact with id `{artifact_id}` already exists.")

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
        path = f"{path_root}/data"

        import_error_message = (
            f"`rubicon_ml` requires `{dataframe_type}` to read dataframes with "
            f"`dataframe_type`='{dataframe_type}'. `pip install {{}}` and try again."
        )

        if dataframe_type in ["dask", "dd"]:
            try:
                import dask.dataframe as dd
            except ImportError as error:
                raise RubiconException(import_error_message.format("dask[dataframe]")) from error

            dataframe_library = dd
            read_parquet_kwargs = {"engine": "pyarrow", "storage_options": self.storage_options}
        elif dataframe_type in ["pandas", "pd"]:
            try:
                import pandas as pd
            except ImportError as error:
                raise RubiconException(import_error_message.format("pandas")) from error

            path += "/data.parquet"

            dataframe_library = pd
            read_parquet_kwargs = {"engine": "pyarrow", "storage_options": self.storage_options}
        elif dataframe_type in ["polars", "pl"]:
            try:
                import polars as pl
            except ImportError as error:
                raise RubiconException(import_error_message.format("polars")) from error

            dataframe_library = pl
            read_parquet_kwargs = {"storage_options": self.storage_options}
        else:
            raise ValueError(
                "`dataframe_type` must be one of 'dask' (or 'dd'), 'pandas' (or 'pd') or "
                "'polars' (or 'pl')."
            )

        try:
            dataframe_data = dataframe_library.read_parquet(path, **read_parquet_kwargs)
        except FileNotFoundError as error:
            raise RubiconException(
                f"No data for dataframe with id `{dataframe_id}` found."
            ) from error

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
        self._make_directories(path_root)

        path = f"{path_root}/data"

        if hasattr(dataframe_data, "write_parquet"):
            dataframe_data.write_parquet(path, storage_options=self.storage_options)
        else:
            if not hasattr(dataframe_data, "compute"):
                self._make_directories(path)
                path += "/data.parquet"

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

    def __init__(self, root_dir: Optional[str] = None):
        super().__init__(root_dir)

    @property
    def protocol(self) -> str:
        return "file"


class MemoryRepository(FsspecRepository):
    """In-memory filesystem repository leveraging `fsspec`."""

    def __init__(self, root_dir: Optional[str] = None, **storage_options: Any):
        root_dir = root_dir if root_dir else "/root"

        super().__init__(root_dir, **storage_options)

        if not self.filesystem.exists(self.root_dir):
            self._make_directories(self.root_dir)

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
        path = f"{path_root}/data"

        try:
            with self.filesystem.open(path, "rb") as dataframe_data_file:
                dataframe_data = pickle.load(dataframe_data_file)
        except FileNotFoundError as error:
            raise RubiconException(
                f"No data for dataframe with id `{dataframe_id}` found."
            ) from error

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
        path = f"{path_root}/data"

        with self.filesystem.open(path, "wb") as dataframe_data_file:
            pickle.dump(dataframe_data, dataframe_data_file)

    @property
    def protocol(self) -> str:
        return "memory"


class S3Repository(FsspecRepository):
    """S3 filesystem repository leveraging `fsspec`."""

    def _make_directories(self, path: str):
        pass

    @property
    def protocol(self) -> str:
        return "s3"
