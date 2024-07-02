import os
import uuid
import warnings
from abc import abstractmethod
from json import JSONDecodeError
from typing import TYPE_CHECKING, List, Literal, Optional

import fsspec

from rubicon_ml.domain import Experiment, TagUpdate
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.imports import (
    try_import_dask_dataframe,
    try_import_pandas_dataframe,
    try_import_polars_dataframe,
)
from rubicon_ml.repository._repository.repository import RepositoryABC
from rubicon_ml.repository.utils import json, slugify
from rubicon_ml.types import safe_is_dask_dataframe, safe_is_pandas_dataframe

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES, DOMAIN_CLASS_TYPES, DOMAIN_TYPES


class FSSpecRepositoryABC(RepositoryABC):
    """rubicon-ml backend repository for `fsspec` based filesystems.

    https://filesystem-spec.readthedocs.io/en/latest/
    """

    def __init__(self, root_dir: str, **storage_options):
        """"""
        protocol = self._get_protocol()
        filesystem = fsspec.filesystem(protocol, **storage_options)

        self.filesystem = filesystem
        self.protocol = protocol
        self.root_dir = root_dir

    @abstractmethod
    def _get_protocol(self) -> str:
        """"""
        ...

    def _get_artifact_data_location(
        self,
        project_name: str,
        experiment_id: Optional[str],
        artifact_id: str,
    ) -> str:
        """"""
        artifact_root = f"{self.root_dir}/{slugify(project_name)}"

        if experiment_id:
            artifact_root = f"{artifact_root}/experiments/{experiment_id}"

        return f"{artifact_root}/artifacts/{artifact_id}/data"

    def _get_artifact_metadata_location(
        self,
        project_name: str,
        experiment_id: Optional[str] = None,
        artifact_id: Optional[str] = None,
    ) -> str:
        """"""
        artifact_root = f"{self.root_dir}/{slugify(project_name)}"

        if experiment_id:
            artifact_root = f"{artifact_root}/experiments/{experiment_id}"

        artifact_root = f"{artifact_root}/artifacts"

        if artifact_id:
            return f"{artifact_root}/{artifact_id}/metadata.json"
        else:
            return artifact_root

    def _get_comment_metadata_location(self, *args) -> str:
        """"""
        return "."

    def _get_dataframe_data_location(
        self,
        project_name: str,
        experiment_id: Optional[str],
        dataframe_id: str,
    ) -> str:
        """"""
        dataframe_root = f"{self.root_dir}/{slugify(project_name)}"

        if experiment_id:
            dataframe_root = f"{dataframe_root}/experiments/{experiment_id}"

        return f"{dataframe_root}/dataframes/{dataframe_id}/data"

    def _get_dataframe_metadata_location(
        self,
        project_name: str,
        experiment_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
    ) -> str:
        """"""
        dataframe_root = f"{self.root_dir}/{slugify(project_name)}"

        if experiment_id:
            dataframe_root = f"{dataframe_root}/experiments/{experiment_id}"

        dataframe_root = f"{dataframe_root}/dataframes"

        if dataframe_id:
            return f"{dataframe_root}/{dataframe_id}/metadata.json"
        else:
            return dataframe_root

    def _get_experiment_metadata_location(
        self,
        project_name: str,
        experiment_id: Optional[str] = None,
    ) -> str:
        """"""
        experiment_root = f"{self.root_dir}/{slugify(project_name)}/experiments"

        if experiment_id:
            return f"{experiment_root}/{experiment_id}/metadata.json"
        else:
            return experiment_root

    def _get_feature_metadata_location(
        self,
        project_name: str,
        experiment_id: str,
        feature_name: Optional[str] = None,
    ) -> str:
        """"""
        feature_root = (
            f"{self.root_dir}/{slugify(project_name)}/experiments/{experiment_id}/features"
        )

        if feature_name:
            return f"{feature_root}/{slugify(feature_name)}/metadata.json"
        else:
            return feature_root

    def _get_metric_metadata_location(
        self,
        project_name: str,
        experiment_id: str,
        metric_name: Optional[str] = None,
    ) -> str:
        """"""
        metric_root = f"{self.root_dir}/{slugify(project_name)}/experiments/{experiment_id}/metrics"

        if metric_name:
            return f"{metric_root}/{slugify(metric_name)}/metadata.json"
        else:
            return metric_root

    def _get_parameter_metadata_location(
        self,
        project_name: str,
        experiment_id: str,
        parameter_name: Optional[str] = None,
    ) -> str:
        """"""
        parameter_root = (
            f"{self.root_dir}/{slugify(project_name)}/experiments/{experiment_id}/parameters"
        )

        if parameter_name:
            return f"{parameter_root}/{slugify(parameter_name)}/metadata.json"
        else:
            return parameter_root

    def _get_project_metadata_location(self, project_name: Optional[str] = None) -> str:
        """"""
        if project_name:
            return f"{self.root_dir}/{slugify(project_name)}/metadata.json"
        else:
            return self.root_dir

    def _get_tag_metadata_location(
        self,
        taggable_type: "DOMAIN_CLASS_TYPES",
        project_name: str,
        experiment_id: str,
        taggable_identifier: Optional[str] = None,
        return_directory: bool = False,
    ) -> str:
        """"""
        if taggable_type == Experiment:
            tag_root = self._get_location(taggable_type, project_name, experiment_id)
        else:
            tag_root = self._get_location(
                taggable_type, project_name, experiment_id, taggable_identifier
            )

        tag_root_directory = os.path.dirname(tag_root)

        if return_directory:
            return tag_root_directory
        else:
            return f"{tag_root_directory}/tags_{uuid.uuid4()}.json"

    def _read_bytes(self, location: str, *args) -> bytes:
        """"""
        try:
            file = self.filesystem.open(location, "rb")
        except FileNotFoundError as error:
            raise RubiconException() from error

        return file.read()

    def _read_dataframe(self, location: str, df_type: Literal["dask", "pandas", "polars"], *args):
        """"""
        acceptable_types = ["dask", "pandas", "polars"]
        read_kwargs = {}

        if df_type == "dask":
            df_library = try_import_dask_dataframe()
            read_kwargs["engine"] = "pyarrow"
        elif df_type == "pandas":
            df_library = try_import_pandas_dataframe()
            location = os.path.join(location, "data.parquet")
            read_kwargs["engine"] = "pyarrow"
        elif df_type == "polars":
            df_library = try_import_polars_dataframe()
            location = os.path.join(location, "data.parquet")
        else:
            raise ValueError(f"`df_type` must be one of {acceptable_types}")

        return df_library.read_parquet(location, **read_kwargs)

    def _read_json(self, location: str, domain_cls: "DOMAIN_CLASS_TYPES", *args) -> "DOMAIN_TYPES":
        """"""
        try:
            file = self.filesystem.open(location)
        except FileNotFoundError as error:
            raise RubiconException() from error

        data = json.load(file)
        if domain_cls:
            data = domain_cls(**data)

        return data

    def _read_jsons(
        self, location: str, domain_cls: "DOMAIN_CLASS_TYPES", *args
    ) -> List["DOMAIN_TYPES"]:
        """"""
        try:
            if domain_cls == TagUpdate:
                metadata_paths = [
                    p.get("name")
                    for p in self.filesystem.ls(location, detail=True)
                    if "tags_" in p.get("name") and p.get("name").endswith(".json")
                ]

                if not metadata_paths:
                    return []
            else:
                metadata_paths = [
                    os.path.join(p.get("name"), "metadata.json")
                    for p in self.filesystem.ls(location, detail=True)
                    if p.get("type", p.get("StorageClass")).lower() == "directory"
                ]
        except FileNotFoundError:
            return []

        domains = []

        for path, metadata in self.filesystem.cat(metadata_paths, on_error="return").items():
            if isinstance(metadata, FileNotFoundError):
                warning = f"{path} not found. Was this file unintentionally created?"
                warnings.warn(warning)
            else:
                try:
                    metadata_contents = json.loads(metadata)
                    domain = domain_cls(**metadata_contents)
                except (JSONDecodeError, TypeError) as error:
                    if isinstance(error, JSONDecodeError):
                        warning = f"Failed to load metadata for `{domain_cls.__name__}` at {path}"
                    else:
                        warning = (
                            f"Failed to instantiate `{domain_cls.__name__}` from metadata at {path}"
                        )

                    warnings.warn(warning)

                    continue

                domains.append(domain)

        if domains:
            domains.sort(key=lambda d: d.created_at)

        return domains

    def _write_bytes(self, data: bytes, location: str, *args):
        """"""
        dir_name = os.path.dirname(location)
        self.filesystem.mkdirs(dir_name, exist_ok=True)

        with self.filesystem.open(location, "wb") as file:
            file.write(data)

    def _write_dataframe(self, data: "DATAFRAME_TYPES", location: str, *args):
        """"""
        if safe_is_dask_dataframe(data):
            data.to_parquet(location, engine="pyarrow")
        else:
            self.filesystem.mkdirs(location, exist_ok=True)
            file_location = os.path.join(location, "data.parquet")

            if safe_is_pandas_dataframe(data):
                data.to_parquet(file_location, engine="pyarrow")
            else:
                data.write_parquet(file_location)

    def _write_json(self, data: "DOMAIN_TYPES", location: str, *args):
        """"""
        if self.filesystem.exists(location):
            raise RubiconException(
                f"A `{type(data).__name__}` already exists at '{os.path.dirname(location)}'"
            )

        dir_name = os.path.dirname(location)
        self.filesystem.mkdirs(dir_name, exist_ok=True)

        with self.filesystem.open(location, "w") as file:
            file.write(json.dumps(data))
