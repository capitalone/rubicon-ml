import os
import warnings
from abc import abstractmethod
from json import JSONDecodeError
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Type, Union

import fsspec

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.imports import try_import_dask_dataframe, try_import_pandas_dataframe
from rubicon_ml.repository._repository.repository import RepositoryABC
from rubicon_ml.repository.utils import json, slugify
from rubicon_ml.types import safe_is_pandas_dataframe

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES, DOMAIN_CLASS_TYPES, DOMAIN_TYPES


class FSSpecRepositoryABC(RepositoryABC):
    """rubicon-ml backend repository for `fsspec` based filesystems.

    https://filesystem-spec.readthedocs.io/en/latest/
    """

    def __init__(self, root_dir: str, **storage_options):
        protocol = self._get_protocol()
        filesystem = fsspec.filesystem(protocol, **storage_options)

        self.filesystem = filesystem
        self.protocol = protocol
        self.root_dir = root_dir

    @abstractmethod
    def _get_protocol(self) -> str:
        """"""
        ...

    def _get_artifact_data_location(self, *args) -> str:
        """"""
        return "."

    def _get_artifact_metadata_location(self, *args) -> str:
        """"""
        return "."

    def _get_comment_metadata_location(self, *args) -> str:
        """"""
        return "."

    def _get_dataframe_data_location(self, *args) -> str:
        """"""
        return "."

    def _get_dataframe_metadata_location(self, *args) -> str:
        """"""
        return "."

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

    def _get_feature_metadata_location(self, *args) -> str:
        """"""
        return "."

    def _get_metric_metadata_location(self, *args) -> str:
        """"""
        return "."

    def _get_parameter_metadata_location(self, *args) -> str:
        """"""
        return "."

    def _get_project_metadata_location(self, project_name: Optional[str] = None) -> str:
        """"""
        if project_name:
            return f"{self.root_dir}/{slugify(project_name)}/metadata.json"
        else:
            return self.root_dir

    def _get_tag_metadata_location(self, *args) -> str:
        """"""
        return "."

    def _write(
        self,
        data: Union[bytes, Dict, "DOMAIN_TYPES", "DATAFRAME_TYPES"],
        location: str,
        *args,
        make_dir: bool = True,
        mode: Literal["w", "wb"] = "w",
    ):
        """"""
        if make_dir:
            dir_name = os.path.dirname(location)
            self.filesystem.mkdirs(dir_name, exist_ok=True)

        with self.filesystem.open(location, mode) as file:
            file.write(data)

    def _read_bytes(self, location: str, *args) -> bytes:
        """"""
        try:
            file = self.filesystem.open(location, "rb")
        except FileNotFoundError as error:
            raise RubiconException() from error

        return file.read()

    def _read_dataframe(self, location: str, df_type: Literal["dask", "pandas"], *args):
        """"""
        acceptable_types = ["pandas", "dask"]

        if df_type == "pandas":
            df_library = try_import_pandas_dataframe()
            location = os.path.join(location, "data.parquet")
        elif df_type == "dask":
            df_library = try_import_dask_dataframe()
        else:
            raise ValueError(f"`df_type` must be one of {acceptable_types}")

        return df_library.read_parquet(location, engine="pyarrow")

    def _read_json(
        self, location: str, domain_cls: Union[Type[Dict], "DOMAIN_CLASS_TYPES"], *args
    ) -> Union[Dict, "DOMAIN_TYPES"]:
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
        self, location: str, domain_cls: Union[Type[Dict], "DOMAIN_CLASS_TYPES"], *args
    ) -> List[Union[Dict, "DOMAIN_TYPES"]]:
        """"""
        try:
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
        self._write(data, location, "wb", *args)

    def _write_dataframe(self, data: "DATAFRAME_TYPES", location: str, *args):
        """"""
        if safe_is_pandas_dataframe(data):
            self.filesystem.mkdirs(location, exist_ok=True)

            location = os.path.join(location, "data.parquet")

        data.to_parquet(location, engine="pyarrow")

    def _write_json(self, data: Union[Dict, "DOMAIN_TYPES"], location: str, *args):
        """"""
        if self.filesystem.exists(location):
            raise RubiconException(
                f"A `{type(data).__name__}` already exists at '{os.path.dirname(location)}'"
            )

        self._write(json.dumps(data), location, *args)
