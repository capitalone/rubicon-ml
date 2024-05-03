import os
from abc import abstractmethod
from typing import TYPE_CHECKING, Literal, Union

import fsspec

from rubicon_ml.repository._repository.repository import RepositoryABC
from rubicon_ml.repository.utils import json
from rubicon_ml.types import safe_is_pandas_dataframe

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES, DOMAIN_TYPES


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

    def _get_artifact_data_path(self, *args) -> str:
        """"""
        return "."

    def _get_artifact_metadata_path(self, *args) -> str:
        """"""
        return "."

    def _get_comment_metadata_path(self, *args) -> str:
        """"""
        return "."

    def _get_dataframe_data_path(self, *args) -> str:
        """"""
        return "."

    def _get_dataframe_metadata_path(self, *args) -> str:
        """"""
        return "."

    def _get_experiment_metadata_path(self, *args) -> str:
        """"""
        return "."

    def _get_feature_metadata_path(self, *args) -> str:
        """"""
        return "."

    def _get_metric_metadata_path(self, *args) -> str:
        """"""
        return "."

    def _get_parameter_metadata_path(self, *args) -> str:
        """"""
        return "."

    def _get_project_metadata_path(self, *args) -> str:
        """"""
        return "."

    def _get_tag_metadata_path(self, *args) -> str:
        """"""
        return "."

    def _write(
        self,
        data: Union[bytes, "DOMAIN_TYPES", "DATAFRAME_TYPES"],
        path: str,
        *args,
        make_dir: bool = True,
        mode: Literal["w", "wb"] = "w",
    ):
        """"""
        if make_dir:
            dir_name = os.path.dirname(path)
            self.filesystem.mkdirs(dir_name, exist_ok=True)

        with self.filesystem.open(path, mode) as file:
            file.write(data)

    def _write_bytes(self, data: bytes, path: str, *args):
        """"""
        self._write(data, path, "wb", *args)

    def _write_dataframe(self, data: "DATAFRAME_TYPES", path: str, *args):
        """"""
        if safe_is_pandas_dataframe(data):
            self.filesystem.mkdirs(path, exist_ok=True)

            path = os.path.join(path, "data.parquet")

        data.to_parquet(path, engine="pyarrow")

    def _write_domain(self, data: "DOMAIN_TYPES", path: str, *args):
        """"""
        self._write(json.dumps(data), path, *args)
