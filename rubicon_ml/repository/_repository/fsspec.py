import os
from abc import abstractmethod
from typing import TYPE_CHECKING, Dict, Literal, Union

import fsspec

from rubicon_ml.repository._repository.repository import RepositoryABC
from rubicon_ml.repository.utils import json
from rubicon_ml.types import safe_is_pandas_dataframe

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES


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

    def _get_path(self) -> str:
        """"""
        return "./"

    @abstractmethod
    def _get_protocol(self) -> str:
        """"""
        ...

    def _write(
        self,
        data: Union[bytes, Dict, "DATAFRAME_TYPES"],
        path: str,
        *args,
        make_dir: bool = True,
        mode: Literal["w", "wb"] = "w",
    ):
        """"""
        if make_dir:
            self.filesystem.mkdirs(os.path.dirname(path), exist_ok=True)
        with self.filesystem.open(path, mode) as file:
            file.write(data)

    def write_bytes(self, data: bytes, *args):
        """"""
        path = self._get_path(*args)

        self._write(data, path, "wb", *args)

    def write_dataframe(self, data: "DATAFRAME_TYPES", *args):
        """"""
        path = self._get_path(*args)

        if safe_is_pandas_dataframe(data):
            self._mkdir(path)

            path = os.path.join(path, "data.parquet")

        data.to_parquet(path, engine="pyarrow")

    def write_json(self, data: Dict, *args):
        """"""
        path = self._get_path(*args)

        self._write(json.dumps(data), path, *args)
