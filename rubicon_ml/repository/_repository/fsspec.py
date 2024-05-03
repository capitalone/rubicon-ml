import os
from abc import abstractmethod

import fsspec

from rubicon_ml.repository._repository.repository import RepositoryABC
from rubicon_ml.repository.utils import json


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

    def _write(self, data, path, *args, make_dir=True, mode="w"):
        """"""
        if make_dir:
            self.filesystem.mkdirs(os.path.dirname(path), exist_ok=True)
        with self.filesystem.open(path, mode) as file:
            file.write(data)

    def write_bytes(self, data, *args):
        """"""
        path = self._get_path(*args)

        self._write(data, path, "wb", *args)

    def write_json(self, data, *args):
        """"""
        path = self._get_path(*args)

        self._write(json.dumps(data), path, *args)
