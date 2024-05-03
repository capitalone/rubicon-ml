import pickle
from typing import TYPE_CHECKING

from rubicon_ml.repository._repository.fsspec import FSSpecRepositoryABC

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES


class MemoryRepository(FSSpecRepositoryABC):
    """rubicon-ml backend repository for in-memory filesystems.

    Leverages `fsspec` and its 'memory' protocol.
    """

    def __init__(self, root_dir: str, **storage_options):
        """"""
        super().__init__(root_dir, **storage_options)

        self._create_filesystem()

    def _create_filesystem(self):
        """"""
        if not self.filesystem.exists(self.root_dir):
            self.filesystem.mkdir(self.root_dir)

    def _get_protocol(self) -> str:
        """"""
        return "memory"

    def write_dataframe(self, data: "DATAFRAME_TYPES", *args):
        """"""
        path = self._get_path(*args)

        with self.filesystem.open(path, "wb") as file:
            pickle.dump(data, file)
