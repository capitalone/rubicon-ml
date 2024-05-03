import pickle
from typing import TYPE_CHECKING, Literal

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

        if not self.filesystem.exists(self.root_dir):
            self.filesystem.mkdir(self.root_dir)

    def _get_protocol(self) -> str:
        """"""
        return "memory"

    def _read_dataframe(
        self, location: str, df_type: Literal["dask", "pandas"], *args
    ) -> "DATAFRAME_TYPES":
        """"""
        with self.filesystem.open(location, "rb") as file:
            data = pickle.load(file)

        return data

    def _write_dataframe(self, data: "DATAFRAME_TYPES", location: str, *args):
        """"""
        with self.filesystem.open(location, "wb") as file:
            pickle.dump(data, file)
