from typing import TYPE_CHECKING, Dict, List, Union

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository._repository.repository import RepositoryABC

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES


class CompositeRepository(RepositoryABC):
    """Composite repository for multiple rubicon-ml backends."""

    def __init__(self, repositories: List[RepositoryABC]):
        """"""
        self.repositories = repositories

    def _read_all_return_one(
        self, func_name, *args, **kwargs
    ) -> Union[bytes, Dict, "DATAFRAME_TYPES"]:
        """"""
        for repository in self.repositories:
            try:
                data = getattr(repository, func_name)(*args, **kwargs)
            except Exception:
                pass
            else:
                return data

        raise RubiconException("All backends failed.")

    def write_json(self):
        """"""
        for repository in self.repositories:
            repository.write_json()

    def read_json(self) -> Dict:
        """"""
        return self._read_all_return_one("read_json")

    def write_bytes(self):
        """"""
        for repository in self.repositories:
            repository.write_bytes()

    def read_bytes(self) -> bytes:
        """"""
        return self._read_all_return_one("read_bytes")

    def write_dataframe(self):
        """"""
        for repository in self.repositories:
            repository.write_dataframe()

    def read_dataframe(self) -> "DATAFRAME_TYPES":
        """"""
        return self._read_all_return_one("read_dataframe")
