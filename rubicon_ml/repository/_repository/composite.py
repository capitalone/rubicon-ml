from typing import TYPE_CHECKING, Dict, List

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository._repository.repository import RepositoryABC

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES


class CompositeRepository(RepositoryABC):
    """Composite repository for multiple rubicon-ml backends."""

    def __init__(self, repositories: List[RepositoryABC]):
        """"""
        self.repositories = repositories

    def read_bytes(self) -> bytes:
        """"""
        for repository in self.repositories:
            try:
                data = repository.read_bytes()
            except Exception:
                pass
            else:
                return data

        raise RubiconException("All backends failed.")

    def read_dataframe(self) -> "DATAFRAME_TYPES":
        """"""
        for repository in self.repositories:
            try:
                data = repository.read_dataframe()
            except Exception:
                pass
            else:
                return data

        raise RubiconException("All backends failed.")

    def read_json(self) -> Dict:
        """"""
        for repository in self.repositories:
            try:
                data = repository.read_json()
            except Exception:
                pass
            else:
                return data

        raise RubiconException("All backends failed.")

    def write_bytes(self, data: bytes, *args):
        """"""
        for repository in self.repositories:
            repository.write_bytes(data, *args)

    def write_dataframe(self, data: "DATAFRAME_TYPES", *args):
        """"""
        for repository in self.repositories:
            repository.write_dataframe(data, *args)

    def write_json(self, data: Dict, *args):
        """"""
        for repository in self.repositories:
            repository.write_json(data, *args)
