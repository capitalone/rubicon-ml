from typing import TYPE_CHECKING, Any, Callable, List

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository._repository.repository import RepositoryABC

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES, DOMAIN_TYPES


def _safe_call_func(func: Callable, *args) -> Any:
    """"""
    try:
        data = func(*args)
    except Exception:
        pass
    else:
        return data


class CompositeRepository(RepositoryABC):
    """Composite repository for multiple rubicon-ml backends."""

    def __init__(self, repositories: List[RepositoryABC]):
        """"""
        self.repositories = repositories

    def read_bytes(self, *args) -> bytes:
        """"""
        for repository in self.repositories:
            return _safe_call_func(repository.read_bytes, *args)

        raise RubiconException("All backends failed.")

    def read_dataframe(self, *args) -> "DATAFRAME_TYPES":
        """"""
        for repository in self.repositories:
            return _safe_call_func(repository.read_dataframe, *args)

        raise RubiconException("All backends failed.")

    def read_domain(self, *args) -> "DOMAIN_TYPES":
        """"""
        for repository in self.repositories:
            return _safe_call_func(repository.read_domain, *args)

        raise RubiconException("All backends failed.")

    def write_bytes(self, data: bytes, *args):
        """"""
        for repository in self.repositories:
            repository.write_bytes(data, *args)

    def write_dataframe(self, data: "DATAFRAME_TYPES", *args):
        """"""
        for repository in self.repositories:
            repository.write_dataframe(data, *args)

    def write_domain(self, data: "DOMAIN_TYPES", *args):
        """"""
        for repository in self.repositories:
            repository.write_domain(data, *args)
