from typing import TYPE_CHECKING, Any, Callable, Dict, List, Type, Union

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository._repository.repository import RepositoryABC

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES, DOMAIN_CLASS_TYPES, DOMAIN_TYPES


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

    def read_json(
        self, domain_cls: Union[Type[Dict], "DOMAIN_CLASS_TYPES"], *args
    ) -> Union[Dict, "DOMAIN_TYPES"]:
        """"""
        for repository in self.repositories:
            return _safe_call_func(repository.read_json, domain_cls, *args)

        raise RubiconException("All backends failed.")

    def read_jsons(
        self, domain_cls: Union[Type[Dict], "DOMAIN_CLASS_TYPES"], *args
    ) -> List[Union[Dict, "DOMAIN_TYPES"]]:
        """"""
        for repository in self.repositories:
            return _safe_call_func(repository.read_jsons, domain_cls, *args)

        raise RubiconException("All backends failed.")

    def write_bytes(self, data: bytes, *args):
        """"""
        for repository in self.repositories:
            repository.write_bytes(data, *args)

    def write_dataframe(self, data: "DATAFRAME_TYPES", *args):
        """"""
        for repository in self.repositories:
            repository.write_dataframe(data, *args)

    def write_json(self, data: Union[Dict, "DOMAIN_TYPES"], *args):
        """"""
        for repository in self.repositories:
            repository.write_json(data, *args)
