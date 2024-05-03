from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from rubicon_ml.types import DATAFRAME_TYPES


class RepositoryABC(ABC):
    """Abstract base for rubicon-ml backend repositories."""

    @abstractmethod
    def read_bytes(self) -> bytes:
        """"""
        ...

    @abstractmethod
    def read_dataframe(self) -> "DATAFRAME_TYPES":
        """"""
        ...

    @abstractmethod
    def read_json(self) -> Dict:
        """"""
        ...

    @abstractmethod
    def write_bytes(self, data: bytes, *args):
        """"""
        ...

    @abstractmethod
    def write_dataframe(self, data: "DATAFRAME_TYPES", *args):
        """"""
        ...

    @abstractmethod
    def write_json(self, data: Dict, *args):
        """"""
        ...
