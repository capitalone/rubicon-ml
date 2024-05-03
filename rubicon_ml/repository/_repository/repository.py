from abc import ABC, abstractmethod


class RepositoryABC(ABC):
    """Abstract base for rubicon-ml backend repositories."""

    @abstractmethod
    def write_bytes(self):
        """"""
        ...

    @abstractmethod
    def write_dataframe(self):
        """"""
        ...

    @abstractmethod
    def write_json(self):
        """"""
        ...

    @abstractmethod
    def read_bytes(self):
        """"""
        ...

    @abstractmethod
    def read_dataframe(self):
        """"""
        ...

    @abstractmethod
    def read_json(self):
        """"""
        ...
