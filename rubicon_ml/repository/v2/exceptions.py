from typing import Any

from rubicon_ml.exceptions import RubiconException


class RubiconNotImplementedError(RubiconException):
    pass


class WriteOnlyMixin:
    # core read/writes

    def read_domain(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(f"{self.__class__.__name__} is write-only.")

    def read_domains(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(f"{self.__class__.__name__} is write-only.")

    # binary read/writes

    def read_artifact_data(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(f"{self.__class__.__name__} is write-only.")

    def read_dataframe_data(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(f"{self.__class__.__name__} is write-only.")


class DomainOnlyMixin:
    # binary read/writes

    def write_artifact_data(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(
            f"{self.__class__.__name__} does not support logging of binary artifact data."
        )

    def stream_artifact_data(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(
            f"{self.__class__.__name__} does not support logging of binary artifact data."
        )

    def write_dataframe_data(self, *args: Any, **kwargs: Any):
        raise RubiconNotImplementedError(
            f"{self.__class__.__name__} does not support logging of binary dataframe data."
        )
