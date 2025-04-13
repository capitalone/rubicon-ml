from typing import Any

from rubicon_ml.exceptions import RubiconException


class RubiconNotImplementedError(RubiconException):
    pass


class WriteOnlyMixin:
    def _raise_write_only_exception(self):
        raise RubiconNotImplementedError(f"`{self.__class__.__name__}` is write-only.")

    # core reads

    def read_domain(self, *args: Any, **kwargs: Any):
        self._raise_write_only_exception()

    def read_domains(self, *args: Any, **kwargs: Any):
        self._raise_write_only_exception()

    # binary reads

    def read_artifact_data(self, *args: Any, **kwargs: Any):
        self._raise_write_only_exception()

    def read_dataframe_data(self, *args: Any, **kwargs: Any):
        self._raise_write_only_exception()


class DomainOnlyMixin:
    def _raise_domain_only_exception(self):
        raise RubiconNotImplementedError(
            f"`{self.__class__.__name__}` does not support logging of binary data."
        )

    # binary read/writes

    def read_artifact_data(self, *args: Any, **kwargs: Any):
        self._raise_domain_only_exception()

    def stream_artifact_data(self, *args: Any, **kwargs: Any):
        self._raise_domain_only_exception()

    def write_artifact_data(self, *args: Any, **kwargs: Any):
        self._raise_domain_only_exception()

    def read_dataframe_data(self, *args: Any, **kwargs: Any):
        self._raise_domain_only_exception()

    def write_dataframe_data(self, *args: Any, **kwargs: Any):
        self._raise_domain_only_exception()
