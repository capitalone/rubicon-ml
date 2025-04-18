from typing import TYPE_CHECKING, Any, Optional

from rubicon_ml.exceptions import RubiconException

if TYPE_CHECKING:
    from rubicon_ml.domain import DomainsVar


class RubiconNotImplementedError(RubiconException):
    pass


class WriteOnlyMixin:
    def _raise_write_only_exception(self):
        raise RubiconNotImplementedError(f"`{self.__class__.__name__}` is write-only.")

    # core reads

    def read_domain(
        self,
        project_name: str,
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
    ):
        self._raise_write_only_exception()

    def read_domains(
        self,
        domain_cls: "DomainsVar",
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
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
