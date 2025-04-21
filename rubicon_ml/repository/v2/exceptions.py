from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from rubicon_ml.exceptions import RubiconException

if TYPE_CHECKING:
    import dask.dataframe as dd
    import pandas as pd
    import polars as pl

    from rubicon_ml.domain import DOMAIN_CLASS_TYPES, DOMAIN_TYPES


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
    ) -> "DOMAIN_TYPES":
        self._raise_write_only_exception()

    def read_domains(
        self,
        domain_cls: Union["DOMAIN_CLASS_TYPES", Literal["CommentUpdate", "TagUpdate"]],
        artifact_id: Optional[str] = None,
        dataframe_id: Optional[str] = None,
        experiment_id: Optional[str] = None,
        feature_name: Optional[str] = None,
        metric_name: Optional[str] = None,
        parameter_name: Optional[str] = None,
        project_name: Optional[str] = None,
    ) -> List[Union["DOMAIN_TYPES", Dict]]:
        self._raise_write_only_exception()

    # binary reads

    def read_artifact_data(
        self,
        artifact_id: str,
        project_name: str,
        experiment_id: Optional[str] = None,
    ) -> bytes:
        self._raise_write_only_exception()

    def read_dataframe_data(
        self,
        dataframe_id: str,
        dataframe_type: Literal["dask", "dd", "pandas", "pd", "polars", "pl"],
        project_name: str,
        experiment_id: Optional[str] = None,
    ) -> Union["dd.DataFrame", "pd.DataFrame", "pl.DataFrame"]:
        self._raise_write_only_exception()


class DomainOnlyMixin:
    def _raise_domain_only_exception(self):
        raise RubiconNotImplementedError(
            f"`{self.__class__.__name__}` does not support logging of binary data."
        )

    # binary read/writes

    def read_artifact_data(
        self,
        artifact_id: str,
        project_name: str,
        experiment_id: Optional[str] = None,
    ) -> bytes:
        self._raise_domain_only_exception()

    def write_artifact_data(
        self,
        artifact_data: bytes,
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        self._raise_domain_only_exception()

    def read_dataframe_data(
        self,
        dataframe_id: str,
        dataframe_type: Literal["dask", "dd", "pandas", "pd", "polars", "pl"],
        project_name: str,
        experiment_id: Optional[str] = None,
    ) -> Union["dd.DataFrame", "pd.DataFrame", "pl.DataFrame"]:
        self._raise_domain_only_exception()

    def write_dataframe_data(
        self,
        dataframe_data: Union["dd.DataFrame", "pd.DataFrame", "pl.DataFrame"],
        dataframe_id: str,
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        self._raise_domain_only_exception()
