from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Type, TypeGuard, Union

    import dask.dataframe as dd
    import pandas as pd
    import polars as pl

    from rubicon_ml.domain.artifact import Artifact
    from rubicon_ml.domain.dataframe import Dataframe
    from rubicon_ml.domain.experiment import Experiment
    from rubicon_ml.domain.feature import Feature
    from rubicon_ml.domain.metric import Metric
    from rubicon_ml.domain.parameter import Parameter
    from rubicon_ml.domain.project import Project

    DATAFRAME_TYPES = Union[dd.DataFrame, pd.DataFrame, pl.DataFrame]
    DATAFRAME_TYPE_NAMES = Literal["dask", "pandas", "polars"]
    DOMAIN_CLASS_TYPES = Union[
        Type[Artifact],
        Type[Dataframe],
        Type[Experiment],
        Type[Feature],
        Type[Metric],
        Type[Parameter],
        Type[Project],
    ]
    DOMAIN_TYPES = Union[Artifact, Dataframe, Experiment, Feature, Metric, Parameter, Project]


def safe_is_dask_dataframe(dataframe: "DATAFRAME_TYPES") -> "TypeGuard[dd.DataFrame]":
    """"""
    try:
        import dask.dataframe as dd
    except ImportError:
        return False
    else:
        return isinstance(dataframe, dd.DataFrame)


def safe_is_pandas_dataframe(dataframe: "DATAFRAME_TYPES") -> "TypeGuard[pd.DataFrame]":
    """"""
    try:
        import pandas as pd
    except ImportError:
        return False
    else:
        return isinstance(dataframe, pd.DataFrame)


def safe_is_polars_dataframe(dataframe: "DATAFRAME_TYPES") -> "TypeGuard[pl.DataFrame]":
    """"""
    try:
        import polars as pl
    except ImportError:
        return False
    else:
        return isinstance(dataframe, pl.DataFrame)
