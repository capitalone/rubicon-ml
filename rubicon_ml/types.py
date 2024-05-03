from typing import TYPE_CHECKING, Union


if TYPE_CHECKING:
    import dask.dataframe as dd
    import pandas as pd

    DATAFRAME_TYPES = Union[dd.DataFrame, pd.DataFrame]


def safe_is_dask_dataframe(dataframe):
    """"""
    try:
        import dask.dataframe as dd
    except ImportError:
        return False
    else:
        return isinstance(dataframe, dd.DataFrame)


def safe_is_pandas_dataframe(dataframe):
    """"""
    try:
        import pandas as pd
    except ImportError:
        return False
    else:
        return isinstance(dataframe, pd.DataFrame)
