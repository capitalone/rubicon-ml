from types import ModuleType

from rubicon_ml.exceptions import RubiconException

dataframe_message = (
    "`rubicon_ml` requires `{library}` to be installed in the current environment "
    "to read and write `{library}` dataframes. `pip install {library}{extras}` or "
    "`conda install {library}` to continue."
)


def try_import_dask_dataframe() -> ModuleType:
    """"""
    try:
        import dask.dataframe as dd
    except ImportError:
        raise RubiconException(dataframe_message.format(library="dask", extras="[dataframe]"))

    return dd


def try_import_pandas_dataframe() -> ModuleType:
    """"""
    try:
        import pandas as pd
    except ImportError:
        raise RubiconException(dataframe_message.format(library="pandas", extras=""))

    return pd
