from typing import TYPE_CHECKING, Union


if TYPE_CHECKING:
    import dask.dataframe as dd
    import pandas as pd

    DATAFRAME_TYPES = Union[dd.DataFrame, pd.DataFrame]
