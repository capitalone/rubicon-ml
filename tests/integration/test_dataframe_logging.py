import pandas as pd
import pytest
from dask import dataframe as dd

from rubicon_ml.exceptions import RubiconException


def test_pandas_df(rubicon_local_filesystem_client):
    rubicon = rubicon_local_filesystem_client
    project = rubicon.create_project("Dataframe Test Project")

    multi_index_df = pd.DataFrame(
        [[0, 1, "a"], [1, 1, "b"], [2, 2, "c"], [3, 2, "d"]], columns=["a", "b", "c"]
    )
    multi_index_df = multi_index_df.set_index(["b", "a"])

    written_dataframe = project.log_dataframe(multi_index_df)

    read_dataframes = project.dataframes()
    read_dataframe = read_dataframes[0]

    assert len(read_dataframes) == 1

    assert read_dataframe.id == written_dataframe.id
    assert read_dataframe.get_data().equals(multi_index_df)


def test_dask_df(rubicon_local_filesystem_client):
    rubicon = rubicon_local_filesystem_client
    project = rubicon.create_project("Dataframe Test Project")

    ddf = dd.from_pandas(pd.DataFrame([0, 1], columns=["a"]), npartitions=1)

    written_dataframe = project.log_dataframe(ddf)

    read_dataframes = project.dataframes()
    read_dataframe = read_dataframes[0]

    assert len(read_dataframes) == 1

    assert read_dataframe.id == written_dataframe.id
    assert read_dataframe.get_data(df_type="dask").compute().equals(ddf.compute())


def test_df_read_error(rubicon_local_filesystem_client):
    rubicon = rubicon_local_filesystem_client
    project = rubicon.create_project("Dataframe Test Project")

    ddf = dd.from_pandas(pd.DataFrame([0, 1], columns=["a"]), npartitions=1)

    written_dataframe = project.log_dataframe(ddf)

    read_dataframes = project.dataframes()
    read_dataframe = read_dataframes[0]

    assert len(read_dataframes) == 1

    assert read_dataframe.id == written_dataframe.id

    # simulate user forgetting to set `df_type` to `dask` when reading a logged dask df
    with pytest.raises(RubiconException) as e:
        read_dataframe.get_data()
    assert (
        "This might have happened if you forgot to set `df_type='dask'` when trying to read a `dask` dataframe."
        in str(e)
    )
