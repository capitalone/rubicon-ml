from rubicon_ml import domain
from rubicon_ml.client import Dataframe


def test_properties(project_client):
    parent = project_client

    domain_dataframe = domain.Dataframe(
        description="some description", tags=["x"], name="test title"
    )
    dataframe = Dataframe(domain_dataframe, parent)

    assert dataframe.id == domain_dataframe.id
    assert dataframe.name == domain_dataframe.name
    assert dataframe.description == domain_dataframe.description
    assert dataframe.tags == domain_dataframe.tags
    assert dataframe.created_at == domain_dataframe.created_at
    assert dataframe.parent == parent


def test_get_data(project_client, test_dataframe):
    parent = project_client
    df = test_dataframe
    logged_df = parent.log_dataframe(df)

    assert logged_df.get_data().compute().equals(df.compute())
