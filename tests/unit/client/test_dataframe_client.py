import pytest

from rubicon_ml import domain
from rubicon_ml.client import Dataframe
from rubicon_ml.exceptions import RubiconException


def test_properties(project_client):
    parent = project_client

    domain_dataframe = domain.Dataframe(
        description="some description",
        tags=["x"],
        comments=["this is a comment"],
        name="test title",
    )
    dataframe = Dataframe(domain_dataframe, parent)

    assert dataframe.id == domain_dataframe.id
    assert dataframe.name == domain_dataframe.name
    assert dataframe.description == domain_dataframe.description
    assert dataframe.tags == domain_dataframe.tags
    assert dataframe.comments == domain_dataframe.comments
    assert dataframe.created_at == domain_dataframe.created_at
    assert dataframe.parent == parent


def test_get_data(project_client, test_dataframe):
    parent = project_client
    df = test_dataframe
    logged_df = parent.log_dataframe(df)

    assert logged_df.get_data().compute().equals(df.compute())


def test_get_data_multiple_backend_error(rubicon_composite_client, test_dataframe):
    project = rubicon_composite_client.create_project("test")
    df = test_dataframe
    logged_df = project.log_dataframe(df)
    for repo in rubicon_composite_client.repositories:
        repo.delete_dataframe(project.name, logged_df.id)
    with pytest.raises(RubiconException) as e:
        logged_df.get_data()
    assert f"No data for dataframe with id `{logged_df.id}`" in str(e)
