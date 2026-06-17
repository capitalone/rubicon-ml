import pytest
from unittest import mock

from rubicon_ml import domain
from rubicon_ml.client import Dataframe
from rubicon_ml.exceptions import RubiconException


def _raise_error():
    raise RubiconException()


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


@mock.patch("rubicon_ml.repository.FsspecRepository.read_dataframe_data")
def test_get_data_multiple_backend_error(
    mock_read_dataframe_data, rubicon_composite_client, test_dataframe
):
    project = rubicon_composite_client.create_project("test")
    df = test_dataframe
    logged_df = project.log_dataframe(df)

    mock_read_dataframe_data.side_effect = _raise_error
    with pytest.raises(RubiconException) as e:
        logged_df.get_data()
    assert "All 2 backends failed" in str(e)
