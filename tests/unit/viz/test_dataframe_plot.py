import pytest
from dash import Dash

from rubicon_ml.exceptions import RubiconException
from rubicon_ml.viz import DataframePlot


def test_dataframe_plot(viz_experiments):
    def test_plotting_func(*args, **kwargs):
        return

    dataframe_plot = DataframePlot(
        "test dataframe",
        experiments=viz_experiments,
        plotting_func=test_plotting_func,
        plotting_func_kwargs={"test": "test"},
        x="test x",
        y="test y",
    )

    expected_experiment_ids = [e.id for e in viz_experiments]

    for experiment in dataframe_plot.experiments:
        assert experiment.id in expected_experiment_ids

        expected_experiment_ids.remove(experiment.id)

    assert len(expected_experiment_ids) == 0
    assert dataframe_plot.dataframe_name == "test dataframe"
    assert dataframe_plot.plotting_func == test_plotting_func
    assert dataframe_plot.plotting_func_kwargs == {"test": "test"}
    assert dataframe_plot.x == "test x"
    assert dataframe_plot.y == "test y"


def test_dataframe_plot_load_data(viz_experiments):
    dataframe_plot = DataframePlot("test dataframe", experiments=viz_experiments)
    dataframe_plot.load_experiment_data()

    expected_experiment_ids = [e.id for e in viz_experiments]

    for eeid in expected_experiment_ids:
        assert eeid in list(dataframe_plot.data_df["experiment_id"])

    expected_len_df = 0
    for experiment in viz_experiments:
        for dataframe in experiment.dataframes():
            expected_len_df = expected_len_df + len(dataframe.get_data())

    assert len(dataframe_plot.data_df) == expected_len_df


def test_dataframe_plot_layout(viz_experiments):
    dataframe_plot = DataframePlot("test dataframe", experiments=viz_experiments)
    dataframe_plot.load_experiment_data()
    layout = dataframe_plot.layout

    assert len(layout.children) == 3
    assert layout.children[0].id == "dummy-callback-trigger"
    assert layout.children[-1].children.id == "dataframe-plot"


@pytest.mark.parametrize("is_linked,expected", [(False, 1), (True, 2)])
def test_dataframe_plot_register_callbacks_link(viz_experiments, is_linked, expected):
    dataframe_plot = DataframePlot("test dataframe", experiments=viz_experiments)
    dataframe_plot.app = Dash(__name__, title="test callbacks")
    dataframe_plot.register_callbacks(link_experiment_table=is_linked)

    callback_values = list(dataframe_plot.app.callback_map.values())

    assert len(callback_values) == 1

    registered_callback_name = callback_values[0]["callback"].__name__
    registered_callback_len_input = len(callback_values[0]["inputs"])

    assert registered_callback_name == "update_dataframe_plot"
    assert registered_callback_len_input == expected


def test_dataframe_no_experiments():
    dataframe_plot = DataframePlot("test dataframe", experiments=[])
    with pytest.raises(RubiconException):
        dataframe_plot.load_experiment_data()


def test_cant_find_dataframes_raise_exception(viz_experiments_no_dataframes):
    for exp in viz_experiments_no_dataframes:
        if len(exp.dataframes()) > 0:
            viz_experiments_no_dataframes.remove(exp)
    dataframe_plot = DataframePlot("test dataframe", experiments=viz_experiments_no_dataframes)
    with pytest.raises(RubiconException):
        dataframe_plot.load_experiment_data()


def test_wrong_dataframe_name(viz_experiments):
    dataframe_plot = DataframePlot("no_name", experiments=viz_experiments)
    with pytest.raises(RubiconException):
        dataframe_plot.load_experiment_data()
