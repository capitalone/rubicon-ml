import pytest
from dash import Dash

from rubicon_ml.viz import MetricCorrelationPlot


def test_metric_correlation_plot(viz_experiments):
    metric_plot = MetricCorrelationPlot(
        experiments=viz_experiments,
        metric_names=["test metric 0", "test metric 1"],
        parameter_names=["test param 1", "test param 2"],
        selected_metric="test metric 1",
    )

    expected_experiment_ids = [e.id for e in viz_experiments]

    for experiment in metric_plot.experiments:
        assert experiment.id in expected_experiment_ids

        expected_experiment_ids.remove(experiment.id)

    assert len(expected_experiment_ids) == 0
    assert metric_plot.metric_names == ["test metric 0", "test metric 1"]
    assert metric_plot.parameter_names == ["test param 1", "test param 2"]
    assert metric_plot.selected_metric == "test metric 1"


def test_metric_correlation_plot_load_data(viz_experiments):
    metric_plot = MetricCorrelationPlot(
        experiments=viz_experiments,
        metric_names=["test metric 0", "test metric 1"],
    )
    metric_plot.load_experiment_data()

    expected_experiment_ids = [e.id for e in viz_experiments]
    expected_metric_names = ["test metric 0", "test metric 1"]
    expected_parameter_names = [p.name for p in viz_experiments[0].parameters()]

    for experiment_id, record in metric_plot.experiment_records.items():
        assert experiment_id in expected_experiment_ids

        for metric_name in record["metrics"].keys():
            assert metric_name in expected_metric_names

        for parameter_name in record["parameters"].keys():
            assert parameter_name in expected_parameter_names

    assert metric_plot.visible_parameter_names.sort() == expected_parameter_names.sort()
    assert metric_plot.visible_metric_names.sort() == expected_metric_names.sort()
    assert metric_plot.selected_metric == expected_metric_names[0]


def test_metric_correlation_plot_load_data_throws_error(viz_experiments):
    metric_plot = MetricCorrelationPlot(
        experiments=viz_experiments,
        metric_names=["test metric 0", "test metric 1"],
        selected_metric="nonexistant metric",
    )

    with pytest.raises(ValueError) as e:
        metric_plot.load_experiment_data()

    assert "no metric named `selected_metric` 'nonexistant metric'" in str(e.value)


def test_metric_correlation_plot_layout(viz_experiments):
    metric_plot = MetricCorrelationPlot(
        experiments=viz_experiments,
        metric_names=["test metric 0", "test metric 1"],
        selected_metric="test metric 1",
    )
    metric_plot.load_experiment_data()
    layout = metric_plot.layout

    assert len(layout.children) == 2
    assert layout.children[-1].children.id == "metric-correlation-plot-container"
    assert layout.children[-1].children.children.id == "metric-correlation-plot"


@pytest.mark.parametrize("is_linked,expected", [(False, 1), (True, 2)])
def test_metric_correlation_plot_register_callbacks(viz_experiments, is_linked, expected):
    metric_plot = MetricCorrelationPlot(
        experiments=viz_experiments,
        metric_names=["test metric 0", "test metric 1"],
    )
    metric_plot.app = Dash(__name__, title="test callbacks")
    metric_plot.register_callbacks(link_experiment_table=is_linked)

    callback_values = list(metric_plot.app.callback_map.values())

    assert len(callback_values) == 1

    registered_callback_name = callback_values[0]["callback"].__name__
    registered_callback_len_input = len(callback_values[0]["inputs"])

    assert registered_callback_name == "update_metric_plot"
    assert registered_callback_len_input == expected


def test_metric_correlation_plot_get_dimensions(viz_experiments):
    metric_plot = MetricCorrelationPlot(
        experiments=viz_experiments,
        metric_names=["test metric 0", "test metric 1"],
        selected_metric="test metric 1",
    )
    metric_plot.load_experiment_data()
    dimension = metric_plot._get_dimension("test dimension", [1, 2, 3, 4])

    assert list(dimension["values"]) == [1, 2, 3, 4]


def test_metric_correlation_plot_get_string_dimensions(viz_experiments):
    metric_plot = MetricCorrelationPlot(
        experiments=viz_experiments,
        metric_names=["test metric 0", "test metric 1"],
        selected_metric="test metric 1",
    )
    metric_plot.load_experiment_data()
    dimension = metric_plot._get_dimension("test dimension", ["A", "B", "A", "B", "C"])

    assert dimension["tickvals"] == [0, 1, 2]
    assert list(dimension["ticktext"]) == ["A", "B", "C"]
    assert list(dimension["values"]) == [0, 1, 0, 1, 2]


def test_metric_correlation_plot_get_boolean_dimensions(viz_experiments):
    metric_plot = MetricCorrelationPlot(
        experiments=viz_experiments,
        metric_names=["test metric 0", "test metric 1"],
        selected_metric="test metric 1",
    )
    metric_plot.load_experiment_data()
    dimension = metric_plot._get_dimension("test dimension", [True, False, True])

    assert dimension["tickvals"] == [0, 1]
    assert list(dimension["ticktext"]) == ["False", "True"]
    assert list(dimension["values"]) == [1, 0, 1]
