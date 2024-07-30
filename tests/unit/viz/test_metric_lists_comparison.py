import pytest
from dash import Dash

from rubicon_ml.viz import MetricListsComparison


@pytest.mark.parametrize("column_names", [["var_0", "var_1", "var_2", "var_3", "var_4"], None])
def test_metric_lists_comparison(viz_experiments, column_names):
    metric_comparison = MetricListsComparison(
        column_names=column_names,
        experiments=viz_experiments,
        selected_metric="test metric 2",
    )

    expected_experiment_ids = [e.id for e in viz_experiments]

    for experiment in metric_comparison.experiments:
        assert experiment.id in expected_experiment_ids

        expected_experiment_ids.remove(experiment.id)

    assert len(expected_experiment_ids) == 0
    if column_names is None:
        assert metric_comparison.column_names == []
    else:
        assert metric_comparison.column_names == ["var_0", "var_1", "var_2", "var_3", "var_4"]
    assert metric_comparison.selected_metric == "test metric 2"


def test_metric_lists_comparison_load_data(viz_experiments):
    metric_comparison = MetricListsComparison(experiments=viz_experiments)
    metric_comparison.load_experiment_data()

    expected_experiment_ids = [e.id for e in viz_experiments]
    expected_metric_names = ["test metric 2", "test metric 3"]

    for experiment_id, record in metric_comparison.experiment_records.items():
        assert experiment_id in expected_experiment_ids

        for metric_name in expected_metric_names:
            assert metric_name in record

    assert metric_comparison.selected_metric == expected_metric_names[0]


def test_metric_lists_comparison_load_data_throws_error(viz_experiments):
    metric_comparison = MetricListsComparison(
        experiments=viz_experiments,
        selected_metric="nonexistant metric",
    )

    with pytest.raises(ValueError) as e:
        metric_comparison.load_experiment_data()

    assert "no metric named `selected_metric` 'nonexistant metric'" in str(e.value)


def test_metric_list_comparison_layout(viz_experiments):
    metric_comparison = MetricListsComparison(experiments=viz_experiments)
    metric_comparison.load_experiment_data()
    layout = metric_comparison.layout

    assert len(layout.children) == 2
    assert layout.children[-1].children.id == "metric-heatmap-container"
    assert layout.children[-1].children.children.id == "metric-heatmap"


@pytest.mark.parametrize("is_linked,expected", [(False, 1), (True, 2)])
def test_metric_correlation_plot_register_callbacks(viz_experiments, is_linked, expected):
    metric_comparison = MetricListsComparison(experiments=viz_experiments)
    metric_comparison.app = Dash(__name__, title="test callbacks")
    metric_comparison.register_callbacks(link_experiment_table=is_linked)

    callback_values = list(metric_comparison.app.callback_map.values())

    assert len(callback_values) == 1

    registered_callback_name = callback_values[0]["callback"].__name__
    registered_callback_len_input = len(callback_values[0]["inputs"])

    assert registered_callback_name == "update_metric_heatmap"
    assert registered_callback_len_input == expected
