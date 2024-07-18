import os

from rubicon_ml.intake_rubicon.viz import ExperimentsTableDataSource, MetricCorrelationPlotDataSource

root = os.path.dirname(__file__)


def test_experiments_table_source():
    catalog_data_sample = {
        "is_selectable": True,
        "metric_names": None,
        "metric_query_tags": None,
        "metric_query_type": None,
        "parameter_names": None,
        "parameter_query_tags": None,
        "parameter_query_type": None,
    }

    source = ExperimentsTableDataSource(catalog_data_sample)
    assert source is not None

    source.discover()

    visualization = source.read()

    assert visualization is not None
    assert visualization.is_selectable == catalog_data_sample["is_selectable"]
    assert visualization.metric_names == catalog_data_sample["metric_names"]
    assert visualization.metric_query_tags == catalog_data_sample["metric_query_tags"]
    assert visualization.metric_query_type == catalog_data_sample["metric_query_type"]
    assert visualization.parameter_names == catalog_data_sample["parameter_names"]
    assert visualization.parameter_query_tags == catalog_data_sample["parameter_query_tags"]
    assert visualization.parameter_query_type == catalog_data_sample["parameter_query_type"]

    source.close()

def test_metric_correlation_plot_source():
    catalog_data_sample = {
        "metric_names": None,
        "parameter_names": None,
        "selected_metric": None,
    }

    source = MetricCorrelationPlotDataSource(catalog_data_sample)
    assert source is not None

    source.discover()

    visualization = source.read()

    assert visualization is not None
    assert visualization.metric_names == catalog_data_sample["metric_names"]
    assert visualization.parameter_names == catalog_data_sample["parameter_names"]
    assert visualization.selected_metric == catalog_data_sample["selected_metric"]

    source.close()

