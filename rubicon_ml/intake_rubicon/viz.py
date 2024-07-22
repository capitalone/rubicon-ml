from rubicon_ml import __version__
from rubicon_ml.intake_rubicon.base import VizDataSourceMixin


class ExperimentsTableDataSource(VizDataSourceMixin):
    """An Intake data source for reading `rubicon` Experiment Table visualizations."""

    version = __version__

    container = "python"
    name = "rubicon_ml_experiments_table"

    def __init__(self, metadata=None, **catalog_data):
        self._catalog_data = catalog_data or {}

        super().__init__(metadata=metadata)

    def _get_schema(self):
        """Creates an Experiments Table visualization and sets it as the visualization object attribute"""
        from rubicon_ml.viz import ExperimentsTable

        self._visualization_object = ExperimentsTable(**self._catalog_data)

        return super()._get_schema()


class MetricCorrelationPlotDataSource(VizDataSourceMixin):
    """An Intake data source for reading `rubicon` Metric Correlation Plot visualizations."""

    version = __version__

    container = "python"
    name = "rubicon_ml_metric_correlation_plot"

    def __init__(self, metadata=None, **catalog_data):
        self._catalog_data = catalog_data or {}

        super().__init__(metadata=metadata)

    def _get_schema(self):
        """Creates a Metric Correlation Plot visualization and sets it as the visualization object attribute"""
        from rubicon_ml.viz import MetricCorrelationPlot

        self._visualization_object = MetricCorrelationPlot(**self._catalog_data)

        return super()._get_schema()


class DataframePlotDataSource(VizDataSourceMixin):
    """An Intake data source for reading `rubicon` Dataframe Plot visualizations."""

    version = __version__

    container = "python"
    name = "rubicon_ml_dataframe_plot"

    def __init__(self, metadata=None, **catalog_data):
        self._catalog_data = catalog_data or {}

        super().__init__(metadata=metadata)

    def _get_schema(self):
        """Creates a Dataframe Plot visualization and sets it as the visualization object attribute"""
        from rubicon_ml.viz import DataframePlot

        self._visualization_object = DataframePlot(**self._catalog_data)

        return super()._get_schema()


class MetricListComparisonDataSource(VizDataSourceMixin):
    """An Intake data source for reading `rubicon` Metric List Comparison visualizations."""

    version = __version__

    container = "python"
    name = "rubicon_ml_metric_list"

    def __init__(self, metadata=None, **catalog_data):
        self._catalog_data = catalog_data or {}

        super().__init__(metadata=metadata)

    def _get_schema(self):
        """Creates a Metric List Comparison visualization and sets it as the visualization object attribute"""
        from rubicon_ml.viz import MetricListsComparison

        self._visualization_object = MetricListsComparison(**self._catalog_data)

        return super()._get_schema()
