import dash_bootstrap_components as dbc
from dash import html

from rubicon_ml.viz.base import VizBase

COL_WIDTH_LOOKUP = {1: 12, 2: 6, 3: 4, 4: 3}


class Dashboard(VizBase):
    """Compose visualizations into a dashboard to view multiple widgets at once.

    Parameters
    ----------
    experiments : list of rubicon_ml.client.experiment.Experiment
        The experiments to visualize.
    widgets : list of lists of superclasses of rubicon_ml.viz.base.VizBase, optional
        The widgets to compose in this dashboard. The widgets should be instantiated
        without experiments prior to passing as an argument to `Dashboard`. Defaults
        to a stacked layout of an ExperimentsTable and a MetricCorrelationPlot.
    link_experiment_table : bool, optional
        True to enable the callbacks that allow instances of `ExperimentsTable` to
        update the experiment inputs of the other widgets in this dashboard. False
        otherwise. Defaults to True.
    """

    def __init__(self, experiments, widgets=None, link_experiment_table=True):
        super().__init__(dash_title="dashboard")

        self.experiments = experiments
        self.link_experiment_table = link_experiment_table

        if widgets is None:
            from rubicon_ml.viz import ExperimentsTable, MetricCorrelationPlot

            self.widgets = [
                [ExperimentsTable(is_selectable=True)],
                [MetricCorrelationPlot()],
            ]
        else:
            self.widgets = widgets

    @property
    def layout(self):
        """Defines the layout for the dashboard.

        Compiles the figures in `self.widgets` based on their positions
        in the given input list.
        """
        dashboard_rows = []
        for row in self.widgets:
            width = COL_WIDTH_LOOKUP[len(row)]

            row_widgets = []
            for widget in row:
                row_widgets.append(dbc.Col(widget.layout, width=width))

            dashboard_rows.append(dbc.Row(row_widgets))

        dashboard_container = html.Div(dashboard_rows)

        return dashboard_container

    def load_experiment_data(self):
        """Load the experiment data required for the dashboard.

        Loads the experiment data for each widget in `self.widgets`.
        """
        for row in self.widgets:
            for widget in row:
                widget.experiments = self.experiments
                widget.load_experiment_data()

    def register_callbacks(self):
        for row in self.widgets:
            for widget in row:
                widget.app = self.app
                widget.register_callbacks(self.link_experiment_table)
