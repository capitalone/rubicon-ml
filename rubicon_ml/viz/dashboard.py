import dash_bootstrap_components as dbc
from dash import html

from rubicon_ml.viz.base import VizBase

COL_WIDTH_LOOKUP = {1: 12, 2: 6, 3: 4, 4: 3}


class Dashboard(VizBase):
    """Compose visualizations into a dashboard to view multiple widgets
    at once.

    Parameters
    ----------
    experiments : list of rubicon_ml.client.experiment.Experiment
        The experiments to visualize.
    widgets : list of lists of superclasses of rubicon_ml.viz.base.VizBase
        The widgets to compose in this dashboard. The widgets should
        be instantiated without experiments prior to passing as an
        argument to `Dashboard`.
    link_experiment_table : bool, optional
        True to enable the callbacks that allow instances of
        `ExperimentsTable` to update the experiment inputs of the other
        widgets in this dashboard. False otherwise. Defaults to True.
    """

    def __init__(self, experiments, widgets, link_experiment_table=True):
        super().__init__(dash_title="dashboard")

        self.experiments = experiments
        self.link_experiment_table = link_experiment_table
        self.widgets = widgets

    @property
    def layout(self):
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
        for row in self.widgets:
            for widget in row:
                widget.experiments = self.experiments
                widget.load_experiment_data()

    def register_callbacks(self):
        for row in self.widgets:
            for widget in row:
                widget.app = self.app
                widget.register_callbacks(self.link_experiment_table)
