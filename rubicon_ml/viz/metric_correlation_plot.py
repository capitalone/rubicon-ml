import json

import numpy as np
import plotly.graph_objects as go
from dash import callback_context, dcc, html
from dash.dependencies import ALL, Input, Output

from rubicon_ml.viz.base import VizBase
from rubicon_ml.viz.common import dropdown_header
from rubicon_ml.viz.common.colors import get_rubicon_colorscale, light_blue, transparent


class MetricCorrelationPlot(VizBase):
    """Visualize the correlation between the parameters and metrics logged
    to the experiments `experiments` using a parallel coordinates plot.

    More info on parallel coordinates plots can be found here:
    https://plotly.com/python/parallel-coordinates-plot/

    Parameters
    ----------
    experiments : list of rubicon_ml.client.experiment.Experiment, optional
        The experiments to visualize. Defaults to None. Can be set as
        attribute after instantiation.
    metric_names : list of str
        The names of the metrics to load. Defaults to None, which loads all
        metrics logged to the experiments `experiments`.
    parameter_names : list of str
        The names of the parameters to load. Defaults to None, which loads all
        parameters logged to the experiments `experiments`.
    selected_metric : str
        The name of the metric to display at launch. Defaults to None, which
        selects the metric loaded first.
    """

    def __init__(
        self,
        experiments=None,
        metric_names=None,
        parameter_names=None,
        selected_metric=None,
    ):
        super().__init__(dash_title="plot metric correlation")

        self.experiments = experiments
        self.metric_names = metric_names
        self.parameter_names = parameter_names
        self.selected_metric = selected_metric

    def _get_dimension(self, label, values):
        """Transforms the input data for use with Plotly's parallel
        coordinates plot.
        """
        if len(values) > 0 and any([isinstance(v, str) or isinstance(v, bool) for v in values]):
            values = [str(v) for v in values]
            unique_values, values = np.unique(values, return_inverse=True)

            dimension = {
                "label": label,
                "ticktext": unique_values,
                "tickvals": list(range(0, len(unique_values))),
                "values": values,
            }
        else:
            dimension = {
                "label": label,
                "values": values,
            }

        return dimension

    @property
    def layout(self):
        """Defines the layout for the metric correlation plot."""
        return html.Div(
            [
                dropdown_header(
                    self.visible_metric_names,
                    self.selected_metric,
                    "comparing metric ",
                    f" over {len(self.experiments)} experiments",
                    "metric-correlation",
                ),
                dcc.Loading(
                    html.Div(
                        dcc.Graph(
                            id="metric-correlation-plot",
                        ),
                        id="metric-correlation-plot-container",
                    ),
                    color=light_blue,
                ),
            ],
            id="metric-correlation-plot-layout-container",
        )

    def load_experiment_data(self):
        """Load the experiment data required for the experiments table.

        Extracts parameter and metric metadata from each experiment in
        `self.experiments`. List metrics are ignored.
        """
        self.experiment_records = {}
        self.visible_metric_names = set()
        self.visible_parameter_names = set()

        for experiment in self.experiments:
            experiment_record = {"metrics": {}, "parameters": {}}

            for metric in experiment.metrics():
                if (
                    self.metric_names is None or metric.name in self.metric_names
                ) and not isinstance(metric.value, list):
                    experiment_record["metrics"][metric.name] = metric.value

                    self.visible_metric_names.add(metric.name)

                    if self.selected_metric is None:
                        self.selected_metric = metric.name

            for parameter in experiment.parameters():
                if self.parameter_names is None or parameter.name in self.parameter_names:
                    experiment_record["parameters"][parameter.name] = parameter.value

                    self.visible_parameter_names.add(parameter.name)

            self.experiment_records[experiment.id] = experiment_record

        if self.selected_metric not in self.visible_metric_names:
            raise ValueError(
                f"no metric named `selected_metric` '{self.selected_metric}'"
                " logged to any experiment in `experiments`."
            )

        self.visible_parameter_names = list(self.visible_parameter_names)
        self.visible_metric_names = list(self.visible_metric_names)
        self.visible_metric_names.sort()

    def register_callbacks(self, link_experiment_table=False):
        outputs = [
            Output("metric-correlation-plot", "figure"),
            Output("metric-correlation-dropdown", "label"),
            Output("metric-correlation-header-right-text", "children"),
        ]
        inputs = [
            Input({"type": "metric-correlation-dropdown-button", "index": ALL}, "n_clicks"),
        ]
        states = []

        if link_experiment_table:
            inputs.append(
                Input("experiment-table", "derived_virtual_selected_row_ids"),
            )

        @self.app.callback(outputs, inputs, states)
        def update_metric_plot(*args):
            """Render the correlation plot based on the currently selected metric.

            Returns the Plotly `Parcoords` figure generated by the values of the
            experiments' selected metric, the name of the currently selected
            metric, and the header text with the metric's name.
            """
            if link_experiment_table:
                selected_row_ids = args[-1]
                selected_row_ids = selected_row_ids if selected_row_ids else []

                experiment_records = [
                    self.experiment_records[row_id] for row_id in selected_row_ids
                ]
            else:
                experiment_records = self.experiment_records.values()

            property_id = callback_context.triggered[0].get("prop_id")
            property_value = property_id[: property_id.index(".")]

            if not property_value or property_value == "experiment-table":
                selected_metric = self.selected_metric
            else:
                selected_metric = json.loads(property_value).get("index")

                self.selected_metric = selected_metric

            header_right_text = (
                f"over {len(experiment_records)} experiment"
                f"{'s' if len(experiment_records) != 1 else ''}"
            )

            parameter_values = {}

            for parameter_name in self.visible_parameter_names:
                parameter_values[parameter_name] = [
                    record["parameters"].get(parameter_name) for record in experiment_records
                ]

            metric_values = [
                record["metrics"].get(selected_metric) for record in experiment_records
            ]

            plot_dimensions = []

            for parameter_name, parameter_value in parameter_values.items():
                if any([p is not None for p in parameter_value]):
                    plot_dimensions.append(self._get_dimension(parameter_name, parameter_value))

            plot_dimensions.append(self._get_dimension(selected_metric, metric_values))

            metric_correlation_plot = go.Figure(
                go.Parcoords(
                    line={
                        "color": [m for m in metric_values if m is not None],
                        "colorscale": get_rubicon_colorscale(len(experiment_records)),
                        "showscale": True,
                    },
                    dimensions=plot_dimensions,
                ),
            )
            metric_correlation_plot.update_layout(paper_bgcolor=transparent)

            return metric_correlation_plot, selected_metric, header_right_text
