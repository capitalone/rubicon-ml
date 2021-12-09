import json

import numpy as np
import plotly.graph_objects as go
from dash import callback_context, dcc, html
from dash.dependencies import ALL, Input, Output, State

from rubicon_ml.viz.base import VizBase
from rubicon_ml.viz.colors import get_rubicon_colorscale, light_blue, transparent
from rubicon_ml.viz.common import dropdown_header


class MetricCorrelationPlot(VizBase):
    def __init__(
        self,
        experiments,
        selected_metric,
        metric_names=None,
        parameter_names=None,
        dash_kwargs={},
    ):
        super().__init__(dash_kwargs=dash_kwargs, dash_title="rubicon-ml: plot metric correlation")

        self.experiments = experiments
        self.selected_metric = selected_metric

        self.experiment_records = {}
        self.metric_names = set()
        self.parameter_names = set()

        for experiment in self.experiments:
            experiment_record = {"metrics": {}, "parameters": {}}

            for metric in experiment.metrics():
                if metric_names is None or metric.name in metric_names:
                    experiment_record["metrics"][metric.name] = metric.value

                    self.metric_names.add(metric.name)

            for parameter in experiment.parameters():
                if parameter_names is None or parameter.name in parameter_names:
                    experiment_record["parameters"][parameter.name] = parameter.value

                    self.parameter_names.add(parameter.name)

            self.experiment_records[experiment.id] = experiment_record

        if self.selected_metric not in self.metric_names:
            raise ValueError(
                f"no metric named `selected_metric` '{self.selected_metric}'"
                " logged to any experiment in `experiments`."
            )

        self.parameter_names = list(self.parameter_names)
        self.metric_names = list(self.metric_names)
        self.metric_names.sort()

        self.app.layout = self._build_frame(self._build_layout())

        _register_callbacks(self.app)

    def _build_layout(self):
        return html.Div(
            [
                self._to_store(ignore_attributes=["app", "experiments"]),
                dropdown_header(
                    self.metric_names,
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


def _register_callbacks(app):
    def _get_dimension(label, values):
        if isinstance(values[0], str):
            unique_values, values = np.unique(values, return_inverse=True)

            dimension = {
                "label": label,
                "ticktext": unique_values,
                "tickvals": list(range(0, len(unique_values))),
                "values": values,
            }
        elif isinstance(values[0], bool):
            values = [int(value) for value in values]

            dimension = {
                "label": label,
                "ticktext": ["False", "True"],
                "tickvals": [0, 1],
                "values": values,
            }
        else:
            dimension = {
                "label": label,
                "values": values,
            }

        return dimension

    @app.callback(
        [
            Output("metric-correlation-plot", "figure"),
            Output("metric-correlation-dropdown", "label"),
        ],
        Input({"type": "metric-correlation-dropdown-button", "index": ALL}, "n_clicks"),
        State("memory-store", "data"),
    )
    def update_selected_metric(n_clicks, data):
        property_id = callback_context.triggered[0].get("prop_id")
        property_value = property_id[: property_id.index(".")]

        if not property_value:
            selected_metric = data["selected_metric"]
        else:
            selected_metric = json.loads(property_value).get("index")

            data["selected_metric"] = selected_metric

        experiment_records = data["experiment_records"]
        parameter_names = data["parameter_names"]
        selected_metric = data["selected_metric"]

        parameter_values = {}

        for parameter_name in parameter_names:
            parameter_values[parameter_name] = [
                record["parameters"][parameter_name] for record in experiment_records.values()
            ]

        metric_values = [
            record["metrics"][selected_metric] for record in experiment_records.values()
        ]

        plot_dimensions = []

        for parameter_name, parameter_values in parameter_values.items():
            plot_dimensions.append(_get_dimension(parameter_name, parameter_values))

        plot_dimensions.append(_get_dimension(selected_metric, metric_values))

        metric_correlation_plot = go.Figure(
            go.Parcoords(
                line={
                    "color": metric_values,
                    "colorscale": get_rubicon_colorscale(len(experiment_records)),
                    "showscale": True,
                },
                dimensions=plot_dimensions,
            ),
        )
        metric_correlation_plot.update_layout(paper_bgcolor=transparent)

        return metric_correlation_plot, selected_metric


def plot_metric_correlation(
    experiments,
    selected_metric,
    metric_names=None,
    parameter_names=None,
    dash_kwargs={},
    i_frame_kwargs={},
    run_server_kwargs={},
):
    if "height" not in i_frame_kwargs:
        i_frame_kwargs["height"] = "550px"

    return MetricCorrelationPlot(
        experiments,
        selected_metric,
        metric_names=metric_names,
        parameter_names=parameter_names,
        dash_kwargs=dash_kwargs,
    ).run_server_inline(
        i_frame_kwargs=i_frame_kwargs,
        **run_server_kwargs,
    )
