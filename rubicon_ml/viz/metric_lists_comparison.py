import copy
import json

import numpy as np
import plotly.figure_factory as ff
from dash import callback_context, dcc, html
from dash.dependencies import ALL, Input, Output, State

from rubicon_ml.viz.base import VizBase
from rubicon_ml.viz.colors import light_blue, plot_background_blue
from rubicon_ml.viz.common import dropdown_header


class CompareMetricLists(VizBase):
    def __init__(self, experiments, selected_metric, column_names=None, dash_kwargs={}):
        super().__init__(dash_kwargs=dash_kwargs, dash_title="rubicon-ml: compare metric lists")

        self.column_names = column_names
        self.experiments = experiments
        self.selected_metric = selected_metric

        self.experiment_ids = {}
        self.list_metrics = {}

        for experiment in self.experiments:
            for metric in experiment.metrics():
                if isinstance(metric.value, list):
                    list_metrics_for_name = self.list_metrics.get(metric.name, [])
                    list_metrics_for_name.append(metric.value)
                    self.list_metrics[metric.name] = list_metrics_for_name

                    experiment_ids_for_name = self.experiment_ids.get(metric.name, [])
                    experiment_ids_for_name.append(experiment.id[:7])
                    self.experiment_ids[metric.name] = experiment_ids_for_name

        if self.selected_metric not in self.list_metrics:
            raise ValueError(
                f"no metric named `selected_metric` '{self.selected_metric}'"
                " logged to any experiment in `experiments`."
            )

        self.app.layout = self._build_frame(self._build_layout())

        _register_callbacks(self.app)

    def _build_layout(self):
        return html.Div(
            [
                self._to_store(ignore_attributes=["app", "experiments"]),
                dropdown_header(
                    self.list_metrics.keys(),
                    self.selected_metric,
                    "comparing metric ",
                    f" over {len(self.experiment_ids)} experiments",
                    "metric",
                ),
                dcc.Loading(
                    html.Div(
                        dcc.Graph(
                            id="metric-heatmap",
                        ),
                        id="metric-heatmap-container",
                    ),
                    color=light_blue,
                ),
            ],
            id="metric-heatmap-layout-container",
        )


def _register_callbacks(app):
    @app.callback(
        [
            Output("metric-heatmap", "figure"),
            Output("metric-heatmap", "style"),
            Output("metric-header-right-text", "children"),
            Output("metric-dropdown", "label"),
        ],
        Input({"type": "metric-dropdown-button", "index": ALL}, "n_clicks"),
        [
            State("metric-heatmap", "style"),
            State("memory-store", "data"),
        ],
    )
    def update_selected_metric(n_clicks, current_style, data):
        property_id = callback_context.triggered[0].get("prop_id")
        property_value = property_id[: property_id.index(".")]

        if not property_value:
            selected_metric = data["selected_metric"]
        else:
            selected_metric = json.loads(property_value).get("index")

            data["selected_metric"] = selected_metric

        column_names = data["column_names"]
        experiment_ids = data["experiment_ids"].get(selected_metric)
        heatmap_data = data["list_metrics"].get(selected_metric)

        header_right_text = f" over {len(experiment_ids)} experiments"

        data_array = np.array(heatmap_data)
        numerator = data_array - data_array.min(axis=0)
        denominator = data_array.max(axis=0) - data_array.min(axis=0)
        denominator[denominator == 0] = 1
        scaled_heatmap_data = numerator / denominator

        annotations = copy.deepcopy(heatmap_data)
        for i, row in enumerate(annotations):
            for j, label in enumerate(row):
                if isinstance(label, float):
                    annotations[i][j] = round(label, 6)

        heatmap = ff.create_annotated_heatmap(
            scaled_heatmap_data,
            annotation_text=annotations,
            colorscale="blues",
            hoverinfo="text",
            text=heatmap_data,
            x=column_names if len(column_names) == len(heatmap_data[0]) else None,
            y=experiment_ids,
        )
        heatmap.update_layout(
            margin_b=30, margin_t=30, modebar_orientation="v", plot_bgcolor=plot_background_blue
        )
        heatmap.update_xaxes(gridcolor="white")
        heatmap.update_yaxes(gridcolor="white")

        heatmap_cell_rem = 6
        heatmap_height = 12 + (len(heatmap_data) * (heatmap_cell_rem / 2))
        heatmap_width = (
            12 + (len(heatmap_data[0]) * heatmap_cell_rem) if len(heatmap_data[0]) > 8 else 72
        )
        heatmap_style = {"height": f"{heatmap_height}rem", "width": f"{heatmap_width}rem"}

        return heatmap, heatmap_style, header_right_text, selected_metric


def compare_metric_lists(
    experiments,
    selected_metric,
    column_names=None,
    dash_kwargs={},
    i_frame_kwargs={},
    run_server_kwargs={},
):
    if "height" not in i_frame_kwargs:
        i_frame_kwargs["height"] = "600px"

    return CompareMetricLists(
        experiments,
        selected_metric,
        column_names=column_names,
        dash_kwargs=dash_kwargs,
    ).run_server_inline(
        i_frame_kwargs=i_frame_kwargs,
        **run_server_kwargs,
    )
