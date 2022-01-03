import copy
import json

import numpy as np
import plotly.figure_factory as ff
from dash import callback_context, dcc, html
from dash.dependencies import ALL, Input, Output

from rubicon_ml.viz.base import VizBase
from rubicon_ml.viz.colors import light_blue, plot_background_blue
from rubicon_ml.viz.common import dropdown_header


class MetricListsComparison(VizBase):
    def __init__(
        self,
        column_names=None,
        experiments=None,
        selected_metric=None,
    ):
        super().__init__(dash_title="compare metric lists")

        self.column_names = column_names
        self.experiments = experiments
        self.selected_metric = selected_metric

    @property
    def layout(self):
        return html.Div(
            [
                dropdown_header(
                    list(self.metric_names),
                    self.selected_metric,
                    "comparing metric ",
                    f" over {len(self.experiments)} experiments",
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

    def load_experiment_data(self):
        self.experiment_records = {}
        self.metric_names = set()

        for experiment in self.experiments:
            for metric in experiment.metrics():
                if isinstance(metric.value, list):
                    self.metric_names.add(metric.name)

                    experiment_record = self.experiment_records.get(experiment.id, {})
                    experiment_record[metric.name] = metric.value
                    self.experiment_records[experiment.id] = experiment_record

                    if self.selected_metric is None:
                        self.selected_metric = metric.name

        if self.selected_metric not in self.metric_names:
            raise ValueError(
                f"no metric named `selected_metric` '{self.selected_metric}'"
                " logged to any experiment in `experiments`."
            )

    def register_callbacks(self, link_experiment_table=False):
        outputs = [
            Output("metric-heatmap", "figure"),
            Output("metric-heatmap", "style"),
            Output("metric-header-right-text", "children"),
            Output("metric-dropdown", "label"),
        ]
        inputs = [Input({"type": "metric-dropdown-button", "index": ALL}, "n_clicks")]
        states = []

        if link_experiment_table:
            inputs.append(
                Input("experiment-table", "derived_virtual_selected_row_ids"),
            )

        @self.app.callback(outputs, inputs, states)
        def update_selected_metric(*args):
            if link_experiment_table:
                selected_row_ids = args[-1]
                selected_row_ids = selected_row_ids if selected_row_ids else []
            else:
                selected_row_ids = self.experiment_records.keys()

            property_id = callback_context.triggered[0].get("prop_id")
            property_value = property_id[: property_id.index(".")]

            if not property_value or property_value == "experiment-table":
                selected_metric = self.selected_metric
            else:
                selected_metric = json.loads(property_value).get("index")

                self.selected_metric = selected_metric

            heatmap_data = []
            experiment_ids = []

            for experiment_id, experiment_record in self.experiment_records.items():
                if experiment_id in selected_row_ids:
                    metric_value = experiment_record.get(selected_metric)

                    if metric_value is not None:
                        heatmap_data.append(metric_value)
                        experiment_ids.append(experiment_id[:7])

            header_right_text = (
                f"over {len(experiment_ids)} experiment"
                f"{'s' if len(experiment_ids) != 1 else ''}"
            )

            if len(heatmap_data) == 0:
                return [], {"display": "none"}, header_right_text, selected_metric

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
                x=self.column_names if len(self.column_names) == len(heatmap_data[0]) else None,
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
