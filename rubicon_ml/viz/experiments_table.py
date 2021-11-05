import dash_bootstrap_components as dbc
from dash import dash_table, html
from dash.dependencies import Input, Output, State

from rubicon_ml.viz.base import VizBase
from rubicon_ml.viz.colors import plot_background_blue


class ExperimentsTable(VizBase):
    def __init__(self, experiments, dash_kwargs={}):
        super().__init__(dash_kwargs=dash_kwargs, dash_title="rubicon-ml: experiment table")

        self.experiments = experiments

        self.experiment_records = []
        self.metric_names = set()
        self.parameter_names = set()
        self.all_columns = ["id", "name", "created_at", "model_name", "commit_hash", "tags"]
        self.hidden_columns = []

        show_columns = {"id", "created_at"}

        for experiment in self.experiments:
            if experiment.commit_hash is not None:
                show_columns.add("commit_hash")

            if experiment.model_name is not None:
                show_columns.add("model_name")

            if experiment.name is not None:
                show_columns.add("name")

            if len(experiment.tags) > 0:
                show_columns.append("tags")

            experiment_record = {
                "id": experiment.id,
                "name": experiment.name,
                "created_at": experiment.created_at,
                "model_name": experiment.model_name,
                "commit_hash": experiment.commit_hash[:7],
                "tags": experiment.tags,
            }

            for parameter in experiment.parameters():
                experiment_record[parameter.name] = parameter.value

                self.parameter_names.add(parameter.name)

            for metric in experiment.metrics():
                experiment_record[metric.name] = metric.value

                self.metric_names.add(metric.name)

            self.experiment_records.append(experiment_record)

        self.metric_names = list(self.metric_names)
        self.parameter_names = list(self.parameter_names)

        self.all_columns.extend(self.parameter_names + self.metric_names)
        self.hidden_columns = [
            column
            for column in self.all_columns
            if column not in list(show_columns) + self.metric_names + self.parameter_names
        ]

        self.app.layout = self._build_frame(self._build_layout())

        _register_callbacks(self.app)

    def _build_layout(self):
        return html.Div(
            [
                self._to_store(ignore_attributes=["app", "experiments"]),
                html.Div(
                    html.H5(
                        f"showing {len(self.experiments)} experiments", className="header-text"
                    ),
                    className="header-row",
                ),
                html.Div(
                    className="button-group",
                    children=[
                        dbc.Button(
                            "select all",
                            id="select-all-button",
                        ),
                        dbc.Button(
                            "clear all",
                            id="clear-all-button",
                        ),
                        dbc.Button(
                            "show all columns",
                            id="show-all-columns-button",
                        ),
                        dbc.Button(
                            "hide all columns",
                            id="hide-all-columns-button",
                        ),
                    ],
                ),
                dash_table.DataTable(
                    id="experiment-table",
                    data=self.experiment_records,
                    columns=[
                        {"name": column, "id": column, "selectable": True, "hideable": True}
                        for column in self.all_columns
                    ],
                    hidden_columns=self.hidden_columns,
                    filter_action="native",
                    fixed_columns={"headers": True, "data": 1},
                    page_size=10,
                    row_selectable="multi",
                    sort_action="native",
                    sort_mode="multi",
                    selected_rows=[],
                    style_data_conditional=[
                        {"if": {"row_index": "odd"}, "backgroundColor": plot_background_blue}
                    ],
                    style_header={"fontWeight": 700},
                    style_cell={"overflow": "hidden", "textOverflow": "ellipsis"},
                    style_table={"minWidth": "100%"},
                ),
            ],
            style={"width": "100%"},
        )


def _register_callbacks(app):
    @app.callback(
        Output("experiment-table", "selected_rows"),
        [
            Input("select-all-button", "n_clicks_timestamp"),
            Input("clear-all-button", "n_clicks_timestamp"),
        ],
        State("experiment-table", "derived_virtual_indices"),
    )
    def _update_selected_experiment_table_rows(
        last_select_click, last_clear_click, experiment_table_indices
    ):
        last_select_click = last_select_click if last_select_click else 0
        last_clear_click = last_clear_click if last_clear_click else 0

        if int(last_select_click) > int(last_clear_click):
            return experiment_table_indices

        return []

    @app.callback(
        Output("experiment-table", "hidden_columns"),
        [
            Input("show-all-columns-button", "n_clicks_timestamp"),
            Input("hide-all-columns-button", "n_clicks_timestamp"),
        ],
        State("memory-store", "data"),
    )
    def _update_selected_experiment_table_cols(
        last_show_click,
        last_hide_click,
        data,
    ):
        last_show_click = last_show_click if last_show_click else 0
        last_hide_click = last_hide_click if last_hide_click else 0

        all_columns = data["all_columns"]
        hidden_columns = data["hidden_columns"]

        if int(last_show_click) >= int(last_hide_click):
            return hidden_columns

        return [column for column in all_columns if column != "id"]
