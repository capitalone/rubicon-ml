import dash_bootstrap_components as dbc
from dash import dash_table, dcc, html
from dash.dependencies import ALL, Input, Output, State

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
        bulk_select_buttons = [
            dbc.Button(
                "select all",
                id="select-all-button",
            ),
            dbc.Button(
                "clear all",
                id="clear-all-button",
            ),
        ]

        experiment_table = dash_table.DataTable(
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
        )

        toggle_columns_dropdown = dbc.DropdownMenu(
            [
                dbc.DropdownMenuItem(
                    "show all",
                    id="show-all-dropdown-button",
                ),
                dbc.DropdownMenuItem(
                    "hide all",
                    id="hide-all-dropdown-button",
                ),
                dbc.DropdownMenuItem(divider=True),
                *[
                    dbc.DropdownMenuItem(
                        dcc.Checklist(
                            inputClassName="column-dropdown-checkbox",
                            labelClassName="column-dropdown-label",
                            options=[{"label": column, "value": column}],
                            value=[column] if column not in self.hidden_columns else [],
                            id={
                                "type": "column-dropdown-checkbox",
                                "index": column,
                            },
                        ),
                        id={"type": "column-dropdown-button", "index": column},
                    )
                    for column in self.all_columns
                ],
            ],
            id="column-selection-dropdown",
            label="toggle columns",
        )

        return html.Div(
            [
                self._to_store(ignore_attributes=["app", "experiments"]),
                html.Div(
                    html.H5(
                        f"showing {len(self.experiments)} experiments", className="header-text"
                    ),
                    className="header-row",
                ),
                dbc.Row(
                    [
                        *[dbc.Col(button) for button in bulk_select_buttons],
                        dbc.Col(toggle_columns_dropdown),
                    ],
                    className="button-group",
                ),
                experiment_table,
            ],
            style={"width": "100%"},
        )


def _register_callbacks(app):
    @app.callback(
        Output({"type": "column-dropdown-checkbox", "index": ALL}, "value"),
        [
            Input("show-all-dropdown-button", "n_clicks_timestamp"),
            Input("hide-all-dropdown-button", "n_clicks_timestamp"),
        ],
        State("memory-store", "data"),
        prevent_initial_call=True,
    )
    def _update_selected_column_checkboxes(last_show_click, last_hide_click, data):
        """Bulk updates for the selections in the "toggle columns" dropdown.

        Returns all if triggered by the "show all" button and returns
        only "id" if triggered by the "hide all" button. The initial
        call is prevented as the default state is neither all nor none.
        """
        last_show_click = last_show_click if last_show_click else 0
        last_hide_click = last_hide_click if last_hide_click else 0

        all_columns = data["all_columns"]

        if last_hide_click > last_show_click:
            hidden_values = [[]] * (len(all_columns) - 1)

            return [["id"], *hidden_values]

        return [[column] for column in all_columns]

    @app.callback(
        Output("experiment-table", "hidden_columns"),
        Input({"type": "column-dropdown-checkbox", "index": ALL}, "value"),
        State("memory-store", "data"),
    )
    def _update_hidden_experiment_table_cols(selected_columns, data):
        """Hide and show the columns in the experiment table.

        Returns the columns that should be hidden based on whether or
        not the column's corresponding value is checked in the "toggle
        columns" dropdown.
        """
        all_columns = data["all_columns"]
        selected_columns = [sc[0] for sc in selected_columns if len(sc) > 0]

        return [column for column in all_columns if column not in selected_columns]

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
        """Bulk selection for the rows of the experiment table.

        Returns all if triggered by the "select all" button and returns
        none if triggered by the "clear all" button.
        """
        last_select_click = last_select_click if last_select_click else 0
        last_clear_click = last_clear_click if last_clear_click else 0

        if last_select_click > last_clear_click:
            return experiment_table_indices

        return []
