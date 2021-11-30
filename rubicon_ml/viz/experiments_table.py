import dash_bootstrap_components as dbc
from dash import dash_table, dcc, html
from dash.dependencies import ALL, Input, Output, State

from rubicon_ml.viz.base import VizBase
from rubicon_ml.viz.colors import light_blue, plot_background_blue


class ExperimentsTable(VizBase):
    def __init__(self, experiments, is_selectable=True, dash_kwargs={}):
        super().__init__(dash_kwargs=dash_kwargs, dash_title="rubicon-ml: experiment table")

        self.experiments = experiments
        self.is_selectable = is_selectable

        self.commit_hash = None
        self.experiment_records = []
        self.github_url = None
        self.metric_names = set()
        self.parameter_names = set()
        self.all_columns = ["id", "name", "created_at", "model_name", "commit_hash", "tags"]
        self.hidden_columns = []

        commit_hashes = set()
        show_columns = {"id", "created_at"}

        for experiment in self.experiments:
            if experiment.commit_hash is not None:
                commit_hashes.add(experiment.commit_hash)
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

        if len(commit_hashes) == 1:
            github_url_root = self.experiments[0].project.github_url[:-4]
            commit_hash = list(commit_hashes)[0]

            self.commit_hash = commit_hash[:7]
            self.github_url = f"{github_url_root}/tree/{commit_hash}"

        self.app.layout = self._build_frame(self._build_layout())

        _register_callbacks(self.app)

    def _build_layout(self):
        bulk_select_buttons = [
            dbc.Button(
                "select all experiments",
                color="primary",
                disabled=not self.is_selectable,
                id="select-all-button",
                outline=True,
                style={"display": "none"} if not self.is_selectable else {},
            ),
            dbc.Button(
                "clear all experiments",
                color="primary",
                disabled=not self.is_selectable,
                id="clear-all-button",
                outline=True,
                style={"display": "none"} if not self.is_selectable else {},
            ),
        ]

        experiment_table = dash_table.DataTable(
            columns=[
                {"name": column, "id": column, "selectable": self.is_selectable}
                for column in self.all_columns
            ],
            data=self.experiment_records,
            filter_action="native",
            fixed_columns={"headers": True, "data": 1},
            hidden_columns=self.hidden_columns,
            id="experiment-table",
            page_size=10,
            row_selectable="multi" if self.is_selectable else False,
            selected_rows=[],
            sort_action="native",
            sort_mode="multi",
            style_cell={"overflow": "hidden", "textOverflow": "ellipsis"},
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": plot_background_blue}
            ],
            style_header={"fontWeight": 700},
            style_table={"minWidth": "100%"},
        )

        header_text = (
            f"showing {len(self.experiments)} experiments "
            f"{'at commit ' if self.commit_hash is not None else ''}"
        )

        header = html.H5(
            [
                html.P(
                    header_text,
                    className="experiment-table-header-text",
                    style={"float": "left"} if self.commit_hash is not None else {},
                ),
                html.A(
                    html.P(
                        [
                            self.commit_hash,
                            html.I(className="bi bi-box-arrow-up-right external-link-icon"),
                        ]
                    ),
                    href=self.github_url,
                    style={"display": "none"} if self.commit_hash is None else {},
                    target="_blank",
                ),
            ],
            className="header-text",
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
            color="secondary",
            id="column-selection-dropdown",
            label="toggle columns",
        )

        return html.Div(
            [
                self._to_store(ignore_attributes=["app", "experiments"]),
                html.Div(
                    header,
                    className="header-row",
                    style={"margin-bottom": "-3.5rem"} if not self.is_selectable else {},
                ),
                dbc.Row(
                    [
                        *[dbc.Col(button, width="auto") for button in bulk_select_buttons],
                        dbc.Col(toggle_columns_dropdown),
                    ],
                    className="button-group",
                ),
                dcc.Loading(experiment_table, color=light_blue),
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


def view_experiments_table(experiments, dash_kwargs={}, i_frame_kwargs={}, run_server_kwargs={}):
    if "height" not in i_frame_kwargs:
        i_frame_kwargs["height"] = "640px"

    return ExperimentsTable(
        experiments, is_selectable=False, dash_kwargs=dash_kwargs
    ).run_server_inline(
        i_frame_kwargs=i_frame_kwargs,
        **run_server_kwargs,
    )
