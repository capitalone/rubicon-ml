import os

import dash_bootstrap_components as dbc
from dash import dash_table, dcc, html
from dash.dependencies import ALL, Input, Output, State

from rubicon_ml import publish
from rubicon_ml.viz.base import VizBase
from rubicon_ml.viz.common.colors import light_blue, plot_background_blue


class ExperimentsTable(VizBase):
    """Visualize the experiments `experiments` and their metadata, metrics,
    and parameters in a tabular format.

    Parameters
    ----------
    experiments : list of rubicon_ml.client.experiment.Experiment, optional
        The experiments to visualize. Defaults to None. Can be set as
        attribute after instantiation.
    is_selectable : bool, optional
        True to enable selection of the rows in the table, False otherwise.
        Defaults to True.
    """

    def __init__(self, experiments=None, is_selectable=True):
        super().__init__(dash_title="experiment table")

        self.experiments = experiments
        self.is_selectable = is_selectable

    @property
    def layout(self):
        """Defines the experiments table's layout."""
        bulk_select_buttons = [
            html.Div(
                dbc.Button(
                    "select all experiments",
                    color="primary",
                    disabled=not self.is_selectable,
                    id="select-all-button",
                    outline=True,
                ),
                className="bulk-select-button-container",
            ),
            html.Div(
                dbc.Button(
                    "clear all experiments",
                    color="primary",
                    disabled=not self.is_selectable,
                    id="clear-all-button",
                    outline=True,
                ),
                className="bulk-select-button-container",
            ),
            html.Div(
                dbc.Button(
                    "publish selected",
                    color="primary",
                    disabled=not self.is_selectable,
                    id="publish-selected-button",
                    outline=True,
                ),
                className="bulk-select-button-container",
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

        publish_modal = dbc.Modal(
            [
                dbc.ModalHeader(
                    dbc.ModalTitle("publish selected experiments"),
                    close_button=True,
                ),
                dbc.ModalBody(
                    [
                        dbc.Label("enter catalog YAML output path:"),
                        dbc.Input(
                            id="publish-path-input",
                            type="text",
                            value=os.path.join(os.getcwd(), "rubicon-ml-catalog.yml"),
                        ),
                    ],
                ),
                dbc.ModalFooter(dbc.Button("publish", id="publish-button")),
            ],
            id="publish-modal",
            centered=True,
            is_open=False,
            size="lg",
        )

        if self.is_selectable:
            header_row = [
                html.Div(
                    header,
                    className="header-row",
                ),
                dbc.Row(
                    [
                        *[dbc.Col(button, width="auto") for button in bulk_select_buttons],
                        dbc.Col(toggle_columns_dropdown),
                    ],
                    className="button-group",
                ),
            ]
        else:
            header_row = [
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                header,
                                className="header-row",
                            ),
                            width=8,
                        ),
                        dbc.Col(toggle_columns_dropdown),
                    ],
                    className="button-group",
                ),
            ]

        return html.Div(
            [
                *header_row,
                publish_modal,
                dcc.Loading(experiment_table, color=light_blue),
            ],
        )

    def load_experiment_data(self):
        """Load the experiment data required for the experiments table.

        Extracts all experiment metadata as well as parameters and metrics
        from each experiment in `self.experiments`. Sets GitHub information
        if applicable.
        """
        self.experiment_records = []
        self.metric_names = set()
        self.parameter_names = set()

        self.all_columns = ["id", "name", "created_at", "model_name", "commit_hash", "tags"]
        self.hidden_columns = []

        self.commit_hash = None
        self.github_url = None

        commit_hashes = set()
        show_columns = {"id", "created_at"}

        for experiment in self.experiments:
            experiment_record = {
                "id": experiment.id,
                "name": experiment.name,
                "created_at": experiment.created_at,
                "model_name": experiment.model_name,
                "commit_hash": None,
                "tags": experiment.tags,
            }

            if experiment.commit_hash is not None:
                experiment_record["commit_hash"] = experiment.commit_hash[:7]

                commit_hashes.add(experiment.commit_hash)
                show_columns.add("commit_hash")

            if experiment.model_name is not None:
                show_columns.add("model_name")

            if experiment.name is not None:
                show_columns.add("name")

            if len(experiment.tags) > 0:
                show_columns.add("tags")

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

    def register_callbacks(self, link_experiment_table=False):
        @self.app.callback(
            Output({"type": "column-dropdown-checkbox", "index": ALL}, "value"),
            [
                Input("show-all-dropdown-button", "n_clicks_timestamp"),
                Input("hide-all-dropdown-button", "n_clicks_timestamp"),
            ],
            prevent_initial_call=True,
        )
        def update_selected_column_checkboxes(last_show_click, last_hide_click):
            """Bulk updates for the selections in the "toggle columns" dropdown.

            Returns all if triggered by the "show all" button and returns
            only "id" if triggered by the "hide all" button. The initial
            call is prevented as the default state is neither all nor none.
            """
            last_show_click = last_show_click if last_show_click else 0
            last_hide_click = last_hide_click if last_hide_click else 0

            if last_hide_click > last_show_click:
                hidden_values = [[]] * (len(self.all_columns) - 1)

                return [["id"], *hidden_values]

            return [[column] for column in self.all_columns]

        @self.app.callback(
            Output("experiment-table", "hidden_columns"),
            Input({"type": "column-dropdown-checkbox", "index": ALL}, "value"),
        )
        def update_hidden_experiment_table_cols(selected_columns):
            """Hide and show the columns in the experiment table.

            Returns the columns that should be hidden based on whether or
            not the column's corresponding value is checked in the "toggle
            columns" dropdown.
            """
            selected_columns = [sc[0] for sc in selected_columns if len(sc) > 0]

            return [column for column in self.all_columns if column not in selected_columns]

        @self.app.callback(
            Output("experiment-table", "selected_rows"),
            [
                Input("select-all-button", "n_clicks_timestamp"),
                Input("clear-all-button", "n_clicks_timestamp"),
            ],
            State("experiment-table", "derived_virtual_indices"),
        )
        def update_selected_experiment_table_rows(
            last_select_click, last_clear_click, experiment_table_indices
        ):
            """Bulk selection for the rows of the experiment table.

            Returns all if triggered by the "select all" button and returns
            none if triggered by the "clear all" button.
            """
            if last_select_click is None and last_clear_click is None:
                return list(range(len(self.experiments)))

            last_select_click = last_select_click if last_select_click else 0
            last_clear_click = last_clear_click if last_clear_click else 0

            if last_select_click > last_clear_click:
                return experiment_table_indices

            return []

        @self.app.callback(
            Output("publish-modal", "is_open"),
            [
                Input("publish-selected-button", "n_clicks_timestamp"),
                Input("publish-button", "n_clicks_timestamp"),
            ],
            [
                State("publish-modal", "is_open"),
                State("publish-path-input", "value"),
                State("experiment-table", "derived_virtual_selected_rows"),
                State("experiment-table", "derived_virtual_data"),
            ],
        )
        def toggle_publish_modal(
            last_publish_selected_click,
            last_publish_click,
            is_modal_open,
            publish_path,
            selected_rows,
            data,
        ):
            last_publish_selected_click = (
                last_publish_selected_click if last_publish_selected_click else 0
            )
            last_publish_click = last_publish_click if last_publish_click else 0

            if last_publish_selected_click > last_publish_click:
                return True
            elif last_publish_click > last_publish_selected_click:
                selected_experiment_ids = [data[row].get("id") for row in selected_rows]
                selected_experiments = [
                    e for e in self.experiments if e.id in selected_experiment_ids
                ]

                publish(selected_experiments, publish_path)

                return False

            return is_modal_open
