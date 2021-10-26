import json

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
from dash import callback_context
from dash.dependencies import ALL, MATCH, Input, Output, State

from rubicon_ml.ui.views.project_explorer import (
    make_empty_view,
    make_individual_project_explorer_layout,
)


def set_project_explorer_callbacks(app):
    @app.callback(
        [
            Output({"type": "group-detail-collapsable", "index": MATCH}, "is_open"),
            Output({"type": "show-group-detail-collapsable-btn", "index": MATCH}, "hidden"),
            Output({"type": "hide-group-detail-collapsable-btn", "index": MATCH}, "hidden"),
        ],
        [
            Input(
                {"type": "show-group-detail-collapsable-btn", "index": MATCH}, "n_clicks_timestamp"
            ),
            Input(
                {"type": "hide-group-detail-collapsable-btn", "index": MATCH}, "n_clicks_timestamp"
            ),
        ],
    )
    def _toggle_experiment_group_collapsable(last_show_click, last_hide_click):
        """The callback to show and collapse each group.

        Also toggles the show/hide buttons. Triggered when the show or
        hide button is clicked.
        """
        last_show_click = last_show_click if last_show_click else 0
        last_hide_click = last_hide_click if last_hide_click else 0

        # "show" is clicked: open collabsable, hide "show" button & un-hide "hide" button
        if int(last_show_click) > int(last_hide_click):
            return True, True, False
        # "hide" is clicked: close collabsable, un-hide "show" button & hide "hide" button
        elif int(last_hide_click) > int(last_show_click):
            return False, False, True
        # nothing yet is clicked: return default states
        else:
            return False, False, True

    @app.callback(
        Output({"type": "experiment-comparison-plot", "index": MATCH}, "children"),
        [
            Input({"type": "experiment-table", "index": MATCH}, "derived_virtual_data"),
            Input({"type": "experiment-table", "index": MATCH}, "derived_virtual_selected_rows"),
            Input({"type": "experiment-table", "index": MATCH}, "hidden_columns"),
            Input({"type": "anchor-dropdown", "index": MATCH}, "value"),
        ],
        [State({"type": "group-store", "index": MATCH}, "data")],
    )
    def _update_experiment_comparison_plot(
        experiment_table_data, experiment_table_selected_rows, hidden_columns, anchor, data
    ):
        """The callback to render a new experiment comparison plot.

        Triggered when new rows in the experiment table are selected.
        or deselected.
        """
        if experiment_table_selected_rows is None or len(experiment_table_selected_rows) == 0:
            return [html.Div()]

        commit_hash = data["commit_hash"]
        selected_experiment_ids = [
            experiment_table_data[row]["id"] for row in experiment_table_selected_rows
        ]

        anchor_data, dimensions = app._rubicon_model.get_dimensions(
            commit_hash, selected_experiment_ids, hidden_columns, anchor
        )

        return [
            dcc.Graph(
                figure=go.Figure(
                    go.Parcoords(
                        line=dict(color=anchor_data, colorscale="plasma", showscale=True),
                        dimensions=dimensions,
                    )
                )
            )
        ]

    @app.callback(
        Output("grouped-project-explorer", "children"),
        [Input({"type": "project-selection--project", "index": ALL}, "n_clicks")],
    )
    def _update_project_explorer(values):
        """The callback to render the grouped project explorer.

        Triggered when a project is selected in the project selection list.
        """

        # if all values are 0, the user hasn't clicked a project yet
        is_waiting_for_first_click = True
        for value in values:
            if value != 0:
                is_waiting_for_first_click = False

        if is_waiting_for_first_click:
            return make_empty_view(
                "Please select a project to view.", app.get_asset_url("search-icon.svg")
            )

        # use `dash.callback_context` to get the id of the clicked project list item
        selected_id = callback_context.triggered[0]["prop_id"].split(".")[0]
        selected_project_name = json.loads(selected_id)["index"]

        app._rubicon_model.update_selected_project(selected_project_name)

        project_explorer_header = dbc.Row(
            id="experiment-deatils-header",
            className="experiment-details-header",
            children=selected_project_name,
        )

        experiment_groups = app._rubicon_model._experiment_table_dfs.items()

        # handle no experiments view
        if len(experiment_groups) == 0:
            return [
                project_explorer_header,
                make_empty_view(
                    "Log some experiments to this project!", app.get_asset_url("search-icon.svg")
                ),
            ]

        _project_explorers = [
            make_individual_project_explorer_layout(app._rubicon_model, group, app._page_size)
            for group, _ in experiment_groups
        ]

        return [project_explorer_header, *_project_explorers]

    @app.callback(
        Output({"type": "experiment-table", "index": MATCH}, "selected_rows"),
        [
            Input({"type": "select-all-btn", "index": MATCH}, "n_clicks_timestamp"),
            Input({"type": "clear-all-btn", "index": MATCH}, "n_clicks_timestamp"),
        ],
        [State({"type": "experiment-table", "index": MATCH}, "derived_virtual_indices")],
    )
    def _update_selected_experiment_table_rows(
        last_select_click, last_clear_click, experiment_table_indices
    ):
        """The callback to select or deselect all rows in the experiment table.

        Triggered when the select all or clear all button is clicked.
        """
        last_select_click = last_select_click if last_select_click else 0
        last_clear_click = last_clear_click if last_clear_click else 0

        # "select all" is clicked: return all row indicies
        if int(last_select_click) > int(last_clear_click):
            return experiment_table_indices

        # "clear all" or nothing yet is clicked: return no row indicies
        return []

    @app.callback(
        Output({"type": "experiment-table", "index": MATCH}, "hidden_columns"),
        [
            Input({"type": "select-all-col-btn", "index": MATCH}, "n_clicks_timestamp"),
            Input({"type": "clear-all-col-btn", "index": MATCH}, "n_clicks_timestamp"),
        ],
        [State({"type": "experiment-table", "index": MATCH}, "columns")],
    )
    def _update_selected_experiment_table_cols(
        last_select_col_click,
        last_clear_col_click,
        experiment_table_cols,
    ):

        """The callback to select or deselect all columns in the experiment table.

        Triggered when the select all columns or clear all columns button is clicked.
        """

        last_select_col_click = last_select_col_click if last_select_col_click else 0
        last_clear_col_click = last_clear_col_click if last_clear_col_click else 0

        if int(last_select_col_click) >= int(last_clear_col_click):
            return []
        # "clear all" or nothing yet is clicked: return no row indicies
        ret = [i["id"] for i in experiment_table_cols if i["id"] != "id"]
        return ret
