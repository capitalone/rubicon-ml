import json
import uuid

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import plotly.graph_objects as go
from dash import callback_context
from dash.dependencies import ALL, MATCH, Input, Output, State

from rubicon_ml.ui.app import app

# INDIVIDUAL PROJECT EXPLORER VIEW


def _get_experiment_table(id, experiments_df):
    """Get a Dash DataTable with the experiments in `experiments_df`."""
    return dash_table.DataTable(
        id={"type": "experiment-table", "index": id},
        columns=[
            {"name": i, "id": i, "selectable": True, "hideable": True}
            for i in experiments_df.columns
        ],
        data=experiments_df.compute().to_dict("records"),
        page_size=app._page_size,
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        row_selectable="multi",
        selected_rows=[],
        fixed_columns={"headers": True, "data": 1},
        style_cell={"overflow": "hidden", "textOverflow": "ellipsis"},
        style_header={"color": "#707171", "fontWeight": 700},
        style_table={"minWidth": "100%"},
        style_cell_conditional=[
            {
                "if": {"column_id": "id"},
                "width": "300px",
                "minWidth": "300px",
                "maxWidth": "300px",
            },
            {
                "if": {"column_id": "commit_hash"},
                "width": "120px",
                "minWidth": "120px",
                "maxWidth": "120px",
            },
        ],
        style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#f1f7fa"}],
    )


def _get_github_commit_url(github_url, commit_hash):
    """Get the github commit url, if it exists"""
    is_github_enabled = github_url is not None
    github_commit_url = f"{github_url[:-4]}/tree/{commit_hash}" if is_github_enabled else None

    return github_commit_url


def make_individual_project_explorer_layout(rubicon_model, commit_hash):
    """The html layout for an individual project explorer view determined by
    `commit_hash` in the dashboard.

    A project explorer view shows a subset of experiments logged
    to a project in a tabular format, as well as plotted on a
    parallel coordinates plot.
    """
    id = str(uuid.uuid4())

    experiment_table_df = rubicon_model.get_experiment_table_df(commit_hash)
    github_commit_url = _get_github_commit_url(
        rubicon_model.selected_project.github_url, commit_hash
    )

    group_store = dcc.Store(
        id={"type": "group-store", "index": id},
        data={"commit_hash": commit_hash},
    )

    group_preview_title = [
        html.P(
            f"{len(experiment_table_df)} experiments",
            id="group-preview-title",
            className="group-preview-title",
        )
    ]

    if commit_hash is not None and rubicon_model.selected_project.github_url is not None:
        group_preview_title.append(
            html.A(
                f"at commit {commit_hash[:7]}",
                id="group-preview-title-link",
                className="group-preview-title-link",
                href=github_commit_url,
                target="_blank",
            )
        )

    group_model_names = rubicon_model.get_model_names(commit_hash)
    if len(group_model_names) > 0:
        group_model_names_text = f"model name: {group_model_names[0]}"
        if len(group_model_names) > 1:
            group_model_names_text += f" (+{len(group_model_names) - 1} more)"

        group_model_names_view = html.P(
            group_model_names_text,
            id="group-preview-model-names",
            className="group-preview-model-names",
        )
    else:
        group_model_names_view = html.P(style={"display": "none"})

    chevron = html.I(className="fas fa-chevron-down")

    group_preview_row = dbc.Row(
        id={"type": "group-preview-row", "index": id},
        className="group-preview-row",
        children=[
            dbc.Row(group_preview_title, style={"margin": "inherit"}),
            group_model_names_view,
            html.Button(
                chevron,
                id={"type": "show-group-detail-collapsable-btn", "index": id},
                className="show-group-detail-collapsable-btn",
            ),
            html.Button(
                chevron,
                id={"type": "hide-group-detail-collapsable-btn", "index": id},
                className="hide-group-detail-collapsable-btn",
                hidden=True,
            ),
        ],
    )

    experiment_table_bulk_action_button_group = html.Div(
        className="btn-group",
        children=[
            html.Button(
                "Select All",
                id={"type": "select-all-btn", "index": id},
                className="btn-progressive",
            ),
            html.Button(
                "Clear All",
                id={"type": "clear-all-btn", "index": id},
                className="btn-progressive",
            ),
        ],
    )

    group_detail_card = dbc.Card(
        id={"type": "group-detail-card", "index": id},
        className="group-detail-card",
        children=[
            dbc.CardBody(
                id={"type": "group-detail-card-body", "index": id},
                className="group-detail-card-body",
                children=[
                    experiment_table_bulk_action_button_group,
                    _get_experiment_table(id, experiment_table_df),
                    _get_comparison_layout(id, rubicon_model, commit_hash),
                ],
            )
        ],
    )

    group_detail_collapsable = dbc.Collapse(
        id={"type": "group-detail-collapsable", "index": id},
        className="group-detail-collapsable",
        children=[group_detail_card],
    )

    return dbc.Row(
        id={"type": "individual-project-explorer", "index": id},
        className="individual-project-explorer",
        children=[dbc.Col([group_store, group_preview_row, group_detail_collapsable])],
    )


def _get_comparison_layout(id, rubicon_model, commit_hash):
    experiment_comparison_plot = html.Div(
        id={"type": "experiment-comparison-plot", "index": id},
        className="experiment-comparison-plot",
    )

    experiment_comparison_tooltip_icon = html.I(
        id="experiment-comparison-question-tooltip", className="far fa-question-circle"
    )

    experiment_comparison_tooltip_text = dbc.Tooltip(
        """Select experiments within the experiment table and then choose an anchor """
        """to see how the other parameters and metrics affect the anchor's value across experiment runs. """
        """Rearrange the axes by dragging the column names. Select a subset by """
        """clicking and dragging along the y axes.""",
        target="experiment-comparison-question-tooltip",
        style={"max-width": "480px"},
        placement="right",
    )

    experiment_comparison_header = dbc.Row(
        id={"type": "experiment-comparison-header", "index": id},
        className="experiment-comparison-header",
        children=[
            "Compare Experiments",
            experiment_comparison_tooltip_icon,
            experiment_comparison_tooltip_text,
        ],
    )

    anchor_options = rubicon_model.get_anchor_options(commit_hash)
    anchor_value = anchor_options[-1]["value"] if len(anchor_options) > 0 else ""

    anchor_dropdown = dcc.Dropdown(
        id={"type": "anchor-dropdown", "index": id},
        className="anchor-dropdown",
        placeholder="Select an anchor metric",
        clearable=False,
        options=anchor_options,
        value=anchor_value,
    )

    return html.Div([experiment_comparison_header, anchor_dropdown, experiment_comparison_plot])


# GROUPED PROJECT EXPLORER VIEW


@app.callback(
    [
        Output({"type": "group-detail-collapsable", "index": MATCH}, "is_open"),
        Output({"type": "show-group-detail-collapsable-btn", "index": MATCH}, "hidden"),
        Output({"type": "hide-group-detail-collapsable-btn", "index": MATCH}, "hidden"),
    ],
    [
        Input({"type": "show-group-detail-collapsable-btn", "index": MATCH}, "n_clicks_timestamp"),
        Input({"type": "hide-group-detail-collapsable-btn", "index": MATCH}, "n_clicks_timestamp"),
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
        return make_empty_view("Please select a project to view.")

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
        return [project_explorer_header, make_empty_view("Log some experiments to this project!")]

    _project_explorers = [
        make_individual_project_explorer_layout(app._rubicon_model, group)
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


def make_empty_view(instructions):
    """The html layout for the dashboard's empty view when no project is selected."""
    title = "Nothing to see here..."

    return html.Div(
        id="empty-view",
        className="empty-view",
        children=[
            html.Img(src=app.get_asset_url("search-icon.svg"), style={"marginBottom": "20px"}),
            html.P(className="empty-view--title", children=title),
            html.P(className="empty-view--instructions", children=instructions),
        ],
    )


def make_project_explorer_layout():
    """The html layout for the dashboard's grouped project explorer view.

    This view holds a collection of collapsable project explorer
    groups. Each group shows some top level information and
    provides interactions to drill into the underlying group's data.
    """
    return dbc.Col(id="grouped-project-explorer", className="grouped-project-explorer")
