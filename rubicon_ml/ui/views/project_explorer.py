import uuid

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table

# INDIVIDUAL PROJECT EXPLORER VIEW


def _get_experiment_table(id, experiments_df, page_size):
    """Get a Dash DataTable with the experiments in `experiments_df`."""
    return dash_table.DataTable(
        id={"type": "experiment-table", "index": id},
        columns=[
            {"name": i, "id": i, "selectable": True, "hideable": True}
            for i in experiments_df.columns
        ],
        data=experiments_df.compute().to_dict("records"),
        page_size=page_size,
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


def make_individual_project_explorer_layout(rubicon_model, commit_hash, page_size):
    """The html layout for an individual project explorer view determined by
    `commit_hash` in the dashboard.

    A project explorer view shows a subset of experiments logged
    to a project in a tabular format, as well as plotted on a
    parallel coordinates plot.
    """
    id = str(uuid.uuid4())

    experiment_table_df = rubicon_model.get_experiment_table_df(commit_hash)

    # coerce param/metric cols to string to allow for filtering/sorting
    # and to avoid render issues if value is dict
    cols_to_coerce = []
    for col in experiment_table_df.columns:
        if col not in ["id", "model_name", "commit_hash", "tags"]:
            cols_to_coerce.append(col)

    for col in cols_to_coerce:
        experiment_table_df = experiment_table_df.astype({col: "string"})

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
                style={"margin-left": "10px"},
                id={"type": "clear-all-btn", "index": id},
                className="btn-progressive",
            ),
            html.Button(
                "Select All Columns",
                style={"margin-left": "10px"},
                id={"type": "select-all-col-btn", "index": id},
                className="btn-progressive",
            ),
            html.Button(
                "Clear All Columns",
                style={"margin-left": "10px"},
                id={"type": "clear-all-col-btn", "index": id},
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
                    _get_experiment_table(id, experiment_table_df, page_size),
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


def make_empty_view(instructions, asset_url):
    """The html layout for the dashboard's empty view when no project is selected."""
    title = "Nothing to see here..."

    return html.Div(
        id="empty-view",
        className="empty-view",
        children=[
            html.Img(src=asset_url, style={"marginBottom": "20px"}),
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
