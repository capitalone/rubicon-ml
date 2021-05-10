import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output

from rubicon_ml.ui.app import app


def make_project_selection_layout():
    """The html layout for the dashboard's project selection view."""

    return html.Div(
        id="project-selection",
        className="project-selection",
        children=[
            html.Div(
                className="project-selection--header",
                children=[
                    html.Div(className="project-selection--label", children="Projects"),
                    html.I(
                        id="project-selection--refresh-projects-btn",
                        n_clicks=0,
                        className="fas fa-sync-alt",
                    ),
                ],
            ),
            html.Div(id="project-selection--list", className="project-selection--list"),
        ],
    )


@app.callback(
    Output("project-selection--list", "children"),
    [Input("project-selection--refresh-projects-btn", "n_clicks")],
)
def _update_project_options(n_clicks):
    """Use the Rubicon client to load the available projects."""
    app._rubicon_model.update_projects()

    return dbc.ListGroup(
        children=[
            dbc.ListGroupItem(
                id={"type": "project-selection--project", "index": project["value"]},
                className="project-selection--project",
                children=project["value"],
                n_clicks=0,
                action=True,
            )
            for project in app._rubicon_model.project_options
        ]
    )
