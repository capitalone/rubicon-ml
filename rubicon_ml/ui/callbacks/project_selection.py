import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output


def set_project_selection_callbacks(app):
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
