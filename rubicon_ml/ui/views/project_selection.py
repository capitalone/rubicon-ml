import dash_html_components as html


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
