import dash_html_components as html


def make_header_layout():
    """The html layout for the dashboard's header view."""

    return html.Div(
        id="header",
        className="header",
        children=[
            html.Div(id="title", className="header--project", children="Rubicon"),
            html.Div(
                id="links",
                className="header--links",
                children=[
                    html.A(
                        className="header--link",
                        href="https://capitalone.github.io/rubicon",
                        children="Docs",
                    ),
                    html.A(
                        className="header--link",
                        href="https://github.com/capitalone/rubicon",
                        children="Github",
                    ),
                ],
            ),
        ],
    )
