from dash import html


def make_footer_layout():
    """The html layout for the dashboard's footer view."""
    return html.Div(
        id="footer",
        className="footer",
        children=[
            html.Div(id="copyright", className="footer--copyright", children="© 2020 Capital One"),
        ],
    )
