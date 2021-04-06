import dash_bootstrap_components as dbc
import dash_html_components as html

from rubicon.ui.app import app
from rubicon.ui.model import RubiconModel
from rubicon.ui.views.footer import make_footer_layout
from rubicon.ui.views.header import make_header_layout
from rubicon.ui.views.project_explorer import make_project_explorer_layout
from rubicon.ui.views.project_selection import make_project_selection_layout


class Dashboard:
    """The Rubicon Dashboard.

    Parameters
    ----------
    persistence : str
        The persistence type. Can be one of ["filesystem", "memory"].
    root_dir : str, optional
        Absolute or relative filepath of the root directory holding Rubicon data.
        Use absolute path for best performance. Defaults to the local filesystem.
        Prefix with s3:// to use s3 instead.
    page_size : int, optional
        The number of rows that will be displayed on a page within the
        experiment table.
    storage_options : dict, optional
        Additional keyword arguments specific to the protocol being chosen. They
        are passed directly to the underlying filesystem class.
    """

    def __init__(self, persistence, root_dir=None, page_size=10, **storage_options):
        self._app = app
        self._app._page_size = page_size
        self.rubicon_model = RubiconModel(persistence, root_dir, **storage_options)
        self._app._rubicon_model = self.rubicon_model

        self._app.layout = html.Div(
            [
                dbc.Row(make_header_layout()),
                dbc.Row(
                    [
                        dbc.Col(make_project_selection_layout(), width=2),
                        dbc.Col(
                            make_project_explorer_layout(),
                            width=10,
                            style={
                                "overflowY": "scroll",
                                "maxHeight": "90vh",
                                "paddingRight": "165px",
                            },
                        ),
                    ],
                ),
                dbc.Row(make_footer_layout()),
            ]
        )

    def run_server(self, **kwargs):
        """Serve the dash app.

        Parameters
        ----------
        kwargs : dict
            Arguments passed to dash.run_server()
        """
        self._app.run_server(**kwargs)
