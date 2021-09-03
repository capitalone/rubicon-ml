import os
import threading

import dash_bootstrap_components as dbc
import dash_html_components as html
from dash import Dash

from rubicon_ml import Rubicon
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.ui.callbacks import (
    set_project_explorer_callbacks,
    set_project_selection_callbacks,
)
from rubicon_ml.ui.model import RubiconModel
from rubicon_ml.ui.views import (
    make_footer_layout,
    make_header_layout,
    make_project_explorer_layout,
    make_project_selection_layout,
)


class Dashboard:
    """A dashboard for exploring logged data. The dashboard
    relies on existing rubicon_ml data, which can be passed
    in the following ways:
        * by passing in an existing Rubicon object, which can be the
          the synchronous or asynchronous version (see param details below).
        * by passing in ``persistence`` and ``root_dir``, which will configure
          a rubicon object for you (useful from CLI or independent dashboard configuration).

    Parameters
    ----------
    rubicon : rubicon_ml.Rubicon or rubicon_ml.client.asynchronous.Rubicon, optional
        A top level Rubicon instance which holds the configuration for
        the data you wish to visualize.
    persistence : str, optional
        The persistence type. Can be one of ["filesystem", "memory"].
    root_dir : str, optional
        Absolute or relative filepath of the root directory holding Rubicon data.
        Use absolute path for best performance. Defaults to the local filesystem.
        Prefix with s3:// to use s3 instead.
    page_size : int, optional
        The number of rows that will be displayed on a page within the
        experiment table.
    dash_options: dict, optional
        Additional arguments specific to the Dash app. Visit the
        `docs <https://dash.plotly.com/reference>`_ to see what's
        available. Note, `requests_pathname_prefix` is useful for proxy
        troubles.
    storage_options : dict, optional
        Additional keyword arguments specific to the protocol being chosen. They
        are passed directly to the underlying filesystem class when ``persistence``
        and ``root_dir`` are used.
    """

    def __init__(
        self,
        rubicon=None,
        persistence=None,
        root_dir=None,
        page_size=10,
        dash_options={},
        **storage_options,
    ):

        if not rubicon and not all([persistence, root_dir]):
            raise RubiconException(
                "Either `rubicon` or both `persistence` and `root_dir` must be provided."
            )

        if not rubicon:
            rubicon = Rubicon(persistence, root_dir, **storage_options)

        self.rubicon_model = RubiconModel(rubicon)

        self._app = Dash(__name__, title="Rubicon", **dash_options)
        self._app._rubicon_model = self.rubicon_model
        self._app._page_size = page_size
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

        set_project_selection_callbacks(self._app)
        set_project_explorer_callbacks(self._app)

    def run_server(self, **kwargs):
        """Serve the dash app on an external web page.

        Parameters
        ----------
        kwargs : dict
            Additional arguments to be passed to `dash.run_server`.
        """
        self._app.run_server(**kwargs)

    def run_server_inline(self, i_frame_kwargs={}, **kwargs):
        """Serve the dash app inline in a Jupyter notebook.

        Parameters
        ----------
        i_frame_kwargs : dict
            Additional arguments to be passed to `IPython.display.IFrame`.
        kwargs : dict
            Additional arguments to be passed to `dash.run_server`.
        """
        from IPython.display import IFrame

        if "proxy" in kwargs:
            host = kwargs.get("proxy").split("::")[-1]
        else:
            port = kwargs.get("port") if "port" in kwargs else 8050
            host = f"http://localhost:{port}"

        if "dev_tools_silence_routes_logging" not in kwargs:
            kwargs["dev_tools_silence_routes_logging"] = True

        if "height" not in i_frame_kwargs:
            i_frame_kwargs["height"] = 800

        if "width" not in i_frame_kwargs:
            i_frame_kwargs["width"] = "100%"

        running_server_thread = threading.Thread(
            name="run_server",
            target=self.run_server,
            kwargs=kwargs,
        )
        running_server_thread.daemon = True
        running_server_thread.start()

        proxied_host = os.path.join(host, self._app.config["requests_pathname_prefix"].lstrip("/"))

        return IFrame(proxied_host, **i_frame_kwargs)
