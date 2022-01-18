import os
import threading
import time

import dash_bootstrap_components as dbc
from dash import Dash, html

from rubicon_ml import __version__ as rubicon_ml_version

_next_available_port = 8050


class VizBase:
    """The base class for all `rubicon_ml` visualizations.

    `VizBase` can not be directly instantatied. New widgets must all
    extend `VizBase`.
    """

    def __init__(
        self,
        dash_title="base",
    ):
        self.dash_title = f"rubicon-ml: {dash_title}"

    @property
    def layout(self):
        raise NotImplementedError("extensions of `VizBase` must implement property `layout(self)`")

    def build_layout(self):
        """Wraps the layout defined by `self.layout` in a container providing
        the `rubicon_ml` header.
        """
        self.app.layout = dbc.Card(
            dbc.CardBody(
                [
                    dbc.Row(
                        [
                            html.Img(
                                id="rubicon-logo-img",
                                src=self.app.get_asset_url("images/rubicon-logo-dark.png"),
                            ),
                        ],
                    ),
                    dbc.Row(html.P(rubicon_ml_version, id="version-text"), id="version-row"),
                    dbc.Row(self.layout),
                ],
                id="frame",
            ),
        )

    def load_experiment_data(self):
        raise NotImplementedError(
            "extensions of `VizBase` must implement `load_experiment_data(self)`"
        )

    def register_callbacks(self, link_experiment_table=False):
        raise NotImplementedError(
            "extensions of `VizBase` must implement `register_callbacks(self)`"
        )

    def serve(self, in_background=False, dash_kwargs={}, run_server_kwargs={}):
        """Serve the Dash app on the next available port to render the visualization.

        Parameters
        ----------
        in_background : bool, optional
            True to run the Dash app on a thread and return execution to the
            interpreter. False to run the Dash app inline and block execution.
            Defaults to False.
        dash_kwargs : dict, optional
            Keyword arguments to be passed along to the newly instantiated
            Dash object. Available options can be found at
            https://dash.plotly.com/reference#dash.dash.
        run_server_kwargs : dict, optional
            Keyword arguments to be passed along to `Dash.run_server`.
            Available options can be found at
            https://dash.plotly.com/reference#app.run_server. Most commonly,
            the 'port' argument can be provided here to serve the app on a
            specific port.
        """
        if self.experiments is None:
            raise RuntimeError(
                f"`{self.__class__}.experiments` can not be None when `serve` is called"
            )

        self.app = Dash(
            __name__,
            external_stylesheets=[dbc.themes.LUX, dbc.icons.BOOTSTRAP],
            title=self.dash_title,
            **dash_kwargs,
        )

        self.load_experiment_data()
        self.build_layout()
        self.register_callbacks()

        global _next_available_port

        default_run_server_kwargs = {
            "dev_tools_silence_routes_logging": True,
            "port": _next_available_port,
        }
        default_run_server_kwargs.update(run_server_kwargs)

        _next_available_port = default_run_server_kwargs["port"] + 1

        if in_background:
            running_server_thread = threading.Thread(
                name="run_server",
                target=self.app.run_server,
                kwargs=default_run_server_kwargs,
            )
            running_server_thread.daemon = True
            running_server_thread.start()

            port = default_run_server_kwargs.get("port")
            if "proxy" in run_server_kwargs:
                host = default_run_server_kwargs.get("proxy").split("::")[-1]
            else:
                host = f"http://localhost:{port}"

            time.sleep(0.1)  # wait for thread to see if requested port is available
            if not running_server_thread.is_alive():
                raise RuntimeError(f"port {port} may already be in use")

            return host
        else:
            self.app.run_server(**default_run_server_kwargs)

    def show(self, i_frame_kwargs={}, dash_kwargs={}, run_server_kwargs={}):
        """Show the Dash app inline in a Jupyter notebook.

        Parameters
        ----------
        i_frame_kwargs : dict, optional
            Keyword arguments to be passed along to the newly instantiated
            IFrame object. Available options include 'height' and 'width'.
        dash_kwargs : dict, optional
            Keyword arguments to be passed along to the newly instantiated
            Dash object. Available options can be found at
            https://dash.plotly.com/reference#dash.dash.
        run_server_kwargs : dict, optional
            Keyword arguments to be passed along to `Dash.run_server`.
            Available options can be found at
            https://dash.plotly.com/reference#app.run_server. Most commonly,
            the 'port' argument can be provided here to serve the app on a
            specific port.
        """
        from IPython.display import IFrame

        host = self.serve(
            in_background=True, dash_kwargs=dash_kwargs, run_server_kwargs=run_server_kwargs
        )
        proxied_host = os.path.join(host, self.app.config["requests_pathname_prefix"].lstrip("/"))

        default_i_frame_kwargs = {
            "height": "600px",
            "width": "100%",
        }
        default_i_frame_kwargs.update(i_frame_kwargs)

        return IFrame(proxied_host, **default_i_frame_kwargs)
