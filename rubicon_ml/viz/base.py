import os
import threading
import time

import dash_bootstrap_components as dbc
from dash import Dash, html

from rubicon_ml import __version__ as rubicon_ml_version

_next_available_port = 8050


class VizBase:
    def __init__(
        self,
        dash_title="base",
    ):
        self.dash_title = f"rubicon-ml: {dash_title}"

    @property
    def layout(self):
        raise NotImplementedError("extensions of `VizBase` must implement property `layout(self)`")

    def build_layout(self):
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
