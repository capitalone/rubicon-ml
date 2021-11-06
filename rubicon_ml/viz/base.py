import os
import threading
import time

import dash_bootstrap_components as dbc
from dash import Dash, dcc, html

from rubicon_ml import __version__ as rubicon_ml_version

_next_available_port = 8050


class VizBase:
    def __init__(
        self,
        dash_kwargs={},
        dash_title="rubicon-ml",
    ):
        self.app = Dash(
            __name__, external_stylesheets=[dbc.themes.LUX], title=dash_title, **dash_kwargs
        )

    def _build_frame(self, layout):
        if not isinstance(layout, list):
            layout = [layout]

        return dbc.Card(
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
                    dbc.Row(layout),
                ],
                id="frame",
            ),
        )

    def _to_store(self, ignore_attributes=[], store_id="memory-store", storage_type="memory"):
        return dcc.Store(
            id=store_id,
            data={
                k: v for k, v in self.__dict__.items() if all([k != a for a in ignore_attributes])
            },
            storage_type=storage_type,
        )

    def run_server_inline(self, i_frame_kwargs={}, **kwargs):
        from IPython.display import IFrame

        global _next_available_port

        run_server_kwargs = {
            "dev_tools_silence_routes_logging": True,
            "port": _next_available_port,
        }
        run_server_kwargs.update(kwargs)

        _next_available_port = run_server_kwargs["port"] + 1

        if "proxy" in run_server_kwargs:
            host = run_server_kwargs.get("proxy").split("::")[-1]
        else:
            port = run_server_kwargs.get("port")
            host = f"http://localhost:{port}"

        running_server_thread = threading.Thread(
            name="run_server",
            target=self.app.run_server,
            kwargs=run_server_kwargs,
        )
        running_server_thread.daemon = True
        running_server_thread.start()

        time.sleep(0.1)  # wait for thread to see if requested port is available
        if not running_server_thread.is_alive():
            raise RuntimeError(f"port {port} is already in use")

        i_frame_kwargs["width"] = "100%"
        proxied_host = os.path.join(host, self.app.config["requests_pathname_prefix"].lstrip("/"))

        return IFrame(proxied_host, **i_frame_kwargs)
