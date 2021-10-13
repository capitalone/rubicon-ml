import os
import threading

import dash_bootstrap_components as dbc
from dash import Dash, dcc, html

from rubicon_ml import __version__ as rubicon_ml_version


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
        return dbc.Card(
            dbc.CardBody(
                [
                    dbc.Row(
                        [
                            html.Img(
                                id="rubicon-logo-img",
                                src=self.app.get_asset_url("rubicon-logo-dark.png"),
                                style={"height": "3.5rem", "padding-left": "1rem"},
                            ),
                        ],
                    ),
                    dbc.Row(html.P(rubicon_ml_version, id="verision-text"), id="version-row"),
                    dbc.Row([layout]),
                ],
                style={"padding": "1rem 1rem 0rem 1rem"},
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

        if "proxy" in kwargs:
            host = kwargs.get("proxy").split("::")[-1]
        else:
            port = kwargs.get("port") if "port" in kwargs else 8050
            host = f"http://localhost:{port}"

        if "dev_tools_silence_routes_logging" not in kwargs:
            kwargs["dev_tools_silence_routes_logging"] = True

        if "height" not in i_frame_kwargs:
            i_frame_kwargs["height"] = 500

        if "width" not in i_frame_kwargs:
            i_frame_kwargs["width"] = "100%"

        running_server_thread = threading.Thread(
            name="run_server",
            target=self.app.run_server,
            kwargs=kwargs,
        )
        running_server_thread.daemon = True
        running_server_thread.start()

        proxied_host = os.path.join(host, self.app.config["requests_pathname_prefix"].lstrip("/"))

        return IFrame(proxied_host, **i_frame_kwargs)
