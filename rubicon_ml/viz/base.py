import warnings
from typing import Dict, Literal, Optional, Union

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
        dash_title: str = "base",
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

    def register_callbacks(self, link_experiment_table: bool = False):
        raise NotImplementedError(
            "extensions of `VizBase` must implement `register_callbacks(self)`"
        )

    def serve(
        self,
        in_background: bool = False,
        jupyter_mode: Literal["external", "inline", "jupyterlab", "tab"] = "external",
        dash_kwargs: Dict = {},
        run_server_kwargs: Dict = {},
    ):
        """Serve the Dash app on the next available port to render the visualization.

        Parameters
        ----------
        in_background : bool, optional
            DEPRECATED. Background processing is now handled by `jupyter_mode`.
        jupyter_mode : "external", "inline", "jupyterlab", or "tab", optional
            How to render the dashboard when running from Jupyterlab.
            * "external" to serve the dashboard at an external link.
            * "inline" to render the dashboard in the current notebook's output
              cell.
            * "jupyterlab" to render the dashboard in a new window within the
              current Jupyterlab session.
            * "tab" to serve the dashboard at an external link and open a new
              browser tab to said link.
            Defaults to "external".
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
        if in_background:
            warnings.warn(
                "The `in_background` argument is deprecated and will have no effect, "
                "Background processing is now handled by `jupyter_mode`.",
                DeprecationWarning,
            )

        JUPYTER_MODES = ["external", "inline", "jupyterlab", "tab"]
        if jupyter_mode not in JUPYTER_MODES:
            raise ValueError(
                f"Invalid `jupyter_mode` '{jupyter_mode}'. Must be one of "
                f"{', '.join(JUPYTER_MODES)}"
            )

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

        if jupyter_mode != "inline":
            default_run_server_kwargs["jupyter_mode"] = jupyter_mode

        _next_available_port = default_run_server_kwargs["port"] + 1

        self.app.run(**default_run_server_kwargs)

    def show(
        self,
        i_frame_kwargs: Dict = {},
        dash_kwargs: Dict = {},
        run_server_kwargs: Dict = {},
        height: Optional[Union[int, str]] = None,
        width: Optional[Union[int, str]] = None,
    ):
        """Serve the Dash app on the next available port to render the visualization.

        Additionally, renders the visualization inline in the current Jupyter notebook.

        Parameters
        ----------
        i_frame_kwargs: dict, optional
            DEPRECATED. Use `height` and `width` instead.
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
        height : int, str or None, optional
            The height of the inline visualizaiton. Integers represent number
            of pixels, strings represent a percentage of the window and must
            end with '%'.
        width : int, str or None, optional
            The width of the inline visualizaiton. Integers represent number
            of pixels, strings represent a percentage of the window and must
            end with '%'.
        """
        if i_frame_kwargs:
            warnings.warn(
                "The `i_frame_kwargs` argument is deprecated and will have no effect, "
                "use `height` and `width` instead.",
                DeprecationWarning,
            )

        if height is not None:
            run_server_kwargs["jupyter_height"] = height
        if width is not None:
            run_server_kwargs["jupyter_width"] = width

        self.serve(
            jupyter_mode="inline",
            dash_kwargs=dash_kwargs,
            run_server_kwargs=run_server_kwargs,
        )
