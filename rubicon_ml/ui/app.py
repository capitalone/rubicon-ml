import sys

from dash import Dash

app = Dash(__name__, title="Rubicon")


def configure_dash_app(title="Rubicon", **kwargs):
    """Apply additional configuration to the dashboard's underlying `Dash` app.

    Raises
    ------
    RuntimeError
        The configuration can not be updated once the dashboard itself has been
        imported. The `Dash` app must be created before the dashboard is imported
        as it applies the configuration at import time.
    """
    if "rubicon_ml.ui.dashboard" in sys.modules:
        raise RuntimeError(
            "`rubicon_ml.ui.dashboard` found in imported modules. "
            "`configure_dash_app` must be run before `rubicon_ml.ui.dashboard.Dashboard` "
            "is imported to apply changes."
        )

    global app
    app = Dash(__name__, title=title, **kwargs)
