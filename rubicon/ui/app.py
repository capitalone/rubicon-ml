import dash
from jupyter_dash import JupyterDash

app = dash.Dash(__name__, title="Rubicon")


def set_jupyter_app():
    global app
    app = JupyterDash(__name__, title="Rubicon")
