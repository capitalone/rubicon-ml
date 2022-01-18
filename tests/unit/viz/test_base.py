from dash import Dash, html

from rubicon_ml.viz.base import VizBase


class VizBaseTest(VizBase):
    app = Dash(__name__, title="test base")

    @property
    def layout(self):
        return html.Div(id="test-layout")


def test_base_build_layout():
    base = VizBaseTest()
    base.build_layout()

    layout = base.app.layout

    assert len(layout) == 8
    assert layout.children.children[-1].children.id == "test-layout"
