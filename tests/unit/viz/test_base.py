import pytest
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


def test_base_serve_jupyter_mode_errors():
    base = VizBaseTest()
    base.build_layout()

    with pytest.raises(ValueError) as error:
        base.serve(jupyter_mode="invalid")

    assert "Invalid `jupyter_mode`" in str(error)


def test_base_serve_missing_experiment_errors():
    base = VizBaseTest()
    base.build_layout()
    base.experiments = None

    with pytest.raises(RuntimeError) as error:
        base.serve()

    assert "experiments` can not be None" in str(error)
