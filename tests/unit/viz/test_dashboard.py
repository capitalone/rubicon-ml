from dash import Dash, html

from rubicon_ml.viz import Dashboard
from rubicon_ml.viz.base import VizBase


class WidgetTest(VizBase):
    def __init__(self, layout_id):
        self.layout_id = layout_id
        self.load_experiment_data_call_count = 0
        self.register_callbacks_call_count = 0

    @property
    def layout(self):
        return html.Div(id=self.layout_id)

    def load_experiment_data(self):
        self.load_experiment_data_call_count += 1

    def register_callbacks(self, link_experiment_table=False):
        self.register_callbacks_call_count += 1


def test_dashboard(viz_experiments):
    widget_a = WidgetTest(layout_id="a")
    widget_b = WidgetTest(layout_id="b")
    dashboard = Dashboard(viz_experiments, [[widget_a, widget_b]])

    expected_experiment_ids = [e.id for e in viz_experiments]

    for experiment in dashboard.experiments:
        assert experiment.id in expected_experiment_ids

        expected_experiment_ids.remove(experiment.id)

    assert dashboard.widgets == [[widget_a, widget_b]]


def test_dashboard_load_data(viz_experiments):
    widget_a = WidgetTest(layout_id="a")
    widget_b = WidgetTest(layout_id="b")

    assert widget_a.load_experiment_data_call_count == 0
    assert widget_b.load_experiment_data_call_count == 0

    dashboard = Dashboard(viz_experiments, [[widget_a, widget_b]])
    dashboard.load_experiment_data()

    assert widget_a.load_experiment_data_call_count == 1
    assert widget_b.load_experiment_data_call_count == 1


def test_dashboard_layout_horizontal(viz_experiments):
    widget_a = WidgetTest(layout_id="a")
    widget_b = WidgetTest(layout_id="b")
    dashboard = Dashboard(viz_experiments, [[widget_a, widget_b]])
    layout = dashboard.layout

    layout.children[0].children[0].children == widget_a.layout
    layout.children[0].children[1].children == widget_b.layout


def test_dashboard_layout_veritcal(viz_experiments):
    widget_a = WidgetTest(layout_id="a")
    widget_b = WidgetTest(layout_id="b")
    dashboard = Dashboard(viz_experiments, [[widget_a], [widget_b]])
    layout = dashboard.layout

    layout.children[0].children[0].children == widget_a.layout
    layout.children[1].children[0].children == widget_b.layout


def test_dashboard_register_callbacks(viz_experiments):
    widget_a = WidgetTest(layout_id="a")
    widget_b = WidgetTest(layout_id="b")

    assert widget_a.register_callbacks_call_count == 0
    assert widget_b.register_callbacks_call_count == 0

    dashboard = Dashboard(viz_experiments, [[widget_a, widget_b]])
    dashboard.app = Dash(__name__, title="test dashboard")
    dashboard.register_callbacks()

    assert widget_a.register_callbacks_call_count == 1
    assert widget_b.register_callbacks_call_count == 1
