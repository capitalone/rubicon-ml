import math
import pickle

from dash import dcc, html
from plotly.subplots import make_subplots

from rubicon_ml.viz.base import VizBase
from rubicon_ml.viz.colors import light_blue
from rubicon_ml.viz.common import dropdown_header


class ArtifactPlot(VizBase):
    def __init__(
        self,
        experiments,
        selected_artifact,
        x,
        y,
        columns=None,
        rows=None,
        stack_plots=False,
        dash_kwargs={},
    ):
        super().__init__(dash_kwargs=dash_kwargs, dash_title="rubicon-ml: plot artifacts")

        self.experiments = experiments
        self.selected_artifact = selected_artifact

        if stack_plots:
            self.columns = 1
            self.rows = 1
        else:
            self.columns = columns
            self.rows = rows

            if self.columns is None and self.rows is None:
                self.columns = 3

            if self.columns is None and self.rows is not None:
                self.columns = math.ceil(len(self.experiments) / self.rows)
            elif self.columns is not None and self.rows is None:
                self.rows = math.ceil(len(self.experiments) / self.columns)

            if self.columns * self.rows < len(self.experiments):
                raise ValueError(
                    f"columns ({self.columns}) * rows ({self.rows}) < "
                    f"len(self.experiments) ({len(self.experiments)})"
                )

        self.plots_figure = make_subplots(
            cols=self.columns,
            rows=self.rows,
            x_title=x,
            y_title=y,
        )
        self.plots_figure.update_layout(modebar_orientation="v")

        for i, experiment in enumerate(self.experiments):
            artifact = [a for a in experiment.artifacts() if a.name == selected_artifact][0]

            plot_figure = pickle.loads(artifact.data)
            plot = plot_figure.data[0]

            plot.name = experiment.id
            plot.showlegend = True

            if stack_plots:
                col = 1
                row = 1
            else:
                col = (i % self.columns) + 1
                row = math.ceil((i + 1) / self.columns)

            self.plots_figure.add_trace(plot, col=col, row=row)

        self.app.layout = self._build_frame(self._build_layout())

        _register_callbacks(self.app)

    def _build_layout(self):
        self.plots_figure.update_layout(margin_t=30)

        return html.Div(
            [
                dropdown_header(
                    [self.selected_artifact],
                    self.selected_artifact,
                    "showing artifact ",
                    f" over {len(self.experiments)} experiments",
                    "artifact-plot",
                ),
                dcc.Loading(dcc.Graph(figure=self.plots_figure), color=light_blue),
            ],
            id="artifact-plot-layout-container",
        )


def _register_callbacks(app):
    @app.callback()
    def update():
        pass


def plot_artifacts(
    experiments,
    selected_artifact,
    x,
    y,
    columns=None,
    rows=None,
    stack_plots=False,
    dash_kwargs={},
    i_frame_kwargs={},
    run_server_kwargs={},
):
    if "height" not in i_frame_kwargs:
        i_frame_kwargs["height"] = "600px"

    return ArtifactPlot(
        experiments,
        selected_artifact,
        x,
        y,
        columns=columns,
        rows=rows,
        stack_plots=stack_plots,
        dash_kwargs=dash_kwargs,
    ).run_server_inline(
        i_frame_kwargs=i_frame_kwargs,
        **run_server_kwargs,
    )
