import plotly.express as px
from dash import dcc, html

from rubicon_ml.viz.base import VizBase
from rubicon_ml.viz.colors import light_blue, plot_background_blue
from rubicon_ml.viz.common import dropdown_header


class DataframePlot(VizBase):
    def __init__(
        self,
        experiments,
        plotting_func,
        selected_dataframe,
        x,
        y,
        dash_kwargs={},
        plotting_func_kwargs={},
    ):
        super().__init__(dash_kwargs=dash_kwargs, dash_title="rubicon-ml: plot dataframes")

        self.experiments = experiments
        self.plotting_func = plotting_func
        self.plotting_func_kwargs = plotting_func_kwargs
        self.selected_dataframe = selected_dataframe
        self.x = x
        self.y = y

        self.data_df = None

        for experiment in self.experiments:
            dataframe = experiment.dataframes(tags=self.selected_dataframe)[0]

            data_df = dataframe.data
            data_df["experiment_id"] = experiment.id

            if self.data_df is None:
                self.data_df = data_df
            else:
                self.data_df = self.data_df.append(data_df)

            self.data_df = self.data_df.reset_index(drop=True)

        self.app.layout = self._build_frame(self._build_layout())

        _register_callbacks(self.app)

    def _build_layout(self):
        if "color" not in self.plotting_func_kwargs:
            self.plotting_func_kwargs["color"] = "experiment_id"
        if "color_discrete_sequence" not in self.plotting_func_kwargs:
            self.plotting_func_kwargs["color_discrete_sequence"] = px.colors.sample_colorscale(
                "Blues", len(self.experiments), low=0.33
            )

        figure = self.plotting_func(
            self.data_df,
            self.x,
            self.y,
            **self.plotting_func_kwargs,
        )
        figure.update_layout(margin_t=30, plot_bgcolor=plot_background_blue)

        for i, experiment in enumerate(self.experiments):
            figure.data[i].name = experiment.id[:7]

        return html.Div(
            [
                dropdown_header(
                    self.selected_dataframe,
                    self.selected_dataframe[0],
                    "showing dataframe ",
                    f" over {len(self.experiments)} experiments",
                    "dataframe-plot",
                ),
                dcc.Loading(dcc.Graph(figure=figure), color=light_blue),
            ],
            id="dataframe-plot-layout-container",
        )


def _register_callbacks(app):
    @app.callback()
    def update():
        pass


def plot_dataframes(
    experiments,
    selected_dataframe,
    x,
    y,
    plotting_func=px.line,
    dash_kwargs={},
    i_frame_kwargs={},
    plotting_func_kwargs={},
    run_server_kwargs={},
):
    if "height" not in i_frame_kwargs:
        i_frame_kwargs["height"] = "600px"

    return DataframePlot(
        experiments,
        plotting_func,
        selected_dataframe,
        x,
        y,
        dash_kwargs=dash_kwargs,
        plotting_func_kwargs=plotting_func_kwargs,
    ).run_server_inline(
        i_frame_kwargs=i_frame_kwargs,
        **run_server_kwargs,
    )
