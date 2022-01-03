import dash_bootstrap_components as dbc
import plotly.express as px
from dash import dcc, html
from dash.dependencies import Input, Output

from rubicon_ml.viz.base import VizBase
from rubicon_ml.viz.colors import (
    get_rubicon_colorscale,
    light_blue,
    plot_background_blue,
)


class DataframePlot(VizBase):
    def __init__(
        self,
        dataframe_name,
        experiments=None,
        plotting_func=px.line,
        plotting_func_kwargs={},
        x=None,
        y=None,
    ):
        super().__init__(dash_title="plot dataframes")

        self.dataframe_name = dataframe_name
        self.experiments = experiments
        self.plotting_func = plotting_func
        self.plotting_func_kwargs = plotting_func_kwargs
        self.x = x
        self.y = y

    @property
    def layout(self):
        header_text = (
            f"showing dataframe '{self.dataframe_name}' "
            f"over {len(self.experiments)} experiment"
            f"{'s' if len(self.experiments) != 1 else ''}"
        )

        return html.Div(
            [
                html.Div(id="dummy-callback-trigger"),
                dbc.Row(
                    html.H5(header_text, id="header-text"),
                    className="header-row",
                ),
                dcc.Loading(dcc.Graph(id="dataframe-plot"), color=light_blue),
            ],
            id="dataframe-plot-layout-container",
        )

    def load_experiment_data(self):
        self.data_df = None

        for experiment in self.experiments:
            dataframe = experiment.dataframe(name=self.dataframe_name)

            data_df = dataframe.data
            data_df["experiment_id"] = experiment.id

            if self.x is None:
                self.x = data_df.columns[0]

            if self.y is None:
                self.y = data_df.columns[1]

            if self.data_df is None:
                self.data_df = data_df
            else:
                self.data_df = self.data_df.append(data_df)

            self.data_df = self.data_df.reset_index(drop=True)

        if "color" not in self.plotting_func_kwargs:
            self.plotting_func_kwargs["color"] = "experiment_id"
        if "color_discrete_sequence" not in self.plotting_func_kwargs:
            self.plotting_func_kwargs["color_discrete_sequence"] = get_rubicon_colorscale(
                len(self.experiments),
            )

    def register_callbacks(self, link_experiment_table=False):
        outputs = [
            Output("dataframe-plot", "figure"),
            Output("header-text", "children"),
        ]
        inputs = [Input("dummy-callback-trigger", "children")]
        states = []

        if link_experiment_table:
            inputs.append(
                Input("experiment-table", "derived_virtual_selected_row_ids"),
            )

        @self.app.callback(outputs, inputs, states)
        def update_dataframe_plot(*args):
            if link_experiment_table:
                selected_row_ids = args[-1]
                selected_row_ids = selected_row_ids if selected_row_ids else []
            else:
                selected_row_ids = [e.id for e in self.experiments]

            df_figure = self.plotting_func(
                self.data_df[self.data_df["experiment_id"].isin(selected_row_ids)],
                self.x,
                self.y,
                **self.plotting_func_kwargs,
            )
            df_figure.update_layout(margin_t=30, plot_bgcolor=plot_background_blue)

            for i in range(len(df_figure.data)):
                df_figure.data[i].name = df_figure.data[i].name[:7]

            header_text = (
                f"showing dataframe '{self.dataframe_name}' "
                f"over {len(selected_row_ids)} experiment"
                f"{'s' if len(selected_row_ids) != 1 else ''}"
            )

            return df_figure, header_text
