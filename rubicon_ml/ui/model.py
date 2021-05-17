import asyncio

import numpy as np
import pandas as pd

from rubicon_ml import Rubicon
from rubicon_ml.client.asynchronous import Rubicon as AsynRubicon


class RubiconModel:
    """The data model for the Rubicon Dashboard.

    Parameters
    ----------
    persistence : str
        The persistence type. Can be one of ["filesystem", "memory"].
    root_dir : str, optional
        Absolute or relative filepath of the root directory holding Rubicon data.
        Use absolute path for best performance. Defaults to the local filesystem.
        Prefix with s3:// to use s3 instead.
    storage_options : dict, optional
        Additional keyword arguments specific to the protocol being chosen. They
        are passed directly to the underlying filesystem class.
    """

    def __init__(self, persistence, root_dir, **storage_options):
        self._rubicon_cls = AsynRubicon if root_dir and root_dir.startswith("s3") else Rubicon
        self._rubicon = self._rubicon_cls(persistence, root_dir, **storage_options)

        self._projects = []
        self._selected_project = None

        self._experiment_table_dfs = {}
        self._experiment_comparison_root_dfs = {}

    def _maybe_run_async(self, rubicon_func, *args, **kwargs):
        """Runs `rubicon_func` asynchronously if logging to S3,
        synchronously otherwise.
        """
        if self._rubicon_cls == AsynRubicon:
            return asyncio.run_coroutine_threadsafe(
                rubicon_func(*args, **kwargs), loop=self._rubicon.repository.filesystem.loop
            ).result()
        else:
            return rubicon_func(*args, **kwargs)

    def get_anchor_options(self, commit_hash):
        """Returns the list of anchor options for the experiments in
        the group with commit hash `commit_hash`.

        Parameters
        ----------
        commit_hash : str
            The commit hash of the group of experiments to get anchor
            options for.

        Returns
        -------
        list of dict
            The anchor options.
        """
        anchors = list(self._experiment_comparison_root_dfs[commit_hash].columns)

        return [{"label": a, "value": a} for a in anchors]

    def get_dimensions(self, commit_hash, selected_experiment_ids, hidden_columns, anchor):
        """Collects and cleans the data necessary to render the Dash parallel
        coordinates plot representing an experiment comparison plot.

        Parameters
        ----------
        commit_hash : str
            The commit hash of the group of experiments to get dimensions for.
        selected_experiment_ids : list of str
            The experiment ID's of the group of experiments to get dimensions
            for. Not all experiments in a group have to be shown on the
            comparison plot.
        hidden_columns : List of columns ids of the columns that are currently
            hidden.
        anchor : str
            The currently selected anchor metric. The anchor metric should be
            represented by the last dimension.

        Returns
        -------
            pandas.Series
                The data corresponding to the selected anchor (to pass to the
                plot as a colorscale).
            list of dict
                The dimensions to render on the plot.
        """
        dimensions = []

        root_df = self.get_experiment_comparison_root_df(commit_hash)
        if hidden_columns is not None:
            # ignore errors since some of the hidden cols could already be dropped, like `id`
            root_df = root_df.drop(columns=hidden_columns, errors="ignore")
        experiment_comparison_df = root_df.loc[selected_experiment_ids]
        experiment_comparison_df = experiment_comparison_df.compute().convert_dtypes()

        for column in experiment_comparison_df.columns:
            clean_column = experiment_comparison_df[column].dropna()

            if isinstance(experiment_comparison_df[column].dtype, pd.StringDtype):
                unique_values = clean_column.unique()
                values = clean_column.map(lambda value: np.where(unique_values == value)[0][0])

                dimension = dict(
                    label=column,
                    ticktext=unique_values,
                    tickvals=list(range(0, len(unique_values))),
                    values=values,
                )
            elif isinstance(experiment_comparison_df[column].dtype, pd.BooleanDtype):
                values = clean_column.astype(int)

                dimension = dict(
                    label=column,
                    ticktext=["False", "True"],
                    tickvals=[0, 1],
                    values=values,
                )
            else:
                values = clean_column

                dimension = dict(label=column, values=values)

            if column == anchor:
                anchor_dimension = dimension
                anchor_data = values
            else:
                dimensions.append(dimension)

        dimensions.append(anchor_dimension)

        return anchor_data, dimensions

    def get_experiment_table_df(self, commit_hash):
        """Returns the experiment table dataframe for the experiments in
        the group with commit hash `commit_hash`.

        Parameters
        ----------
        commit_hash : str
            The commit hash of the group of experiments to get the
            experiment table dataframe for.

        Returns
        -------
        dask.DataFrame
            The dataframe representing the experiment table.
        """
        return self._experiment_table_dfs[commit_hash]

    def get_experiment_comparison_root_df(self, commit_hash):
        """Returns the experiment comparison dataframe for the experiments
        in the group with commit hash `commit_hash`.

        Parameters
        ----------
        commit_hash : str
            The commit hash of the group of experiments to get the
            experiment comparison dataframe for.

        Returns
        -------
        dask.DataFrame
            The dataframe representing the experiment comparison plot.
        """
        return self._experiment_comparison_root_dfs[commit_hash]

    def get_model_names(self, commit_hash):
        """Returns the model name(s) of the experiments in the group with
        commit hash `commit_hash`.
        """
        return list(self._experiment_table_dfs[commit_hash]["model_name"].unique())

    def update_projects(self):
        """Updates the list of available projects and the list
        of dropdown options representing those projects.
        """
        self._projects = self._maybe_run_async(self._rubicon.projects)

    def update_selected_project(self, selected_project_name):
        """Updates the seleted project and loads the relevant data
        for that project.

        Parameters
        ----------
        selected_project_name : str
            The name of the project to set as selected and load data for.
        """
        self._selected_project, *_ = [p for p in self._projects if p.name == selected_project_name]

        grouped_experiment_dfs = self._maybe_run_async(
            self._selected_project.to_dask_df, group_by="commit_hash"
        )

        et_commit_hash_display_length = 7
        et_drop_columns = ["description", "name", "created_at"]
        et_type_castings = {"tags": "str"}

        ec_drop_columns = ["commit_hash", "model_name", "tags"]
        ec_index = "id"

        # no experiments logged to the project yet
        if len(grouped_experiment_dfs.items()) == 0:
            self._experiment_table_dfs = {}

        for commit_hash, df in grouped_experiment_dfs.items():
            experiment_table_df = df.drop(columns=et_drop_columns)
            experiment_table_df = experiment_table_df.astype(et_type_castings)
            experiment_table_df["commit_hash"] = experiment_table_df["commit_hash"].map(
                lambda ch: ch[:et_commit_hash_display_length] if ch is not None else None
            )

            self._experiment_table_dfs[commit_hash] = experiment_table_df

            experiment_comparison_df = experiment_table_df.set_index(ec_index)
            experiment_comparison_df = experiment_comparison_df.drop(columns=ec_drop_columns)

            self._experiment_comparison_root_dfs[commit_hash] = experiment_comparison_df

    @property
    def project_options(self):
        """The available project options.

        Returns
        -------
        list of dict
            The project options.
        """
        return [{"label": p.name, "value": p.name} for p in self._projects]

    @property
    def selected_project(self):
        """The currently selected Rubicon project.

        Returns
        -------
        rubicon.Project or rubicon.client.asynchronous.Project
        """
        return self._selected_project
