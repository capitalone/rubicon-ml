import os
import warnings

import fsspec
import pandas as pd

from rubicon_ml import domain
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.utils import json, slugify


class BaseRepository:
    """The base repository defines all the shared interactions
    between the various Rubicon persistence options.

    `BaseRepositoy` itself should never be used directly. Use
    one of the repositories that extends this class to persist
    Rubicon data:

        * `rubicon.repository.MemoryRepository`
        * `rubicon.repository.LocalRepository`
        * `rubicon.repository.S3Repository`

    Parameters
    ----------
    root_dir : str
        Absolute path to the root directory to persist Rubicon
        data to.
    storage_options : dict, optional
        Additional keyword arguments that are passed directly to
        the underlying filesystem class.
    """

    def __init__(self, root_dir, **storage_options):
        self.filesystem = fsspec.filesystem(self.PROTOCOL, **storage_options)
        self.root_dir = root_dir.rstrip("/")

    def _ls_directories_only(self, path):
        """Returns the names of all the directories at path `path`."""
        directories = [
            os.path.join(p.get("name"), "metadata.json")
            for p in self.filesystem.ls(path, detail=True)
            if p.get("type", p.get("StorageClass")).lower() == "directory"
        ]

        return directories

    def _cat_paths(self, metadata_paths):
        """Cat `metadata_paths` to get the list of files to include.
        Ignore FileNotFoundErrors to avoid misc file errors, like hidden
        dotfiles.
        """
        files = []
        for path, metadata in self.filesystem.cat(metadata_paths, on_error="return").items():
            if isinstance(metadata, FileNotFoundError):
                warning = f"{path} not found. Was this file unintentionally created?"
                warnings.warn(warning)
            else:
                files.append(metadata)

        return files

    # -------- Projects --------

    def _get_project_metadata_path(self, project_name):
        """Returns the path of the project with name `project_name`'s
        metadata.
        """
        return f"{self.root_dir}/{slugify(project_name)}/metadata.json"

    def create_project(self, project):
        """Persist a project to the configured filesystem.

        Parameters
        ----------
        project : rubicon.domain.Project
            The project to persist.
        """
        project_metadata_path = self._get_project_metadata_path(project.name)

        if self.filesystem.exists(project_metadata_path):
            raise RubiconException(f"A project with name '{project.name}' already exists.")

        self._persist_domain(project, project_metadata_path)

    def get_project(self, project_name):
        """Retrieve a project from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project to retrieve.

        Returns
        -------
        rubicon.domain.Project
            The project with name `project_name`.
        """
        project_metadata_path = self._get_project_metadata_path(project_name)

        try:
            project = json.loads(self.filesystem.cat(project_metadata_path))
        except FileNotFoundError:
            raise RubiconException(f"No project with name '{project_name}' found.")

        return domain.Project(**project)

    def get_projects(self):
        """Get the list of projects from the filesystem.

        Returns
        -------
        list of rubicon.domain.Project
            The list of projects from the filesystem.
        """
        try:
            project_metadata_paths = self._ls_directories_only(self.root_dir)
            projects = [
                domain.Project(**json.loads(metadata))
                for metadata in self._cat_paths(project_metadata_paths)
            ]
            projects.sort(key=lambda p: p.created_at)
        except FileNotFoundError:
            return []

        return projects

    # ------ Experiments -------

    def _get_experiment_metadata_root(self, project_name):
        """Returns the experiments directory of the project with
        name `project_name`.
        """
        return f"{self.root_dir}/{slugify(project_name)}/experiments"

    def _get_experiment_metadata_path(self, project_name, experiment_id):
        """Returns the path of the experiment with ID
        `experiment_id`'s metadata.
        """
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/metadata.json"

    def create_experiment(self, experiment):
        """Persist an experiment to the configured filesystem.

        Parameters
        ----------
        experiment : rubicon.domain.Experiment
            The experiment to persist.
        """
        experiment_metadata_path = self._get_experiment_metadata_path(
            experiment.project_name, experiment.id
        )

        self._persist_domain(experiment, experiment_metadata_path)

    def get_experiment(self, project_name, experiment_id):
        """Retrieve an experiment from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment to retrieve.

        Returns
        -------
        rubicon.domain.Experiment
            The experiment with ID `experiment_id`.
        """
        experiment_metadata_path = self._get_experiment_metadata_path(project_name, experiment_id)

        try:
            open_file = self.filesystem.open(experiment_metadata_path)
        except FileNotFoundError:
            raise RubiconException(f"No experiment with id `{experiment_id}` found.")

        with open_file as f:
            experiment = json.load(f)

        return domain.Experiment(**experiment)

    def get_experiments(self, project_name):
        """Retrieve all experiments from the configured filesystem
        that belong to the project with name `project_name`.

        Parameters
        ----------
        project_name : str
            The name of the project to retrieve all experiments
            from.

        Returns
        -------
        list of rubicon.domain.Experiment
            The experiments logged to the project with name
            `project_name`.
        """
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        try:
            experiment_metadata_paths = self._ls_directories_only(experiment_metadata_root)
            experiments = [
                domain.Experiment(**json.loads(metadata))
                for metadata in self._cat_paths(experiment_metadata_paths)
            ]
            experiments.sort(key=lambda e: e.created_at)
        except FileNotFoundError:
            return []

        return experiments

    # ------- Artifacts --------

    def _get_artifact_metadata_root(self, project_name, experiment_id=None):
        """Returns the artifacts directory of the project with name
        `project_name` or experiment with ID `experiment_id`.
        """
        if experiment_id is not None:
            experiment_metadata_root = self._get_experiment_metadata_root(project_name)

            return f"{experiment_metadata_root}/{experiment_id}/artifacts"
        else:
            return f"{self.root_dir}/{slugify(project_name)}/artifacts"

    def _get_artifact_metadata_path(self, project_name, experiment_id, artifact_id):
        """Returns the path of the artifact with ID `artifact_id`'s
        metadata.
        """
        artifact_metadata_root = self._get_artifact_metadata_root(project_name, experiment_id)

        return f"{artifact_metadata_root}/{artifact_id}/metadata.json"

    def _get_artifact_data_path(self, project_name, experiment_id, artifact_id):
        """Returns the path of the artifact with ID `artifact_id`'s
        raw data.
        """
        artifact_metadata_root = self._get_artifact_metadata_root(project_name, experiment_id)

        return f"{artifact_metadata_root}/{artifact_id}/data"

    def create_artifact(self, artifact, data, project_name, experiment_id=None):
        """Persist an artifact to the configured filesystem.

        Parameters
        ----------
        artifact : rubicon.domain.Artifact
            The artifact to persist.
        data : bytes
            The raw data to persist as an artifact.
        project_name : str
            The name of the project this artifact belongs to.
        experiment_id : str, optional
            The ID of the experiment this artifact belongs to.
            Artifacts do not need to belong to an experiment.
        """
        artifact_metadata_path = self._get_artifact_metadata_path(
            project_name, experiment_id, artifact.id
        )
        artifact_data_path = self._get_artifact_data_path(project_name, experiment_id, artifact.id)

        self._persist_bytes(data, artifact_data_path)
        self._persist_domain(artifact, artifact_metadata_path)

    def get_artifact_metadata(self, project_name, artifact_id, experiment_id=None):
        """Retrieve an artifact's metadata from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the artifact with ID
            `artifact_id` is logged to.
        artifact_id : str
            The ID of the artifact to retrieve.
        experiment_id : str, optional
            The ID of the experiment the artifact with ID
            `artifact_id` is logged to. Artifacts do not
            need to belong to an experiment.

        Returns
        -------
        rubicon.domain.Artifact
            The artifact with ID `artifact_id`.
        """
        artifact_metadata_path = self._get_artifact_metadata_path(
            project_name, experiment_id, artifact_id
        )

        try:
            open_file = self.filesystem.open(artifact_metadata_path)
        except FileNotFoundError:
            raise RubiconException(f"No artifact with id `{artifact_id}` found.")

        with open_file as f:
            artifact = json.load(f)

        return domain.Artifact(**artifact)

    def get_artifacts_metadata(self, project_name, experiment_id=None):
        """Retrieve all artifacts' metadata from the configured
        filesystem that belong to the specified object.

        Parameters
        ----------
        project_name : str
            The name of the project to retrieve all artifacts
            from.
        experiment_id : str, optional
            The ID of the experiment to retrieve all artifacts
            from. Artifacts do not need to belong to an
            experiment.

        Returns
        -------
        list of rubicon.domain.Artifact
            The artifacts logged to the specified object.
        """
        artifact_metadata_root = self._get_artifact_metadata_root(project_name, experiment_id)

        try:
            artifact_metadata_paths = self._ls_directories_only(artifact_metadata_root)
            artifacts = [
                domain.Artifact(**json.loads(metadata))
                for metadata in self._cat_paths(artifact_metadata_paths)
            ]
            artifacts.sort(key=lambda a: a.created_at)
        except FileNotFoundError:
            return []

        return artifacts

    def get_artifact_data(self, project_name, artifact_id, experiment_id=None):
        """Retrieve an artifact's raw data.

        Parameters
        ----------
        project_name : str
            The name of the project the artifact with ID
            `artifact_id` is logged to.
        artifact_id : str
            The ID of the artifact to retrieve data from.
        experiment_id : str, optional
            The ID of the experiment the artifact with ID
            `artifact_id` is logged to. Artifacts do not
            need to belong to an experiment.

        Returns
        -------
        bytes
            The artifact with ID `artifact_id`'s raw data.
        """
        artifact_data_path = self._get_artifact_data_path(project_name, experiment_id, artifact_id)

        try:
            open_file = self.filesystem.open(artifact_data_path, "rb")
        except FileNotFoundError:
            raise RubiconException(f"No data for artifact with id `{artifact_id}` found.")

        return open_file.read()

    def delete_artifact(self, project_name, artifact_id, experiment_id=None):
        """Delete an artifact from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the artifact with ID
            `artifact_id` is logged to.
        artifact_id : str
            The ID of the artifact to delete.
        experiment_id : str, optional
            The ID of the experiment the artifact with ID
            `artifact_id` is logged to. Artifacts do not
            need to belong to an experiment.
        """
        artifact_metadata_root = self._get_artifact_metadata_root(project_name, experiment_id)

        try:
            self.filesystem.rm(f"{artifact_metadata_root}/{artifact_id}", recursive=True)
        except FileNotFoundError:
            raise RubiconException(f"No artifact with id `{artifact_id}` found.")

    # ------- Dataframes -------

    def _get_dataframe_metadata_root(self, project_name, experiment_id=None):
        """Returns the dataframes directory of the project with name
        `project_name` or experiment with ID `experiment_id`.
        """
        if experiment_id is not None:
            experiment_metadata_root = self._get_experiment_metadata_root(project_name)

            return f"{experiment_metadata_root}/{experiment_id}/dataframes"
        else:
            return f"{self.root_dir}/{slugify(project_name)}/dataframes"

    def _get_dataframe_metadata_path(self, project_name, experiment_id, dataframe_id):
        """Returns the path of the dataframe with ID `dataframe_id`'s
        metadata.
        """
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        return f"{dataframe_metadata_root}/{dataframe_id}/metadata.json"

    def _get_dataframe_data_path(self, project_name, experiment_id, dataframe_id):
        """Returns the path of the dataframe with ID `dataframe_id`'s
        raw data.
        """
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        return f"{dataframe_metadata_root}/{dataframe_id}/data"

    def _persist_dataframe(self, df, path):
        """Persists the dataframe `df` to the configured filesystem.

        Note
        ----
        `dask` dataframes will automatically be split into chunks by `dask.dataframe.to_parquet`.
        `pandas` dataframes, however, will be saved as a single file with the hope that users
        would leverage dask for large dataframes.
        """
        if isinstance(df, pd.DataFrame):
            self.filesystem.mkdir(path, parents=True, exist_ok=True)
            path = f"{path}/data.parquet"

        df.to_parquet(path, engine="pyarrow")

    def _read_dataframe(self, path, df_type="pandas"):
        """Reads the dataframe `df` from the configured filesystem."""
        df = None
        acceptable_types = ["pandas", "dask"]
        if df_type not in acceptable_types:
            raise ValueError(f"`df_type` must be one of {acceptable_types}")

        if df_type == "pandas":
            path = f"{path}/data.parquet"
            df = pd.read_parquet(path, engine="pyarrow")
        else:
            try:
                from dask import dataframe as dd
            except ImportError:
                raise RubiconException(
                    "`rubicon_ml` requires `dask` to be installed in the current environment "
                    "to read dataframes with `df_type`='dask'. `pip install dask[dataframe]` "
                    "or `conda install dask` to continue."
                )

            df = dd.read_parquet(path, engine="pyarrow")

        return df

    def create_dataframe(self, dataframe, data, project_name, experiment_id=None):
        """Persist a dataframe to the configured filesystem.

        Parameters
        ----------
        dataframe : rubicon.domain.Dataframe
            The dataframe to persist.
        data : dask.dataframe.DataFrame or pandas.DataFrame
            The raw data to persist as a dataframe. All
            dataframes are persisted as `dask` dataframes.
        project_name : str
            The name of the project this dataframe belongs to.
        experiment_id : str, optional
            The ID of the experiment this dataframe belongs to.
            Dataframes do not need to belong to an experiment.
        """
        dataframe_metadata_path = self._get_dataframe_metadata_path(
            project_name, experiment_id, dataframe.id
        )
        dataframe_data_path = self._get_dataframe_data_path(
            project_name, experiment_id, dataframe.id
        )

        self._persist_dataframe(data, dataframe_data_path)
        self._persist_domain(dataframe, dataframe_metadata_path)

    def get_dataframe_metadata(self, project_name, dataframe_id, experiment_id=None):
        """Retrieve a dataframes's metadata from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the dataframe with ID
            `dataframe_id` is logged to.
        dataframe_id : str
            The ID of the dataframe to retrieve.
        experiment_id : str, optional
            The ID of the experiment the dataframe with ID
            `dataframe_id` is logged to. Dataframes do not
            need to belong to an experiment.

        Returns
        -------
        rubicon.domain.Dataframe
            The dataframe with ID `dataframe_id`.
        """
        dataframe_metadata_path = self._get_dataframe_metadata_path(
            project_name, experiment_id, dataframe_id
        )

        try:
            open_file = self.filesystem.open(dataframe_metadata_path)
        except FileNotFoundError:
            raise RubiconException(f"No dataframe with id `{dataframe_id}` found.")

        with open_file as f:
            dataframe = json.load(f)

        return domain.Dataframe(**dataframe)

    def get_dataframes_metadata(self, project_name, experiment_id=None):
        """Retrieve all dataframes' metadata from the configured
        filesystem that belong to the specified object.

        Parameters
        ----------
        project_name : str
            The name of the project to retrieve all dataframes
            from.
        experiment_id : str, optional
            The ID of the experiment to retrieve all dataframes
            from. Dataframes do not need to belong to an
            experiment.

        Returns
        -------
        list of rubicon.domain.Dataframe
            The dataframes logged to the specified object.
        """
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        try:
            dataframe_metadata_paths = self._ls_directories_only(dataframe_metadata_root)
            dataframes = [
                domain.Dataframe(**json.loads(metadata))
                for metadata in self._cat_paths(dataframe_metadata_paths)
            ]
            dataframes.sort(key=lambda d: d.created_at)
        except FileNotFoundError:
            return []

        return dataframes

    def get_dataframe_data(self, project_name, dataframe_id, experiment_id=None, df_type="pandas"):
        """Retrieve a dataframe's raw data.

        Parameters
        ----------
        project_name : str
            The name of the project the dataframe with ID
            `dataframe_id` is logged to.
        dataframe_id : str
            The ID of the dataframe to retrieve data from.
        experiment_id : str, optional
            The ID of the experiment the dataframe with ID
            `artifact_id` is logged to. Dataframes do not
            need to belong to an experiment.
        df_type : str, optional
            The type of dataframe. Can be either `pandas` or `dask`.

        Returns
        -------
        dask.dataframe.DataFrame
            The dataframe with ID `dataframe_id`'s raw data.
        """
        dataframe_data_path = self._get_dataframe_data_path(
            project_name, experiment_id, dataframe_id
        )

        try:
            df = self._read_dataframe(dataframe_data_path, df_type)
        except FileNotFoundError:
            raise RubiconException(
                f"No data for dataframe with id `{dataframe_id}` found. This might have "
                "happened if you forgot to set `df_type='dask'` when trying to read a `dask` dataframe."
            )

        return df

    def delete_dataframe(self, project_name, dataframe_id, experiment_id=None):
        """Delete a dataframe from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the dataframe with ID
            `dataframe_id` is logged to.
        dataframe_id : str
            The ID of the dataframe to delete.
        experiment_id : str, optional
            The ID of the experiment the dataframe with ID
            `artifact_id` is logged to. Dataframes do not
            need to belong to an experiment.
        """
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        try:
            self.filesystem.rm(f"{dataframe_metadata_root}/{dataframe_id}", recursive=True)
        except FileNotFoundError:
            raise RubiconException(f"No dataframe with id `{dataframe_id}` found.")

    # -------- Features --------

    def _get_feature_metadata_root(self, project_name, experiment_id):
        """Returns the features directory of the experiment with
        ID `experiment_id`.
        """
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/features"

    def _get_feature_metadata_path(self, project_name, experiment_id, feature_name):
        """Returns the path of the feature with name `feature_name`'s
        metadata.
        """
        feature_metadata_root = self._get_feature_metadata_root(project_name, experiment_id)

        return f"{feature_metadata_root}/{slugify(feature_name)}/metadata.json"

    def create_feature(self, feature, project_name, experiment_id):
        """Persist a feature to the configured filesystem.

        Parameters
        ----------
        feature : rubicon.domain.Feature
            The feature to persist.
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment this feature belongs to.
        """
        feature_metadata_path = self._get_feature_metadata_path(
            project_name, experiment_id, feature.name
        )

        if self.filesystem.exists(feature_metadata_path):
            raise RubiconException(f"A feature with name '{feature.name}' already exists.")

        self._persist_domain(feature, feature_metadata_path)

    def get_feature(self, project_name, experiment_id, feature_name):
        """Retrieve a feature from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment the feature with name
            `feature_name` is logged to.
        feature_name : str
            The name of the feature to retrieve.

        Returns
        -------
        rubicon.domain.Feature
            The feature with name `feature_name`.
        """
        feature_metadata_path = self._get_feature_metadata_path(
            project_name, experiment_id, feature_name
        )

        try:
            open_file = self.filesystem.open(feature_metadata_path)
        except FileNotFoundError:
            raise RubiconException(f"No feature with name '{feature_name}' found.")

        with open_file as f:
            feature = json.load(f)

        return domain.Feature(**feature)

    def get_features(self, project_name, experiment_id):
        """Retrieve all features from the configured filesystem
        that belong to the experiment with ID `experiment_id`.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment to retrieve all features
            from.

        Returns
        -------
        list of rubicon.domain.Feature
            The features logged to the experiment with ID
            `experiment_id`.
        """
        feature_metadata_root = self._get_feature_metadata_root(project_name, experiment_id)

        try:
            feature_metadata_paths = self._ls_directories_only(feature_metadata_root)
            features = [
                domain.Feature(**json.loads(metadata))
                for metadata in self._cat_paths(feature_metadata_paths)
            ]
            features.sort(key=lambda f: f.created_at)
        except FileNotFoundError:
            return []

        return features

    # -------- Metrics ---------

    def _get_metric_metadata_root(self, project_name, experiment_id):
        """Returns the metrics directory of the experiment with
        ID `experiment_id`.
        """
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/metrics"

    def _get_metric_metadata_path(self, project_name, experiment_id, metric_name):
        """Returns the path of the metric with name `metric_name`'s
        metadata.
        """
        metric_metadata_root = self._get_metric_metadata_root(project_name, experiment_id)

        return f"{metric_metadata_root}/{slugify(metric_name)}/metadata.json"

    def create_metric(self, metric, project_name, experiment_id):
        """Persist a metric to the configured filesystem.

        Parameters
        ----------
        metric : rubicon.domain.Metric
            The metric to persist.
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment this metric belongs to.
        """
        metric_metadata_path = self._get_metric_metadata_path(
            project_name, experiment_id, metric.name
        )

        if self.filesystem.exists(metric_metadata_path):
            raise RubiconException(f"A metric with name '{metric.name}' already exists.")

        self._persist_domain(metric, metric_metadata_path)

    def get_metric(self, project_name, experiment_id, metric_name):
        """Retrieve a metric from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment the metric with name
            `metric_name` is logged to.
        metric_name : str
            The name of the metric to retrieve.

        Returns
        -------
        rubicon.domain.Metric
            The metric with name `metric_name`.
        """
        metric_metadata_path = self._get_metric_metadata_path(
            project_name, experiment_id, metric_name
        )

        try:
            open_file = self.filesystem.open(metric_metadata_path)
        except FileNotFoundError:
            raise RubiconException(f"No metric with name '{metric_name}' found.")

        with open_file as f:
            metric = json.load(f)

        return domain.Metric(**metric)

    def get_metrics(self, project_name, experiment_id):
        """Retrieve all metrics from the configured filesystem
        that belong to the experiment with ID `experiment_id`.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment to retrieve all metrics
            from.

        Returns
        -------
        list of rubicon.domain.Metric
            The metrics logged to the experiment with ID
            `experiment_id`.
        """
        metric_metadata_root = self._get_metric_metadata_root(project_name, experiment_id)

        try:
            metric_metadata_paths = self._ls_directories_only(metric_metadata_root)
            metrics = [
                domain.Metric(**json.loads(metadata))
                for metadata in self._cat_paths(metric_metadata_paths)
            ]
            metrics.sort(key=lambda m: m.created_at)
        except FileNotFoundError:
            return []

        return metrics

    # ------- Parameters -------

    def _get_parameter_metadata_root(self, project_name, experiment_id):
        """Returns the parameters directory of the experiment with
        ID `experiment_id`.
        """
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/parameters"

    def _get_parameter_metadata_path(self, project_name, experiment_id, parameter_name):
        """Returns the path of the parameter with name `parameter_name`'s
        metadata.
        """
        parameter_metadata_root = self._get_parameter_metadata_root(project_name, experiment_id)

        return f"{parameter_metadata_root}/{slugify(parameter_name)}/metadata.json"

    def create_parameter(self, parameter, project_name, experiment_id):
        """Persist a parameter to the configured filesystem.

        Parameters
        ----------
        parameter : rubicon.domain.Parameter
            The parameter to persist.
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment this parameter belongs to.
        """
        parameter_metadata_path = self._get_parameter_metadata_path(
            project_name, experiment_id, parameter.name
        )

        if self.filesystem.exists(parameter_metadata_path):
            raise RubiconException(f"A parameter with name '{parameter.name}' already exists.")

        self._persist_domain(parameter, parameter_metadata_path)

    def get_parameter(self, project_name, experiment_id, parameter_name):
        """Retrieve a parameter from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project this parameter belongs to.
        experiment_id : str
            The ID of the experiment the parameter with name
            `parameter_name` is logged to.
        parameter_name : str
            The name of the parameter to retrieve.

        Returns
        -------
        rubicon.domain.Parameter
            The parameter with name `parameter_name`.
        """
        parameter_metadata_path = self._get_parameter_metadata_path(
            project_name, experiment_id, parameter_name
        )

        try:
            open_file = self.filesystem.open(parameter_metadata_path)
        except FileNotFoundError:
            raise RubiconException(f"No parameter with name '{parameter_name}' found.")

        with open_file as f:
            parameter = json.load(f)

        return domain.Parameter(**parameter)

    def get_parameters(self, project_name, experiment_id):
        """Retrieve all parameters from the configured filesystem
        that belong to the experiment with ID `experiment_id`.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment to retrieve all parameters
            from.

        Returns
        -------
        list of rubicon.domain.Parameter
            The parameters logged to the experiment with ID
            `experiment_id`.
        """
        parameter_metadata_root = self._get_parameter_metadata_root(project_name, experiment_id)

        try:
            parameter_metadata_paths = self._ls_directories_only(parameter_metadata_root)
            parameters = [
                domain.Parameter(**json.loads(metadata))
                for metadata in self._cat_paths(parameter_metadata_paths)
            ]
            parameters.sort(key=lambda p: p.created_at)
        except FileNotFoundError:
            return []

        return parameters

    # ---------- Tags ----------

    def _get_tag_metadata_root(
        self, project_name, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Returns the directory to write tags to."""
        get_metadata_root_lookup = {
            "Artifact": self._get_artifact_metadata_root,
            "Dataframe": self._get_dataframe_metadata_root,
            "Experiment": self._get_experiment_metadata_root,
            "Metric": self._get_metric_metadata_root,
            "Feature": self._get_feature_metadata_root,
            "Parameter": self._get_parameter_metadata_root,
        }

        try:
            get_metadata_root = get_metadata_root_lookup[entity_type]
        except KeyError:
            raise ValueError("`experiment_id` and `entity_identifier` can not both be `None`.")

        if entity_type == "Experiment":
            experiment_metadata_root = get_metadata_root(project_name)

            return f"{experiment_metadata_root}/{experiment_id}"
        else:
            entity_metadata_root = get_metadata_root(project_name, experiment_id)

            # We want to slugify the names of Metrics, Features, and Parameters- not Artifacts, Dataframes, or Experiments
            if entity_type in ["Metric", "Feature", "Parameter"]:
                entity_identifier = slugify(entity_identifier)
            return f"{entity_metadata_root}/{entity_identifier}"

    def add_tags(
        self, project_name, tags, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Persist tags to the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the object to tag
            belongs to.
        tags : list of str
            The tag values to persist.
        experiment_id : str, optional
            The ID of the experiment to apply the tags
            `tags` to.
        entity_identifier : str, optional
            The ID or name of the entity to apply the tags
            `tags` to.
        entity_type : str, optional
            The name of the entity's type as returned by
            `entity_cls.__class__.__name__`.
        """
        tag_metadata_root = self._get_tag_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )
        tag_metadata_path = f"{tag_metadata_root}/tags_{domain.utils.uuid.uuid4()}.json"

        self._persist_domain({"added_tags": tags}, tag_metadata_path)

    def remove_tags(
        self, project_name, tags, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Delete tags from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the object to delete
            tags from belongs to.
        tags : list of str
            The tag values to delete.
        experiment_id : str, optional
            The ID of the experiment to delete the tags
            `tags` from.
        entity_identifier : str, optional
            The ID or name of the entity to apply the tags
            `tags` to.
        entity_type : str, optional
            The name of the entity's type as returned by
            `entity_cls.__class__.__name__`.
        """
        tag_metadata_root = self._get_tag_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )
        tag_metadata_path = f"{tag_metadata_root}/tags_{domain.utils.uuid.uuid4()}.json"

        self._persist_domain({"removed_tags": tags}, tag_metadata_path)

    def _sort_tag_paths(self, tag_paths):
        """Sorts the paths in `tags_paths` by when they were
        created.
        """
        if isinstance(tag_paths, dict):
            tag_paths = tag_paths.values()

        tag_paths_with_timestamps = [
            (t.get("created", t.get("LastModified")), t.get("name")) for t in tag_paths
        ]
        tag_paths_with_timestamps.sort()

        return tag_paths_with_timestamps

    def get_tags(self, project_name, experiment_id=None, entity_identifier=None, entity_type=None):
        """Retrieve tags from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the object to retrieve
            tags from belongs to.
        experiment_id : str, optional
            The ID of the experiment to retrieve tags from.
        entity_identifier : str, optional
            The ID or name of the entity to apply the tags
            `tags` to.
        entity_type : str, optional
            The name of the entity's type as returned by
            `entity_cls.__class__.__name__`.

        Returns
        -------
        list of dict
            A list of dictionaries with one key each,
            `added_tags` or `removed_tags`, where the
            value is a list of tag names that have been
            added to or removed from the specified object.
        """
        tag_metadata_root = self._get_tag_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )
        tag_metadata_glob = f"{tag_metadata_root}/tags_*.json"

        tag_paths = self.filesystem.glob(tag_metadata_glob, detail=True)
        if len(tag_paths) == 0:
            return []

        sorted_tag_paths = self._sort_tag_paths(tag_paths)

        tag_data = self.filesystem.cat([p for _, p in sorted_tag_paths])
        sorted_tag_data = [json.loads(tag_data[p]) for _, p in sorted_tag_paths]

        return sorted_tag_data
