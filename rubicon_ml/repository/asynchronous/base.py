import asyncio
import os
import warnings

import fsspec

from rubicon_ml import domain
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import BaseRepository
from rubicon_ml.repository.utils import json


def connect(func):
    """This decorator connects to the underlying persistence layer
    before executing the decorated function if not already connected.
    """

    async def connect_first(*args, **kwargs):
        repo, *_ = args

        if not repo._is_connected:
            await repo._connect()
            repo._is_connected = True

        return await func(*args, **kwargs)

    return connect_first


def invalidate_cache(func):
    """This decorator clears the underlying `fsspec` filesystem's
    cache after executing the decorated function.

    Note
    ----
    This seems like an `fsspec` bug because the synchronous S3
    filesystem executes this cache invalidation on its own.
    """

    async def invalidate_cache_after(*args, **kwargs):
        result = await func(*args, **kwargs)

        repo, *_ = args
        repo.filesystem.invalidate_cache()

        return result

    return invalidate_cache_after


class AsynchronousBaseRepository(BaseRepository):
    """The asynchronous repository defines all the shared interactions
    between the various asynchronous Rubicon persistence options.

    `AsynchronousRepositoy` itself should never be used directly. Use
    one of the repositories that extends this class to persist
    Rubicon data:

        * `rubicon.repository.asynchronous.S3Repository`

    Parameters
    ----------
    root_dir : str
        Absolute path to the root directory to persist Rubicon
        data to.
    loop : asyncio.unix_events._UnixSelectorEventLoop, optional
        The event loop the asynchronous calling program is running on.
        It should not be necessary to provide this parameter in
        standard asynchronous operating cases.
    storage_options : dict, optional
        Additional keyword arguments that are passed directly to
        the underlying filesystem class.
    """

    PROTOCOL = None

    def __init__(self, root_dir, loop=None, **storage_options):
        self.root_dir = root_dir
        self.filesystem = fsspec.filesystem(
            self.PROTOCOL, asynchronous=True, loop=loop, **storage_options
        )

        self._is_connected = False

    async def _ls_directories_only(self, path):
        """Overrides `rubicon.repository.BaseRepository._ls_directories_only` to
        asynchronously return the names of all the directories at path `path`.
        """
        return [
            os.path.join(p.get("name"), "metadata.json")
            for p in await self.filesystem._ls(path, detail=True)
            if p.get("type", p.get("StorageClass")).lower() == "directory"
        ]

    async def _cat_paths(self, metadata_paths):
        """Cat `metadata_paths` to get the list of files to include.
        Ignore FileNotFoundErrors to avoid misc file errors, like hidden
        dotfiles.
        """
        files = []
        paths = await self.filesystem._cat(metadata_paths, on_error="return")
        for metadata in paths.values():
            if isinstance(metadata, FileNotFoundError):
                warning = f"{metadata} not found. Was this file unintentionally created?"
                warnings.warn(warning)
            else:
                files.append(metadata)

        return files

    async def _connect(self):
        """Asynchronously connect to the underlying persistence layer.

        Note
        ----
        This noop can be overridden by any repo that needs to issue
        a connect command.
        """
        pass

    # -------- Projects --------

    @connect
    @invalidate_cache
    async def create_project(self, project):
        """Overrides `rubicon.repository.BaseRepository.create_project`
        to asynchronously persist a project to the configured filesystem.

        Parameters
        ----------
        project : rubicon.domain.Project
            The project to persist.
        """
        project_metadata_path = self._get_project_metadata_path(project.name)

        if await self.filesystem._exists(project_metadata_path):
            raise RubiconException(f"A project with name '{project.name}' already exists.")

        await self._persist_domain(project, project_metadata_path)

    @connect
    async def get_project(self, project_name):
        """Overrides `rubicon.repository.BaseRepository.get_project` to
        asynchronously retrieve a project from the configured filesystem.

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
            project = json.loads(await self.filesystem._cat_file(project_metadata_path))
        except FileNotFoundError:
            raise RubiconException(f"No project with name '{project_name}' found.")

        return domain.Project(**project)

    @connect
    async def get_projects(self):
        """Overrides `rubicon.repository.BaseRepository.get_projects` to
        asynchronously get the list of projects from the filesystem.

        Returns
        -------
        list of rubicon.domain.Project
            The list of projects from the filesystem.
        """
        try:
            project_metadata_paths = await self._ls_directories_only(self.root_dir)
            projects = [
                domain.Project(**json.loads(metadata))
                for metadata in await self._cat_paths(project_metadata_paths)
            ]
        except FileNotFoundError:
            return []

        return projects

    # ------ Experiments -------

    @connect
    @invalidate_cache
    async def create_experiment(self, experiment):
        """Overrides `rubicon.repository.BaseRepository.create_experiment` to
        asynchronously persist an experiment to the configured filesystem.

        Parameters
        ----------
        experiment : rubicon.domain.Experiment
            The experiment to persist.
        """
        experiment_metadata_path = self._get_experiment_metadata_path(
            experiment.project_name, experiment.id
        )

        await self._persist_domain(experiment, experiment_metadata_path)

    @connect
    async def get_experiment(self, project_name, experiment_id):
        """Overrides `rubicon.repository.BaseRepository.get_experiment` to
        asynchronously retrieve an experiment from the configured filesystem.

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
            experiment = json.loads(await self.filesystem._cat_file(experiment_metadata_path))
        except FileNotFoundError:
            raise RubiconException(f"No experiment with id `{experiment_id}` found.")

        return domain.Experiment(**experiment)

    @connect
    async def get_experiments(self, project_name):
        """Overrides `rubicon.repository.BaseRepository.get_experiments` to
        asynchronously retrieve all experiments from the configured filesystem
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
            experiment_metadata_paths = await self._ls_directories_only(experiment_metadata_root)
            experiments = [
                domain.Experiment(**json.loads(metadata))
                for metadata in await self._cat_paths(experiment_metadata_paths)
            ]
        except FileNotFoundError:
            return []

        return experiments

    # -------- Features --------

    @connect
    @invalidate_cache
    async def create_feature(self, feature, project_name, experiment_id):
        """Overrides `rubicon.repository.BaseRepository.create_feature` to
        asynchronously persist a feature to the configured filesystem.

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

        if await self.filesystem._exists(feature_metadata_path):
            raise RubiconException(f"A feature with name '{feature.name}' already exists.")

        await self._persist_domain(feature, feature_metadata_path)

    @connect
    async def get_feature(self, project_name, experiment_id, feature_name):
        """Overrides `rubicon.repository.BaseRepository.get_feature` to
        asynchronously retrieve a feature from the configured filesystem.

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
            feature = json.loads(await self.filesystem._cat_file(feature_metadata_path))
        except FileNotFoundError:
            raise RubiconException(f"No feature with name '{feature_name}' found.")

        return domain.Feature(**feature)

    @connect
    async def get_features(self, project_name, experiment_id):
        """Overrides `rubicon.repository.BaseRepository.get_features` to
        asynchronously retrieve all features from the configured filesystem
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
            feature_metadata_paths = await self._ls_directories_only(feature_metadata_root)
            features = [
                domain.Feature(**json.loads(metadata))
                for metadata in await self._cat_paths(feature_metadata_paths)
            ]
        except FileNotFoundError:
            return []

        return features

    # ------- Parameters -------

    @connect
    @invalidate_cache
    async def create_parameter(self, parameter, project_name, experiment_id):
        """Overrides `rubicon.repository.BaseRepository.create_parameter` to
        asynchronously persist a parameter to the configured filesystem.

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

        if await self.filesystem._exists(parameter_metadata_path):
            raise RubiconException(f"A parameter with name '{parameter.name}' already exists.")

        await self._persist_domain(parameter, parameter_metadata_path)

    @connect
    async def get_parameter(self, project_name, experiment_id, parameter_name):
        """Overrides `rubicon.repository.BaseRepository.get_parameter` to
        asynchronously retrieve a parameter from the configured filesystem.

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
            parameter = json.loads(await self.filesystem._cat_file(parameter_metadata_path))
        except FileNotFoundError:
            raise RubiconException(f"No parameter with name '{parameter_name}' found.")

        return domain.Parameter(**parameter)

    @connect
    async def get_parameters(self, project_name, experiment_id):
        """Overrides `rubicon.repository.BaseRepository.get_parameters` to
        asynchronously retrieve all parameters from the configured filesystem
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
            parameter_metadata_paths = await self._ls_directories_only(parameter_metadata_root)
            parameters = [
                domain.Parameter(**json.loads(metadata))
                for metadata in await self._cat_paths(parameter_metadata_paths)
            ]
        except FileNotFoundError:
            return []

        return parameters

    # -------- Metrics ---------

    @connect
    @invalidate_cache
    async def create_metric(self, metric, project_name, experiment_id):
        """Overrides `rubicon.repository.BaseRepository.create_metric` to
        asynchronously persist a metric to the configured filesystem.

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

        if await self.filesystem._exists(metric_metadata_path):
            raise RubiconException(f"A metric with name '{metric.name}' already exists.")

        await self._persist_domain(metric, metric_metadata_path)

    @connect
    async def get_metric(self, project_name, experiment_id, metric_name):
        """Overrides `rubicon.repository.BaseRepository.get_metric` to
        asynchronously retrieve a metric from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project this metric belongs to.
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
            metric = json.loads(await self.filesystem._cat_file(metric_metadata_path))
        except FileNotFoundError:
            raise RubiconException(f"No metric with name '{metric_name}' found.")

        return domain.Metric(**metric)

    @connect
    async def get_metrics(self, project_name, experiment_id):
        """Overrides `rubicon.repository.BaseRepository.get_metrics` to
        asynchronously retrieve all metrics from the configured filesystem
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
            metric_metadata_paths = await self._ls_directories_only(metric_metadata_root)
            metrics = [
                domain.Metric(**json.loads(metadata))
                for metadata in await self._cat_paths(metric_metadata_paths)
            ]
        except FileNotFoundError:
            return []

        return metrics

    # ------- Dataframes -------

    @connect
    @invalidate_cache
    async def create_dataframe(self, dataframe, data, project_name, experiment_id=None):
        """Overrides `rubicon.repository.BaseRepository.create_dataframe` to
        asynchronously persist a dataframe to the configured filesystem.

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
        await self._persist_domain(dataframe, dataframe_metadata_path)

    @connect
    async def get_dataframe_metadata(self, project_name, dataframe_id, experiment_id=None):
        """Overrides `rubicon.repository.BaseRepository.get_dataframe_metadata`
        to asynchronously retrieve a dataframes's metadata from the configured
        filesystem.

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
            dataframe = json.loads(await self.filesystem._cat_file(dataframe_metadata_path))
        except FileNotFoundError:
            raise RubiconException(f"No dataframe with id `{dataframe_id}` found.")

        return domain.Dataframe(**dataframe)

    @connect
    async def get_dataframes_metadata(self, project_name, experiment_id=None):
        """Overrides `rubicon.repository.BaseRepository.get_dataframes_metadata`
        to asynchronously retrieve all dataframes' metadata from the configured
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
            dataframe_metadata_paths = await self._ls_directories_only(dataframe_metadata_root)
            dataframes = [
                domain.Dataframe(**json.loads(metadata))
                for metadata in await self._cat_paths(dataframe_metadata_paths)
            ]
        except FileNotFoundError:
            return []

        return dataframes

    @connect
    @invalidate_cache
    async def delete_dataframe(self, project_name, dataframe_id, experiment_id=None):
        """Overrides `rubicon.repository.BaseRepository.delete_dataframe` to
        asynchronously delete a dataframe from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the dataframe with ID
            `dataframe_id` is logged to.
        dataframe_id : str
            The ID of the dataframe to delete.
        experiment_id : str, optional
            The ID of the experiment the dataframe with ID
            `dataframe_id` is logged to. Dataframes do not
            need to belong to an experiment.
        """
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        try:
            await self.filesystem._rm(f"{dataframe_metadata_root}/{dataframe_id}", recursive=True)
        except FileNotFoundError:
            raise RubiconException(f"No dataframe with id `{dataframe_id}` found.")

    # ------- Artifacts --------

    @connect
    @invalidate_cache
    async def create_artifact(self, artifact, data, project_name, experiment_id=None):
        """Overrides `rubicon.repository.BaseRepository.create_artifact` to
        asynchronously persist an artifact to the configured filesystem.

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

        await asyncio.gather(
            self._persist_bytes(data, artifact_data_path),
            self._persist_domain(artifact, artifact_metadata_path),
        )

    @connect
    async def get_artifact_metadata(self, project_name, artifact_id, experiment_id=None):
        """Overrides `rubicon.repository.BaseRepository.get_artifact_metadata`
        to asynchronously retrieve an artifact's metadata from the configured
        filesystem.

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
            artifact = json.loads(await self.filesystem._cat_file(artifact_metadata_path))
        except FileNotFoundError:
            raise RubiconException(f"No artifact with id `{artifact_id}` found.")

        return domain.Artifact(**artifact)

    @connect
    async def get_artifacts_metadata(self, project_name, experiment_id=None):
        """Overrides `rubicon.repository.BaseRepository.get_artifacts_metadata`
        to asynchronously retrieve all artifacts' metadata from the configured
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
            artifact_metadata_paths = await self._ls_directories_only(artifact_metadata_root)
            artifacts = [
                domain.Artifact(**json.loads(metadata))
                for metadata in await self._cat_paths(artifact_metadata_paths)
            ]
        except FileNotFoundError:
            return []

        return artifacts

    @connect
    async def get_artifact_data(self, project_name, artifact_id, experiment_id=None):
        """Overrides `rubicon.repository.BaseRepository.get_artifact_data`
        to asynchronously retrieve an artifact's raw data.

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
            data = await self.filesystem._cat_file(artifact_data_path)
        except FileNotFoundError:
            raise RubiconException(f"No data for artifact with id `{artifact_id}` found.")

        return data

    @connect
    @invalidate_cache
    async def delete_artifact(self, project_name, artifact_id, experiment_id=None):
        """Overrides `rubicon.repository.BaseRepository.delete_artifact` to
        asynchronously delete an artifact from the configured filesystem.

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
            await self.filesystem._rm(f"{artifact_metadata_root}/{artifact_id}", recursive=True)
        except FileNotFoundError:
            raise RubiconException(f"No artifact with id `{artifact_id}` found.")

    # ---------- Tags ----------

    @connect
    @invalidate_cache
    async def add_tags(self, project_name, tags, experiment_id=None, dataframe_id=None):
        """Overrides `rubicon.repository.BaseRepository.add_tags
        to asynchronously persist tags to the configured filesystem.

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
        dataframe_id : str, optional
            The ID of the dataframe to apply the tags
            `tags` to.
        """
        tag_metadata_root = self._get_tag_metadata_root(project_name, experiment_id, dataframe_id)
        tag_metadata_path = f"{tag_metadata_root}/tags_{domain.utils.uuid.uuid4()}.json"

        await self._persist_domain({"added_tags": tags}, tag_metadata_path)

    @connect
    @invalidate_cache
    async def remove_tags(self, project_name, tags, experiment_id=None, dataframe_id=None):
        """Overrides `rubicon.repository.BaseRepository.remove_tags`
        to asynchronously delete tags from the configured filesystem.

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
        dataframe_id : str, optional
            The ID of the dataframe to delete the tags
            `tags` from.
        """
        tag_metadata_root = self._get_tag_metadata_root(project_name, experiment_id, dataframe_id)
        tag_metadata_path = f"{tag_metadata_root}/tags_{domain.utils.uuid.uuid4()}.json"

        await self._persist_domain({"removed_tags": tags}, tag_metadata_path)

    @connect
    async def get_tags(self, project_name, experiment_id=None, dataframe_id=None):
        """Overrides `rubicon.repository.BaseRepository.get_tags` to
        asynchronously retrieve tags from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the object to retrieve
            tags from belongs to.
        experiment_id : str, optional
            The ID of the experiment to retrieve tags from.
        dataframe_id : str, optional
            The ID of the dataframe to retrieve tags from.

        Returns
        -------
        list of dict
            A list of dictionaries with one key each,
            `added_tags` or `removed_tags`, where the
            value is a list of tag names that have been
            added to or removed from the specified object.
        """
        tag_metadata_root = self._get_tag_metadata_root(project_name, experiment_id, dataframe_id)

        all_paths = await self.filesystem._lsdir(tag_metadata_root)
        tag_paths = [p for p in all_paths if "/tags_" in p["name"]]
        if len(tag_paths) == 0:
            return []

        sorted_tag_paths = self._sort_tag_paths(tag_paths)

        raw_sorted_tag_data = await asyncio.gather(
            *[self.filesystem._cat_file(p) for _, p in sorted_tag_paths]
        )
        sorted_tag_data = [json.loads(t) for t in raw_sorted_tag_data]

        return sorted_tag_data
