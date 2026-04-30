import logging
import os
import tempfile
import time
from json import JSONDecodeError
from typing import Any, Dict, List, Optional, Type

import pandas as pd

from rubicon_ml import domain
from rubicon_ml.domain.utils.uuid import uuid4
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.fsspec import FsspecRepository
from rubicon_ml.repository.utils import json, slugify

LOGGER = logging.getLogger(__name__)


class WandBRepository(FsspecRepository):
    """Repository for reading and writing rubicon-ml data to Weights & Biases.

    The `WandBRepository` is experimental and may contain breaking changes in future versions.
    If you encounter any bugs or missing features, open an issue on GitHub.

    All rubicon-ml domain objects are serialized and stored as private W&B Config, e.g. the
    `_rubicon_experiment_metadata` key would contain the complete representation of the logged
    experiment. While all non-project entities below log this metadata to W&B Config, some
    log additional information as follows:
    - rubicon-ml Projects → W&B Projects
    - rubicon-ml Experiments → W&B Runs
    - rubicon-ml Feature importances → W&B Metrics, named by rubicon-ml feature name
    - rubicon-ml Metric values → W&B Metrics, named by rubicon-ml metric name
    - rubicon-ml Parameter values → W&B Config, named by rubicon-ml parameter name
    - rubicon-ml Artifacts → W&B Artifacts
    - rubicon-ml Dataframes → W&B Artifacts and Tables

    Parameters
    ----------
    entity : str or none, optional
        The W&B entity (username or team) to read from and write to. Defaults to none,
        leveraging the default entity from W&B config.
    root_dir : str, optional
        DEPRECATED. Unused. Required for `BaseRepository` compatibility.
    wandb_init_kwargs : dict or none, optional
        Additional keyword arguments to be passed to `wandb.init` calls. Defaults to none.
    warn : bool, optional
        Whether to warn about the experimental nature of this repository. Defaults to true.
    **storage_options
        Additional keyword arguments to be passed to `wandb.API` initialization.
    """

    def __init__(
        self,
        entity: Optional[str] = None,
        root_dir: str = "WANDB",
        wandb_init_kwargs: Optional[Dict[str, Any]] = None,
        warn: bool = True,
        **storage_options,
    ):
        if warn:
            LOGGER.warning(
                "The `WandBRepository` is experimental and may contain breaking changes in future "
                "versions. If you encounter any bugs or missing features, open an issue on GitHub."
            )

        self.root_dir = root_dir

        self.entity = entity
        self.storage_options = storage_options
        self.wandb_init_kwargs = wandb_init_kwargs or {}

        self._active_run = None
        self._current_project = None

    # ------ WandB Helpers ------

    @property
    def api(self):
        """Get a W&B API client.

        The W&B API client needs to be reinitialized each usage to guarantee full data retrieval.

        Returns
        -------
        wandb.Api
            The W&B API client.
        """
        return self.wandb.Api(**self.storage_options)

    @property
    def wandb(self):
        try:
            import wandb
        except ImportError:
            raise RubiconException(
                "Weights & Biases is not installed. `pip install wandb` to use this repository."
            )

        return wandb

    def _get_active_run(self, project_name: str, experiment_id: str, force_reinit: bool = False):
        """Get or create an active W&B run for the given project and experiment.

        Caches the run to avoid repeated wandb.init calls. Only re-initializes
        if the experiment_id changes.

        Parameters
        ----------
        project_name : str
            The W&B project name.
        experiment_id : str
            The W&B run ID (experiment ID).
        force_reinit : bool, optional
            Whether to force re-initialization of the run, by default False.

        Returns
        -------
        wandb.Run
            An active W&B run.
        """
        if (self._active_run and self._active_run.id != experiment_id) or force_reinit:
            self.finish()

        if self._active_run is None:
            run_config = {
                "project": project_name,
                "id": experiment_id,
                "resume": "must",
            }
            run_config.update(self.wandb_init_kwargs)

            if self.entity:
                run_config["entity"] = self.entity

            self._active_run = self.wandb.init(**run_config)

        return self._active_run

    def _get_api_run(self, project_name: str, experiment_id: str):
        """Get a fresh API run object with up-to-date data from the server.

        Parameters
        ----------
        project_name : str
            The W&B project name.
        experiment_id : str
            The W&B run ID (experiment ID).

        Returns
        -------
        wandb.apis.public.Run
            A W&B API run object with fresh data from the server.
        """
        return self.api.run(self._get_wandb_path(project_name, experiment_id))

    def _get_wandb_path(self, project_name: str, run_id: Optional[str] = None) -> str:
        """Construct a W&B path for API calls.

        Parameters
        ----------
        project_name : str
            The W&B project name.
        run_id : str, optional
            The W&B run ID.

        Returns
        -------
        str
            The W&B path (e.g., 'entity/project' or 'entity/project/run_id' or just 'project')
        """
        if self.entity:
            path = f"{self.entity}/{project_name}"
        else:
            path = project_name

        if run_id:
            path = f"{path}/{run_id}"

        return path

    def _log_to_run(self, data: Dict[str, Any]):
        """Log data to W&B run history.

        Handles both active runs and API runs.

        Parameters
        ----------
        data : dict
            The data to log (e.g., {"metric_name": value}).
        """
        self.wandb.log(data)

    def _set_config_value(self, key: str, value: Any):
        """Set a value in W&B config.

        Handles both active runs and API runs.

        Parameters
        ----------
        key : str
            The config key.
        value
            The value to set.
        """
        self._active_run.config[key] = value

    def _read_domain_from_config(
        self, run, metadata_key: str, domain_class: Type[domain.DomainsVar]
    ) -> Optional[domain.DomainsVar]:
        """Reconstruct a domain object from stored metadata.

        Parameters
        ----------
        run : wandb.apis.public.Run
            The W&B run object.
        metadata_key : str
            The config key where metadata is stored.
        domain_class : class
            The domain class to instantiate.

        Returns
        -------
        domain object or None
            The reconstructed domain object, or None if not found.
        """
        config = run.config
        if isinstance(config, str):
            config = json.loads(config)

        # If not found in active run's config, try API for fresh data
        if metadata_key not in config:
            api_run = self._get_api_run(run.project, run.id)
            config = api_run.config
            if isinstance(config, str):
                config = json.loads(config)

        if metadata_key in config:
            # we double serialize domain objects, so we need to deserialize the value again
            try:
                data = json.loads(config[metadata_key]["value"])
            except TypeError:
                # experiments don't have a value key
                data = json.loads(config[metadata_key])

            return domain_class(**data)

        return None

    def _read_domains_from_config(
        self, run, prefix: str, domain_class: Type[domain.DomainsVar]
    ) -> List[domain.DomainsVar]:
        """Reconstruct a list of domain objects from stored metadata.

        Parameters
        ----------
        run : wandb.apis.public.Run
            The W&B run object.
        prefix : str
            The config key prefix to search for.
        domain_class : class
            The domain class to instantiate.

        Returns
        -------
        list
            List of reconstructed domain objects (empty list if none found).
        """
        config = run.config
        if isinstance(config, str):
            config = json.loads(config)

        # Check if any keys match the prefix
        has_prefix = any(k.startswith(prefix) for k in config.keys())

        # If not found in active run's config, try API for fresh data
        if not has_prefix:
            api_run = self._get_api_run(run.project, run.id)
            config = api_run.config
            if isinstance(config, str):
                config = json.loads(config)

        objects = []
        for metadata_key in config.keys():
            if metadata_key.startswith(prefix):
                # we double serialize domain objects, so we need to deserialize the value again
                try:
                    data = json.loads(config[metadata_key]["value"])
                except TypeError:
                    # experiments don't have a value key
                    data = json.loads(config[metadata_key])

                objects.append(domain_class(**data))

        return objects

    def finish(self):
        if self._active_run is not None:
            self._active_run.finish()
            self._active_run = None

    # --- Filesystem Helpers ---

    def _cat(self, path: str):
        """W&B backend doesn't use filesystem paths."""
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use the specific get_* methods instead."
        )

    def _cat_paths(self, metadata_paths: List[str]):
        """W&B backend doesn't use filesystem paths."""
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use the specific get_* methods instead."
        )

    def _exists(self, path: str) -> bool:
        """W&B backend doesn't use filesystem paths."""
        return False

    def _glob(self, globstring: str) -> List:
        """W&B backend doesn't use filesystem paths."""
        return []

    def _ls_directories_only(self, path: str):
        """W&B backend doesn't use filesystem paths."""
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use the specific get_* methods instead."
        )

    def _mkdir(self, dirpath: str) -> bool:
        return True

    def _persist_domain(self, entity: Any, path: str):
        """Persist a domain object to W&B.

        This method stores domain objects in two ways:
        1. Native W&B format (metrics as logs, parameters as config, etc.)
        2. Complete domain metadata in config for full reconstruction
        """
        if isinstance(entity, domain.Project):
            self._current_project = entity

        elif isinstance(entity, domain.Experiment):
            run_config = {
                "project": self._current_project.name,
                "reinit": "finish_previous",
            }
            run_config.update(self.wandb_init_kwargs)

            if self.entity:
                run_config["entity"] = self.entity
            if entity.name:
                run_config["name"] = entity.name
            if entity.tags:
                run_config["tags"] = entity.tags

            self._active_run = self.wandb.init(**run_config)

            entity.id = self._active_run.id  # for accurate W&B retrieval
            if entity.name is None:
                entity.name = self._active_run.name

            self._set_config_value("_rubicon_experiment_metadata", json.dumps(entity))

        elif isinstance(entity, domain.Feature):
            entity_name = slugify(entity.name, separator="_")
            self._set_config_value(f"_rubicon_feature_{entity_name}", json.dumps(entity))

            if entity.importance is not None:
                self._log_to_run({f"{entity_name}_importance": entity.importance})

        elif isinstance(entity, domain.Metric):
            entity_name = slugify(entity.name, separator="_")
            self._log_to_run({entity.name: entity.value})
            self._set_config_value(f"_rubicon_metric_{entity_name}", json.dumps(entity))

        elif isinstance(entity, domain.Parameter):
            entity_name = slugify(entity.name, separator="_")
            self._set_config_value(entity.name, entity.value)
            self._set_config_value(f"_rubicon_parameter_{entity_name}", json.dumps(entity))

    def _read_bytes(self, path: str, err_msg: Optional[str] = None) -> bytes:
        """W&B backend doesn't use filesystem paths."""
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use get_artifact_data() or get_dataframe_data() instead."
        )

    def _read_domain(self, path: str, err_msg: Optional[str] = None) -> domain.DomainsVar:
        """W&B backend doesn't use filesystem paths."""
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use the specific get_* methods instead."
        )

    def _rm(self, path: str):
        """W&B backend doesn't support deletion."""
        raise RubiconException(
            "The W&B backend doesn't support deletion through the API. "
            "Delete runs manually through the W&B UI."
        )

    # -------- Projects --------

    def get_project(self, project_name: str) -> domain.Project:
        """Retrieve a project from W&B.

        Parameters
        ----------
        project_name : str
            The name of the W&B project to retrieve.

        Returns
        -------
        rubicon.domain.Project
            The project with name `project_name`.
        """
        if self._current_project is not None:
            return self._current_project

        try:
            self.api.runs(self._get_wandb_path(project_name), per_page=1)
        except Exception as e:
            raise RubiconException(f"No project with name '{project_name}' found.") from e

        # W&B projects don't have metadata like rubicon-ml projects, so we create a minimal object
        self._current_project = domain.Project(
            name=project_name,
            description=f"W&B project {project_name}",
        )

        return self._current_project

    def get_projects(self) -> List[domain.Project]:
        """Get the list of projects from W&B.

        Returns
        -------
        list of rubicon.domain.Project
            The list of projects from W&B for the configured entity.
        """
        raise RubiconException(
            "The W&B backend doesn't support listing all projects. "
            "Use `get_project(name)` with a specific project name instead."
        )

    # -------- Experiments (W&B Runs) --------

    def get_experiment(self, project_name: str, experiment_id: str) -> domain.Experiment:
        """Retrieve an experiment (W&B run) from W&B.

        Resumes the W&B run using wandb.init(resume="must") to get an active run
        that supports full logging capabilities.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        experiment_id : str
            The ID of the W&B run (experiment).

        Returns
        -------
        rubicon.domain.Experiment
            The experiment with ID `experiment_id`.
        """
        experiment = self._read_domain_from_config(
            self._get_active_run(project_name, experiment_id, force_reinit=True),
            "_rubicon_experiment_metadata",
            domain.Experiment,
        )

        if experiment is None:
            raise RubiconException(
                f"No experiment metadata found for experiment '{experiment_id}'. "
                "This run was not created with `rubicon-ml`."
            )

        return experiment

    def get_experiments(self, project_name: str) -> List[domain.Experiment]:
        """Retrieve all experiments (W&B runs) from a project.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.

        Returns
        -------
        list of rubicon.domain.Experiment
            The experiments logged to the project.
        """
        try:
            runs = self.api.runs(self._get_wandb_path(project_name))
        except Exception as e:
            raise RubiconException(
                f"Failed to retrieve experiments from project '{project_name}'."
            ) from e

        experiments = []
        for run in runs:
            experiment = self._read_domain_from_config(
                run, "_rubicon_experiment_metadata", domain.Experiment
            )
            if experiment is not None:
                experiments.append(experiment)

        return experiments

    # -------- Metrics --------

    def get_metric(self, project_name: str, experiment_id: str, metric_name: str) -> domain.Metric:
        """Retrieve a metric from a W&B run.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        experiment_id : str
            The ID of the W&B run.
        metric_name : str
            The name of the metric to retrieve.

        Returns
        -------
        rubicon.domain.Metric
            The metric with name `metric_name`.
        """
        metric = self._read_domain_from_config(
            self._get_active_run(project_name, experiment_id),
            f"_rubicon_metric_{slugify(metric_name, separator='_')}",
            domain.Metric,
        )

        if metric is None:
            raise RubiconException(
                f"No metric with name '{metric_name}' found in experiment '{experiment_id}'."
            )

        return metric

    def get_metrics(self, project_name: str, experiment_id: str) -> List[domain.Metric]:
        """Retrieve all metrics from a W&B run.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        experiment_id : str
            The ID of the W&B run.

        Returns
        -------
        list of rubicon.domain.Metric
            The metrics logged to the experiment.
        """
        return self._read_domains_from_config(
            self._get_active_run(project_name, experiment_id),
            "_rubicon_metric_",
            domain.Metric,
        )

    # -------- Parameters --------

    def get_parameter(
        self, project_name: str, experiment_id: str, parameter_name: str
    ) -> domain.Parameter:
        """Retrieve a parameter from a W&B run's config.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        experiment_id : str
            The ID of the W&B run.
        parameter_name : str
            The name of the parameter to retrieve.

        Returns
        -------
        rubicon.domain.Parameter
            The parameter with name `parameter_name`.
        """
        parameter = self._read_domain_from_config(
            self._get_active_run(project_name, experiment_id),
            f"_rubicon_parameter_{slugify(parameter_name, separator='_')}",
            domain.Parameter,
        )

        if parameter is None:
            raise RubiconException(
                f"No parameter with name '{parameter_name}' found in experiment '{experiment_id}'."
            )

        return parameter

    def get_parameters(self, project_name: str, experiment_id: str) -> List[domain.Parameter]:
        """Retrieve all parameters from a W&B run's config.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        experiment_id : str
            The ID of the W&B run.

        Returns
        -------
        list of rubicon.domain.Parameter
            The parameters logged to the experiment.
        """
        return self._read_domains_from_config(
            self._get_active_run(project_name, experiment_id),
            "_rubicon_parameter_",
            domain.Parameter,
        )

    # -------- Features --------

    def get_feature(
        self, project_name: str, experiment_id: str, feature_name: str
    ) -> domain.Feature:
        """Retrieve a feature from a W&B run.

        Features are stored in a W&B table named 'features'.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        experiment_id : str
            The ID of the W&B run.
        feature_name : str
            The name of the feature to retrieve.

        Returns
        -------
        rubicon.domain.Feature
            The feature with name `feature_name`.
        """
        features = self.get_features(project_name, experiment_id)

        for feature in features:
            if feature.name == feature_name:
                return feature

        raise RubiconException(
            f"No feature with name '{feature_name}' found in experiment '{experiment_id}'."
        )

    def get_features(self, project_name: str, experiment_id: str) -> List[domain.Feature]:
        """Retrieve all features from a W&B run.

        Features are stored in a W&B table named 'features' with columns: name, importance.
        Note: Accessing W&B table data through the API is limited, so this may not
        return all features if they were logged in a complex format.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        experiment_id : str
            The ID of the W&B run.

        Returns
        -------
        list of rubicon.domain.Feature
            The features logged to the experiment.
        """
        return self._read_domains_from_config(
            self._get_active_run(project_name, experiment_id),
            "_rubicon_feature_",
            domain.Feature,
        )

    # -------- Artifacts --------

    def create_artifact(
        self,
        artifact: domain.Artifact,
        data: bytes,
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        """Persist an artifact to W&B.

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

        Raises
        ------
        RubiconException
            If experiment_id is None. W&B does not support project-level
            artifacts as artifacts must be associated with a run.
        """
        if experiment_id is None:
            raise RubiconException(
                "The W&B backend does not support project-level artifacts. "
                "Artifacts must be logged to an experiment (W&B run)."
            )

        self._get_active_run(project_name, experiment_id)

        with tempfile.NamedTemporaryFile(delete=False) as file:
            file.write(data)
            file.seek(0)
            temp_path = file.name

        try:
            wandb_artifact = self.wandb.Artifact(name=artifact.name, type="model")
            wandb_artifact.add_file(temp_path, name=artifact.name)
            wandb_artifact.save()

            entity_id = artifact.id
            artifact.id = f"{artifact.id}:{artifact.name}"  # for accurate W&B retrieval

            self._set_config_value(f"_rubicon_artifact_{entity_id}", json.dumps(artifact))
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def get_artifact_metadata(
        self, project_name: str, artifact_id: str, experiment_id: str
    ) -> domain.Artifact:
        """Retrieve artifact metadata from W&B.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        artifact_id : str
            The ID of the artifact.
        experiment_id : str
            The ID of the W&B run.

        Returns
        -------
        rubicon.domain.Artifact
            The artifact metadata.
        """
        artifact_id, _ = artifact_id.split(":")
        artifact = self._read_domain_from_config(
            self._get_active_run(project_name, experiment_id),
            f"_rubicon_artifact_{artifact_id}",
            domain.Artifact,
        )

        if artifact is None:
            raise RubiconException(
                f"No artifact with id '{artifact_id}' found in experiment '{experiment_id}'."
            )

        return artifact

    def get_artifacts_metadata(
        self, project_name: str, experiment_id: str
    ) -> List[domain.Artifact]:
        """Retrieve all artifacts metadata from W&B.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        experiment_id : str
            The ID of the W&B run to get artifacts from.

        Returns
        -------
        list of rubicon.domain.Artifact
            The artifacts logged to the experiment.
        """
        return self._read_domains_from_config(
            self._get_active_run(project_name, experiment_id),
            "_rubicon_artifact_",
            domain.Artifact,
        )

    def get_artifact_data(
        self, project_name: str, artifact_id: str, experiment_id: Optional[str] = None
    ) -> bytes:
        """Retrieve artifact data from W&B.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        artifact_id : str
            The name of the W&B artifact.
        experiment_id : str, optional
            The ID of the W&B run (not used in W&B artifact lookup).

        Returns
        -------
        bytes
            The artifact's raw data.
        """
        try:
            wandb_path = self._get_wandb_path(project_name)

            artifact_id, artifact_name = artifact_id.split(":")
            artifact = self.api.artifact(f"{wandb_path}/{artifact_name}:latest")
            artifact_dir = artifact.download()
            artifact_files = os.listdir(artifact_dir)

            if not artifact_files:
                raise RubiconException(f"No files found in artifact '{artifact_id}'.")

            file_path = os.path.join(artifact_dir, artifact_files[0])
            with open(file_path, "rb") as f:
                return f.read()

        except Exception as e:
            raise RubiconException(f"Failed to retrieve artifact data for '{artifact_id}'.") from e

    # -------- Dataframes --------

    def create_dataframe(
        self,
        dataframe: domain.Dataframe,
        data: Any,
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        """Persist a dataframe to W&B.

        Parameters
        ----------
        dataframe : rubicon.domain.Dataframe
            The dataframe to persist.
        data : pandas.DataFrame, dask.dataframe.DataFrame, or polars DataFrame
            The raw data to persist as a dataframe.
        project_name : str
            The name of the project this dataframe belongs to.
        experiment_id : str, optional
            The ID of the experiment this dataframe belongs to.

        Raises
        ------
        RubiconException
            If experiment_id is None. W&B does not support project-level
            dataframes as they must be associated with a run.
        """
        if experiment_id is None:
            raise RubiconException(
                "The W&B backend does not support project-level dataframes. "
                "Dataframes must be logged to an experiment (W&B run)."
            )

        self._get_active_run(project_name, experiment_id)

        # 1. Save as W&B Artifact (type="dataset") for reliable retrieval
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".parquet", delete=False) as file:
            temp_path = file.name
            data.to_parquet(temp_path, index=False)

        try:
            artifact = self.wandb.Artifact(
                name=f"dataframe-{dataframe.id}",
                type="dataset",
                description=dataframe.description or f"Dataframe {dataframe.name or dataframe.id}",
            )
            artifact.add_file(temp_path, name=f"{dataframe.name or dataframe.id}.parquet")
            artifact.save()
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

        # 2. Also log as W&B Table for visualization in UI
        dataframe_table = self.wandb.Table(dataframe=data)
        self.wandb.log({dataframe.name or dataframe.id: dataframe_table})

        # 3. Store complete dataframe metadata for reconstruction
        self._set_config_value(f"_rubicon_dataframe_{dataframe.id}", json.dumps(dataframe))

    def get_dataframe_metadata(
        self, project_name: str, dataframe_id: str, experiment_id: Optional[str] = None
    ) -> domain.Dataframe:
        """Retrieve dataframe metadata from W&B.

        Dataframes are stored as W&B tables.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        dataframe_id : str
            The name of the dataframe (table key in W&B).
        experiment_id : str, optional
            The ID of the W&B run.

        Returns
        -------
        rubicon.domain.Dataframe
            The dataframe metadata.
        """
        if not experiment_id:
            raise RubiconException("experiment_id is required to retrieve dataframes from W&B.")

        dataframe = self._read_domain_from_config(
            self._get_active_run(project_name, experiment_id),
            f"_rubicon_dataframe_{dataframe_id}",
            domain.Dataframe,
        )

        if dataframe is None:
            raise RubiconException(
                f"No dataframe with id '{dataframe_id}' found in experiment '{experiment_id}'."
            )

        return dataframe

    def get_dataframes_metadata(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> List[domain.Dataframe]:
        """Retrieve all dataframes metadata from W&B.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        experiment_id : str, optional
            The ID of the W&B run.

        Returns
        -------
        list of rubicon.domain.Dataframe
            The dataframes logged to the experiment.
        """
        if not experiment_id:
            raise RubiconException("experiment_id is required to retrieve dataframes from W&B.")

        return self._read_domains_from_config(
            self._get_active_run(project_name, experiment_id),
            "_rubicon_dataframe_",
            domain.Dataframe,
        )

    def get_dataframe_data(
        self,
        project_name: str,
        dataframe_id: str,
        experiment_id: Optional[str] = None,
        df_type: str = "pandas",
    ):
        """Retrieve dataframe data from W&B.

        Dataframes are stored as W&B artifacts (type="dataset") in parquet format
        for reliable retrieval.

        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        dataframe_id : str
            The ID of the dataframe.
        experiment_id : str, optional
            The ID of the W&B run.
        df_type : str, optional
            The type of dataframe to return ("pandas", "dask", or "polars").

        Returns
        -------
        pandas.DataFrame, dask.DataFrame, or polars.DataFrame
            The dataframe data.
        """
        if not experiment_id:
            raise RubiconException("experiment_id is required to retrieve dataframes from W&B.")

        # Use API run for logged_artifacts() - active runs don't have this method
        api_run = self._get_api_run(project_name, experiment_id)
        artifact_name = f"dataframe-{dataframe_id}"

        try:
            artifact = None
            for logged_artifact in api_run.logged_artifacts():
                if logged_artifact.name.startswith(artifact_name):
                    artifact = logged_artifact
                    break

            if artifact is None:
                raise RubiconException(
                    f"No artifact found for dataframe '{dataframe_id}' in experiment '{experiment_id}'."
                )

            artifact_dir = artifact.download()

            parquet_files = [f for f in os.listdir(artifact_dir) if f.endswith(".parquet")]
            if not parquet_files:
                raise RubiconException(
                    f"No parquet file found in artifact for dataframe '{dataframe_id}'."
                )

            parquet_path = os.path.join(artifact_dir, parquet_files[0])

            if df_type == "pandas":
                return pd.read_parquet(parquet_path)
            elif df_type == "dask":
                import dask.dataframe as dd

                return dd.read_parquet(parquet_path)
            elif df_type == "polars":
                import polars as pl

                return pl.read_parquet(parquet_path)
            else:
                raise RubiconException(
                    f"Unsupported dataframe type: {df_type}. Must be 'pandas', 'dask', or 'polars'."
                )

        except Exception as e:
            if isinstance(e, RubiconException):
                raise
            raise RubiconException(
                f"Failed to retrieve dataframe data for '{dataframe_id}': {str(e)}"
            ) from e

    # -------- Tags --------

    def _get_tag_comment_key_prefix(
        self, entity_type: str, entity_identifier: str, key_type: str = "tags"
    ) -> str:
        """Get the prefix for tag/comment config keys.

        Parameters
        ----------
        entity_type : str
            The type of entity (e.g., 'Experiment', 'Metric', 'Feature').
        entity_identifier : str
            The identifier for the entity (ID or name).
        key_type : str
            Either 'tags' or 'comments'.

        Returns
        -------
        str
            The key prefix for config storage.
        """
        # Slugify names for Metrics, Features, and Parameters to match BaseRepository behavior
        if entity_type in ["Metric", "Feature", "Parameter"]:
            entity_identifier = slugify(entity_identifier, separator="_")

        return f"_rubicon_{key_type}_{entity_type.lower()}_{entity_identifier}_"

    def add_tags(
        self,
        project_name: str,
        tags: List[str],
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        """Persist tags to W&B run.

        Uses W&B's native run.tags for actual tag storage, and also stores
        the operation history in config for rubicon-ml compatibility.

        Parameters
        ----------
        project_name : str
            The name of the project the object to tag belongs to.
        tags : list of str
            The tag values to persist.
        experiment_id : str, optional
            The ID of the experiment to apply the tags to.
        entity_identifier : str, optional
            The ID or name of the entity to apply the tags to.
        entity_type : str, optional
            The name of the entity's type (e.g., 'Experiment', 'Metric').
        """
        if experiment_id is None:
            raise RubiconException("experiment_id is required to add tags in W&B.")

        run = self._get_active_run(project_name, experiment_id)

        if entity_type == "Experiment":
            current_tags = list(run.tags) if run.tags else []
            new_tags = current_tags + [t for t in tags if t not in current_tags]
            run.tags = new_tags

        key_prefix = self._get_tag_comment_key_prefix(entity_type, entity_identifier, "tags")
        key = f"{key_prefix}{uuid4()}"
        run.config[key] = json.dumps({"added_tags": tags, "_timestamp": time.time()})

    def remove_tags(
        self,
        project_name: str,
        tags: List[str],
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        """Remove tags from W&B run.

        Uses W&B's native run.tags for actual tag removal, and also stores
        the operation history in config for rubicon-ml compatibility.

        Parameters
        ----------
        project_name : str
            The name of the project the object to untag belongs to.
        tags : list of str
            The tag values to remove.
        experiment_id : str, optional
            The ID of the experiment to remove the tags from.
        entity_identifier : str, optional
            The ID or name of the entity to remove the tags from.
        entity_type : str, optional
            The name of the entity's type (e.g., 'Experiment', 'Metric').
        """
        if experiment_id is None:
            raise RubiconException("experiment_id is required to remove tags in W&B.")

        run = self._get_active_run(project_name, experiment_id)

        if entity_type == "Experiment":
            current_tags = list(run.tags) if run.tags else []
            new_tags = [t for t in current_tags if t not in tags]
            run.tags = new_tags

        key_prefix = self._get_tag_comment_key_prefix(entity_type, entity_identifier, "tags")
        key = f"{key_prefix}{uuid4()}"
        run.config[key] = json.dumps({"removed_tags": tags, "_timestamp": time.time()})

    def get_tags(
        self,
        project_name: str,
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ) -> List[dict]:
        """Retrieve tags from W&B run config.

        Parameters
        ----------
        project_name : str
            The name of the project the object to retrieve tags from belongs to.
        experiment_id : str, optional
            The ID of the experiment to retrieve tags from.
        entity_identifier : str, optional
            The ID or name of the entity to retrieve tags from.
        entity_type : str, optional
            The name of the entity's type (e.g., 'Experiment', 'Metric').

        Returns
        -------
        list of dict
            A list of dictionaries with one key each, `added_tags` or `removed_tags`,
            where the value is a list of tag names.
        """
        if experiment_id is None:
            raise RubiconException("experiment_id is required to get tags in W&B.")

        run = self._get_active_run(project_name, experiment_id)
        key_prefix = self._get_tag_comment_key_prefix(entity_type, entity_identifier, "tags")

        config = run.config
        if isinstance(config, str):
            config = json.loads(config)

        # Check if any keys match the prefix
        has_prefix = any(k.startswith(key_prefix) for k in config.keys())

        # If not found in active run's config, try API for fresh data
        if not has_prefix:
            api_run = self._get_api_run(project_name, experiment_id)
            config = api_run.config
            if isinstance(config, str):
                config = json.loads(config)

        tags_data = []
        for key, value in config.items():
            if key.startswith(key_prefix):
                try:
                    # Handle both direct values and wrapped values with 'value' key
                    if isinstance(value, dict) and "value" in value:
                        data = json.loads(value["value"])
                    elif isinstance(value, str):
                        data = json.loads(value)
                    else:
                        data = value
                    tags_data.append(data)
                except (TypeError, JSONDecodeError):
                    continue

        tags_data.sort(key=lambda x: x.get("_timestamp", 0))

        return tags_data

    # -------- Comments --------

    def add_comments(
        self,
        project_name: str,
        comments: List[str],
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        """Persist comments to W&B run config.

        Parameters
        ----------
        project_name : str
            The name of the project the object to comment belongs to.
        comments : list of str
            The comment values to persist.
        experiment_id : str, optional
            The ID of the experiment to apply the comments to.
        entity_identifier : str, optional
            The ID or name of the entity to apply the comments to.
        entity_type : str, optional
            The name of the entity's type (e.g., 'Experiment', 'Metric').
        """
        if experiment_id is None:
            raise RubiconException("experiment_id is required to add comments in W&B.")

        run = self._get_active_run(project_name, experiment_id)
        key_prefix = self._get_tag_comment_key_prefix(entity_type, entity_identifier, "comments")
        key = f"{key_prefix}{uuid4()}"

        run.config[key] = json.dumps({"added_comments": comments, "_timestamp": time.time()})

    def remove_comments(
        self,
        project_name: str,
        comments: List[str],
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ):
        """Remove comments from W&B run config.

        Parameters
        ----------
        project_name : str
            The name of the project the object to remove comments from belongs to.
        comments : list of str
            The comment values to remove.
        experiment_id : str, optional
            The ID of the experiment to remove the comments from.
        entity_identifier : str, optional
            The ID or name of the entity to remove the comments from.
        entity_type : str, optional
            The name of the entity's type (e.g., 'Experiment', 'Metric').
        """
        if experiment_id is None:
            raise RubiconException("experiment_id is required to remove comments in W&B.")

        run = self._get_active_run(project_name, experiment_id)
        key_prefix = self._get_tag_comment_key_prefix(entity_type, entity_identifier, "comments")
        key = f"{key_prefix}{uuid4()}"

        run.config[key] = json.dumps({"removed_comments": comments, "_timestamp": time.time()})

    def get_comments(
        self,
        project_name: str,
        experiment_id: Optional[str] = None,
        entity_identifier: Optional[str] = None,
        entity_type: Optional[str] = None,
    ) -> List[dict]:
        """Retrieve comments from W&B run config.

        Parameters
        ----------
        project_name : str
            The name of the project the object to retrieve comments from belongs to.
        experiment_id : str, optional
            The ID of the experiment to retrieve comments from.
        entity_identifier : str, optional
            The ID or name of the entity to retrieve comments from.
        entity_type : str, optional
            The name of the entity's type (e.g., 'Experiment', 'Metric').

        Returns
        -------
        list of dict
            A list of dictionaries with one key each, `added_comments` or `removed_comments`,
            where the value is a list of comment strings.
        """
        if experiment_id is None:
            raise RubiconException("experiment_id is required to get comments in W&B.")

        run = self._get_active_run(project_name, experiment_id)
        key_prefix = self._get_tag_comment_key_prefix(entity_type, entity_identifier, "comments")

        config = run.config
        if isinstance(config, str):
            config = json.loads(config)

        # Check if any keys match the prefix
        has_prefix = any(k.startswith(key_prefix) for k in config.keys())

        # If not found in active run's config, try API for fresh data
        if not has_prefix:
            api_run = self._get_api_run(project_name, experiment_id)
            config = api_run.config
            if isinstance(config, str):
                config = json.loads(config)

        comments_data = []
        for key, value in config.items():
            if key.startswith(key_prefix):
                try:
                    # Handle both direct values and wrapped values with 'value' key
                    if isinstance(value, dict) and "value" in value:
                        data = json.loads(value["value"])
                    elif isinstance(value, str):
                        data = json.loads(value)
                    else:
                        data = value
                    comments_data.append(data)
                except (TypeError, JSONDecodeError):
                    continue

        comments_data.sort(key=lambda x: x.get("_timestamp", 0))

        return comments_data
