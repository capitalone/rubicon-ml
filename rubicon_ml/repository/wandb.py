import logging
import os
import tempfile
from typing import List, Optional

import pandas as pd

from rubicon_ml import domain
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import BaseRepository
from rubicon_ml.repository.utils import json

LOGGER = logging.getLogger(__name__)


class WandBRepository(BaseRepository):
    """Repository for reading and writing rubicon-ml data to Weights & Biases.

    The `WandBRepository` is experimental and may contain breaking changes in future versions.
    If you encounter any bugs or missing features, open an issue on GitHub.

    This repository maps rubicon-ml concepts to W&B as follows:
    - rubicon-ml Projects → W&B Projects
    - rubicon-ml Experiments → W&B Runs
    - rubicon-ml Parameters → W&B Config
    - rubicon-ml Features → W&B Config (w/ importances as Metrics)
    - rubicon-ml Metrics → W&B Metrics
    - rubicon-ml Artifacts → W&B Artifacts
    - rubicon-ml Dataframes → W&B Tables (and Artifacts for retrieval)

    Parameters
    ----------
    entity : str, optional
        The W&B entity (username or team) to use for reading data.
        If not provided, will use the default entity from wandb config.
    root_dir : str, optional
        NOT USED. Required for backwards compatibility. Will be removed in a future version.
    warn : bool, optional
        Whether to warn about the experimental nature of this repository. Defaults to true.
    **storage_options
        Additional options passed to W&B API initialization.
    """

    def __init__(
        self,
        entity: Optional[str] = None,
        root_dir: str = "WANDB",
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

        self._current_artifact_bytes = None
        self._current_dataframe = None
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

    def _get_wandb_run(self, project_name: str, experiment_id: str):
        """Get a W&B run object.

        Parameters
        ----------
        project_name : str
            The W&B project name.
        experiment_id : str
            The W&B run ID.

        Returns
        -------
        wandb.apis.public.Run
            The W&B run object.

        Raises
        ------
        RubiconException
            If the run is not found.
        """
        try:
            wandb_path = self._get_wandb_path(project_name, experiment_id)

            return self.api.run(wandb_path)
        except Exception as e:
            raise RubiconException(f"No experiment with id '{experiment_id}' found.") from e

    def _persist_domain_to_config(self, key: str, domain_obj):
        """Store a domain object's metadata in W&B config.

        Parameters
        ----------
        key : str
            The config key to store metadata under.
        domain_obj : domain object
            The domain object to serialize and store.
        """
        # serialize domain object ourselves to leverage rubicon-ml's custom serializers
        self.wandb.config.update({key: json.dumps(domain_obj)})

    def _read_domain_from_config(self, run, metadata_key: str, domain_class):
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

        if metadata_key in config:
            # we double serialize domain objects, so we need to deserialize the value again
            try:
                data = json.loads(config[metadata_key]["value"])
            except TypeError:
                # experiments don't have a value key
                data = json.loads(config[metadata_key])

            return domain_class(**data)

        return None

    def _read_domains_from_config(self, run, prefix: str, domain_class):
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

    # --- Filesystem Helpers ---

    def _cat(self, path):
        """W&B backend doesn't use filesystem paths."""
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use the specific get_* methods instead."
        )

    def _cat_paths(self, metadata_paths):
        """W&B backend doesn't use filesystem paths."""
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use the specific get_* methods instead."
        )

    def _exists(self, path):
        """W&B backend doesn't use filesystem paths."""
        return False

    def _glob(self, globstring):
        """W&B backend doesn't use filesystem paths."""
        return []

    def _ls_directories_only(self, path):
        """W&B backend doesn't use filesystem paths."""
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use the specific get_* methods instead."
        )

    def _mkdir(self, dirpath):
        return True

    def _persist_bytes(self, bytes_data, path):
        self._current_artifact_bytes = bytes_data

    def _persist_dataframe(self, df, path):
        self._current_dataframe = df

    def _persist_domain(self, entity, path):
        """Persist a domain object to W&B.

        This method stores domain objects in two ways:
        1. Native W&B format (metrics as logs, parameters as config, etc.)
        2. Complete domain metadata in config for full reconstruction
        """
        if isinstance(entity, domain.Project):
            self._current_project = entity

        elif isinstance(entity, domain.Experiment):
            run_config = {"project": self._current_project.name}
            if entity.tags:
                run_config["tags"] = entity.tags

            run = self.wandb.init(**run_config)

            entity.id = run.id  # for accurate W&B retrieval
            if entity.name is None:
                entity.name = run.name

            self._persist_domain_to_config("_rubicon_experiment_metadata", entity)

        elif isinstance(entity, domain.Feature):
            self._persist_domain_to_config(f"_rubicon_feature_{entity.name}", entity)

            if entity.importance is not None:
                self.wandb.log({f"{entity.name}_importance": entity.importance})

        elif isinstance(entity, domain.Metric):
            self.wandb.log({entity.name: entity.value})
            self._persist_domain_to_config(f"_rubicon_metric_{entity.name}", entity)

        elif isinstance(entity, domain.Parameter):
            self.wandb.config[entity.name] = entity.value
            self._persist_domain_to_config(f"_rubicon_parameter_{entity.name}", entity)

        elif isinstance(entity, domain.Artifact):
            with tempfile.NamedTemporaryFile(delete=False) as file:
                file.write(self._current_artifact_bytes)
                file.seek(0)
                temp_path = file.name

            try:
                self._current_artifact_bytes = None
                artifact = self.wandb.Artifact(name=entity.name, type="model")
                artifact.add_file(temp_path, name=entity.name)
                artifact.save()

                entity_id = entity.id
                entity.id = entity.name  # for accurate W&B retrieval

                self._persist_domain_to_config(f"_rubicon_artifact_{entity_id}", entity)
            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)

        elif isinstance(entity, domain.Dataframe):
            # 1. Save as W&B Artifact (type="dataset") for reliable retrieval
            with tempfile.NamedTemporaryFile(mode="wb", suffix=".parquet", delete=False) as file:
                temp_path = file.name
                self._current_dataframe.to_parquet(temp_path, index=False)

            try:
                artifact = self.wandb.Artifact(
                    name=f"dataframe-{entity.id}",
                    type="dataset",
                    description=entity.description or f"Dataframe {entity.name or entity.id}",
                )
                artifact.add_file(temp_path, name=f"{entity.name or entity.id}.parquet")
                artifact.save()
            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)

            # 2. Also log as W&B Table for visualization in UI
            dataframe_table = self.wandb.Table(dataframe=self._current_dataframe)
            self.wandb.log({entity.name or entity.id: dataframe_table})

            # 3. Store complete dataframe metadata for reconstruction
            self._persist_domain_to_config(f"_rubicon_dataframe_{entity.id}", entity)

    def _read_bytes(self, path, err_msg=None):
        """W&B backend doesn't use filesystem paths."""
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use get_artifact_data() or get_dataframe_data() instead."
        )

    def _read_domain(self, path, err_msg=None):
        """W&B backend doesn't use filesystem paths."""
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use the specific get_* methods instead."
        )

    def _rm(self, path):
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
        try:
            wandb_path = self._get_wandb_path(project_name)
            list(self.api.runs(wandb_path, per_page=1))
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
        run = self._get_wandb_run(project_name, experiment_id)

        result = self._read_domain_from_config(
            run, "_rubicon_experiment_metadata", domain.Experiment
        )

        if result is None:
            raise RubiconException(
                f"No experiment metadata found for experiment '{experiment_id}'. "
                "This run was not created through Rubicon."
            )

        return result

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
            wandb_path = self._get_wandb_path(project_name)
            runs = self.api.runs(wandb_path)
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
        run = self._get_wandb_run(project_name, experiment_id)

        result = self._read_domain_from_config(run, f"_rubicon_metric_{metric_name}", domain.Metric)

        if result is None:
            raise RubiconException(
                f"No metric with name '{metric_name}' found in experiment '{experiment_id}'."
            )

        return result

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
        run = self._get_wandb_run(project_name, experiment_id)

        return self._read_domains_from_config(run, "_rubicon_metric_", domain.Metric)

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
        run = self._get_wandb_run(project_name, experiment_id)

        result = self._read_domain_from_config(
            run, f"_rubicon_parameter_{parameter_name}", domain.Parameter
        )

        if result is None:
            raise RubiconException(
                f"No parameter with name '{parameter_name}' found in experiment '{experiment_id}'."
            )

        return result

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
        run = self._get_wandb_run(project_name, experiment_id)

        return self._read_domains_from_config(run, "_rubicon_parameter_", domain.Parameter)

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
        run = self._get_wandb_run(project_name, experiment_id)

        return self._read_domains_from_config(run, "_rubicon_feature_", domain.Feature)

    # -------- Artifacts --------

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
        run = self._get_wandb_run(project_name, experiment_id)

        result = self._read_domain_from_config(
            run, f"_rubicon_artifact_{artifact_id}", domain.Artifact
        )

        if result is None:
            raise RubiconException(
                f"No artifact with id '{artifact_id}' found in experiment '{experiment_id}'."
            )

        return result

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
        run = self._get_wandb_run(project_name, experiment_id)

        return self._read_domains_from_config(run, "_rubicon_artifact_", domain.Artifact)

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
            artifact = self.api.artifact(f"{wandb_path}/{artifact_id}:latest")
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

        run = self._get_wandb_run(project_name, experiment_id)

        result = self._read_domain_from_config(
            run, f"_rubicon_dataframe_{dataframe_id}", domain.Dataframe
        )

        if result is None:
            raise RubiconException(
                f"No dataframe with id '{dataframe_id}' found in experiment '{experiment_id}'."
            )

        return result

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

        run = self._get_wandb_run(project_name, experiment_id)

        return self._read_domains_from_config(run, "_rubicon_dataframe_", domain.Dataframe)

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

        run = self._get_wandb_run(project_name, experiment_id)
        artifact_name = f"dataframe-{dataframe_id}"

        try:
            artifact = None
            for logged_artifact in run.logged_artifacts():
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
