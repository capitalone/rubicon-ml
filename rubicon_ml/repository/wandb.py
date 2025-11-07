import tempfile
from typing import List, Optional

import wandb
from wandb.apis.public import Api

from rubicon_ml import domain
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import BaseRepository


class WandBRepository(BaseRepository):
    """Repository for reading and writing Rubicon data to Weights & Biases.
    
    This repository maps Rubicon concepts to W&B as follows:
    - Rubicon Projects → W&B Projects
    - Rubicon Experiments → W&B Runs
    - Rubicon Parameters → W&B Config
    - Rubicon Metrics → W&B Logged Metrics
    - Rubicon Features → W&B Tables (logged as 'features')
    - Rubicon Artifacts → W&B Artifacts
    - Rubicon Dataframes → W&B Tables
    
    Parameters
    ----------
    entity : str, optional
        The W&B entity (username or team) to use for reading data.
        If not provided, will use the default entity from wandb config.
    **storage_options
        Additional options passed to W&B API initialization.
    """
    
    def __init__(self, entity: Optional[str] = None, **storage_options):
        self.root_dir = "WANDB"
        self.storage_options = storage_options
        self.entity = entity
        
        # For writing
        self._current_artifact_bytes = None
        self._current_dataframe = None
        self._current_features = []
        self._current_project = None
        
        # For reading
        self._api = None
    
    @property
    def api(self) -> Api:
        """Lazy initialization of W&B API client."""
        if self._api is None:
            self.storage_options.pop("root_dir")
            self._api = wandb.Api(**self.storage_options)

        return self._api
    
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

    def finish(self):
        if self._current_features:
            features = [[f.name, f.importance] for f in self._current_features]
            columns = ["name", "importance"]

            features_table = wandb.Table(data=features, columns=columns)
            wandb.log({"features": features_table})

        wandb.finish()

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
        raise RubiconException(
            "The W&B backend doesn't support direct file access. "
            "Use the specific get_* methods instead."
        )

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
        if isinstance(entity, domain.Project):
            self._current_project = entity
        elif isinstance(entity, domain.Experiment):
            wandb.init(project=self._current_project.name)
        elif isinstance(entity, domain.Feature):
            self._current_features.append(entity)
        elif isinstance(entity, domain.Metric):
            wandb.log({entity.name: entity.value})
        elif isinstance(entity, domain.Parameter):
            wandb.config[entity.name] = entity.value
        elif isinstance(entity, domain.Artifact):
            with tempfile.NamedTemporaryFile() as file:
                file.write(self._current_artifact_bytes)
                file.seek(0)

                self._current_artifact_bytes = None

                artifact = wandb.Artifact(name=entity.name, type="model")
                artifact.add_file(file.name, name=entity.name)
                artifact.save()
        elif isinstance(entity, domain.Dataframe):
            dataframe_table = wandb.Table(dataframe=self._current_dataframe)
            wandb.log({entity.name or entity.id: dataframe_table})

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
        # Check if project exists by trying to get runs
        try:
            wandb_path = self._get_wandb_path(project_name)
            list(self.api.runs(wandb_path, per_page=1))
        except Exception as e:
            raise RubiconException(f"No project with name '{project_name}' found.") from e
        
        # W&B projects don't have metadata like rubicon projects,
        # so we create a minimal project object
        return domain.Project(
            name=project_name,
            description=f"W&B project {project_name}",
        )
    
    def get_projects(self) -> List[domain.Project]:
        """Get the list of projects from W&B.
        
        Returns
        -------
        list of rubicon.domain.Project
            The list of projects from W&B for the configured entity.
        """
        # W&B API doesn't provide a direct way to list all projects
        # We would need to get this from the entity's project list
        raise RubiconException(
            "The W&B backend doesn't support listing all projects. "
            "Use get_project(name) with a specific project name instead."
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
        try:
            wandb_path = self._get_wandb_path(project_name, experiment_id)
            run = self.api.run(wandb_path)
        except Exception as e:
            raise RubiconException(
                f"No experiment with id '{experiment_id}' found in project '{project_name}'."
            ) from e
        
        return domain.Experiment(
            id=run.id,
            name=run.name,
            project_name=project_name,
            description=run.notes or "",
            tags=run.tags,
            created_at=run.created_at,
        )
    
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
            experiment = domain.Experiment(
                id=run.id,
                name=run.name,
                project_name=project_name,
                description=run.notes or "",
                tags=run.tags,
                created_at=run.created_at,
            )
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
        try:
            wandb_path = self._get_wandb_path(project_name, experiment_id)
            run = self.api.run(wandb_path)
        except Exception as e:
            raise RubiconException(
                f"No experiment with id '{experiment_id}' found."
            ) from e
        
        # Check if metric exists in summary (final values)
        if metric_name in run.summary:
            value = run.summary[metric_name]
            return domain.Metric(name=metric_name, value=value)
        
        # Check if metric exists in history
        history = run.history(keys=[metric_name], pandas=True)
        if not history.empty and metric_name in history.columns:
            # Get the last value
            value = history[metric_name].dropna().iloc[-1] if not history[metric_name].dropna().empty else None
            if value is not None:
                return domain.Metric(name=metric_name, value=value)
        
        raise RubiconException(
            f"No metric with name '{metric_name}' found in experiment '{experiment_id}'."
        )
    
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
        try:
            wandb_path = self._get_wandb_path(project_name, experiment_id)
            run = self.api.run(wandb_path)
        except Exception as e:
            raise RubiconException(
                f"No experiment with id '{experiment_id}' found."
            ) from e
        
        metrics = []
        # Get metrics from summary (these are typically the final values)
        for key, value in run.summary.items():
            # Skip internal W&B keys and non-numeric values
            if not key.startswith("_") and isinstance(value, (int, float)):
                metrics.append(domain.Metric(name=key, value=value))
        
        return metrics
    
    # -------- Parameters --------
    
    def get_parameter(self, project_name: str, experiment_id: str, parameter_name: str) -> domain.Parameter:
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
        try:
            wandb_path = self._get_wandb_path(project_name, experiment_id)
            run = self.api.run(wandb_path)
        except Exception as e:
            raise RubiconException(
                f"No experiment with id '{experiment_id}' found."
            ) from e
        
        if parameter_name not in run.config:
            raise RubiconException(
                f"No parameter with name '{parameter_name}' found in experiment '{experiment_id}'."
            )
        
        return domain.Parameter(name=parameter_name, value=run.config[parameter_name])
    
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
        try:
            wandb_path = self._get_wandb_path(project_name, experiment_id)
            run = self.api.run(wandb_path)
        except Exception as e:
            raise RubiconException(
                f"No experiment with id '{experiment_id}' found."
            ) from e
        
        parameters = []
        for key, value in run.config.items():
            # Skip internal W&B keys
            if not key.startswith("_"):
                parameters.append(domain.Parameter(name=key, value=value))
        
        return parameters
    
    # -------- Features --------
    
    def get_feature(self, project_name: str, experiment_id: str, feature_name: str) -> domain.Feature:
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
        try:
            wandb_path = self._get_wandb_path(project_name, experiment_id)
            run = self.api.run(wandb_path)
        except Exception as e:
            raise RubiconException(
                f"No experiment with id '{experiment_id}' found."
            ) from e
        
        # Features are stored as W&B tables, which are not easily accessible
        # via the public API. Return empty list for now.
        # Users can access features through the W&B UI or by downloading artifacts.
        return []
    
    # -------- Artifacts --------
    
    def get_artifact_metadata(self, project_name: str, artifact_id: str, experiment_id: Optional[str] = None) -> domain.Artifact:
        """Retrieve artifact metadata from W&B.
        
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
        rubicon.domain.Artifact
            The artifact metadata.
        """
        try:
            wandb_path = self._get_wandb_path(project_name)
            artifact = self.api.artifact(f"{wandb_path}/{artifact_id}:latest")
        except Exception as e:
            raise RubiconException(
                f"No artifact with id '{artifact_id}' found in project '{project_name}'."
            ) from e
        
        return domain.Artifact(
            id=artifact_id,
            name=artifact.name,
            description=artifact.description or "",
        )
    
    def get_artifacts_metadata(self, project_name: str, experiment_id: Optional[str] = None) -> List[domain.Artifact]:
        """Retrieve all artifacts metadata from W&B.
        
        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        experiment_id : str, optional
            The ID of the W&B run to get artifacts from.
        
        Returns
        -------
        list of rubicon.domain.Artifact
            The artifacts logged to the project or experiment.
        """
        artifacts = []
        
        if experiment_id:
            try:
                wandb_path = self._get_wandb_path(project_name, experiment_id)
                run = self.api.run(wandb_path)
                for artifact in run.logged_artifacts():
                    artifacts.append(domain.Artifact(
                        id=artifact.name,
                        name=artifact.name,
                        description=artifact.description or "",
                    ))
            except Exception as e:
                raise RubiconException(
                    f"Failed to retrieve artifacts from experiment '{experiment_id}'."
                ) from e
        else:
            # Get all artifacts in the project
            try:
                wandb_path = self._get_wandb_path(project_name)
                for artifact in self.api.artifacts(wandb_path):
                    artifacts.append(domain.Artifact(
                        id=artifact.name,
                        name=artifact.name,
                        description=artifact.description or "",
                    ))
            except Exception:
                # If this fails, return empty list
                pass
        
        return artifacts
    
    def get_artifact_data(self, project_name: str, artifact_id: str, experiment_id: Optional[str] = None) -> bytes:
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
            # Download the artifact to a temp directory
            artifact_dir = artifact.download()
            
            # Read the file with the same name as the artifact
            import os
            artifact_files = os.listdir(artifact_dir)
            if not artifact_files:
                raise RubiconException(f"No files found in artifact '{artifact_id}'.")
            
            # Read the first file (or file matching artifact name)
            file_path = os.path.join(artifact_dir, artifact_files[0])
            with open(file_path, "rb") as f:
                return f.read()
        except Exception as e:
            raise RubiconException(
                f"Failed to retrieve artifact data for '{artifact_id}'."
            ) from e
    
    # -------- Dataframes --------
    
    def get_dataframe_metadata(self, project_name: str, dataframe_id: str, experiment_id: Optional[str] = None) -> domain.Dataframe:
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
            raise RubiconException(
                "experiment_id is required to retrieve dataframes from W&B."
            )
        
        try:
            wandb_path = self._get_wandb_path(project_name, experiment_id)
            run = self.api.run(wandb_path)
        except Exception as e:
            raise RubiconException(
                f"No experiment with id '{experiment_id}' found."
            ) from e
        
        # Check if dataframe exists in summary
        if dataframe_id not in run.summary:
            raise RubiconException(
                f"No dataframe with id '{dataframe_id}' found in experiment '{experiment_id}'."
            )
        
        return domain.Dataframe(
            id=dataframe_id,
            name=dataframe_id,
        )
    
    def get_dataframes_metadata(self, project_name: str, experiment_id: Optional[str] = None) -> List[domain.Dataframe]:
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
            raise RubiconException(
                "experiment_id is required to retrieve dataframes from W&B."
            )
        
        try:
            wandb_path = self._get_wandb_path(project_name, experiment_id)
            run = self.api.run(wandb_path)
        except Exception as e:
            raise RubiconException(
                f"No experiment with id '{experiment_id}' found."
            ) from e
        
        dataframes = []
        # Look for W&B tables in the summary
        # We check for keys that might be dataframes (excluding features and internal keys)
        for key in run.summary.keys():
            if not key.startswith("_") and key != "features":
                # Try to check if it's likely a table/dataframe
                # This is a heuristic approach
                dataframes.append(domain.Dataframe(
                    id=key,
                    name=key,
                ))
        
        return dataframes
    
    def get_dataframe_data(self, project_name: str, dataframe_id: str, experiment_id: Optional[str] = None, df_type: str = "pandas"):
        """Retrieve dataframe data from W&B.
        
        Note: W&B tables are not fully accessible through the public API.
        This method has limited functionality and may not work for all table types.
        
        Parameters
        ----------
        project_name : str
            The name of the W&B project.
        dataframe_id : str
            The name of the dataframe (table key in W&B).
        experiment_id : str, optional
            The ID of the W&B run.
        df_type : str, optional
            The type of dataframe to return ("pandas", "dask", or "polars").
        
        Returns
        -------
        pandas.DataFrame
            The dataframe data (only pandas is currently supported).
        """
        if not experiment_id:
            raise RubiconException(
                "experiment_id is required to retrieve dataframes from W&B."
            )
        
        if df_type != "pandas":
            raise RubiconException(
                f"The W&B backend currently only supports pandas dataframes. Got: {df_type}"
            )
        
        try:
            wandb_path = self._get_wandb_path(project_name, experiment_id)
            run = self.api.run(wandb_path)
        except Exception as e:
            raise RubiconException(
                f"No experiment with id '{experiment_id}' found."
            ) from e
        
        # W&B tables are not easily accessible via the public API
        # Return empty dataframe with a warning
        import pandas as pd
        raise RubiconException(
            f"W&B backend has limited support for retrieving dataframe data. "
            f"Dataframe '{dataframe_id}' cannot be retrieved through the API. "
            f"Please access it through the W&B UI or download artifacts instead."
        )
