import logging
import os
import tempfile
import time
from json import JSONDecodeError
from typing import Any, Dict, List, Optional, Type

import pandas as pd

from rubicon_ml import domain
from rubicon_ml.domain.comment_update import CommentUpdate
from rubicon_ml.domain.tag_update import TagUpdate
from rubicon_ml.domain.utils.uuid import uuid4
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.base import RepositoryBase
from rubicon_ml.repository.utils import json, slugify

LOGGER = logging.getLogger(__name__)


class WandBRepository(RepositoryBase):
    """Repository for reading and writing rubicon-ml data to Weights & Biases.

    The ``WandBRepository`` is experimental and may contain breaking changes in future versions.
    If you encounter any bugs or missing features, open an issue on GitHub.

    All rubicon-ml domain objects are serialized and stored as private W&B Config, e.g. the
    ``_rubicon_experiment_metadata`` key would contain the complete representation of the logged
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
        Unused. Kept for backwards compatibility.
    wandb_init_kwargs : dict or none, optional
        Additional keyword arguments to be passed to ``wandb.init`` calls. Defaults to none.
    warn : bool, optional
        Whether to warn about the experimental nature of this repository. Defaults to true.
    **storage_options
        Additional keyword arguments to be passed to ``wandb.API`` initialization.
    """

    PROTOCOL = "wandb"

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
        """Get or create an active W&B run for the given project and experiment."""
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
        """Get a fresh API run object with up-to-date data from the server."""
        return self.api.run(self._get_wandb_path(project_name, experiment_id))

    def _get_wandb_path(self, project_name: str, run_id: Optional[str] = None) -> str:
        """Construct a W&B path for API calls."""
        if self.entity:
            path = f"{self.entity}/{project_name}"
        else:
            path = project_name

        if run_id:
            path = f"{path}/{run_id}"

        return path

    def _log_to_run(self, data: Dict[str, Any]):
        """Log data to W&B run history."""
        self.wandb.log(data)

    def _set_config_value(self, key: str, value: Any):
        """Set a value in W&B config."""
        self._active_run.config[key] = value

    def _read_domain_from_config(
        self, run, metadata_key: str, domain_class: Type[domain.DomainsVar]
    ) -> Optional[domain.DomainsVar]:
        """Reconstruct a domain object from stored metadata in W&B config."""
        config = run.config
        if isinstance(config, str):
            config = json.loads(config)

        if metadata_key not in config:
            api_run = self._get_api_run(run.project, run.id)
            config = api_run.config
            if isinstance(config, str):
                config = json.loads(config)

        if metadata_key in config:
            try:
                data = json.loads(config[metadata_key]["value"])
            except TypeError:
                data = json.loads(config[metadata_key])

            return domain_class(**data)

        return None

    def _read_domains_from_config(
        self, run, prefix: str, domain_class: Type[domain.DomainsVar]
    ) -> List[domain.DomainsVar]:
        """Reconstruct a list of domain objects from stored metadata in W&B config."""
        config = run.config
        if isinstance(config, str):
            config = json.loads(config)

        has_prefix = any(k.startswith(prefix) for k in config.keys())

        if not has_prefix:
            api_run = self._get_api_run(run.project, run.id)
            config = api_run.config
            if isinstance(config, str):
                config = json.loads(config)

        objects = []
        for metadata_key in config.keys():
            if metadata_key.startswith(prefix):
                try:
                    data = json.loads(config[metadata_key]["value"])
                except TypeError:
                    data = json.loads(config[metadata_key])

                objects.append(domain_class(**data))

        return objects

    def _get_tag_comment_key_prefix(
        self, entity_type: str, entity_identifier: str, key_type: str = "tags"
    ) -> str:
        """Get the prefix for tag/comment config keys."""
        if entity_type in ["Metric", "Feature", "Parameter"]:
            entity_identifier = slugify(entity_identifier, separator="_")

        return f"_rubicon_{key_type}_{entity_type.lower()}_{entity_identifier}_"

    def _read_tag_comment_data(
        self, project_name, experiment_id, entity_identifier, entity_type, key_type
    ):
        """Read tag or comment data from W&B config, sorted by timestamp."""
        run = self._get_active_run(project_name, experiment_id)
        key_prefix = self._get_tag_comment_key_prefix(entity_type, entity_identifier, key_type)

        config = run.config
        if isinstance(config, str):
            config = json.loads(config)

        has_prefix = any(k.startswith(key_prefix) for k in config.keys())

        if not has_prefix:
            api_run = self._get_api_run(project_name, experiment_id)
            config = api_run.config
            if isinstance(config, str):
                config = json.loads(config)

        data = []
        for key, value in config.items():
            if key.startswith(key_prefix):
                try:
                    if isinstance(value, dict) and "value" in value:
                        entry = json.loads(value["value"])
                    elif isinstance(value, str):
                        entry = json.loads(value)
                    else:
                        entry = value
                    data.append(entry)
                except (TypeError, JSONDecodeError):
                    continue

        data.sort(key=lambda x: x.get("_timestamp", 0))

        return data

    def finish(self):
        if self._active_run is not None:
            self._active_run.finish()
            self._active_run = None

    # -------- Abstract Method Implementations --------

    def write_domain(
        self,
        domain_obj,
        project_name,
        *,
        experiment_id=None,
        entity_identifier=None,
        entity_type=None,
    ):
        """Persist a domain object to W&B."""
        if isinstance(domain_obj, domain.Project):
            self._current_project = domain_obj

        elif isinstance(domain_obj, domain.Experiment):
            run_config = {
                "project": self._current_project.name,
                "reinit": "finish_previous",
            }
            run_config.update(self.wandb_init_kwargs)

            if self.entity:
                run_config["entity"] = self.entity
            if domain_obj.name:
                run_config["name"] = domain_obj.name
            if domain_obj.tags:
                run_config["tags"] = domain_obj.tags

            self._active_run = self.wandb.init(**run_config)

            domain_obj.id = self._active_run.id
            if domain_obj.name is None:
                domain_obj.name = self._active_run.name

            self._set_config_value("_rubicon_experiment_metadata", json.dumps(domain_obj))

        elif isinstance(domain_obj, domain.Feature):
            entity_name = slugify(domain_obj.name, separator="_")
            self._set_config_value(f"_rubicon_feature_{entity_name}", json.dumps(domain_obj))

            if domain_obj.importance is not None:
                self._log_to_run({f"{entity_name}_importance": domain_obj.importance})

        elif isinstance(domain_obj, domain.Metric):
            entity_name = slugify(domain_obj.name, separator="_")
            self._log_to_run({domain_obj.name: domain_obj.value})
            self._set_config_value(f"_rubicon_metric_{entity_name}", json.dumps(domain_obj))

        elif isinstance(domain_obj, domain.Parameter):
            entity_name = slugify(domain_obj.name, separator="_")
            self._set_config_value(domain_obj.name, domain_obj.value)
            self._set_config_value(f"_rubicon_parameter_{entity_name}", json.dumps(domain_obj))

        elif isinstance(domain_obj, domain.Artifact):
            entity_id = entity_identifier or domain_obj.id
            self._set_config_value(f"_rubicon_artifact_{entity_id}", json.dumps(domain_obj))

        elif isinstance(domain_obj, domain.Dataframe):
            self._set_config_value(f"_rubicon_dataframe_{domain_obj.id}", json.dumps(domain_obj))

        elif isinstance(domain_obj, TagUpdate):
            if experiment_id is None:
                raise RubiconException("experiment_id is required to add/remove tags in W&B.")

            run = self._get_active_run(project_name, experiment_id)

            # Sync with W&B native run.tags for experiment-level tags
            if entity_type == "Experiment":
                current_tags = list(run.tags) if run.tags else []
                if domain_obj.added_tags:
                    new_tags = current_tags + [
                        t for t in domain_obj.added_tags if t not in current_tags
                    ]
                    run.tags = new_tags
                elif domain_obj.removed_tags:
                    new_tags = [t for t in current_tags if t not in domain_obj.removed_tags]
                    run.tags = new_tags

            key_prefix = self._get_tag_comment_key_prefix(entity_type, entity_identifier, "tags")
            key = f"{key_prefix}{uuid4()}"
            if domain_obj.added_tags:
                run.config[key] = json.dumps(
                    {"added_tags": domain_obj.added_tags, "_timestamp": time.time()}
                )
            else:
                run.config[key] = json.dumps(
                    {"removed_tags": domain_obj.removed_tags, "_timestamp": time.time()}
                )

        elif isinstance(domain_obj, CommentUpdate):
            if experiment_id is None:
                raise RubiconException("experiment_id is required to add/remove comments in W&B.")

            run = self._get_active_run(project_name, experiment_id)
            key_prefix = self._get_tag_comment_key_prefix(
                entity_type, entity_identifier, "comments"
            )
            key = f"{key_prefix}{uuid4()}"
            if domain_obj.added_comments:
                run.config[key] = json.dumps(
                    {"added_comments": domain_obj.added_comments, "_timestamp": time.time()}
                )
            else:
                run.config[key] = json.dumps(
                    {"removed_comments": domain_obj.removed_comments, "_timestamp": time.time()}
                )

        else:
            raise RubiconException(f"Cannot persist domain object of type {type(domain_obj)}")

    def read_domain(
        self,
        domain_cls,
        project_name,
        *,
        experiment_id=None,
        entity_identifier=None,
        entity_type=None,
    ):
        """Read a single domain object from W&B."""
        if domain_cls is domain.Project:
            if self._current_project is not None:
                return self._current_project

            try:
                self.api.runs(self._get_wandb_path(project_name), per_page=1)
            except Exception as e:
                raise RubiconException(f"No project with name '{project_name}' found.") from e

            self._current_project = domain.Project(
                name=project_name,
                description=f"W&B project {project_name}",
            )
            return self._current_project

        elif domain_cls is domain.Experiment:
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

        elif domain_cls is domain.Metric:
            metric = self._read_domain_from_config(
                self._get_active_run(project_name, experiment_id),
                f"_rubicon_metric_{slugify(entity_identifier, separator='_')}",
                domain.Metric,
            )
            if metric is None:
                raise RubiconException(
                    f"No metric with name '{entity_identifier}' found in experiment '{experiment_id}'."
                )
            return metric

        elif domain_cls is domain.Parameter:
            parameter = self._read_domain_from_config(
                self._get_active_run(project_name, experiment_id),
                f"_rubicon_parameter_{slugify(entity_identifier, separator='_')}",
                domain.Parameter,
            )
            if parameter is None:
                raise RubiconException(
                    f"No parameter with name '{entity_identifier}' found in experiment '{experiment_id}'."
                )
            return parameter

        elif domain_cls is domain.Feature:
            # Feature uses get_features + filter (same as original)
            features = self.read_domains(domain.Feature, project_name, experiment_id=experiment_id)
            for feature in features:
                if feature.name == entity_identifier:
                    return feature
            raise RubiconException(
                f"No feature with name '{entity_identifier}' found in experiment '{experiment_id}'."
            )

        elif domain_cls is domain.Artifact:
            artifact_id = entity_identifier
            original_id, _ = artifact_id.split(":")
            artifact = self._read_domain_from_config(
                self._get_active_run(project_name, experiment_id),
                f"_rubicon_artifact_{original_id}",
                domain.Artifact,
            )
            if artifact is None:
                raise RubiconException(
                    f"No artifact with id '{original_id}' found in experiment '{experiment_id}'."
                )
            return artifact

        elif domain_cls is domain.Dataframe:
            if not experiment_id:
                raise RubiconException("experiment_id is required to retrieve dataframes from W&B.")
            dataframe = self._read_domain_from_config(
                self._get_active_run(project_name, experiment_id),
                f"_rubicon_dataframe_{entity_identifier}",
                domain.Dataframe,
            )
            if dataframe is None:
                raise RubiconException(
                    f"No dataframe with id '{entity_identifier}' found in experiment '{experiment_id}'."
                )
            return dataframe

        else:
            raise RubiconException(f"Cannot read domain object of type {domain_cls}")

    def read_domains(
        self,
        domain_cls,
        project_name=None,
        *,
        experiment_id=None,
        entity_identifier=None,
        entity_type=None,
    ):
        """Read all domain objects of a given type from W&B."""
        if domain_cls is domain.Project:
            raise RubiconException(
                "The W&B backend doesn't support listing all projects. "
                "Use `get_project(name)` with a specific project name instead."
            )

        elif domain_cls is domain.Experiment:
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

        elif domain_cls is domain.Metric:
            return self._read_domains_from_config(
                self._get_active_run(project_name, experiment_id),
                "_rubicon_metric_",
                domain.Metric,
            )

        elif domain_cls is domain.Parameter:
            return self._read_domains_from_config(
                self._get_active_run(project_name, experiment_id),
                "_rubicon_parameter_",
                domain.Parameter,
            )

        elif domain_cls is domain.Feature:
            return self._read_domains_from_config(
                self._get_active_run(project_name, experiment_id),
                "_rubicon_feature_",
                domain.Feature,
            )

        elif domain_cls is domain.Artifact:
            return self._read_domains_from_config(
                self._get_active_run(project_name, experiment_id),
                "_rubicon_artifact_",
                domain.Artifact,
            )

        elif domain_cls is domain.Dataframe:
            if not experiment_id:
                raise RubiconException("experiment_id is required to retrieve dataframes from W&B.")
            return self._read_domains_from_config(
                self._get_active_run(project_name, experiment_id),
                "_rubicon_dataframe_",
                domain.Dataframe,
            )

        elif domain_cls is TagUpdate:
            if not experiment_id:
                raise RubiconException("experiment_id is required to get tags in W&B.")
            data = self._read_tag_comment_data(
                project_name, experiment_id, entity_identifier, entity_type, "tags"
            )
            return [TagUpdate(**{k: v for k, v in d.items() if k != "_timestamp"}) for d in data]

        elif domain_cls is CommentUpdate:
            if not experiment_id:
                raise RubiconException("experiment_id is required to get comments in W&B.")
            data = self._read_tag_comment_data(
                project_name, experiment_id, entity_identifier, entity_type, "comments"
            )
            return [
                CommentUpdate(**{k: v for k, v in d.items() if k != "_timestamp"}) for d in data
            ]

        else:
            raise RubiconException(f"Cannot read domains of type {domain_cls}")

    def remove_domain(
        self,
        domain_cls,
        project_name,
        *,
        experiment_id=None,
        entity_identifier=None,
        entity_type=None,
    ):
        """W&B does not support deletion through the API."""
        raise RubiconException(
            "The W&B backend doesn't support deletion through the API. "
            "Delete runs manually through the W&B UI."
        )

    def write_artifact_data(self, data, project_name, artifact_id, *, experiment_id=None):
        """Persist artifact binary data to W&B as a W&B Artifact."""
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
            # artifact_id is "original_id:name" after create_artifact mutates it
            _, artifact_name = artifact_id.split(":")
            wandb_artifact = self.wandb.Artifact(name=artifact_name, type="model")
            wandb_artifact.add_file(temp_path, name=artifact_name)
            wandb_artifact.save()
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def read_artifact_data(self, project_name, artifact_id, *, experiment_id=None):
        """Read artifact binary data from W&B."""
        try:
            wandb_path = self._get_wandb_path(project_name)

            _, artifact_name = artifact_id.split(":")
            artifact = self.api.artifact(f"{wandb_path}/{artifact_name}:latest")
            artifact_dir = artifact.download()
            artifact_files = os.listdir(artifact_dir)

            if not artifact_files:
                raise RubiconException(f"No files found in artifact '{artifact_id}'.")

            file_path = os.path.join(artifact_dir, artifact_files[0])
            with open(file_path, "rb") as f:
                return f.read()

        except Exception as e:
            if isinstance(e, RubiconException):
                raise
            raise RubiconException(f"Failed to retrieve artifact data for '{artifact_id}'.") from e

    def write_dataframe_data(self, df, project_name, dataframe_id, *, experiment_id=None):
        """Persist dataframe data to W&B as a W&B Artifact and Table."""
        if experiment_id is None:
            raise RubiconException(
                "The W&B backend does not support project-level dataframes. "
                "Dataframes must be logged to an experiment (W&B run)."
            )

        self._get_active_run(project_name, experiment_id)

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".parquet", delete=False) as file:
            temp_path = file.name
            df.to_parquet(temp_path, index=False)

        try:
            artifact = self.wandb.Artifact(
                name=f"dataframe-{dataframe_id}",
                type="dataset",
            )
            artifact.add_file(temp_path, name=f"{dataframe_id}.parquet")
            artifact.save()
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

        dataframe_table = self.wandb.Table(dataframe=df)
        self.wandb.log({dataframe_id: dataframe_table})

    def read_dataframe_data(
        self, project_name, dataframe_id, *, experiment_id=None, df_type="pandas"
    ):
        """Read dataframe data from W&B."""
        if not experiment_id:
            raise RubiconException("experiment_id is required to retrieve dataframes from W&B.")

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
                    f"No artifact found for dataframe '{dataframe_id}' "
                    f"in experiment '{experiment_id}'."
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

    # -------- Convenience Method Overrides --------

    def create_artifact(self, artifact, data, project_name, experiment_id=None):
        """Persist an artifact to W&B.

        Overrides the default because W&B artifact creation requires mutating
        ``artifact.id`` to include the artifact name for later retrieval, and
        the write order (metadata before data) differs from filesystem backends.
        """
        if experiment_id is None:
            raise RubiconException(
                "The W&B backend does not support project-level artifacts. "
                "Artifacts must be logged to an experiment (W&B run)."
            )

        self._get_active_run(project_name, experiment_id)

        entity_id = artifact.id
        artifact.id = f"{artifact.id}:{artifact.name}"  # for accurate W&B retrieval

        self.write_domain(
            artifact,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=entity_id,
            entity_type="Artifact",
        )
        self.write_artifact_data(data, project_name, artifact.id, experiment_id=experiment_id)

    def create_dataframe(self, dataframe, data, project_name, experiment_id=None):
        """Persist a dataframe to W&B.

        Overrides the default because W&B needs the dataframe description for the
        artifact metadata and write order differs.
        """
        if experiment_id is None:
            raise RubiconException(
                "The W&B backend does not support project-level dataframes. "
                "Dataframes must be logged to an experiment (W&B run)."
            )

        self.write_domain(
            dataframe,
            project_name,
            experiment_id=experiment_id,
            entity_identifier=dataframe.id,
            entity_type="Dataframe",
        )
        self.write_dataframe_data(data, project_name, dataframe.id, experiment_id=experiment_id)

    def get_projects(self) -> List[domain.Project]:
        """W&B does not support listing all projects."""
        raise RubiconException(
            "The W&B backend doesn't support listing all projects. "
            "Use `get_project(name)` with a specific project name instead."
        )
