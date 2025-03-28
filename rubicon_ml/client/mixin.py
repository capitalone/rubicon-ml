from __future__ import annotations

import contextlib
import json
import os
import pickle
import subprocess
import tempfile
import warnings
import zipfile
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TextIO, Union

import fsspec

from rubicon_ml import client, domain
from rubicon_ml.client.utils.exception_handling import failsafe
from rubicon_ml.client.utils.tags import TagContainer, filter_children
from rubicon_ml.domain import Artifact as ArtifactDomain
from rubicon_ml.exceptions import RubiconException

if TYPE_CHECKING:
    import dask.dataframe as dd
    import pandas as pd
    import polars as pl
    import xgboost as xgb

    from rubicon_ml.client import Artifact, Dataframe
    from rubicon_ml.domain import DOMAIN_TYPES


class ArtifactMixin:
    """Adds artifact support to a client object."""

    _domain: ArtifactDomain

    def _validate_data(self, data_bytes, data_directory, data_file, data_object, data_path, name):
        """Raises a `RubiconException` if the data to log as an artifact is improperly provided."""
        if not any([data_bytes, data_directory, data_file, data_object, data_path]):
            raise RubiconException(
                "One of `data_bytes`, `data_directory`, `data_file`, `data_object` or "
                "`data_path` must be provided."
            )

        if name is None:
            if data_path is not None:
                name = os.path.basename(data_path)
            else:
                raise RubiconException("`name` must be provided if not using `data_path`.")

        if data_directory is not None:
            temp_file_context = tempfile.TemporaryDirectory
        else:
            temp_file_context = contextlib.nullcontext

        if data_bytes is None:
            with temp_file_context() as temp_dir:
                if data_object is not None:
                    data_bytes = pickle.dumps(data_object)
                else:
                    if data_directory is not None:
                        temp_zip_name = Path(f"{temp_dir}/{name}")

                        with zipfile.ZipFile(str(temp_zip_name), "w") as zip_file:
                            for dir_path, _, files in os.walk(data_directory):
                                for file in files:
                                    zip_file.write(Path(f"{dir_path}/{file}"), arcname=file)

                        file = fsspec.open(temp_zip_name, "rb")
                    elif data_file is not None:
                        file = data_file
                    elif data_path is not None:
                        file = fsspec.open(data_path, "rb")

                    with file as open_file:
                        data_bytes = open_file.read()

        return data_bytes, name

    @failsafe
    def log_artifact(
        self,
        data_bytes: Optional[bytes] = None,
        data_directory: Optional[str] = None,
        data_file: Optional[TextIO] = None,
        data_object: Optional[Any] = None,
        data_path: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        comments: Optional[List[str]] = None,
    ) -> Artifact:
        """Log an artifact to this client object.

        Parameters
        ----------
        data_bytes : bytes, optional
            The raw bytes to log as an artifact.
        data_directory : str, optional
            The path to a directory to zip and log as an artifact.
        data_file : TextIOWrapper, optional
            The open file to log as an artifact.
        data_object : python object, optional
            The python object to log as an artifact.
        data_path : str, optional
            The absolute or relative local path or S3 path
            to the data to log as an artifact. S3 paths
            must be prepended with 's3://'.
        name : str, optional
            The name of the artifact file. Required if
            `data_path` is not provided.
        description : str, optional
            A description of the artifact. Use to provide
            additional context.
        tags : list of str, optional
            Values to tag the experiment with. Use tags to organize and
            filter your artifacts.
        comments : list of str, optional
            Values to comment the experiment with. Use comments to organize and
            filter your artifacts.

        Notes
        -----
        Only one of `data_bytes`, `data_file`, `data_object`, and `data_path`
        should be provided. If more than one is given, the order
        of precedence is `data_bytes`, `data_object`, `data_file`, `data_path`.

        Returns
        -------
        rubicon.client.Artifact
            The new artifact.

        Examples
        --------
        >>> # Log with bytes
        >>> experiment.log_artifact(
        ...     data_bytes=b'hello rubicon!',
        ...     name="bytes_artifact",
        ...     description="log artifact from bytes",
        ... )

        >>> # Log zipped directory
        >>> experiment.log_artifact(
        ...     data_directory="./path/to/directory/",
        ...     name="directory.zip",
        ...     description="log artifact from zipped directory",
        ... )

        >>> # Log with file
        >>> with open('./path/to/artifact.txt', 'rb') as file:
        >>>     project.log_artifact(
        ...         data_file=file,
        ...         name="file_artifact",
        ...         description="log artifact from file",
        ...     )

        >>> # Log with file path
        >>> experiment.log_artifact(
        ...     data_path="./path/to/artifact.pkl",
        ...     description="log artifact from file path",
        ... )
        """
        if tags is None:
            tags = []
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        if comments is None:
            comments = []
        if not isinstance(comments, list) or not all(
            [isinstance(comment, str) for comment in comments]
        ):
            raise ValueError("`comments` must be `list` of type `str`")

        data_bytes, name = self._validate_data(
            data_bytes, data_directory, data_file, data_object, data_path, name
        )

        artifact = domain.Artifact(
            name=name,
            description=description,
            parent_id=self._domain.id,
            tags=tags,
            comments=comments,
        )

        project_name, experiment_id = self._get_identifiers()
        for repo in self.repositories:
            repo.create_artifact(artifact, data_bytes, project_name, experiment_id=experiment_id)

        return client.Artifact(artifact, self)

    def _get_environment_bytes(self, export_cmd: List[str]) -> bytes:
        """Get the working environment as a sequence of bytes.

        Parameters
        ----------
        export_cmd : list of str
            The command to export the environment.

        Returns
        -------
        bytes
            A bytes sequence of the environment.
        """
        try:
            completed_process = subprocess.run(export_cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            raise RubiconException(e.stderr)

        return completed_process.stdout

    @failsafe
    def log_conda_environment(self, artifact_name: Optional[str] = None) -> Artifact:
        """Log the conda environment as an artifact to this client object.
        Useful for recreating your exact environment at a later date.

        Parameters
        ----------
        artifact_name : str, optional
            The name of the artifact (the exported conda environment).

        Returns
        -------
        rubicon.client.Artifact
            The new artifact.

        Notes
        -----
        Relies on running with an active conda environment.
        """
        if artifact_name is None:
            artifact_name = f"environment-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.yml"

        env_bytes = self._get_environment_bytes(["conda", "env", "export"])
        artifact = self.log_artifact(data_bytes=env_bytes, name=artifact_name)

        return artifact

    @failsafe
    def log_h2o_model(
        self,
        h2o_model,
        artifact_name: Optional[str] = None,
        export_cross_validation_predictions: bool = False,
        use_mojo: bool = False,
        **log_artifact_kwargs,
    ) -> Artifact:
        """Log an `h2o` model as an artifact using `h2o.save_model`.

        Parameters
        ----------
        h2o_model : h2o.model.ModelBase
            The `h2o` model to log as an artifact.
        artifact_name : str, optional (default None)
            The name of the artifact. Defaults to None, using `h2o_model`'s class name.
        export_cross_validation_predictions: bool, optional (default False)
            Passed directly to `h2o.save_model`.
        use_mojo: bool, optional (default False)
            Whether to log the model in MOJO format. If False, the model will be
            logged in binary format.
        log_artifact_kwargs : dict
            Additional kwargs to be passed directly to `self.log_artifact`.
        """
        import h2o

        if self.repository.PROTOCOL == "memory":
            raise RubiconException("`h2o` models cannot be logged in memory with `log_h2o_model`.")

        if artifact_name is None:
            artifact_name = h2o_model.__class__.__name__

        with tempfile.TemporaryDirectory() as temp_dir_name:
            if use_mojo:
                model_data_path = f"{temp_dir_name}/{artifact_name}.zip"
                h2o_model.download_mojo(path=model_data_path)
            else:
                model_data_path = h2o.save_model(
                    h2o_model,
                    export_cross_validation_predictions=export_cross_validation_predictions,
                    filename=artifact_name,
                    path=temp_dir_name,
                )

            artifact = self.log_artifact(
                name=artifact_name,
                data_path=model_data_path,
                **log_artifact_kwargs,
            )

        return artifact

    @failsafe
    def log_xgboost_model(
        self,
        xgboost_model: "xgb.Booster",
        artifact_name: Optional[str] = None,
        **log_artifact_kwargs: Any,
    ) -> Artifact:
        """Log an XGBoost model as a JSON file to this client object.

        Please note that we do not currently support logging directly from the SKLearn interface.

        Parameters
        ----------
        xgboost_model: Booster
            An xgboost model object in the Booster format
        artifact_name : str, optional
            The name of the artifact (the exported XGBoost model).
        log_artifact_kwargs : Any
            Additional kwargs to be passed directly to `self.log_artifact`.

        Returns
        -------
        rubicon.client.Artifact
            The new artifact.
        """
        if artifact_name is None:
            artifact_name = xgboost_model.__class__.__name__

        # TODO: handle sklearn
        booster = xgboost_model

        with tempfile.TemporaryDirectory() as temp_dir_name:
            model_location = f"{temp_dir_name}/{artifact_name}.json"
            booster.save_model(model_location)

            artifact = self.log_artifact(
                name=artifact_name,
                data_path=model_location,
                **log_artifact_kwargs,
            )

        return artifact

    @failsafe
    def log_pip_requirements(self, artifact_name: Optional[str] = None) -> Artifact:
        """Log the pip requirements as an artifact to this client object.
        Useful for recreating your exact environment at a later date.

        Parameters
        ----------
        artifact_name : str, optional
            The name of the artifact (the exported pip environment).

        Returns
        -------
        rubicon.client.Artifact
            The new artifact.
        """
        if artifact_name is None:
            artifact_name = f"requirements-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.txt"

        requirements_bytes = self._get_environment_bytes(["pip", "freeze"])
        artifact = self.log_artifact(data_bytes=requirements_bytes, name=artifact_name)

        return artifact

    @failsafe
    def artifacts(
        self,
        name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        qtype: str = "or",
    ) -> List[Artifact]:
        """Get the artifacts logged to this client object.

        Parameters
        ----------
        name : str, optional
            The name value to filter results on.
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.

        Returns
        -------
        list of rubicon.client.Artifact
            The artifacts previously logged to this client object.
        """
        if tags is None:
            tags = []
        project_name, experiment_id = self._get_identifiers()
        return_err = None
        for repo in self.repositories:
            try:
                artifacts = [
                    client.Artifact(a, self)
                    for a in repo.get_artifacts_metadata(project_name, experiment_id=experiment_id)
                ]
            except Exception as err:
                return_err = err
            else:
                self._artifacts = filter_children(artifacts, tags, qtype, name)
                return self._artifacts

        self._raise_rubicon_exception(return_err)

    @failsafe
    def artifact(self, name: Optional[str] = None, id: Optional[str] = None) -> Artifact:
        """Get an artifact logged to this project by id or name.

        Parameters
        ----------
        id : str
            The id of the artifact to get.
        name : str
            The name of the artifact to get.

        Returns
        -------
        rubicon.client.Artifact
            The artifact logged to this project with id `id` or name 'name'.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        if name is not None:
            artifacts = self.artifacts(name=name)

            if len(artifacts) == 0:
                raise RubiconException(f"No artifact found with name '{name}'.")
            if len(artifacts) > 1:
                warnings.warn(
                    f"Multiple artifacts found with name '{name}'. Returning most recently logged."
                )

            artifact = artifacts[-1]
            return artifact
        else:
            project_name, experiment_id = self._get_identifiers()
            return_err = None
            for repo in self.repositories:
                try:
                    artifact = client.Artifact(
                        repo.get_artifact_metadata(project_name, id, experiment_id),
                        self,
                    )
                except Exception as err:
                    return_err = err
                else:
                    return artifact

        self._raise_rubicon_exception(return_err)

    @failsafe
    def delete_artifacts(self, ids: List[str]):
        """Delete the artifacts logged to with client object
        with ids `ids`.

        Parameters
        ----------
        ids : list of str
            The ids of the artifacts to delete.
        """
        project_name, experiment_id = self._get_identifiers()

        for artifact_id in ids:
            for repo in self.repositories:
                repo.delete_artifact(project_name, artifact_id, experiment_id=experiment_id)

    @failsafe
    def log_json(
        self,
        json_object: Dict[str, Any],
        name: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Artifact:
        """Log a python dictionary to a JSON file.

        Parameters
        ----------
        json_object : Dict[str, Any]
            A python dictionary capable of being converted to JSON.
        name : Optional[str], optional
            A name for this JSON file, by default None
        description : Optional[str], optional
            A description for this file, by default None
        tags : Optional[List[str]], optional
            Any Rubicon tags, by default None

        Returns
        -------
        Artifact
            The new artifact.

        """
        if name is None:
            json_name = f"dictionary-{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.json"
        else:
            json_name = name

        artifact = self.log_artifact(
            data_bytes=bytes(json.dumps(json_object), "utf-8"),
            name=json_name,
            description=description,
            tags=tags,
        )

        return artifact


class DataframeMixin:
    """Adds dataframe support to a client object."""

    _domain: DOMAIN_TYPES

    @failsafe
    def log_dataframe(
        self,
        df: Union[pd.DataFrame, "dd.DataFrame", "pl.DataFrame"],
        description: Optional[str] = None,
        name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        comments: Optional[List[str]] = None,
    ) -> Dataframe:
        """Log a dataframe to this client object.

        Parameters
        ----------
        df : pandas.DataFrame, dask.dataframe.DataFrame, or polars DataFrame
            The dataframe to log.
        description : str, optional
            The dataframe's description. Use to provide
            additional context.
        tags : list of str
            The values to tag the dataframe with.
        comments: list of str
            The values to comment the dataframe with.

        Returns
        -------
        rubicon.client.Dataframe
            The new dataframe.
        """
        if tags is None:
            tags = []
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        if comments is None:
            comments = []
        if not isinstance(comments, list) or not all(
            [isinstance(comment, str) for comment in comments]
        ):
            raise ValueError("`comments` must be `list` of type `str`")

        dataframe = domain.Dataframe(
            parent_id=self._domain.id,
            description=description,
            name=name,
            tags=tags,
            comments=comments,
        )

        project_name, experiment_id = self._get_identifiers()
        for repo in self.repositories:
            repo.create_dataframe(dataframe, df, project_name, experiment_id=experiment_id)

        return client.Dataframe(dataframe, self)

    @failsafe
    def dataframes(
        self,
        name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        qtype: str = "or",
    ) -> List[Dataframe]:
        """Get the dataframes logged to this client object.

        Parameters
        ----------
        name : str, optional
            The name value to filter results on.
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.

        Returns
        -------
        list of rubicon.client.Dataframe
            The dataframes previously logged to this client object.
        """
        if tags is None:
            tags = []
        project_name, experiment_id = self._get_identifiers()
        return_err = None
        for repo in self.repositories:
            try:
                dataframes = [
                    client.Dataframe(d, self)
                    for d in repo.get_dataframes_metadata(project_name, experiment_id=experiment_id)
                ]
            except Exception as err:
                return_err = err
            else:
                self._dataframes = filter_children(dataframes, tags, qtype, name)
                return self._dataframes

        self._raise_rubicon_exception(return_err)

    @failsafe
    def dataframe(self, name: Optional[str] = None, id: Optional[str] = None) -> Dataframe:
        """
        Get the dataframe logged to this client object.

        Parameters
        ----------
        id : str
            The id of the dataframe to get.
        name : str
            The name of the dataframe to get.
        Returns
        -------
        rubicon.client.Dataframe
            The dataframe logged to this project with id `id` or name 'name'.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        elif name is not None:
            dataframes = self.dataframes(name=name)

            if len(dataframes) == 0:
                raise RubiconException(f"No dataframe found with name '{name}'.")
            elif len(dataframes) > 1:
                warnings.warn(
                    f"Multiple dataframes found with name '{name}'."
                    " Returning most recently logged."
                )

            dataframe = dataframes[-1]
            return dataframe
        else:
            project_name, experiment_id = self._get_identifiers()
            return_err = None
            for repo in self.repositories:
                try:
                    dataframe = client.Dataframe(
                        repo.get_dataframe_metadata(
                            project_name, experiment_id=experiment_id, dataframe_id=id
                        ),
                        self,
                    )
                except Exception as err:
                    return_err = err
                else:
                    return dataframe

        self._raise_rubicon_exception(return_err)

    @failsafe
    def delete_dataframes(self, ids: List[str]):
        """Delete the dataframes with ids `ids` logged to
        this client object.

        Parameters
        ----------
        ids : list of str
            The ids of the dataframes to delete.
        """
        project_name, experiment_id = self._get_identifiers()

        for dataframe_id in ids:
            for repo in self.repositories:
                repo.delete_dataframe(project_name, dataframe_id, experiment_id=experiment_id)


class TagMixin:
    """Adds tag support to a client object."""

    _domain: DOMAIN_TYPES

    def _get_taggable_identifiers(self):
        project_name, experiment_id = self._parent._get_identifiers()
        entity_identifier = None

        # experiments do not return an entity identifier - they are the entity
        if isinstance(self, client.Experiment):
            experiment_id = self.id
        # dataframes and artifacts are identified by their `id`s
        elif isinstance(self, client.Dataframe) or isinstance(self, client.Artifact):
            entity_identifier = self.id
        # everything else is identified by its `name`
        else:
            entity_identifier = self.name

        return project_name, experiment_id, entity_identifier

    @failsafe
    def add_tags(self, tags: List[str]):
        """Add tags to this client object.

        Parameters
        ----------
        tags : list of str
            The tag values to add.
        """
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        project_name, experiment_id, entity_identifier = self._get_taggable_identifiers()

        self._domain.add_tags(tags)
        for repo in self.repositories:
            repo.add_tags(
                project_name,
                tags,
                experiment_id=experiment_id,
                entity_identifier=entity_identifier,
                entity_type=self.__class__.__name__,
            )

    @failsafe
    def remove_tags(self, tags: List[str]):
        """Remove tags from this client object.

        Parameters
        ----------
        tags : list of str
             The tag values to remove.
        """
        project_name, experiment_id, entity_identifier = self._get_taggable_identifiers()

        self._domain.remove_tags(tags)
        for repo in self.repositories:
            repo.remove_tags(
                project_name,
                tags,
                experiment_id=experiment_id,
                entity_identifier=entity_identifier,
                entity_type=self.__class__.__name__,
            )

    def _update_tags(self, tag_data):
        """Add or remove the tags in `tag_data` based on
        their key.
        """
        for tag in tag_data:
            self._domain.add_tags(tag.get("added_tags", []))
            self._domain.remove_tags(tag.get("removed_tags", []))

    @property
    def tags(self) -> TagContainer:
        """Get this client object's tags."""
        project_name, experiment_id, entity_identifier = self._get_taggable_identifiers()
        return_err = None
        for repo in self.repositories:
            try:
                tag_data = repo.get_tags(
                    project_name,
                    experiment_id=experiment_id,
                    entity_identifier=entity_identifier,
                    entity_type=self.__class__.__name__,
                )
            except Exception as err:
                return_err = err
            else:
                self._update_tags(tag_data)

                return TagContainer(self._domain.tags)

        self._raise_rubicon_exception(return_err)


class CommentMixin:
    """Adds comment support to a client object."""

    _domain: DOMAIN_TYPES

    def _get_commentable_identifiers(self):
        project_name, experiment_id = self._parent._get_identifiers()
        entity_identifier = None

        # experiments do not return an entity identifier - they are the entity
        if isinstance(self, client.Experiment):
            experiment_id = self.id
        # dataframes and artifacts are identified by their `id`s
        elif isinstance(self, client.Dataframe) or isinstance(self, client.Artifact):
            entity_identifier = self.id
        # everything else is identified by its `name`
        else:
            entity_identifier = self.name

        return project_name, experiment_id, entity_identifier

    @failsafe
    def add_comments(self, comments: List[str]):
        """Add comments to this client object.

        Parameters
        ----------
        comments : list of str
            The comment values to add.
        """
        if not isinstance(comments, list) or not all(
            [isinstance(comment, str) for comment in comments]
        ):
            raise ValueError("`comments` must be `list` of type `str`")

        project_name, experiment_id, entity_identifier = self._get_commentable_identifiers()

        self._domain.add_comments(comments)
        for repo in self.repositories:
            repo.add_comments(
                project_name,
                comments,
                experiment_id=experiment_id,
                entity_identifier=entity_identifier,
                entity_type=self.__class__.__name__,
            )

    @failsafe
    def remove_comments(self, comments: List[str]):
        """Remove comments from this client object.

        Parameters
        ----------
        comments : list of str
             The comment values to remove.
        """
        project_name, experiment_id, entity_identifier = self._get_commentable_identifiers()

        self._domain.remove_comments(comments)
        for repo in self.repositories:
            repo.remove_comments(
                project_name,
                comments,
                experiment_id=experiment_id,
                entity_identifier=entity_identifier,
                entity_type=self.__class__.__name__,
            )

    def _update_comments(self, comment_data):
        """Add or remove the comments in `comment_data` based on
        their key.
        """
        for comment in comment_data:
            self._domain.add_comments(comment.get("added_comments", []))
            self._domain.remove_comments(comment.get("removed_comments", []))

    @property
    def comments(self) -> List[str]:
        """Get this client object's comments."""
        project_name, experiment_id, entity_identifier = self._get_commentable_identifiers()
        return_err = None
        for repo in self.repositories:
            try:
                comment_data = repo.get_comments(
                    project_name,
                    experiment_id=experiment_id,
                    entity_identifier=entity_identifier,
                    entity_type=self.__class__.__name__,
                )
            except Exception as err:
                return_err = err
            else:
                self._update_comments(comment_data)

                return self._domain.comments

        self._raise_rubicon_exception(return_err)
