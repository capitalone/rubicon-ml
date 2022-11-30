import os
import pickle
import subprocess
import warnings
from datetime import datetime

import fsspec

from rubicon_ml import client, domain
from rubicon_ml.client.utils.exception_handling import failsafe
from rubicon_ml.client.utils.tags import filter_children
from rubicon_ml.exceptions import RubiconException


class ArtifactMixin:
    """Adds artifact support to a client object."""

    def _validate_data(self, data_bytes, data_file, data_object, data_path, name):
        """Raises a `RubiconException` if the data to log as
        an artifact is improperly provided.
        """
        if not any([data_bytes, data_file, data_object, data_path]):
            raise RubiconException(
                "One of `data_bytes`, `data_file`, `data_object` or `data_path` must be provided."
            )

        if name is None:
            if data_path is not None:
                name = os.path.basename(data_path)
            else:
                raise RubiconException("`name` must be provided if not using `data_path`.")

        if data_bytes is None:
            if data_object is not None:
                data_bytes = pickle.dumps(data_object)
            else:
                if data_file is not None:
                    f = data_file
                elif data_path is not None:
                    f = fsspec.open(data_path, "rb")

                with f as open_file:
                    data_bytes = open_file.read()

        return data_bytes, name

    @failsafe
    def log_artifact(
        self,
        data_bytes=None,
        data_file=None,
        data_object=None,
        data_path=None,
        name=None,
        description=None,
        tags=[],
    ):
        """Log an artifact to this client object.

        Parameters
        ----------
        data_bytes : bytes, optional
            The raw bytes to log as an artifact.
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
        ...     data_bytes=b'hello rubicon!', name='bytes_artifact', description="log artifact from bytes"
        ... )

        >>> # Log with file
        >>> with open('some_relevant_file', 'rb') as f:
        >>>     project.log_artifact(
        ...         data_file=f, name='file_artifact', description="log artifact from file"
        ... )

        >>> # Log with file path
        >>> experiment.log_artifact(
        ...     data_path="./path/to/artifact.pkl", description="log artifact from file path"
        ... )
        """

        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        data_bytes, name = self._validate_data(data_bytes, data_file, data_object, data_path, name)

        artifact = domain.Artifact(
            name=name,
            description=description,
            parent_id=self._domain.id,
            tags=tags,
        )

        project_name, experiment_id = self._get_identifiers()
        self.repository.create_artifact(
            artifact, data_bytes, project_name, experiment_id=experiment_id
        )

        return client.Artifact(artifact, self)

    def _get_environment_bytes(self, export_cmd):
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
    def log_conda_environment(self, artifact_name=None):
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
    def log_pip_requirements(self, artifact_name=None):
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
    def artifacts(self, name=None, tags=[], qtype="or"):
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
        project_name, experiment_id = self._get_identifiers()
        artifacts = [
            client.Artifact(a, self)
            for a in self.repository.get_artifacts_metadata(
                project_name, experiment_id=experiment_id
            )
        ]

        self._artifacts = filter_children(artifacts, tags, qtype, name)

        return self._artifacts

    @failsafe
    def artifact(self, name=None, id=None):
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
        else:
            project_name, experiment_id = self._get_identifiers()
            artifact = client.Artifact(
                self.repository.get_artifact_metadata(project_name, id, experiment_id), self
            )

        return artifact

    @failsafe
    def delete_artifacts(self, ids):
        """Delete the artifacts logged to with client object
        with ids `ids`.

        Parameters
        ----------
        ids : list of str
            The ids of the artifacts to delete.
        """
        project_name, experiment_id = self._get_identifiers()

        for artifact_id in ids:
            self.repository.delete_artifact(project_name, artifact_id, experiment_id=experiment_id)


class DataframeMixin:
    """Adds dataframe support to a client object."""

    @failsafe
    def log_dataframe(self, df, description=None, name=None, tags=[]):
        """Log a dataframe to this client object.

        Parameters
        ----------
        df : pandas.DataFrame or dask.dataframe.DataFrame
            The `dask` or `pandas` dataframe to log.
        description : str, optional
            The dataframe's description. Use to provide
            additional context.
        tags : list of str
            The values to tag the dataframe with.

        Returns
        -------
        rubicon.client.Dataframe
            The new dataframe.
        """
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        dataframe = domain.Dataframe(
            parent_id=self._domain.id,
            description=description,
            name=name,
            tags=tags,
        )

        project_name, experiment_id = self._get_identifiers()
        self.repository.create_dataframe(dataframe, df, project_name, experiment_id=experiment_id)

        return client.Dataframe(dataframe, self)

    @failsafe
    def dataframes(self, name=None, tags=[], qtype="or"):
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
        project_name, experiment_id = self._get_identifiers()
        dataframes = [
            client.Dataframe(d, self)
            for d in self.repository.get_dataframes_metadata(
                project_name, experiment_id=experiment_id
            )
        ]

        self._dataframes = filter_children(dataframes, tags, qtype, name)

        return self._dataframes

    @failsafe
    def dataframe(self, name=None, id=None):
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
        else:
            project_name, experiment_id = self._get_identifiers()
            dataframe = client.Dataframe(
                self.repository.get_dataframe_metadata(
                    project_name, experiment_id=experiment_id, dataframe_id=id
                ),
                self,
            )

        return dataframe

    @failsafe
    def delete_dataframes(self, ids):
        """Delete the dataframes with ids `ids` logged to
        this client object.

        Parameters
        ----------
        ids : list of str
            The ids of the dataframes to delete.
        """
        project_name, experiment_id = self._get_identifiers()

        for dataframe_id in ids:
            self.repository.delete_dataframe(
                project_name, dataframe_id, experiment_id=experiment_id
            )


class TagMixin:
    """Adds tag support to a client object."""

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
    def add_tags(self, tags):
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
        self.repository.add_tags(
            project_name,
            tags,
            experiment_id=experiment_id,
            entity_identifier=entity_identifier,
            entity_type=self.__class__.__name__,
        )

    @failsafe
    def remove_tags(self, tags):
        """Remove tags from this client object.

        Parameters
        ----------
        tags : list of str
             The tag values to remove.
        """
        project_name, experiment_id, entity_identifier = self._get_taggable_identifiers()

        self._domain.remove_tags(tags)
        self.repository.remove_tags(
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
    def tags(self):
        """Get this client object's tags."""
        project_name, experiment_id, entity_identifier = self._get_taggable_identifiers()
        tag_data = self.repository.get_tags(
            project_name,
            experiment_id=experiment_id,
            entity_identifier=entity_identifier,
            entity_type=self.__class__.__name__,
        )

        self._update_tags(tag_data)

        return self._domain.tags
