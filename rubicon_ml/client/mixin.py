import os
import subprocess
from datetime import datetime

import fsspec

from rubicon_ml import client, domain
from rubicon_ml.client.utils.tags import has_tag_requirements
from rubicon_ml.exceptions import RubiconException


class MultiParentMixin:
    """Adds utils for client objects that can be logged
    to either a `Project` or `Experiment`.
    """

    def _get_parent_identifiers(self):
        """Get the project name and experiment ID (or
        `None`) of this client object's parent(s).
        """
        experiment_id = None

        if isinstance(self, client.Project):
            project_name = self.name
        else:
            project_name = self.project.name
            experiment_id = self.id

        return project_name, experiment_id


class ArtifactMixin(MultiParentMixin):
    """Adds artifact support to a client object."""

    def _validate_data(self, data_bytes, data_file, data_path, name):
        """Raises a `RubiconException` if the data to log as
        an artifact is improperly provided.
        """
        if not any([data_bytes, data_file, data_path]):
            raise RubiconException(
                "One of `data_bytes`, `data_file` or `data_path` must be provided."
            )

        if name is None:
            if data_path is not None:
                name = os.path.basename(data_path)
            else:
                raise RubiconException("`name` must be provided if not using `data_path`.")

        if data_bytes is None:
            if data_file is not None:
                f = data_file
            elif data_path is not None:
                f = fsspec.open(data_path, "rb")

            with f as open_file:
                data_bytes = open_file.read()

        return data_bytes, name

    def log_artifact(
        self, data_bytes=None, data_file=None, data_path=None, name=None, description=None
    ):
        """Log an artifact to this client object.

        Parameters
        ----------
        data_bytes : bytes, optional
            The raw bytes to log as an artifact.
        data_file : TextIOWrapper, optional
            The open file to log as an artifact.
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

        Notes
        -----
        Only one of `data_bytes`, `data_file`, and `data_path`
        should be provided. If more than one is given, the order
        of precedence is `data_bytes`, `data_file`, `data_path`.

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
        data_bytes, name = self._validate_data(data_bytes, data_file, data_path, name)

        artifact = domain.Artifact(name=name, description=description, parent_id=self._domain.id)

        project_name, experiment_id = self._get_parent_identifiers()
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

    def artifacts(self):
        """Get the artifacts logged to this client object.

        Returns
        -------
        list of rubicon.client.Artifact
            The artifacts previously logged to this client object.
        """
        project_name, experiment_id = self._get_parent_identifiers()

        self._artifacts = [
            client.Artifact(a, self)
            for a in self.repository.get_artifacts_metadata(
                project_name, experiment_id=experiment_id
            )
        ]

        return self._artifacts

    def delete_artifacts(self, ids):
        """Delete the artifacts logged to with client object
        with ids `ids`.

        Parameters
        ----------
        ids : list of str
            The ids of the artifacts to delete.
        """
        project_name, experiment_id = self._get_parent_identifiers()

        for artifact_id in ids:
            self.repository.delete_artifact(project_name, artifact_id, experiment_id=experiment_id)


class DataframeMixin(MultiParentMixin):
    """Adds dataframe support to a client object."""

    def log_dataframe(self, df, description=None, tags=[]):
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
        dataframe = domain.Dataframe(parent_id=self._domain.id, description=description, tags=tags)

        project_name, experiment_id = self._get_parent_identifiers()
        self.repository.create_dataframe(dataframe, df, project_name, experiment_id=experiment_id)

        return client.Dataframe(dataframe, self)

    def _filter_dataframes(self, dataframes, tags, qtype):
        """Filters the provided dataframes by `tags` using
        query type `qtype`.
        """
        if len(tags) > 0:
            filtered_dataframes = []
            [
                filtered_dataframes.append(d)
                for d in dataframes
                if has_tag_requirements(d.tags, tags, qtype)
            ]
            self._dataframes = filtered_dataframes
        else:
            self._dataframes = dataframes

    def dataframes(self, tags=[], qtype="or"):
        """Get the dataframes logged to this client object.

        Parameters
        ----------
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
        project_name, experiment_id = self._get_parent_identifiers()
        dataframes = [
            client.Dataframe(d, self)
            for d in self.repository.get_dataframes_metadata(
                project_name, experiment_id=experiment_id
            )
        ]

        self._filter_dataframes(dataframes, tags, qtype)

        return self._dataframes

    def delete_dataframes(self, ids):
        """Delete the dataframes with ids `ids` logged to
        this client object.

        Parameters
        ----------
        ids : list of str
            The ids of the dataframes to delete.
        """
        project_name, experiment_id = self._get_parent_identifiers()

        for dataframe_id in ids:
            self.repository.delete_dataframe(
                project_name, dataframe_id, experiment_id=experiment_id
            )


class TagMixin:
    """Adds tag support to a client object."""

    def _get_taggable_identifiers(self):
        dataframe_id = None

        if isinstance(self, client.Dataframe):
            project_name, experiment_id = self.parent._get_parent_identifiers()
            dataframe_id = self.id
        else:
            project_name, experiment_id = self._get_parent_identifiers()

        return project_name, experiment_id, dataframe_id

    def add_tags(self, tags):
        """Add tags to this client object.

        Parameters
        ----------
        tags : list of str
            The tag values to add.
        """
        project_name, experiment_id, dataframe_id = self._get_taggable_identifiers()

        self._domain.add_tags(tags)
        self.repository.add_tags(
            project_name, tags, experiment_id=experiment_id, dataframe_id=dataframe_id
        )

    def remove_tags(self, tags):
        """Remove tags from this client object.

        Parameters
        ----------
        tags : list of str
             The tag values to remove.
        """
        project_name, experiment_id, dataframe_id = self._get_taggable_identifiers()

        self._domain.remove_tags(tags)
        self.repository.remove_tags(
            project_name, tags, experiment_id=experiment_id, dataframe_id=dataframe_id
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
        project_name, experiment_id, dataframe_id = self._get_taggable_identifiers()
        tag_data = self.repository.get_tags(
            project_name,
            experiment_id=experiment_id,
            dataframe_id=dataframe_id,
        )

        self._update_tags(tag_data)

        return self._domain.tags
