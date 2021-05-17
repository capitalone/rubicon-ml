import asyncio
from datetime import datetime

from rubicon_ml import domain
from rubicon_ml.client import asynchronous as client
from rubicon_ml.client.mixin import MultiParentMixin
from rubicon_ml.client.utils.tags import has_tag_requirements


class ArtifactMixin(MultiParentMixin):
    """Adds artifact support to an asynchronous client object."""

    async def log_artifact(
        self, data_bytes=None, data_file=None, data_path=None, name=None, description=None
    ):
        """Overrides `rubicon.client.ArtifactMixin.log_artifact` to
        asynchronously log an artifact to this client object.

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
        >>> await experiment.log_artifact(
        ...     data_bytes=b'hello rubicon!', name='bytes_artifact', description="log artifact from bytes"
        ... )

        >>> # Log with file
        >>> with open('some_relevant_file', 'rb') as f:
        >>>     await project.log_artifact(
        ...         data_file=f, name='file_artifact', description="log artifact from file"
        ...     )

        >>> # Log with file path
        >>> await experiment.log_artifact(
        ...     data_path="./path/to/artifact.pkl", description="log artifact from file path"
        ... )
        """
        data_bytes, name = self._validate_data(data_bytes, data_file, data_path, name)

        artifact = domain.Artifact(name=name, description=description, parent_id=self._domain.id)

        project_name, experiment_id = self._get_parent_identifiers()
        await self.repository.create_artifact(
            artifact, data_bytes, project_name, experiment_id=experiment_id
        )

        return client.Artifact(artifact, self)

    async def log_conda_environment(self, artifact_name=None):
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

        env_bytes = self._get_environment_bytes("conda env export")
        artifact = await self.log_artifact(data_bytes=env_bytes, name=artifact_name)

        return artifact

    async def log_pip_requirements(self, artifact_name=None):
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

        requirements_bytes = self._get_environment_bytes("pip freeze")
        artifact = await self.log_artifact(data_bytes=requirements_bytes, name=artifact_name)

        return artifact

    async def artifacts(self):
        """Overrides `rubicon.client.ArtifcatMixin.artifacts` to
        asynchronously get the artifacts logged to this client object.

        Returns
        -------
        list of rubicon.client.Artifact
            The artifacts previously logged to this client object.
        """
        project_name, experiment_id = self._get_parent_identifiers()

        self._artifacts = [
            client.Artifact(a, self)
            for a in await self.repository.get_artifacts_metadata(
                project_name, experiment_id=experiment_id
            )
        ]

        return self._artifacts

    async def delete_artifacts(self, ids):
        """Overrides `rubicon.client.ArtifcatMixin.delete_artifacts` to
        asynchronously delete the artifacts logged to with client
        objects with ids `ids`.

        Parameters
        ----------
        ids : list of str
            The ids of the artifacts to delete.
        """
        project_name, experiment_id = self._get_parent_identifiers()

        await asyncio.gather(
            *[
                self.repository.delete_artifact(
                    project_name, artifact_id, experiment_id=experiment_id
                )
                for artifact_id in ids
            ]
        )


class DataframeMixin(MultiParentMixin):
    """Adds dataframe support to an asynchronous client object."""

    async def log_dataframe(self, df, description=None, tags=[]):
        """Overrides `rubicon.client.DataframeMixin.log_dataframe` to
        asynchronously log a dataframe to this client object.

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
        await self.repository.create_dataframe(
            dataframe, df, project_name, experiment_id=experiment_id
        )

        return client.Dataframe(dataframe, self)

    async def _filter_dataframes(self, dataframes, tags, qtype):
        """Filters the provided dataframes by `tags` using
        query type `qtype`.
        """
        if len(tags) > 0:
            filtered_dataframes = []
            [
                filtered_dataframes.append(d)
                for d in dataframes
                if has_tag_requirements(await d.tags, tags, qtype)
            ]
            self._dataframes = filtered_dataframes
        else:
            self._dataframes = dataframes

    async def dataframes(self, tags=[], qtype="or"):
        """Overrides `rubicon.client.DataframeMixin.dataframes` to
        asynchronously get the dataframes logged to this client object.

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
            for d in await self.repository.get_dataframes_metadata(
                project_name, experiment_id=experiment_id
            )
        ]

        await self._filter_dataframes(dataframes, tags, qtype)

        return self._dataframes

    async def delete_dataframes(self, ids):
        """Overrides `rubicon.client.DataframeMixin.delete_dataframe`
        to asynchronously delete the dataframes with ids `ids` logged
        to this client object.

        Parameters
        ----------
        ids : list of str
            The ids of the dataframes to delete.
        """
        project_name, experiment_id = self._get_parent_identifiers()

        await asyncio.gather(
            *[
                self.repository.delete_dataframe(
                    project_name, dataframe_id, experiment_id=experiment_id
                )
                for dataframe_id in ids
            ]
        )


class TagMixin:
    """Adds tag support to an asynchronous client object."""

    async def add_tags(self, tags):
        """Overrides `rubicon.client.TagMixin.add_tags` to
        asynchronously add tags to this client object.

        Parameters
        ----------
        tags : list of str
            The tag values to add.
        """
        project_name, experiment_id, dataframe_id = self._get_taggable_identifiers()

        self._domain.add_tags(tags)
        await self.repository.add_tags(
            project_name, tags, experiment_id=experiment_id, dataframe_id=dataframe_id
        )

    async def remove_tags(self, tags):
        """Overrides `rubicon.client.TagMixin.remove_tags` to
        asynchronously remove tags from this client object.

        Parameters
        ----------
        tags : list of str
             The tag values to remove.
        """
        project_name, experiment_id, dataframe_id = self._get_taggable_identifiers()

        self._domain.remove_tags(tags)
        await self.repository.remove_tags(
            project_name, tags, experiment_id=experiment_id, dataframe_id=dataframe_id
        )

    @property
    async def tags(self):
        """Overrides `rubicon.client.TagMixin.tags` to
        asynchronously get this client object's tags.
        """
        project_name, experiment_id, dataframe_id = self._get_taggable_identifiers()
        tag_data = await self.repository.get_tags(
            project_name,
            experiment_id=experiment_id,
            dataframe_id=dataframe_id,
        )

        self._update_tags(tag_data)

        return self._domain.tags
