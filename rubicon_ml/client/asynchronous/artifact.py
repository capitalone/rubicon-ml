import fsspec

from rubicon_ml.client import Artifact as SyncArtifact


class Artifact(SyncArtifact):
    """An asynchronous client artifact.

    An `artifact` is a catch-all for any other type of
    data that can be logged to a file.

    For example, a snapshot of a trained model (.pkl) can
    be logged to the `experiment` created during its run.
    Or, a base model for the model in development can be
    logged to a `project` when leveraging transfer learning.

    An `artifact` is logged to a `project` or an `experiment`.

    Parameters
    ----------
    domain : rubicon.domain.Artifact
        The artifact domain model.
    parent : rubicon.client.Project or rubicon.client.Experiment
        The project or experiment that the artifact is
        logged to.
    """

    async def _get_data(self):
        """Overrides `rubicon.client.Artifact._get_data` to
        asynchronously load the data associated with this artifact.
        """
        project_name, experiment_id = self.parent._get_parent_identifiers()

        self._data = await self.repository.get_artifact_data(
            project_name, self.id, experiment_id=experiment_id
        )

    async def download(self, location=None, name=None):
        """Overrides `rubicon.client.Artifact.download` to
        asynchronously download this artifact's data.

        Parameters
        ----------
        location : str, optional
            The S3 bucket to download the artifact to. S3 buckets
            must be prepended with 's3://'.
        name : str, optional
            The name to give the downloaded artifact file.
            Defaults to the artifact's given name when logged.
        """
        if location is None:
            location = "."

        if name is None:
            name = self._domain.name

        with fsspec.open(f"{location.rstrip('/')}/{name}", "wb", auto_mkdir=False) as f:
            f.write(await self.data)

    @property
    async def data(self):
        """Overrides `rubicon.client.Artifact.data` to
        asynchronously get the artifact's raw data.
        """
        if self._data is None:
            await self._get_data()

        return self._data
