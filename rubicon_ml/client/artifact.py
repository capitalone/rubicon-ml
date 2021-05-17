import fsspec

from rubicon_ml.client import Base


class Artifact(Base):
    """A client artifact.

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

    def __init__(self, domain, parent):
        super().__init__(domain, parent._config)

        self._data = None
        self._parent = parent

    def _get_data(self):
        """Loads the data associated with this artifact."""
        project_name, experiment_id = self.parent._get_parent_identifiers()

        self._data = self.repository.get_artifact_data(
            project_name, self.id, experiment_id=experiment_id
        )

    def download(self, location=None, name=None):
        """Download this artifact's data.

        Parameters
        ----------
        location : str, optional
            The absolute or relative local directory or S3
            bucket to download the artifact to. S3 buckets
            must be prepended with 's3://'. Defaults to the
            current local working directory.
        name : str, optional
            The name to give the downloaded artifact file.
            Defaults to the artifact's given name when logged.
        """
        if location is None:
            location = "."

        if name is None:
            name = self._domain.name

        with fsspec.open(f"{location.rstrip('/')}/{name}", "wb", auto_mkdir=False) as f:
            f.write(self.data)

    @property
    def id(self):
        """Get the artifact's id."""
        return self._domain.id

    @property
    def name(self):
        """Get the artifact's name."""
        return self._domain.name

    @property
    def description(self):
        """Get the artifact's description."""
        return self._domain.description

    @property
    def created_at(self):
        """Get the time this dataframe was created."""
        return self._domain.created_at

    @property
    def data(self):
        """Get the artifact's raw data."""
        if self._data is None:
            self._get_data()

        return self._data

    @property
    def parent(self):
        """Get the dataframe's parent client object."""
        return self._parent
