from __future__ import annotations

import contextlib
import json
import os
import pickle
import tempfile
import warnings
import zipfile
from typing import TYPE_CHECKING, Literal, Optional

import fsspec

from rubicon_ml.client.base import Base
from rubicon_ml.client.mixin import CommentMixin, TagMixin
from rubicon_ml.client.utils.exception_handling import failsafe

if TYPE_CHECKING:
    from rubicon_ml.client import Project
    from rubicon_ml.domain import Artifact as ArtifactDomain


class Artifact(Base, TagMixin, CommentMixin):
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

    def __init__(self, domain: ArtifactDomain, parent: Project):
        super().__init__(domain, parent._config)

        self._data = None
        self._parent = parent

    @failsafe
    def get_data(
        self,
        deserialize: Optional[Literal["h2o", "pickle"]] = None,
        unpickle: bool = False,
    ):
        """Loads the data associated with this artifact and
        unpickles if needed.

        Parameters
        ----------
        deseralize : str, optional
            Method to use to deseralize this artifact's data.
            * None to disable deseralization and return the raw data.
            * "h2o" to use `h2o.load_model` to load the data.
            * "pickle" to use the builtin `pickle` library to load the data.
            Defaults to None.
        unpickle : bool, optional
            DEPRECATED. Set `deserialize` to 'unpickle' instead.
        """
        if unpickle:
            warnings.warn(
                "`unpickle` is deprecated, please use `deserialize='pickle'` instead",
                DeprecationWarning,
            )
            deserialize = "pickle"

        project_name, experiment_id = self.parent._get_identifiers()

        if deserialize == "h2o":
            import h2o

            self._data = h2o.load_model(
                self.repository._get_artifact_data_location(project_name, experiment_id, self.id)
            )
        else:
            self._data = self.repository.read_bytes(
                project_name,
                experiment_id,
                self.id,
            )

            if deserialize == "pickle":
                self._data = pickle.loads(self._data)

        return self._data

    @failsafe
    def get_json(self):
        return json.loads(self.get_data())

    @failsafe
    def download(
        self,
        location: Optional[str] = None,
        name: Optional[str] = None,
        unzip: bool = False,
    ):
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
        unzip : bool, optional
            True to unzip the artifact data. False otherwise.
            Defaults to False.
        """
        if location is None:
            location = os.getcwd()

        if name is None:
            name = self._domain.name

        if unzip:
            temp_file_context = tempfile.TemporaryDirectory
        else:
            temp_file_context = contextlib.nullcontext

        with temp_file_context() as temp_dir:
            if unzip:
                location_path = os.path.join(temp_dir, "temp_file.zip")
            else:
                location_path = os.path.join(location, name)

            with fsspec.open(location_path, "wb", auto_mkdir=False) as f:
                f.write(self.get_data())

            if unzip:
                with zipfile.ZipFile(location_path, "r") as zip_file:
                    zip_file.extractall(location)

    @contextlib.contextmanager
    @failsafe
    def temporary_download(self, unzip: bool = False):
        """Temporarily download this artifact's data within a context manager.

        Parameters
        ----------
        unzip : bool, optional
            True to unzip the artifact data. False otherwise.
            Defaults to False.

        Yields
        ------
        file
            An open file pointer into the directory the artifact data was
            temporarily downloaded into. If the artifact is a single file,
            its name is stored in the `artifact.name` attribute.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            self.download(location=temp_dir, unzip=unzip)

            yield temp_dir

    @property
    def id(self) -> str:
        """Get the artifact's id."""
        return self._domain.id

    @property
    def name(self) -> str:
        """Get the artifact's name."""
        return self._domain.name

    @property
    def description(self) -> str:
        """Get the artifact's description."""
        return self._domain.description

    @property
    def created_at(self):
        """Get the time this dataframe was created."""
        return self._domain.created_at

    @property
    def parent(self):
        """Get the artifact's parent client object."""
        return self._parent
