import os
import shutil
import tempfile
import warnings
from datetime import datetime
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Type, Union
from zipfile import ZipFile

import fsspec
import pandas as pd

from rubicon_ml import domain
from rubicon_ml.domain.utils.uuid import uuid4
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.utils import json, slugify

if TYPE_CHECKING:
    import dask.dataframe as dd
    import polars as pl


class BaseRepository:
    """The base repository defines all the shared interactions
    between the various Rubicon persistence options.

    `BaseRepositoy` itself should never be used directly. Use
    one of the repositories that extends this class to persist
    Rubicon data:

        * `rubicon.repository.MemoryRepository`
        * `rubicon.repository.LocalRepository`
        * `rubicon.repository.S3Repository`

    Parameters
    ----------
    root_dir : str
        Absolute path to the root directory to persist Rubicon
        data to.
    storage_options : dict, optional
        Additional keyword arguments that are passed directly to
        the underlying filesystem class.
    """

    def __init__(self, root_dir: str, **storage_options):
        self._df_storage_options = {}  # should only be non-empty for S3 logging

        self.filesystem = fsspec.filesystem(self.PROTOCOL, **storage_options)
        self.root_dir = root_dir.rstrip("/")

    # --- Filesystem Helpers ---

    def _cat(self, path: str):
        """Returns the contents of the file at `path`."""
        return self.filesystem.cat(path)

    def _cat_paths(self, metadata_paths: List[str]) -> Dict[str, Any]:
        """Cat `metadata_paths` to get the list of files to include.
        Ignore FileNotFoundErrors to avoid misc file errors, like hidden
        dotfiles.
        """
        if not isinstance(metadata_paths, list):
            metadata_paths = [metadata_paths]
        if not metadata_paths:
            return {}

        files = {}

        for path, metadata in self.filesystem.cat(metadata_paths, on_error="return").items():
            if isinstance(metadata, FileNotFoundError):
                warning = f"{path} not found. Was this file unintentionally created?"
                warnings.warn(warning)
            else:
                files[path] = metadata

        return files

    def _exists(self, path: str) -> bool:
        """Returns True if a file exists at `path`, False otherwise."""
        return self.filesystem.exists(path)

    def _glob(self, globstring: str):
        """Returns the names of the files matching `globstring`."""
        return self.filesystem.glob(globstring, detail=True)

    def _ls_directories_only(self, path: str) -> List[str]:
        """Returns the names of all the directories at path `path`."""
        directories = [
            os.path.join(p.get("name"), "metadata.json")
            for p in self.filesystem.ls(path, detail=True)
            if p.get("type", p.get("StorageClass")).lower() == "directory"
        ]

        return directories

    def _ls(self, path: str):
        return self.filesystem.ls(path)

    def _mkdir(self, dirpath: str):
        """Creates a directory `dirpath` with parents."""
        return self.filesystem.mkdirs(dirpath, exist_ok=True)

    def _modified(self, path: str):
        return self.filesystem.modified(path)

    def _persist_bytes(self, bytes_data, path):
        """Write bytes to the filesystem.

        To be implemented by extensions of the base filesystem.
        """
        raise NotImplementedError()

    def _persist_domain(self, domain, path):
        """Write a domain object to the filesystem.

        To be implemented by extensions of the base filesystem.
        """
        raise NotImplementedError()

    def _read_bytes(self, path, err_msg=None):
        """Read bytes from the file at `path`."""
        try:
            open_file = self.filesystem.open(path, "rb")
        except FileNotFoundError:
            raise RubiconException(err_msg)

        return open_file.read()

    def _read_domain(self, path, err_msg=None):
        """Read a domain object from the file at `path`."""
        try:
            open_file = self.filesystem.open(path)
        except FileNotFoundError:
            raise RubiconException(err_msg)

        return json.load(open_file)

    def _rm(self, path):
        """Recursively remove all files at `path`."""
        return self.filesystem.rm(path, recursive=True)

    def _load_metadata_files(
        self, metadata_root: str, domain_type: Type[domain.DomainsVar]
    ) -> List[domain.DomainsVar]:
        """Load metadata files from the given root directory and return a list of domain objects."""
        # find all directories, prepare a list of those plus `metadata.yaml`
        try:
            metadata_paths = self._ls_directories_only(metadata_root)
        except FileNotFoundError:
            return []

        loaded_domains = []
        # cat_paths will check for FileNotFoundErrors and skip any missing files
        # it loads the contents of the found files
        for path, metadata in self._cat_paths(metadata_paths).items():
            try:
                metadata_contents = json.loads(metadata)
            except JSONDecodeError:
                warnings.warn(f"Failed to load metadata for {domain_type.__name__} at {path}")
                continue

            try:
                loaded_domain = domain_type(**metadata_contents)
            except TypeError:
                warnings.warn(f"Failed to load {domain_type.__name__} from metadata at {path}")
                continue

            loaded_domains.append(loaded_domain)

        if loaded_domains:
            loaded_domains.sort(key=lambda d: d.created_at)

        return loaded_domains

    # -------- Projects --------

    def _get_project_metadata_path(self, project_name: str):
        """Returns the path of the project with name `project_name`'s
        metadata.
        """
        return f"{self.root_dir}/{slugify(project_name)}/metadata.json"

    def create_project(self, project: domain.Project):
        """Persist a project to the configured filesystem.

        Parameters
        ----------
        project : rubicon.domain.Project
            The project to persist.
        """
        project_metadata_path = self._get_project_metadata_path(project.name)

        if self._exists(project_metadata_path):
            raise RubiconException(f"A project with name '{project.name}' already exists.")

        self._persist_domain(project, project_metadata_path)

    def get_project(self, project_name: str) -> domain.Project:
        """Retrieve a project from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project to retrieve.

        Returns
        -------
        rubicon.domain.Project
            The project with name `project_name`.
        """
        project_metadata_path = self._get_project_metadata_path(project_name)
        project = self._read_domain(
            project_metadata_path,
            f"No project with name '{project_name}' found.",
        )

        return domain.Project(**project)

    def get_projects(self) -> List[domain.Project]:
        """Get the list of projects from the filesystem.

        Returns
        -------
        list of rubicon.domain.Project
            The list of projects from the filesystem.
        """
        return self._load_metadata_files(self.root_dir, domain.Project)

    # ------ Experiments -------

    def _get_experiment_metadata_root(self, project_name: str):
        """Returns the experiments directory of the project with
        name `project_name`.
        """
        return f"{self.root_dir}/{slugify(project_name)}/experiments"

    def _get_experiment_metadata_path(self, project_name, experiment_id):
        """Returns the path of the experiment with ID
        `experiment_id`'s metadata.
        """
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/metadata.json"

    def create_experiment(self, experiment: domain.Experiment):
        """Persist an experiment to the configured filesystem.

        Parameters
        ----------
        experiment : rubicon.domain.Experiment
            The experiment to persist.
        """
        experiment_metadata_path = self._get_experiment_metadata_path(
            experiment.project_name, experiment.id
        )

        self._persist_domain(experiment, experiment_metadata_path)

    def get_experiment(self, project_name: str, experiment_id: str) -> domain.Experiment:
        """Retrieve an experiment from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment to retrieve.

        Returns
        -------
        rubicon.domain.Experiment
            The experiment with ID `experiment_id`.
        """
        experiment_metadata_path = self._get_experiment_metadata_path(project_name, experiment_id)
        experiment = self._read_domain(
            experiment_metadata_path,
            f"No experiment with id `{experiment_id}` found.",
        )

        return domain.Experiment(**experiment)

    def get_experiments(self, project_name: str) -> List[domain.Experiment]:
        """Retrieve all experiments from the configured filesystem
        that belong to the project with name `project_name`.

        Parameters
        ----------
        project_name : str
            The name of the project to retrieve all experiments
            from.

        Returns
        -------
        list of rubicon.domain.Experiment
            The experiments logged to the project with name
            `project_name`.
        """
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return self._load_metadata_files(experiment_metadata_root, domain.Experiment)

    # ------- Archiving --------

    def _archive(
        self,
        project_name,
        experiments: Optional[List] = None,
        remote_rubicon_root: Optional[str] = None,
    ):
        """Archive the experiments logged to this project.

        Parameters
        ----------
        project_name : str
            Name of the calling project (project to create archive for)
        experiments : list of Experiments, optional
            The rubicon.client.Experiment objects to archive. If None all logged experiments are archived.
        remote_rubicon_root : str or pathlike object, optional
            The remote root of the repository to archive to

        Returns
        -------
        filepath of newly created archive
        """
        remote_s3 = True if remote_rubicon_root and remote_rubicon_root.startswith("s3") else False
        root_dir = remote_rubicon_root if remote_rubicon_root is not None else self.root_dir
        archive_dir = os.path.join(root_dir, slugify(project_name), "archives")
        ts = datetime.timestamp(datetime.now())
        archive_path = os.path.join(archive_dir, "archive-" + str(ts))
        zip_archive_filename = str(archive_path + ".zip")
        experiments_path = self._get_experiment_metadata_root(project_name)

        if not remote_s3:
            if not self._exists(archive_dir):
                self._mkdir(archive_dir)

        file_name = None
        with tempfile.NamedTemporaryFile() as tf:
            if experiments is not None:
                with ZipFile(tf, "x") as archive:
                    experiment_paths = []
                    for experiment in experiments:
                        experiment_paths.append(os.path.join(experiments_path, experiment.id))
                    for file_path in experiment_paths:
                        archive.write(file_path, os.path.basename(file_path))
                file_name = archive.filename

            else:
                file_name = shutil.make_archive(tf.name, "zip", experiments_path)

            with fsspec.open(zip_archive_filename, "wb") as fp:
                with open(file_name, "rb") as tf:
                    fp.write(tf.read())

        return zip_archive_filename

    def _experiments_from_archive(
        self,
        project_name,
        remote_rubicon_root: str,
        latest_only: Optional[bool] = False,
    ):
        """Retrieve archived experiments into this project's experiments folder.

        Parameters
        ----------
        project_name : str
            Name of the calling project (project to read experiments into)
        remote_rubicon_root : str or pathlike object
            The remote Rubicon object with the repository containing archived experiments to read in
        latest_only : bool, optional
            Indicates whether or not experiments should only be read from the latest archive
        """
        root_dir = self.root_dir
        shutil.copy(
            os.path.join(remote_rubicon_root, slugify(project_name), "metadata.json"),
            os.path.join(root_dir, slugify(project_name)),
        )
        archive_dir = os.path.join(remote_rubicon_root, slugify(project_name), "archives")
        if not self._exists(archive_dir):
            raise ValueError("`remote_rubicon_root` has no archives")

        dest_experiments_dir = self._get_experiment_metadata_root(project_name)
        if not self._exists(dest_experiments_dir):
            self._mkdir(dest_experiments_dir)

        og_num_experiments = len(self._ls(dest_experiments_dir))

        if not latest_only:
            for zip_archive_name in self._ls(archive_dir):
                zip_archive_filepath = os.path.join(archive_dir, zip_archive_name)
                with ZipFile(zip_archive_filepath, "r") as curr_archive:
                    curr_archive.extractall(dest_experiments_dir)
        else:
            latest_zip_archive_filepath = None
            latest_time = None
            for zip_archive in self._ls(archive_dir):
                zip_archive_filepath = os.path.join(archive_dir, zip_archive)
                mod_time = self._modified(zip_archive_filepath)
                if latest_time is None:
                    latest_time = mod_time
                    latest_zip_archive_filepath = zip_archive_filepath
                elif mod_time > latest_time:
                    latest_zip_archive_filepath = zip_archive_filepath
                    latest_time = mod_time
            with ZipFile(latest_zip_archive_filepath, "r") as zip_archive:
                zip_archive.extractall(dest_experiments_dir)

        if len(self._ls(dest_experiments_dir)) > og_num_experiments:
            print("experiments read from archive")
        else:
            print("experiments not read from archive")

    # ------- Artifacts --------

    def _get_artifact_metadata_root(self, project_name, experiment_id=None):
        """Returns the artifacts directory of the project with name
        `project_name` or experiment with ID `experiment_id`.
        """
        if experiment_id is not None:
            experiment_metadata_root = self._get_experiment_metadata_root(project_name)

            return f"{experiment_metadata_root}/{experiment_id}/artifacts"
        else:
            return f"{self.root_dir}/{slugify(project_name)}/artifacts"

    def _get_artifact_metadata_path(self, project_name, experiment_id, artifact_id):
        """Returns the path of the artifact with ID `artifact_id`'s
        metadata.
        """
        artifact_metadata_root = self._get_artifact_metadata_root(project_name, experiment_id)

        return f"{artifact_metadata_root}/{artifact_id}/metadata.json"

    def _get_artifact_data_path(self, project_name, experiment_id, artifact_id):
        """Returns the path of the artifact with ID `artifact_id`'s
        raw data.
        """
        artifact_metadata_root = self._get_artifact_metadata_root(project_name, experiment_id)

        return f"{artifact_metadata_root}/{artifact_id}/data"

    def create_artifact(self, artifact, data, project_name, experiment_id=None):
        """Persist an artifact to the configured filesystem.

        Parameters
        ----------
        artifact : rubicon.domain.Artifact
            The artifact to persist.
        data : bytes
            The raw data to persist as an artifact.
        project_name : str
            The name of the project this artifact belongs to.
        experiment_id : str, optional
            The ID of the experiment this artifact belongs to.
            Artifacts do not need to belong to an experiment.
        """
        artifact_metadata_path = self._get_artifact_metadata_path(
            project_name, experiment_id, artifact.id
        )
        artifact_data_path = self._get_artifact_data_path(project_name, experiment_id, artifact.id)

        self._persist_bytes(data, artifact_data_path)
        self._persist_domain(artifact, artifact_metadata_path)

    def get_artifact_metadata(self, project_name, artifact_id, experiment_id=None):
        """Retrieve an artifact's metadata from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the artifact with ID
            `artifact_id` is logged to.
        artifact_id : str
            The ID of the artifact to retrieve.
        experiment_id : str, optional
            The ID of the experiment the artifact with ID
            `artifact_id` is logged to. Artifacts do not
            need to belong to an experiment.

        Returns
        -------
        rubicon.domain.Artifact
            The artifact with ID `artifact_id`.
        """
        artifact_metadata_path = self._get_artifact_metadata_path(
            project_name, experiment_id, artifact_id
        )
        artifact = self._read_domain(
            artifact_metadata_path,
            f"No artifact with id `{artifact_id}` found.",
        )

        return domain.Artifact(**artifact)

    def get_artifacts_metadata(self, project_name, experiment_id=None):
        """Retrieve all artifacts' metadata from the configured
        filesystem that belong to the specified object.

        Parameters
        ----------
        project_name : str
            The name of the project to retrieve all artifacts
            from.
        experiment_id : str, optional
            The ID of the experiment to retrieve all artifacts
            from. Artifacts do not need to belong to an
            experiment.

        Returns
        -------
        list of rubicon.domain.Artifact
            The artifacts logged to the specified object.
        """
        artifact_metadata_root = self._get_artifact_metadata_root(project_name, experiment_id)

        return self._load_metadata_files(artifact_metadata_root, domain.Artifact)

    def get_artifact_data(self, project_name, artifact_id, experiment_id=None):
        """Retrieve an artifact's raw data.

        Parameters
        ----------
        project_name : str
            The name of the project the artifact with ID
            `artifact_id` is logged to.
        artifact_id : str
            The ID of the artifact to retrieve data from.
        experiment_id : str, optional
            The ID of the experiment the artifact with ID
            `artifact_id` is logged to. Artifacts do not
            need to belong to an experiment.

        Returns
        -------
        bytes
            The artifact with ID `artifact_id`'s raw data.
        """
        artifact_data_path = self._get_artifact_data_path(project_name, experiment_id, artifact_id)
        artifact_data = self._read_bytes(
            artifact_data_path,
            f"No data for artifact with id `{artifact_id}` found.",
        )

        return artifact_data

    def delete_artifact(self, project_name, artifact_id, experiment_id=None):
        """Delete an artifact from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the artifact with ID
            `artifact_id` is logged to.
        artifact_id : str
            The ID of the artifact to delete.
        experiment_id : str, optional
            The ID of the experiment the artifact with ID
            `artifact_id` is logged to. Artifacts do not
            need to belong to an experiment.
        """
        artifact_metadata_root = self._get_artifact_metadata_root(project_name, experiment_id)

        try:
            self._rm(f"{artifact_metadata_root}/{artifact_id}")
        except FileNotFoundError:
            raise RubiconException(f"No artifact with id `{artifact_id}` found.")

    # ------- Dataframes -------

    def _get_dataframe_metadata_root(self, project_name, experiment_id=None):
        """Returns the dataframes directory of the project with name
        `project_name` or experiment with ID `experiment_id`.
        """
        if experiment_id is not None:
            experiment_metadata_root = self._get_experiment_metadata_root(project_name)

            return f"{experiment_metadata_root}/{experiment_id}/dataframes"
        else:
            return f"{self.root_dir}/{slugify(project_name)}/dataframes"

    def _get_dataframe_metadata_path(self, project_name, experiment_id, dataframe_id):
        """Returns the path of the dataframe with ID `dataframe_id`'s
        metadata.
        """
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        return f"{dataframe_metadata_root}/{dataframe_id}/metadata.json"

    def _get_dataframe_data_path(self, project_name, experiment_id, dataframe_id):
        """Returns the path of the dataframe with ID `dataframe_id`'s
        raw data.
        """
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        return f"{dataframe_metadata_root}/{dataframe_id}/data"

    def _persist_dataframe(
        self, df: Union[pd.DataFrame, "dd.DataFrame", "pl.DataFrame"], path: str
    ):
        """Persists the dataframe `df` to the configured filesystem.

        Note
        ----
        `dask` dataframes will automatically be split into chunks by `dask.dataframe.to_parquet`.
        `pandas` dataframes, however, will be saved as a single file with the hope that users
        would leverage dask for large dataframes.
        """
        if isinstance(df, pd.DataFrame):
            self._mkdir(path)
            path = f"{path}/data.parquet"

        if hasattr(df, "write_parquet"):
            # handle Polars
            df.write_parquet(path)
        else:
            # Dask or pandas
            df.to_parquet(path, engine="pyarrow", storage_options=self._df_storage_options)

    def _read_dataframe(self, path, df_type: Literal["pandas", "dask", "polars"] = "pandas"):
        """Reads the dataframe `df` from the configured filesystem."""
        df = None
        acceptable_types = ["pandas", "dask", "polars"]

        if df_type == "pandas":
            path = f"{path}/data.parquet"
            df = pd.read_parquet(path, engine="pyarrow", storage_options=self._df_storage_options)
        elif df_type == "polars":
            try:
                from polars import read_parquet
            except ImportError:
                raise RubiconException(
                    "`rubicon_ml` requires `polars` to be installed in the current environment "
                    "to read dataframes with `df_type`='polars'. `pip install polars` "
                    "or `conda install polars` to continue."
                )
            df = read_parquet(path, storage_options=self._df_storage_options)

        elif df_type == "dask":
            try:
                from dask import dataframe as dd
            except ImportError:
                raise RubiconException(
                    "`rubicon_ml` requires `dask` to be installed in the current environment "
                    "to read dataframes with `df_type`='dask'. `pip install dask[dataframe]` "
                    "or `conda install dask` to continue."
                )

            df = dd.read_parquet(path, engine="pyarrow", storage_options=self._df_storage_options)
        else:
            raise ValueError(f"`df_type` must be one of {acceptable_types}")

        return df

    def create_dataframe(
        self,
        dataframe: domain.Dataframe,
        data: Union[pd.DataFrame, "dd.DataFrame", "pl.DataFrame"],
        project_name: str,
        experiment_id: Optional[str] = None,
    ):
        """Persist a dataframe to the configured filesystem.

        Parameters
        ----------
        dataframe : rubicon.domain.Dataframe
            The dataframe to persist.
        data : dask.dataframe.DataFrame or pandas.DataFrame
            The raw data to persist as a dataframe. All
            dataframes are persisted as `dask` dataframes.
        project_name : str
            The name of the project this dataframe belongs to.
        experiment_id : str, optional
            The ID of the experiment this dataframe belongs to.
            Dataframes do not need to belong to an experiment.
        """
        dataframe_metadata_path = self._get_dataframe_metadata_path(
            project_name, experiment_id, dataframe.id
        )
        dataframe_data_path = self._get_dataframe_data_path(
            project_name, experiment_id, dataframe.id
        )

        self._persist_dataframe(data, dataframe_data_path)
        self._persist_domain(dataframe, dataframe_metadata_path)

    def get_dataframe_metadata(self, project_name, dataframe_id, experiment_id=None):
        """Retrieve a dataframes's metadata from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the dataframe with ID
            `dataframe_id` is logged to.
        dataframe_id : str
            The ID of the dataframe to retrieve.
        experiment_id : str, optional
            The ID of the experiment the dataframe with ID
            `dataframe_id` is logged to. Dataframes do not
            need to belong to an experiment.

        Returns
        -------
        rubicon.domain.Dataframe
            The dataframe with ID `dataframe_id`.
        """
        dataframe_metadata_path = self._get_dataframe_metadata_path(
            project_name, experiment_id, dataframe_id
        )
        dataframe = self._read_domain(
            dataframe_metadata_path,
            f"No dataframe with id `{dataframe_id}` found.",
        )

        return domain.Dataframe(**dataframe)

    def get_dataframes_metadata(
        self, project_name: str, experiment_id: Optional[str] = None
    ) -> List[domain.Dataframe]:
        """Retrieve all dataframes' metadata from the configured
        filesystem that belong to the specified object.

        Parameters
        ----------
        project_name : str
            The name of the project to retrieve all dataframes
            from.
        experiment_id : str, optional
            The ID of the experiment to retrieve all dataframes
            from. Dataframes do not need to belong to an
            experiment.

        Returns
        -------
        list of rubicon.domain.Dataframe
            The dataframes logged to the specified object.
        """
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        return self._load_metadata_files(dataframe_metadata_root, domain.Dataframe)

    def get_dataframe_data(
        self,
        project_name: str,
        dataframe_id: str,
        experiment_id: Optional[str] = None,
        df_type: Literal["pandas", "dask", "polars"] = "pandas",
    ):
        """Retrieve a dataframe's raw data.

        Parameters
        ----------
        project_name : str
            The name of the project the dataframe with ID
            `dataframe_id` is logged to.
        dataframe_id : str
            The ID of the dataframe to retrieve data from.
        experiment_id : str, optional
            The ID of the experiment the dataframe with ID
            `artifact_id` is logged to. Dataframes do not
            need to belong to an experiment.
        df_type : str, optional
            The type of dataframe. Can be `pandas`, `dask`, or `polars`.

        Returns
        -------
        dask.dataframe.DataFrame
            The dataframe with ID `dataframe_id`'s raw data.
        """
        dataframe_data_path = self._get_dataframe_data_path(
            project_name, experiment_id, dataframe_id
        )

        try:
            df = self._read_dataframe(dataframe_data_path, df_type)
        except FileNotFoundError:
            raise RubiconException(
                f"No data for dataframe with id `{dataframe_id}` found. This might have "
                "happened if you forgot to set `df_type='dask'` when trying to read a `dask` dataframe."
            )

        return df

    def delete_dataframe(self, project_name, dataframe_id, experiment_id=None):
        """Delete a dataframe from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the dataframe with ID
            `dataframe_id` is logged to.
        dataframe_id : str
            The ID of the dataframe to delete.
        experiment_id : str, optional
            The ID of the experiment the dataframe with ID
            `artifact_id` is logged to. Dataframes do not
            need to belong to an experiment.
        """
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        try:
            self._rm(f"{dataframe_metadata_root}/{dataframe_id}")
        except FileNotFoundError:
            raise RubiconException(f"No dataframe with id `{dataframe_id}` found.")

    # -------- Features --------

    def _get_feature_metadata_root(self, project_name, experiment_id):
        """Returns the features directory of the experiment with
        ID `experiment_id`.
        """
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/features"

    def _get_feature_metadata_path(self, project_name, experiment_id, feature_name):
        """Returns the path of the feature with name `feature_name`'s
        metadata.
        """
        feature_metadata_root = self._get_feature_metadata_root(project_name, experiment_id)

        return f"{feature_metadata_root}/{slugify(feature_name)}/metadata.json"

    def create_feature(self, feature, project_name, experiment_id):
        """Persist a feature to the configured filesystem.

        Parameters
        ----------
        feature : rubicon.domain.Feature
            The feature to persist.
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment this feature belongs to.
        """
        feature_metadata_path = self._get_feature_metadata_path(
            project_name, experiment_id, feature.name
        )

        if self._exists(feature_metadata_path):
            raise RubiconException(f"A feature with name '{feature.name}' already exists.")

        self._persist_domain(feature, feature_metadata_path)

    def get_feature(
        self, project_name: str, experiment_id: str, feature_name: str
    ) -> domain.Feature:
        """Retrieve a feature from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment the feature with name
            `feature_name` is logged to.
        feature_name : str
            The name of the feature to retrieve.

        Returns
        -------
        rubicon.domain.Feature
            The feature with name `feature_name`.
        """
        feature_metadata_path = self._get_feature_metadata_path(
            project_name, experiment_id, feature_name
        )
        feature = self._read_domain(
            feature_metadata_path,
            f"No feature with name '{feature_name}' found.",
        )

        return domain.Feature(**feature)

    def get_features(self, project_name: str, experiment_id: str) -> List[domain.Feature]:
        """Retrieve all features from the configured filesystem
        that belong to the experiment with ID `experiment_id`.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment to retrieve all features
            from.

        Returns
        -------
        list of rubicon.domain.Feature
            The features logged to the experiment with ID
            `experiment_id`.
        """
        feature_metadata_root = self._get_feature_metadata_root(project_name, experiment_id)

        return self._load_metadata_files(feature_metadata_root, domain.Feature)

    # -------- Metrics ---------

    def _get_metric_metadata_root(self, project_name, experiment_id):
        """Returns the metrics directory of the experiment with
        ID `experiment_id`.
        """
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/metrics"

    def _get_metric_metadata_path(self, project_name, experiment_id, metric_name):
        """Returns the path of the metric with name `metric_name`'s
        metadata.
        """
        metric_metadata_root = self._get_metric_metadata_root(project_name, experiment_id)

        return f"{metric_metadata_root}/{slugify(metric_name)}/metadata.json"

    def create_metric(self, metric, project_name, experiment_id):
        """Persist a metric to the configured filesystem.

        Parameters
        ----------
        metric : rubicon.domain.Metric
            The metric to persist.
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment this metric belongs to.
        """
        metric_metadata_path = self._get_metric_metadata_path(
            project_name, experiment_id, metric.name
        )

        if self._exists(metric_metadata_path):
            raise RubiconException(f"A metric with name '{metric.name}' already exists.")

        self._persist_domain(metric, metric_metadata_path)

    def get_metric(self, project_name: str, experiment_id: str, metric_name: str) -> domain.Metric:
        """Retrieve a metric from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment the metric with name
            `metric_name` is logged to.
        metric_name : str
            The name of the metric to retrieve.

        Returns
        -------
        rubicon.domain.Metric
            The metric with name `metric_name`.
        """
        metric_metadata_path = self._get_metric_metadata_path(
            project_name, experiment_id, metric_name
        )
        metric = self._read_domain(
            metric_metadata_path,
            f"No metric with name '{metric_name}' found.",
        )

        return domain.Metric(**metric)

    def get_metrics(self, project_name: str, experiment_id: str) -> List[domain.Metric]:
        """Retrieve all metrics from the configured filesystem
        that belong to the experiment with ID `experiment_id`.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment to retrieve all metrics
            from.

        Returns
        -------
        list of rubicon.domain.Metric
            The metrics logged to the experiment with ID
            `experiment_id`.
        """
        metric_metadata_root = self._get_metric_metadata_root(project_name, experiment_id)

        return self._load_metadata_files(metric_metadata_root, domain.Metric)

    # ------- Parameters -------

    def _get_parameter_metadata_root(self, project_name, experiment_id):
        """Returns the parameters directory of the experiment with
        ID `experiment_id`.
        """
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/parameters"

    def _get_parameter_metadata_path(self, project_name, experiment_id, parameter_name):
        """Returns the path of the parameter with name `parameter_name`'s
        metadata.
        """
        parameter_metadata_root = self._get_parameter_metadata_root(project_name, experiment_id)

        return f"{parameter_metadata_root}/{slugify(parameter_name)}/metadata.json"

    def create_parameter(self, parameter, project_name, experiment_id):
        """Persist a parameter to the configured filesystem.

        Parameters
        ----------
        parameter : rubicon.domain.Parameter
            The parameter to persist.
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment this parameter belongs to.
        """
        parameter_metadata_path = self._get_parameter_metadata_path(
            project_name, experiment_id, parameter.name
        )

        if self._exists(parameter_metadata_path):
            raise RubiconException(f"A parameter with name '{parameter.name}' already exists.")

        self._persist_domain(parameter, parameter_metadata_path)

    def get_parameter(self, project_name, experiment_id, parameter_name):
        """Retrieve a parameter from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project this parameter belongs to.
        experiment_id : str
            The ID of the experiment the parameter with name
            `parameter_name` is logged to.
        parameter_name : str
            The name of the parameter to retrieve.

        Returns
        -------
        rubicon.domain.Parameter
            The parameter with name `parameter_name`.
        """
        parameter_metadata_path = self._get_parameter_metadata_path(
            project_name, experiment_id, parameter_name
        )
        parameter = self._read_domain(
            parameter_metadata_path,
            f"No parameter with name '{parameter_name}' found.",
        )

        return domain.Parameter(**parameter)

    def get_parameters(self, project_name: str, experiment_id: str) -> List[domain.Parameter]:
        """Retrieve all parameters from the configured filesystem
        that belong to the experiment with ID `experiment_id`.

        Parameters
        ----------
        project_name : str
            The name of the project the experiment with ID
            `experiment_id` is logged to.
        experiment_id : str
            The ID of the experiment to retrieve all parameters
            from.

        Returns
        -------
        list of rubicon.domain.Parameter
            The parameters logged to the experiment with ID
            `experiment_id`.
        """
        parameter_metadata_root = self._get_parameter_metadata_root(project_name, experiment_id)

        return self._load_metadata_files(parameter_metadata_root, domain.Parameter)

    # ---------- Tags ----------

    def _get_tag_metadata_root(
        self, project_name, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Returns the directory to write tags to."""
        get_metadata_root_lookup = {
            "Artifact": self._get_artifact_metadata_root,
            "Dataframe": self._get_dataframe_metadata_root,
            "Experiment": self._get_experiment_metadata_root,
            "Metric": self._get_metric_metadata_root,
            "Feature": self._get_feature_metadata_root,
            "Parameter": self._get_parameter_metadata_root,
        }

        try:
            get_metadata_root = get_metadata_root_lookup[entity_type]
        except KeyError:
            raise ValueError("`experiment_id` and `entity_identifier` can not both be `None`.")

        if entity_type == "Experiment":
            experiment_metadata_root = get_metadata_root(project_name)

            return f"{experiment_metadata_root}/{experiment_id}"
        else:
            entity_metadata_root = get_metadata_root(project_name, experiment_id)

            # We want to slugify the names of Metrics, Features, and Parameters- not Artifacts, Dataframes, or Experiments
            if entity_type in ["Metric", "Feature", "Parameter"]:
                entity_identifier = slugify(entity_identifier)
            return f"{entity_metadata_root}/{entity_identifier}"

    def add_tags(
        self,
        project_name,
        tags,
        experiment_id=None,
        entity_identifier=None,
        entity_type=None,
    ):
        """Persist tags to the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the object to tag
            belongs to.
        tags : list of str
            The tag values to persist.
        experiment_id : str, optional
            The ID of the experiment to apply the tags
            `tags` to.
        entity_identifier : str, optional
            The ID or name of the entity to apply the tags
            `tags` to.
        entity_type : str, optional
            The name of the entity's type as returned by
            `entity_cls.__class__.__name__`.
        """
        tag_metadata_root = self._get_tag_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )
        tag_metadata_path = f"{tag_metadata_root}/tags_{uuid4()}.json"

        self._persist_domain({"added_tags": tags}, tag_metadata_path)

    def remove_tags(
        self,
        project_name,
        tags,
        experiment_id=None,
        entity_identifier=None,
        entity_type=None,
    ):
        """Delete tags from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the object to delete
            tags from belongs to.
        tags : list of str
            The tag values to delete.
        experiment_id : str, optional
            The ID of the experiment to delete the tags
            `tags` from.
        entity_identifier : str, optional
            The ID or name of the entity to apply the tags
            `tags` to.
        entity_type : str, optional
            The name of the entity's type as returned by
            `entity_cls.__class__.__name__`.
        """
        tag_metadata_root = self._get_tag_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )
        tag_metadata_path = f"{tag_metadata_root}/tags_{uuid4()}.json"

        self._persist_domain({"removed_tags": tags}, tag_metadata_path)

    def _sort_tag_paths(self, tag_paths):
        """Sorts the paths in `tags_paths` by when they were
        created.
        """
        if isinstance(tag_paths, dict):
            tag_paths = tag_paths.values()

        tag_paths_with_timestamps = [
            (t.get("created", t.get("LastModified")), t.get("name")) for t in tag_paths
        ]
        tag_paths_with_timestamps.sort()

        return tag_paths_with_timestamps

    def get_tags(self, project_name, experiment_id=None, entity_identifier=None, entity_type=None):
        """Retrieve tags from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the object to retrieve
            tags from belongs to.
        experiment_id : str, optional
            The ID of the experiment to retrieve tags from.
        entity_identifier : str, optional
            The ID or name of the entity to apply the tags
            `tags` to.
        entity_type : str, optional
            The name of the entity's type as returned by
            `entity_cls.__class__.__name__`.

        Returns
        -------
        list of dict
            A list of dictionaries with one key each,
            `added_tags` or `removed_tags`, where the
            value is a list of tag names that have been
            added to or removed from the specified object.
        """
        tag_metadata_root = self._get_tag_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )
        tag_metadata_glob = f"{tag_metadata_root}/tags_*.json"

        tag_paths = self._glob(tag_metadata_glob)
        if len(tag_paths) == 0:
            return []

        sorted_tag_paths = self._sort_tag_paths(tag_paths)

        tag_data = self._cat([p for _, p in sorted_tag_paths])
        sorted_tag_data = [json.loads(tag_data[p]) for _, p in sorted_tag_paths]

        return sorted_tag_data

    # ---------- Comments ----------

    def _get_comment_metadata_root(
        self, project_name, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Returns the directory to write comments to."""
        # comments and tags are currently written to the same root with a different filename
        return self._get_tag_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )

    def add_comments(
        self,
        project_name,
        comments,
        experiment_id=None,
        entity_identifier=None,
        entity_type=None,
    ):
        """Persist comments to the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the object to comment
            belongs to.
        comments : list of str
            The comment values to persist.
        experiment_id : str, optional
            The ID of the experiment to apply the comments
            `comments` to.
        entity_identifier : str, optional
            The ID or name of the entity to apply the comments
            `comments` to.
        entity_type : str, optional
            The name of the entity's type as returned by
            `entity_cls.__class__.__name__`.
        """
        comment_metadata_root = self._get_comment_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )
        comment_metadata_path = f"{comment_metadata_root}/comments_{uuid4()}.json"

        self._persist_domain({"added_comments": comments}, comment_metadata_path)

    def remove_comments(
        self,
        project_name,
        comments,
        experiment_id=None,
        entity_identifier=None,
        entity_type=None,
    ):
        """Delete comments from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the object to delete
            comments from belongs to.
        comments : list of str
            The comment values to delete.
        experiment_id : str, optional
            The ID of the experiment to delete the comments
            `comments` from.
        entity_identifier : str, optional
            The ID or name of the entity to apply the comments
            `comments` to.
        entity_type : str, optional
            The name of the entity's type as returned by
            `entity_cls.__class__.__name__`.
        """
        comment_metadata_root = self._get_comment_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )
        comment_metadata_path = f"{comment_metadata_root}/comments_{uuid4()}.json"

        self._persist_domain({"removed_comments": comments}, comment_metadata_path)

    def _sort_comment_paths(self, comment_paths):
        """Sorts the paths in `comment_paths` by when they were
        created.
        """
        return self._sort_tag_paths(comment_paths)

    def get_comments(
        self, project_name, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Retrieve comments from the configured filesystem.

        Parameters
        ----------
        project_name : str
            The name of the project the object to retrieve
            comments from belongs to.
        experiment_id : str, optional
            The ID of the experiment to retrieve comments from.
        entity_identifier : str, optional
            The ID or name of the entity to apply the comments
            `comments` to.
        entity_type : str, optional
            The name of the entity's type as returned by
            `entity_cls.__class__.__name__`.

        Returns
        -------
        dict
            A dictionary, `added_comments` where the
            value is a list of comment names that have
            been added to the specified object.
        """
        comment_metadata_root = self._get_comment_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )
        comment_metadata_glob = f"{comment_metadata_root}/comments_*.json"

        comment_paths = self._glob(comment_metadata_glob)
        if len(comment_paths) == 0:
            return []

        sorted_comment_paths = self._sort_comment_paths(comment_paths)

        comment_data = self._cat([p for _, p in sorted_comment_paths])
        sorted_comment_data = [json.loads(comment_data[p]) for _, p in sorted_comment_paths]

        return sorted_comment_data
