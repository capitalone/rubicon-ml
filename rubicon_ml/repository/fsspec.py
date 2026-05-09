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
from rubicon_ml.domain.comment_update import CommentUpdate
from rubicon_ml.domain.tag_update import TagUpdate
from rubicon_ml.domain.utils.uuid import uuid4
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.base import RepositoryBase
from rubicon_ml.repository.utils import json, slugify

if TYPE_CHECKING:
    import dask.dataframe as dd
    import polars as pl


class FsspecRepository(RepositoryBase):
    """Repository backend using fsspec for filesystem-based persistence.

    ``FsspecRepository`` implements all 8 abstract methods from ``RepositoryBase``
    using fsspec's unified filesystem interface. It also provides filesystem-specific
    functionality like archiving.

    Subclasses (``LocalRepository``, ``S3Repository``, ``MemoryRepository``) must
    implement two sub-abstract methods: ``_persist_bytes`` and ``_persist_domain``.

    Parameters
    ----------
    root_dir : str
        Absolute path to the root directory to persist Rubicon data to.
    storage_options : dict, optional
        Additional keyword arguments that are passed directly to the
        underlying filesystem class.
    """

    def __init__(self, root_dir: str, **storage_options):
        self._df_storage_options = {}  # should only be non-empty for S3 logging

        self.filesystem = fsspec.filesystem(self.PROTOCOL, **storage_options)
        self.root_dir = root_dir.rstrip("/")

    # -------- Filesystem Helpers --------

    def _cat(self, path: str):
        """Returns the contents of the file at ``path``."""
        return self.filesystem.cat(path)

    def _cat_paths(self, metadata_paths: List[str]) -> Dict[str, Any]:
        """Cat ``metadata_paths`` to get the list of files to include.
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
        """Returns True if a file exists at ``path``, False otherwise."""
        return self.filesystem.exists(path)

    def _glob(self, globstring: str):
        """Returns the names of the files matching ``globstring``."""
        return self.filesystem.glob(globstring, detail=True)

    def _ls_directories_only(self, path: str) -> List[str]:
        """Returns the names of all the directories at path ``path``."""
        directories = [
            os.path.join(p.get("name"), "metadata.json")
            for p in self.filesystem.ls(path, detail=True)
            if p.get("type", p.get("StorageClass")).lower() == "directory"
        ]

        return directories

    def _ls(self, path: str):
        return self.filesystem.ls(path)

    def _mkdir(self, dirpath: str):
        """Creates a directory ``dirpath`` with parents."""
        return self.filesystem.mkdirs(dirpath, exist_ok=True)

    def _modified(self, path: str):
        return self.filesystem.modified(path)

    def _persist_bytes(self, bytes_data, path):
        """Write bytes to the filesystem.

        To be implemented by extensions of the fsspec repository.
        """
        raise NotImplementedError()

    def _persist_domain(self, domain, path):
        """Write a domain object to the filesystem.

        To be implemented by extensions of the fsspec repository.
        """
        raise NotImplementedError()

    def _read_bytes(self, path, err_msg=None):
        """Read bytes from the file at ``path``."""
        try:
            open_file = self.filesystem.open(path, "rb")
        except FileNotFoundError:
            raise RubiconException(err_msg)

        return open_file.read()

    def _read_domain_json(self, path, err_msg=None):
        """Read a domain object from the JSON file at ``path``."""
        try:
            open_file = self.filesystem.open(path)
        except FileNotFoundError:
            raise RubiconException(err_msg)

        return json.load(open_file)

    def _rm(self, path):
        """Recursively remove all files at ``path``."""
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

    # -------- Path Constructors --------

    def _get_project_metadata_path(self, project_name: str):
        """Returns the path of the project with name ``project_name``'s metadata."""
        return f"{self.root_dir}/{slugify(project_name)}/metadata.json"

    def _get_experiment_metadata_root(self, project_name: str):
        """Returns the experiments directory of the project with name ``project_name``."""
        return f"{self.root_dir}/{slugify(project_name)}/experiments"

    def _get_experiment_metadata_path(self, project_name, experiment_id):
        """Returns the path of the experiment with ID ``experiment_id``'s metadata."""
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/metadata.json"

    def _get_artifact_metadata_root(self, project_name, experiment_id=None):
        """Returns the artifacts directory of the project or experiment."""
        if experiment_id is not None:
            experiment_metadata_root = self._get_experiment_metadata_root(project_name)

            return f"{experiment_metadata_root}/{experiment_id}/artifacts"
        else:
            return f"{self.root_dir}/{slugify(project_name)}/artifacts"

    def _get_artifact_metadata_path(self, project_name, experiment_id, artifact_id):
        """Returns the path of the artifact with ID ``artifact_id``'s metadata."""
        artifact_metadata_root = self._get_artifact_metadata_root(project_name, experiment_id)

        return f"{artifact_metadata_root}/{artifact_id}/metadata.json"

    def _get_artifact_data_path(self, project_name, experiment_id, artifact_id):
        """Returns the path of the artifact with ID ``artifact_id``'s raw data."""
        artifact_metadata_root = self._get_artifact_metadata_root(project_name, experiment_id)

        return f"{artifact_metadata_root}/{artifact_id}/data"

    def _get_dataframe_metadata_root(self, project_name, experiment_id=None):
        """Returns the dataframes directory of the project or experiment."""
        if experiment_id is not None:
            experiment_metadata_root = self._get_experiment_metadata_root(project_name)

            return f"{experiment_metadata_root}/{experiment_id}/dataframes"
        else:
            return f"{self.root_dir}/{slugify(project_name)}/dataframes"

    def _get_dataframe_metadata_path(self, project_name, experiment_id, dataframe_id):
        """Returns the path of the dataframe with ID ``dataframe_id``'s metadata."""
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        return f"{dataframe_metadata_root}/{dataframe_id}/metadata.json"

    def _get_dataframe_data_path(self, project_name, experiment_id, dataframe_id):
        """Returns the path of the dataframe with ID ``dataframe_id``'s raw data."""
        dataframe_metadata_root = self._get_dataframe_metadata_root(project_name, experiment_id)

        return f"{dataframe_metadata_root}/{dataframe_id}/data"

    def _get_feature_metadata_root(self, project_name, experiment_id):
        """Returns the features directory of the experiment with ID ``experiment_id``."""
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/features"

    def _get_feature_metadata_path(self, project_name, experiment_id, feature_name):
        """Returns the path of the feature with name ``feature_name``'s metadata."""
        feature_metadata_root = self._get_feature_metadata_root(project_name, experiment_id)

        return f"{feature_metadata_root}/{slugify(feature_name)}/metadata.json"

    def _get_metric_metadata_root(self, project_name, experiment_id):
        """Returns the metrics directory of the experiment with ID ``experiment_id``."""
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/metrics"

    def _get_metric_metadata_path(self, project_name, experiment_id, metric_name):
        """Returns the path of the metric with name ``metric_name``'s metadata."""
        metric_metadata_root = self._get_metric_metadata_root(project_name, experiment_id)

        return f"{metric_metadata_root}/{slugify(metric_name)}/metadata.json"

    def _get_parameter_metadata_root(self, project_name, experiment_id):
        """Returns the parameters directory of the experiment with ID ``experiment_id``."""
        experiment_metadata_root = self._get_experiment_metadata_root(project_name)

        return f"{experiment_metadata_root}/{experiment_id}/parameters"

    def _get_parameter_metadata_path(self, project_name, experiment_id, parameter_name):
        """Returns the path of the parameter with name ``parameter_name``'s metadata."""
        parameter_metadata_root = self._get_parameter_metadata_root(project_name, experiment_id)

        return f"{parameter_metadata_root}/{slugify(parameter_name)}/metadata.json"

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

            # We want to slugify the names of Metrics, Features, and Parameters-
            # not Artifacts, Dataframes, or Experiments
            if entity_type in ["Metric", "Feature", "Parameter"]:
                entity_identifier = slugify(entity_identifier)
            return f"{entity_metadata_root}/{entity_identifier}"

    def _get_comment_metadata_root(
        self, project_name, experiment_id=None, entity_identifier=None, entity_type=None
    ):
        """Returns the directory to write comments to."""
        # comments and tags are written to the same root with a different filename
        return self._get_tag_metadata_root(
            project_name, experiment_id, entity_identifier, entity_type
        )

    # -------- Dataframe Helpers --------

    def _persist_dataframe(
        self, df: Union[pd.DataFrame, "dd.DataFrame", "pl.DataFrame"], path: str
    ):
        """Persists the dataframe ``df`` to the configured filesystem."""
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
        """Reads the dataframe from the configured filesystem."""
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

    # -------- Tag/Comment Sort Helpers --------

    def _sort_tag_paths(self, tag_paths):
        """Sorts the paths in ``tag_paths`` by when they were created."""
        if isinstance(tag_paths, dict):
            tag_paths = tag_paths.values()

        tag_paths_with_timestamps = [
            (t.get("created", t.get("LastModified")), t.get("name")) for t in tag_paths
        ]
        tag_paths_with_timestamps.sort()

        return tag_paths_with_timestamps

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
        """Persist a domain object to the filesystem."""
        if isinstance(domain_obj, domain.Project):
            path = self._get_project_metadata_path(project_name)
        elif isinstance(domain_obj, domain.Experiment):
            path = self._get_experiment_metadata_path(project_name, experiment_id)
        elif isinstance(domain_obj, domain.Artifact):
            path = self._get_artifact_metadata_path(project_name, experiment_id, entity_identifier)
        elif isinstance(domain_obj, domain.Dataframe):
            path = self._get_dataframe_metadata_path(project_name, experiment_id, entity_identifier)
        elif isinstance(domain_obj, domain.Feature):
            path = self._get_feature_metadata_path(project_name, experiment_id, entity_identifier)
            if self._exists(path):
                raise RubiconException(f"A feature with name '{entity_identifier}' already exists.")
        elif isinstance(domain_obj, domain.Metric):
            path = self._get_metric_metadata_path(project_name, experiment_id, entity_identifier)
            if self._exists(path):
                raise RubiconException(f"A metric with name '{entity_identifier}' already exists.")
        elif isinstance(domain_obj, domain.Parameter):
            path = self._get_parameter_metadata_path(project_name, experiment_id, entity_identifier)
            if self._exists(path):
                raise RubiconException(
                    f"A parameter with name '{entity_identifier}' already exists."
                )
        elif isinstance(domain_obj, TagUpdate):
            tag_root = self._get_tag_metadata_root(
                project_name, experiment_id, entity_identifier, entity_type
            )
            path = f"{tag_root}/tags_{uuid4()}.json"
            # Serialize only the non-empty field to preserve on-disk format compatibility
            if domain_obj.added_tags:
                self._persist_domain({"added_tags": domain_obj.added_tags}, path)
            else:
                self._persist_domain({"removed_tags": domain_obj.removed_tags}, path)
            return
        elif isinstance(domain_obj, CommentUpdate):
            comment_root = self._get_comment_metadata_root(
                project_name, experiment_id, entity_identifier, entity_type
            )
            path = f"{comment_root}/comments_{uuid4()}.json"
            if domain_obj.added_comments:
                self._persist_domain({"added_comments": domain_obj.added_comments}, path)
            else:
                self._persist_domain({"removed_comments": domain_obj.removed_comments}, path)
            return
        else:
            raise RubiconException(f"Cannot persist domain object of type {type(domain_obj)}")

        self._persist_domain(domain_obj, path)

    def read_domain(
        self,
        domain_cls,
        project_name,
        *,
        experiment_id=None,
        entity_identifier=None,
        entity_type=None,
    ):
        """Read a single domain object from the filesystem."""
        if domain_cls is domain.Project:
            path = self._get_project_metadata_path(project_name)
            err_msg = f"No project with name '{project_name}' found."
            data = self._read_domain_json(path, err_msg)
            return domain.Project(**data)
        elif domain_cls is domain.Experiment:
            path = self._get_experiment_metadata_path(project_name, experiment_id)
            err_msg = f"No experiment with id `{experiment_id}` found."
            data = self._read_domain_json(path, err_msg)
            return domain.Experiment(**data)
        elif domain_cls is domain.Artifact:
            path = self._get_artifact_metadata_path(project_name, experiment_id, entity_identifier)
            err_msg = f"No artifact with id `{entity_identifier}` found."
            data = self._read_domain_json(path, err_msg)
            return domain.Artifact(**data)
        elif domain_cls is domain.Dataframe:
            path = self._get_dataframe_metadata_path(project_name, experiment_id, entity_identifier)
            err_msg = f"No dataframe with id `{entity_identifier}` found."
            data = self._read_domain_json(path, err_msg)
            return domain.Dataframe(**data)
        elif domain_cls is domain.Feature:
            path = self._get_feature_metadata_path(project_name, experiment_id, entity_identifier)
            err_msg = f"No feature with name '{entity_identifier}' found."
            data = self._read_domain_json(path, err_msg)
            return domain.Feature(**data)
        elif domain_cls is domain.Metric:
            path = self._get_metric_metadata_path(project_name, experiment_id, entity_identifier)
            err_msg = f"No metric with name '{entity_identifier}' found."
            data = self._read_domain_json(path, err_msg)
            return domain.Metric(**data)
        elif domain_cls is domain.Parameter:
            path = self._get_parameter_metadata_path(project_name, experiment_id, entity_identifier)
            err_msg = f"No parameter with name '{entity_identifier}' found."
            data = self._read_domain_json(path, err_msg)
            return domain.Parameter(**data)
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
        """Read all domain objects of a given type from the filesystem."""
        if domain_cls is domain.Project:
            return self._load_metadata_files(self.root_dir, domain.Project)
        elif domain_cls is domain.Experiment:
            root = self._get_experiment_metadata_root(project_name)
            return self._load_metadata_files(root, domain.Experiment)
        elif domain_cls is domain.Artifact:
            root = self._get_artifact_metadata_root(project_name, experiment_id)
            return self._load_metadata_files(root, domain.Artifact)
        elif domain_cls is domain.Dataframe:
            root = self._get_dataframe_metadata_root(project_name, experiment_id)
            return self._load_metadata_files(root, domain.Dataframe)
        elif domain_cls is domain.Feature:
            root = self._get_feature_metadata_root(project_name, experiment_id)
            return self._load_metadata_files(root, domain.Feature)
        elif domain_cls is domain.Metric:
            root = self._get_metric_metadata_root(project_name, experiment_id)
            return self._load_metadata_files(root, domain.Metric)
        elif domain_cls is domain.Parameter:
            root = self._get_parameter_metadata_root(project_name, experiment_id)
            return self._load_metadata_files(root, domain.Parameter)
        elif domain_cls is TagUpdate:
            tag_root = self._get_tag_metadata_root(
                project_name, experiment_id, entity_identifier, entity_type
            )
            tag_glob = f"{tag_root}/tags_*.json"

            tag_paths = self._glob(tag_glob)
            if len(tag_paths) == 0:
                return []

            sorted_tag_paths = self._sort_tag_paths(tag_paths)
            tag_data = self._cat([p for _, p in sorted_tag_paths])

            return [TagUpdate(**json.loads(tag_data[p])) for _, p in sorted_tag_paths]
        elif domain_cls is CommentUpdate:
            comment_root = self._get_comment_metadata_root(
                project_name, experiment_id, entity_identifier, entity_type
            )
            comment_glob = f"{comment_root}/comments_*.json"

            comment_paths = self._glob(comment_glob)
            if len(comment_paths) == 0:
                return []

            sorted_comment_paths = self._sort_tag_paths(comment_paths)
            comment_data = self._cat([p for _, p in sorted_comment_paths])

            return [CommentUpdate(**json.loads(comment_data[p])) for _, p in sorted_comment_paths]
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
        """Remove a domain object from the filesystem."""
        if domain_cls is domain.Artifact:
            root = self._get_artifact_metadata_root(project_name, experiment_id)
            try:
                self._rm(f"{root}/{entity_identifier}")
            except FileNotFoundError:
                raise RubiconException(f"No artifact with id `{entity_identifier}` found.")
        elif domain_cls is domain.Dataframe:
            root = self._get_dataframe_metadata_root(project_name, experiment_id)
            try:
                self._rm(f"{root}/{entity_identifier}")
            except FileNotFoundError:
                raise RubiconException(f"No dataframe with id `{entity_identifier}` found.")
        else:
            raise RubiconException(f"Cannot remove domain object of type {domain_cls}")

    def write_artifact_data(self, data, project_name, artifact_id, *, experiment_id=None):
        """Persist artifact binary data to the filesystem."""
        path = self._get_artifact_data_path(project_name, experiment_id, artifact_id)
        self._persist_bytes(data, path)

    def read_artifact_data(self, project_name, artifact_id, *, experiment_id=None):
        """Read artifact binary data from the filesystem."""
        path = self._get_artifact_data_path(project_name, experiment_id, artifact_id)
        return self._read_bytes(path, f"No data for artifact with id `{artifact_id}` found.")

    def write_dataframe_data(self, df, project_name, dataframe_id, *, experiment_id=None):
        """Persist dataframe data to the filesystem."""
        path = self._get_dataframe_data_path(project_name, experiment_id, dataframe_id)
        self._persist_dataframe(df, path)

    def read_dataframe_data(
        self, project_name, dataframe_id, *, experiment_id=None, df_type="pandas"
    ):
        """Read dataframe data from the filesystem."""
        path = self._get_dataframe_data_path(project_name, experiment_id, dataframe_id)
        try:
            return self._read_dataframe(path, df_type)
        except FileNotFoundError:
            raise RubiconException(
                f"No data for dataframe with id `{dataframe_id}` found. This might have "
                "happened if you forgot to set `df_type='dask'` when trying to read a "
                "`dask` dataframe."
            )

    # -------- Convenience Method Overrides (existence checks) --------

    def create_project(self, project: domain.Project):
        """Persist a project to the filesystem.

        Raises ``RubiconException`` if a project with the same name already exists.
        """
        project_metadata_path = self._get_project_metadata_path(project.name)

        if self._exists(project_metadata_path):
            raise RubiconException(f"A project with name '{project.name}' already exists.")

        self.write_domain(project, project.name)

    # -------- Archiving (filesystem-specific) --------

    def _archive(
        self,
        project_name,
        experiments: Optional[List] = None,
        remote_rubicon_root: Optional[str] = None,
    ):
        """Archive the experiments logged to this project."""
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
        """Retrieve archived experiments into this project's experiments folder."""
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
