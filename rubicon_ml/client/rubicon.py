import os
import subprocess
import warnings
from typing import Any, Dict, List, Optional, Tuple, Union

from rubicon_ml import domain
from rubicon_ml.client import Config, Project
from rubicon_ml.client.utils.exception_handling import failsafe
from rubicon_ml.domain.utils import TrainingMetadata
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository import CompositeRepository, RepositoryBase
from rubicon_ml.repository.utils import slugify


class Rubicon:
    """The ``rubicon`` client's entry point.

    Creates a ``Config`` and injects it into the client level
    objects at run-time.

    Parameters
    ----------
    persistence : str, optional
        The persistence type. Can be one of ["filesystem", "memory", "wandb"].
        Defaults to "filesystem".
    root_dir : str, optional
        Absolute or relative filepath. Use absolute path for best performance.
        Defaults to the local filesystem. Prefix with s3:// to use s3 instead.
    auto_git_enabled : bool, optional
        True to use the ``git`` command to automatically log relevant repository
        information to projects and experiments logged with this client instance,
        False otherwise. Defaults to False.
    composite_config : list of dict, optional
        A list of configuration dicts (each with "persistence" and "root_dir" keys)
        to create a composite backend that fans out writes and fails over reads.
    storage_options : dict, optional
        Additional keyword arguments specific to the protocol being chosen. They
        are passed directly to the underlying filesystem class.
    """

    def __init__(
        self,
        persistence: Optional[str] = "filesystem",
        root_dir: Optional[str] = None,
        auto_git_enabled: bool = False,
        composite_config: Optional[List[Dict[str, Any]]] = None,
        **storage_options,
    ):
        if composite_config is not None:
            configs = [
                Config(
                    persistence=config["persistence"],
                    root_dir=config["root_dir"],
                    is_auto_git_enabled=auto_git_enabled,
                    **storage_options,
                )
                for config in composite_config
            ]
            self._config = configs[0]
            self._config.repository = CompositeRepository([c.repository for c in configs])
        else:
            self._config = Config(
                persistence=persistence,
                root_dir=root_dir,
                is_auto_git_enabled=auto_git_enabled,
                **storage_options,
            )

    @property
    def config(self) -> Config:
        """The client configuration."""
        return self._config

    @property
    def repository(self) -> RepositoryBase:
        """The configured repository backend."""
        return self._config.repository

    @repository.setter
    def repository(self, value):
        self._config.repository = value

    @property
    def repositories(self) -> List[RepositoryBase]:
        """Deprecated. Use ``.repository`` instead."""
        warnings.warn(
            "`.repositories` is deprecated. Use `.repository` instead. "
            "Multi-backend fan-out is now handled by `CompositeRepository`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return [self._config.repository]

    @property
    def configs(self) -> List[Config]:
        """Deprecated. Use ``.config`` instead."""
        warnings.warn(
            "`.configs` is deprecated. Use `.config` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return [self._config]

    @classmethod
    def compose(cls, *rubicons: "Rubicon") -> "Rubicon":
        """Create a new Rubicon backed by a CompositeRepository.

        The first argument is the primary backend — its errors propagate
        on writes, and it's tried first on reads.

        Parameters
        ----------
        *rubicons : Rubicon
            Two or more Rubicon instances to compose.

        Returns
        -------
        Rubicon
            A new Rubicon backed by a CompositeRepository wrapping
            all provided backends.
        """
        if len(rubicons) < 2:
            raise ValueError("Rubicon.compose requires at least 2 Rubicon instances.")

        repos = [rb.repository for rb in rubicons]
        instance = cls.__new__(cls)
        instance._config = Config.__new__(Config)
        instance._config.repository = CompositeRepository(repos)
        instance._config.persistence = "composite"
        instance._config.root_dir = repos[0].root_dir
        instance._config.is_auto_git_enabled = any(rb.is_auto_git_enabled() for rb in rubicons)
        return instance

    def __add__(self, other: "Rubicon") -> "Rubicon":
        """Compose two Rubicon instances into a new CompositeRepository-backed Rubicon.

        The left operand is the primary backend.

        Returns
        -------
        Rubicon
            A new Rubicon backed by a CompositeRepository.
        """
        return Rubicon.compose(self, other)

    def _get_github_url(self):
        """Returns the repository URL of the ``git`` repo it is called from."""
        completed_process = subprocess.run(["git", "remote", "-v"], capture_output=True)
        remotes = completed_process.stdout.decode("utf8").replace("\t", " ").split("\n")

        try:
            origin = [remote for remote in remotes if remote.startswith("origin")][0]
            github_url = origin.split(" ")[1]
        except IndexError:
            github_url = None

        return github_url

    def is_auto_git_enabled(self) -> bool:
        """Check if git is enabled."""
        return self._config.is_auto_git_enabled

    def _create_project_domain(
        self,
        name: str,
        description: Optional[str],
        github_url: Optional[str],
        training_metadata: Optional[Union[List[Tuple], Tuple]],
    ):
        """Instantiates and returns a project domain object."""
        if self.is_auto_git_enabled() and github_url is None:
            github_url = self._get_github_url()

        if training_metadata is not None:
            training_metadata_class = TrainingMetadata(training_metadata)
        else:
            training_metadata_class = None

        return domain.Project(
            name,
            description=description,
            github_url=github_url,
            training_metadata=training_metadata_class,
        )

    @failsafe
    def create_project(
        self,
        name: str,
        description: Optional[str] = None,
        github_url: Optional[str] = None,
        training_metadata: Optional[Union[Tuple, List[Tuple]]] = None,
    ) -> Project:
        """Create a project.

        Parameters
        ----------
        name : str
            The project's name.
        description : str, optional
            The project's description.
        github_url : str, optional
            The URL of the GitHub repository associated with this
            project. If omitted and automatic ``git`` logging is
            enabled, it will be retrieved via ``git remote``.
        training_metadata : tuple or list of tuples, optional
            Metadata associated with the training dataset(s)
            used across each experiment in this project.

        Returns
        -------
        rubicon.client.Project
            The created project.
        """
        project = self._create_project_domain(name, description, github_url, training_metadata)
        self.repository.create_project(project)

        return Project(project, self._config)

    @failsafe
    def get_project(self, name: Optional[str] = None, id: Optional[str] = None) -> Project:
        """Get a project.

        Parameters
        ----------
        name : str, optional
            The name of the project to get.
        id : str, optional
            The id of the project to get.

        Returns
        -------
        rubicon.client.Project
            The project with name ``name`` or id ``id``.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        if name is not None:
            project = self.repository.get_project(name)
            return Project(project, self._config)
        else:
            return [p for p in self.projects() if p.id == id][0]

    def get_project_as_dask_df(self, name, group_by=None):
        """DEPRECATED: Available for backwards compatibility."""
        warnings.warn(
            "`get_project_as_dask_df` is deprecated and will be removed in a future "
            "release. use `get_project_as_df('name', df_type='dask') instead.",
            DeprecationWarning,
        )

        return self.get_project_as_df(name, df_type="dask", group_by=group_by)

    @failsafe
    def get_project_as_df(self, name, df_type="pandas", group_by=None):
        """Get a dask or pandas dataframe representation of a project.

        Parameters
        ----------
        name : str
            The name of the project to get.
        df_type : str, optional
            The type of dataframe to return. Valid options include
            ["dask", "pandas"]. Defaults to "pandas".
        group_by : str or None, optional
            How to group the project's experiments in the returned
            DataFrame(s). Valid options include ["commit_hash"].

        Returns
        -------
        pandas.DataFrame or list of pandas.DataFrame or dask.DataFrame or list of dask.DataFrame
            If ``group_by`` is ``None``, a dask or pandas dataframe holding the project's
            data. Otherwise a list of dask or pandas dataframes holding the project's
            data grouped by ``group_by``.
        """
        project = self.get_project(name)

        return project.to_df(df_type=df_type, group_by=None)

    @failsafe
    def get_or_create_project(self, name: str, **kwargs) -> Project:
        """Get or create a project.

        Parameters
        ----------
        name : str
            The project's name.
        kwargs : dict
            Additional keyword arguments to be passed to
            ``Rubicon.create_project``.

        Returns
        -------
        rubicon.client.Project
            The corresponding project.
        """
        try:
            project = self.get_project(name)
        except RubiconException:
            project = self.create_project(name, **kwargs)
        else:  # check for None in case of failure mode being set to "log" or "warn"
            if project is None:
                project = self.create_project(name, **kwargs)

        return project

    @failsafe
    def projects(self):
        """Get a list of available projects.

        Returns
        -------
        list of rubicon.client.Project
            The list of available projects.
        """
        return [Project(project, self._config) for project in self.repository.get_projects()]

    @failsafe
    def sync(
        self,
        project_name: str,
        s3_root_dir: str,
        aws_profile: Optional[str] = None,
        aws_shared_credentials_file: Optional[str] = None,
    ):
        """Sync a local project to S3.

        Parameters
        ----------
        project_name : str
            The name of the project to sync.
        s3_root_dir : str
            The S3 path where the project's data
            will be synced.
        aws_profile : str
            Specifies the name of the AWS CLI profile with the credentials and options to use.
            Defaults to None, using the AWS default name 'default'.
        aws_shared_credentials_file : str
            Specifies the location of the file that the AWS CLI uses to store access keys.
            Defaults to None, using the AWS default path '~/.aws/credentials'.

        Notes
        -----
        Use sync to backup your local project data to S3 as an alternative to direct S3 logging.
        Leverages the AWS CLI's ``aws s3 sync``. Ensure that any credentials are set and that any
        proxies are enabled.
        """
        if self._config.persistence != "filesystem":
            raise RubiconException("Projects can only be synced from local or S3 filesystems.")

        cmd_root = "aws s3 sync"

        if aws_profile:
            cmd_root += f" --profile {aws_profile}"

        original_aws_shared_credentials_file = os.environ.get("AWS_SHARED_CREDENTIALS_FILE")

        if aws_shared_credentials_file:
            os.environ["AWS_SHARED_CREDENTIALS_FILE"] = aws_shared_credentials_file

        project = self.get_project(project_name)
        local_path = f"{self._config.root_dir}/{slugify(project.name)}"
        cmd = f"{cmd_root} {local_path} {s3_root_dir}/{slugify(project.name)}"

        try:
            subprocess.run(cmd, shell=True, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            raise RubiconException(e.stderr)
        finally:
            if aws_shared_credentials_file:
                if original_aws_shared_credentials_file:
                    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = original_aws_shared_credentials_file
                else:
                    del os.environ["AWS_SHARED_CREDENTIALS_FILE"]
