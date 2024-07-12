import os
import subprocess
import warnings
from typing import Any, Dict, List, Optional, Tuple, Union

from rubicon_ml import domain
from rubicon_ml.client import Config, Project
from rubicon_ml.client.utils.exception_handling import failsafe
from rubicon_ml.domain.utils import TrainingMetadata
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.repository.utils import slugify


class Rubicon:
    """The `rubicon` client's entry point.

    Creates a `Config` and injects it into the client level
    objects at run-time.

    Parameters
    ----------
    persistence : str, optional
        The persistence type. Can be one of ["filesystem", "memory"].
        Defaults to "filesystem".
    root_dir : str, optional
        Absolute or relative filepath. Use absolute path for best performance.
        Defaults to the local filesystem. Prefix with s3:// to use s3 instead.
    auto_git_enabled : bool, optional
        True to use the `git` command to automatically log relevant repository
        information to projects and experiments logged with this client instance,
        False otherwise. Defaults to False.
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
            self.configs = [
                Config(
                    persistence=config["persistence"],
                    root_dir=config["root_dir"],
                    is_auto_git_enabled=auto_git_enabled,
                    **storage_options,
                )
                for config in composite_config
            ]
        else:
            self.configs = [
                Config(
                    persistence=persistence,
                    root_dir=root_dir,
                    is_auto_git_enabled=auto_git_enabled,
                    **storage_options,
                ),
            ]

    @property
    def config(self):
        """
        Returns a single config.

        Exists to promote backwards compatibility.

        Returns
        -------
        Config
            A single Config
        """
        return self.configs[0]

    @property
    def repository(self):
        if len(self.configs) > 1:
            raise ValueError("More than one repository available. Use `.repositories` instead.")
        return self.configs[0].repository

    @property
    def repositories(self):
        return [config.repository for config in self.configs]

    @repository.setter
    def repository(self, value):
        if len(self.configs) > 1:
            raise ValueError("Cannot set when more than one repository available!")

        self.configs[0].repository = value

    def _get_github_url(self):
        """Returns the repository URL of the `git` repo it is called from."""
        completed_process = subprocess.run(["git", "remote", "-v"], capture_output=True)
        remotes = completed_process.stdout.decode("utf8").replace("\t", " ").split("\n")

        try:
            origin = [remote for remote in remotes if remote.startswith("origin")][0]
            github_url = origin.split(" ")[1]
        except IndexError:
            github_url = None

        return github_url

    def is_auto_git_enabled(self) -> bool:
        """Check if git is enabled for any of the configs."""
        if isinstance(self.configs, list):
            return any(_config.is_auto_git_enabled for _config in self.configs)

        if self.configs is None:
            return False

        return self.configs.is_auto_git_enabled

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
            project. If omitted and automatic `git` logging is
            enabled, it will be retrieved via `git remote`.
        training_metadata : tuple or list of tuples, optional
            Metadata associated with the training dataset(s)
            used across each experiment in this project.

        Returns
        -------
        rubicon.client.Project
            The created project.
        """
        project = self._create_project_domain(name, description, github_url, training_metadata)
        for repo in self.repositories:
            repo.create_project(project)

        return Project(project, self.configs)

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
            The project with name `name` or id `id`.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        if name is not None:
            return_err = None

            for repo in self.repositories:
                try:
                    project = repo.get_project(name)
                except Exception as err:
                    return_err = err
                else:
                    return Project(project, self.config)

            if len(self.repositories) > 1:
                raise RubiconException("all configured storage backends failed") from return_err
            else:
                raise return_err
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
            If `group_by` is `None`, a dask or pandas dataframe holding the project's
            data. Otherwise a list of dask or pandas dataframes holding the project's
            data grouped by `group_by`.
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
            `Rubicon.create_project`.

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
        return_err = None

        for repo in self.repositories:
            try:
                projects = [Project(project, self.config) for project in repo.get_projects()]
            except Exception as err:
                return_err = err
            else:
                return projects

        if len(self.repositories) > 1:
            raise RubiconException("all configured storage backends failed") from return_err
        else:
            raise return_err

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
        Leverages the AWS CLI's `aws s3 sync`. Ensure that any credentials are set and that any
        proxies are enabled.
        """
        if self.config.persistence != "filesystem":
            raise RubiconException("Projects can only be synced from local or S3 filesystems.")

        cmd_root = "aws s3 sync"

        if aws_profile:
            cmd_root += f" --profile {aws_profile}"

        original_aws_shared_credentials_file = os.environ.get("AWS_SHARED_CREDENTIALS_FILE")

        if aws_shared_credentials_file:
            os.environ["AWS_SHARED_CREDENTIALS_FILE"] = aws_shared_credentials_file

        project = self.get_project(project_name)
        local_path = f"{self.config.root_dir}/{slugify(project.name)}"
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
