import subprocess

import fsspec
import yaml

from rubicon_ml import domain
from rubicon_ml.client import Config, Project
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
        self, persistence="filesystem", root_dir=None, auto_git_enabled=False, **storage_options
    ):
        self.config = Config(persistence, root_dir, auto_git_enabled, **storage_options)

    @property
    def repository(self):
        return self.config.repository

    @repository.setter
    def repository(self, value):
        self.config.repository = value

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

    def _create_project_domain(self, name, description, github_url, training_metadata):
        """Instantiates and returns a project domain object."""
        if self.config.is_auto_git_enabled and github_url is None:
            github_url = self._get_github_url()

        if training_metadata is not None:
            training_metadata = domain.utils.TrainingMetadata(training_metadata)

        return domain.Project(
            name,
            description=description,
            github_url=github_url,
            training_metadata=training_metadata,
        )

    def create_project(self, name, description=None, github_url=None, training_metadata=None):
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
        self.repository.create_project(project)

        return Project(project, self.config)

    def get_project(self, name):
        """Get a project.

        Parameters
        ----------
        name : str
            The name of the project to get.

        Returns
        -------
        rubicon.client.Project
            The project with name `name`.
        """
        project = self.repository.get_project(name)

        return Project(project, self.config)

    def get_project_as_dask_df(self, name, group_by=None):
        """Get a dask dataframe representation of a project.

        Parameters
        ----------
        name : str
            The name of the project to get.
        group_by : str or None, optional
            How to group the project's experiments in the returned
            DataFrame(s). Valid options include ["commit_hash"].

        Returns
        -------
        dask.DataFrame or list of dask.DataFrame
            If `group_by` is `None`, a dask dataframe holding the project's
            data. Otherwise a list of dask dataframes holding the project's
            data grouped by `group_by`.
        """
        project = self.get_project(name)

        return project.to_dask_df(group_by=None)

    def get_or_create_project(self, name, **kwargs):
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

        return project

    def projects(self):
        """Get a list of available projects.

        Returns
        -------
        list of rubicon.client.Project
            The list of available projects.
        """
        return [Project(project, self.config) for project in self.repository.get_projects()]

    def publish(
        self,
        project_name,
        experiment_ids=None,
        experiment_tags=None,
        experiment_qtype="or",
        output_filepath=None,
    ):
        """Publish a project and its experiments to an intake
        catalog that can be read by the `intake-rubicon` driver.

        Parameters
        ----------
        project_name : str
            The name of the project to publish.
        experiment_ids : list of str, optional
            The ids of the experiments belonging to the project
            named `project_name` to publish.
        experiment_tags : list of str, optional
            The tags of the experiments belonging to the project
            named `project_name` to publish.
        experiment_qtype : str, optional
            The query type to filter experiment tags on. Can be
            'or' or 'and'. Defaults to 'or'.
        output_filepath : str, optional
            The absolute or relative local filepath or S3
            bucket and key to log the output catalog YAML file
            to. S3 buckets must be prepended with 's3://'. If
            left as None, only returns the YAML string without
            writing.

        Notes
        -----
        This function is not threadsafe.

        If neither `experiment_ids` nor `experiment_tags` are
        provided, all experiments belonging to the project
        named `project_name` will be published. If both are
        provided, only `experiment_ids` will be used.

        Returns
        -------
        str
            The YAML string representation of the catalog.
        """
        catalog = {"sources": {}}

        project = self.get_project(project_name)
        project_catalog = {
            "driver": "rubicon_ml_project",
            "args": {"urlpath": self.repository.root_dir, "project_name": project.name},
        }

        catalog["sources"][f"project_{project.id.replace('-', '_')}"] = project_catalog

        if experiment_ids is not None:
            experiments = []
            for e_id in experiment_ids:
                experiments.append(project.experiment(e_id))
        elif experiment_tags is not None:
            experiments = project.experiments(tags=experiment_tags, qtype=experiment_qtype)
        else:
            experiments = project.experiments()

        for e in experiments:
            experiment_catalog = {
                "driver": "rubicon_ml_experiment",
                "args": {
                    "urlpath": self.repository.root_dir,
                    "project_name": project.name,
                    "experiment_id": e.id,
                },
            }

            catalog["sources"][f"experiment_{e.id.replace('-', '_')}"] = experiment_catalog

        catalog_yaml = yaml.dump(catalog)

        if output_filepath is not None:
            with fsspec.open(output_filepath, "w", auto_mkdir=False) as f:
                f.write(catalog_yaml)

        return catalog_yaml

    def sync(self, project_name, s3_root_dir):
        """Sync a local project to S3.

        Parameters
        ----------
        project_name : str
            The name of the project to sync.
        s3_root_dir : str
            The S3 path where the project's data
            will be synced.

        Notes
        -----
        Use to backup your local project data to S3, as an alternative to direct S3 logging.
        Relies on AWS CLI's sync. Ensure that your credentials are set and that your Proxy
        is on.
        """
        if self.config.persistence != "filesystem":
            raise RubiconException(
                "You can't sync projects written to memory. Sync from either local filesystem or S3."
            )

        project = self.get_project(project_name)
        local_path = f"{self.config.root_dir}/{slugify(project.name)}"
        cmd = f"aws s3 sync {local_path} {s3_root_dir}/{slugify(project.name)}"

        try:
            subprocess.run(cmd, shell=True, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            raise RubiconException(e.stderr)
