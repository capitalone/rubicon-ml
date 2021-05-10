from rubicon_ml.client import Rubicon as SyncRubicon
from rubicon_ml.client.asynchronous import Config, Project
from rubicon_ml.exceptions import RubiconException


class Rubicon(SyncRubicon):
    """The asynchronous `rubicon` client's entry point.

    Creates a `Config` and injects it into the client level
    objects at run-time.

    Parameters
    ----------
    persistence : str, optional
        The persistence type. Can be one of ["filesystem"].
    root_dir : str, optional
        Absolute or relative filepath. Currently, only s3
        paths are supported asynchronously.
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

    async def create_project(self, name, description=None, github_url=None, training_metadata=None):
        """Overrides `rubicon.client.Rubicon.create_experiment`
        to asynchronously create a project.

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
        rubicon.client.asynchronous.Project
            The created project.
        """
        project = self._create_project_domain(name, description, github_url, training_metadata)
        await self.repository.create_project(project)

        return Project(project, self.config)

    async def get_project(self, name):
        """Overrides `rubicon.client.Rubicon.get_project`
        to asynchronously get a project.

        Parameters
        ----------
        name : str
            The name of the project to get.

        Returns
        -------
        rubicon.client.asynchronous.Project
            The project with name `name`.
        """
        project = await self.repository.get_project(name)

        return Project(project, self.config)

    async def get_project_as_dask_df(self, name, group_by=None):
        """Overrides `rubicon.client.Rubicon.get_project_as_dask_df`
        to asynchronously get a dask dataframe representation of a project.

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
        project = await self.get_project(name)

        return await project.to_dask_df(group_by=group_by)

    async def get_or_create_project(self, name, **kwargs):
        """Overrides `rubicon.client.Rubicon.get_or_create_project`
        to asynchronously get or create a project.

        Parameters
        ----------
        name : str
            The project's name.
        kwargs : dict
            Additional keyword arguments to be passed to
            `Rubicon.create_project`.

        Returns
        -------
        rubicon.client.asynchronous.Project
            The corresponding project.
        """
        try:
            project = await self.get_project(name)
        except RubiconException:
            project = await self.create_project(name, **kwargs)

        return project

    async def projects(self):
        """Overrides `rubicon.client.Rubicon.projects` to
        asynchronously get a list of available projects.

        Returns
        -------
        list of rubicon.client.Project
            The list of available projects.
        """
        return [Project(project, self.config) for project in await self.repository.get_projects()]
