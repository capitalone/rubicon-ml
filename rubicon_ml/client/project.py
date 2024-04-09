from __future__ import annotations

import subprocess
import warnings
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

import pandas as pd

from rubicon_ml import domain
from rubicon_ml.client import ArtifactMixin, Base, DataframeMixin, Experiment
from rubicon_ml.client.utils.exception_handling import failsafe
from rubicon_ml.client.utils.tags import filter_children
from rubicon_ml.exceptions import RubiconException
from rubicon_ml.schema.logger import SchemaMixin

if TYPE_CHECKING:
    import dask.dataframe as dd

    from rubicon_ml import Rubicon
    from rubicon_ml.client import Config, Dataframe
    from rubicon_ml.domain import Project as ProjectDomain


class Project(Base, ArtifactMixin, DataframeMixin, SchemaMixin):
    """A client project.

    A `project` is a collection of `experiments`,
    `dataframes`, and `artifacts` identified by a unique name.

    Parameters
    ----------
    domain : rubicon.domain.Project
        The project domain model.
    config : rubicon.client.Config
        The config, which specifies the underlying repository.
    """

    def __init__(self, domain: ProjectDomain, config: Optional[Union[Config, List[Config]]] = None):
        super().__init__(domain, config)

        self._domain: ProjectDomain

        self._artifacts = []
        self._dataframes = []
        self._experiments = []

    def _get_branch_name(self) -> str:
        """Returns the name of the active branch of the `git` repo
        it is called from.
        """
        command = ["git", "rev-parse", "--abbrev-ref", "HEAD"]
        completed_process = subprocess.run(command, capture_output=True)

        return completed_process.stdout.decode("utf8").replace("\n", "")

    def _get_commit_hash(self) -> str:
        """Returns the hash of the last commit to the active branch
        of the `git` repo it is called from.
        """
        command = ["git", "rev-parse", "HEAD"]
        completed_process = subprocess.run(command, capture_output=True)

        return completed_process.stdout.decode("utf8").replace("\n", "")

    def _get_identifiers(self) -> Tuple[Optional[str], None]:
        """Get the project's name."""
        return self.name, None

    def _create_experiment_domain(
        self,
        name,
        description,
        model_name,
        branch_name,
        commit_hash,
        training_metadata,
        tags,
        comments,
    ):
        """Instantiates and returns an experiment domain object."""
        if self.is_auto_git_enabled():
            if branch_name is None:
                branch_name = self._get_branch_name()
            if commit_hash is None:
                commit_hash = self._get_commit_hash()

        if training_metadata is not None:
            training_metadata = domain.utils.TrainingMetadata(training_metadata)

        return domain.Experiment(
            project_name=self._domain.name,
            name=name,
            description=description,
            model_name=model_name,
            branch_name=branch_name,
            commit_hash=commit_hash,
            training_metadata=training_metadata,
            tags=tags,
            comments=comments,
        )

    def _group_experiments(self, experiments: List[Experiment], group_by: Optional[str] = None):
        """Groups experiments by `group_by`. Valid options include ["commit_hash"].

        Returns
        -------
        dict
            A dictionary of (group name, DataFrame) key-value pairs.
        """
        GROUP_BY_OPTIONS = ["commit_hash"]
        if group_by is not None and group_by not in GROUP_BY_OPTIONS:
            raise ValueError(f"`group_by` must be one of {GROUP_BY_OPTIONS} or `None`.")

        if group_by is not None:
            grouped_experiments = {}

            if group_by == "commit_hash":
                for experiment in experiments:
                    current_experiments = grouped_experiments.get(experiment.commit_hash, [])
                    current_experiments.append(experiment)
                    grouped_experiments[experiment.commit_hash] = current_experiments
        else:
            grouped_experiments = {None: experiments}

        return grouped_experiments

    def to_dask_df(self, group_by: Optional[str] = None):
        """DEPRECATED: Available for backwards compatibility."""
        warnings.warn(
            "`to_dask_df` is deprecated and will be removed in a future release. "
            "use `to_df(df_type='dask') instead.",
            DeprecationWarning,
        )

        return self.to_df(df_type="dask", group_by=group_by)

    @failsafe
    def to_df(
        self, df_type: str = "pandas", group_by: Optional[str] = None
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame], dd.DataFrame, Dict[str, dd.DataFrame]]:
        """Loads the project's data into dask or pandas dataframe(s) sorted by
        `created_at`. This includes the experiment details along with parameters
        and metrics.

        Parameters
        ----------
        df_type : str, optional
            The type of dataframe to return. Valid options include
            ["dask", "pandas"]. Defaults to "pandas".
        group_by : str or None, optional
            How to group the project's experiments in the returned
            dataframe(s). Valid options include ["commit_hash"].

        Returns
        -------
        pandas.DataFrame or dict of pandas.DataFrame or dask.DataFrame or dict of dask.DataFrame
            If `group_by` is `None`, a dask or pandas dataframe holding the project's
            data. Otherwise a dict of dask or pandas dataframes holding the project's
            data grouped by `group_by`.
        """
        DEFAULT_COLUMNS = [
            "id",
            "name",
            "description",
            "model_name",
            "commit_hash",
            "tags",
            "created_at",
        ]

        experiments = self.experiments()
        grouped_experiments = self._group_experiments(experiments, group_by=group_by)

        experiment_dfs = {}
        for group, experiments in grouped_experiments.items():
            experiment_records = []
            parameter_names = set()
            metric_names = set()

            for experiment in experiments:
                experiment_record = {
                    "id": experiment.id,
                    "name": experiment.name,
                    "description": experiment.description,
                    "model_name": experiment.model_name,
                    "commit_hash": experiment.commit_hash,
                    "tags": experiment.tags,
                    "created_at": experiment.created_at,
                }

                for parameter in experiment.parameters():
                    experiment_record[f"{parameter.name}"] = parameter.value
                    parameter_names.add(parameter.name)

                for metric in experiment.metrics():
                    experiment_record[f"{metric.name}"] = metric.value
                    metric_names.add(metric.name)

                # TODO - features, artifacts, dataframes represented here?

                experiment_records.append(experiment_record)

            columns = DEFAULT_COLUMNS + list(parameter_names) + list(metric_names)
            df = pd.DataFrame.from_records(experiment_records, columns=columns)
            df = df.sort_values(by=["created_at"], ascending=False).reset_index(drop=True)

            if df_type == "dask":
                try:
                    from dask import dataframe as dd
                except ImportError:
                    raise RubiconException(
                        "`rubicon_ml` requires `dask` to be installed in the current "
                        "environment to create dataframes with `df_type`='dask'. `pip install "
                        "dask[dataframe]` or `conda install dask` to continue."
                    )

                df = dd.from_pandas(df, npartitions=1)

            experiment_dfs[group] = df

        return experiment_dfs if group_by is not None else list(experiment_dfs.values())[0]

    @failsafe
    def log_experiment(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        model_name: Optional[str] = None,
        branch_name: Optional[str] = None,
        commit_hash: Optional[str] = None,
        training_metadata: Optional[Union[Tuple, List[Tuple]]] = None,
        tags: Optional[List[str]] = None,
        comments: Optional[List[str]] = None,
    ) -> Experiment:
        """Log a new experiment to this project.

        Parameters
        ----------
        name : str
            The experiment's name.
        description : str, optional
            The experiment's description. Use to provide
            additional context.
        model_name : str, optional
            The experiment's model name. For example, this
            could be the name of the registered model in Model One.
        branch_name : str, optional
            The name of the active branch of the `git` repo this experiment
            is logged from. If omitted and automatic `git` logging is enabled,
            it will be retrieved via `git rev-parse`.
        commit_hash : str, optional
            The hash of the last commit to the active branch of the `git` repo
            this experiment is logged from. If omitted and automatic `git`
            logging is enabled, it will be retrieved via `git rev-parse`.
        training_metadata : tuple or list of tuples, optional
            Metadata associated with the experiment's
            training dataset(s).
        tags : list of str, optional
            Values to tag the experiment with. Use tags to organize and
            filter your experiments. For example, tags could be used
            to differentiate between the type of model or classifier
            used during the experiment (i.e. `linear regression`
            or `random forest`).
        comments : list of str, optional
            Values to comment the experiment with.

        Returns
        -------
        rubicon.client.Experiment
            The created experiment.
        """
        if tags is None:
            tags = []

        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        if comments is None:
            comments = []

        if not isinstance(comments, list) or not all(
            [isinstance(comment, str) for comment in comments]
        ):
            raise ValueError("`comments` must be `list` of type `str`")

        experiment = self._create_experiment_domain(
            name,
            description,
            model_name,
            branch_name,
            commit_hash,
            training_metadata,
            tags,
            comments,
        )
        for repo in self.repositories:
            repo.create_experiment(experiment)

        return Experiment(experiment, self)

    @failsafe
    def experiment(self, id: Optional[str] = None, name: Optional[str] = None) -> Experiment:
        """Get an experiment logged to this project by id or name.

        Parameters
        ----------
        id : str
            The id of the experiment to get.
        name : str
            The name of the experiment to get.

        Returns
        -------
        rubicon.client.Experiment
            The experiment logged to this project with id `id` or name 'name'.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        if name is not None:
            experiments = [e for e in self.experiments() if e.name == name]

            if len(experiments) == 0:
                raise RubiconException(f"No experiment found with name '{name}'.")
            elif len(experiments) > 1:
                warnings.warn(
                    f"Multiple experiments found with name '{name}'."
                    " Returning most recently logged."
                )

            return experiments[-1]
        else:
            return_err = None

            for repo in self.repositories:
                try:
                    experiment = Experiment(repo.get_experiment(self.name, id), self)
                except Exception as err:
                    return_err = err
                else:
                    return experiment

            self._raise_rubicon_exception(return_err)

    @failsafe
    def experiments(
        self, tags: Optional[List[str]] = None, qtype: str = "or", name: Optional[str] = None
    ) -> List[Experiment]:
        """Get the experiments logged to this project.

        Parameters
        ----------
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.
        name:
            The name of the experiment(s) to filter results on.

        Returns
        -------
        list of rubicon.client.Experiment
            The experiments previously logged to this project.
        """
        if tags is None:
            tags = []

        return_err = None

        for repo in self.repositories:
            try:
                experiments = [Experiment(e, self) for e in repo.get_experiments(self.name)]
            except Exception as err:
                return_err = err
            else:
                self._experiments = filter_children(experiments, tags, qtype, name)

                return self._experiments

        self._raise_rubicon_exception(return_err)

    @failsafe
    def dataframes(
        self,
        tags: Optional[List[str]] = None,
        qtype: str = "or",
        recursive: bool = False,
        name: Optional[str] = None,
    ) -> List[Dataframe]:
        """Get the dataframes logged to this project.

        Parameters
        ----------
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.
        recursive : bool, optional
            If true, get the dataframes logged to this project's
            experiments as well. Defaults to false.
        name : str
            The name value to filter results on.

        Returns
        -------
        list of rubicon.client.Dataframe
            The dataframes previously logged to this client object.
        """
        if tags is None:
            tags = []
        super().dataframes(tags=tags, qtype=qtype, name=name)

        if recursive is True:
            for experiment in self.experiments():
                self._dataframes.extend(experiment.dataframes(tags=tags, qtype=qtype, name=name))

        return self._dataframes

    @failsafe
    def archive(
        self,
        experiments: Optional[List[Experiment]] = None,
        remote_rubicon: Optional[Rubicon] = None,
    ):
        """Archive the experiments logged to this project.

        Parameters
        ----------
        experiments : list of Experiments, optional
            The rubicon.client.Experiment objects to archive. If None all logged experiments are archived.
        remote_rubicon : rubicon_ml.Rubicon object, optional
            The remote Rubicon object with the repository to archive to

        Returns
        -------
        filepath of newly created archive
        """
        if len(self.experiments()) == 0:
            raise ValueError("`project` has no logged `experiments` to archive")
        if experiments is not None:
            if not isinstance(experiments, list) or not all(
                [isinstance(experiment, Experiment) for experiment in experiments]
            ):
                raise ValueError(
                    "`experiments` must be `list` of type `rubicon_ml.client.Experiment`"
                )

        if remote_rubicon is not None:
            from rubicon_ml import Rubicon

            if not isinstance(remote_rubicon, Rubicon):
                raise ValueError("`remote_rubicon` must be of type `rubicon_ml.client.Rubicon`")
            else:
                return self.repository._archive(
                    self.name, experiments, remote_rubicon.repository.root_dir
                )
        else:
            return self.repository._archive(self.name, experiments, None)

    @failsafe
    def experiments_from_archive(self, remote_rubicon, latest_only: Optional[bool] = False):
        """Retrieve archived experiments into this project's experiments folder.

        Parameters
        ----------
        remote_rubicon : rubicon_ml.Rubicon object
            The remote Rubicon object with the repository containing archived experiments to read in
        latest_only : bool, optional
            Indicates whether or not experiments should only be read from the latest archive
        """
        from rubicon_ml import Rubicon

        if not isinstance(remote_rubicon, Rubicon):
            raise ValueError("`remote_rubicon` must be of type `rubicon_ml.client.Rubicon`")
        self.repository._experiments_from_archive(
            self.name, remote_rubicon.repository.root_dir, latest_only
        )

    @property
    def name(self):
        """Get the project's name."""
        return self._domain.name

    @property
    def id(self):
        """Get the project's id."""
        return self._domain.id

    @property
    def description(self):
        """Get the project's description."""
        return self._domain.description

    @property
    def github_url(self):
        """Get the project's GitHub repository URL."""
        return self._domain.github_url

    @property
    def training_metadata(self):
        """Get the project's training metadata."""
        training_metadata = self._domain.training_metadata.training_metadata

        if len(training_metadata) == 1:
            training_metadata = training_metadata[0]

        return training_metadata

    @property
    def created_at(self):
        """Get the time the project was created."""
        return self._domain.created_at
