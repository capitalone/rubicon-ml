import subprocess
import warnings

import dask.dataframe as dd
import pandas as pd

from rubicon_ml import domain
from rubicon_ml.client import ArtifactMixin, Base, DataframeMixin, Experiment
from rubicon_ml.client.utils.exception_handling import failsafe
from rubicon_ml.client.utils.tags import filter_children
from rubicon_ml.exceptions import RubiconException


class Project(Base, ArtifactMixin, DataframeMixin):
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

    def __init__(self, domain, config=None):
        super().__init__(domain, config)

        self._artifacts = []
        self._dataframes = []
        self._experiments = []

    def _get_branch_name(self):
        """Returns the name of the active branch of the `git` repo
        it is called from.
        """
        command = ["git", "rev-parse", "--abbrev-ref", "HEAD"]
        completed_process = subprocess.run(command, capture_output=True)

        return completed_process.stdout.decode("utf8").replace("\n", "")

    def _get_commit_hash(self):
        """Returns the hash of the last commit to the active branch
        of the `git` repo it is called from.
        """
        command = ["git", "rev-parse", "HEAD"]
        completed_process = subprocess.run(command, capture_output=True)

        return completed_process.stdout.decode("utf8").replace("\n", "")

    def _get_identifiers(self):
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
    ):
        """Instantiates and returns an experiment domain object."""
        if self._config.is_auto_git_enabled:
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
        )

    def _group_experiments(self, experiments, group_by=None):
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

    def to_dask_df(self, group_by=None):
        """DEPRECATED: Available for backwards compatibility."""
        warnings.warn(
            "`to_dask_df` is deprecated and will be removed in a future release. "
            "use `to_df(df_type='dask') instead.",
            DeprecationWarning,
        )

        return self.to_df(df_type="dask", group_by=group_by)

    @failsafe
    def to_df(self, df_type="pandas", group_by=None):
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
        pandas.DataFrame or list of pandas.DataFrame or dask.DataFrame or list of dask.DataFrame
            If `group_by` is `None`, a dask or pandas dataframe holding the project's
            data. Otherwise a list of dask or pandas dataframes holding the project's
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
                df = dd.from_pandas(df, npartitions=1)

            experiment_dfs[group] = df

        return experiment_dfs if group_by is not None else list(experiment_dfs.values())[0]

    @failsafe
    def log_experiment(
        self,
        name=None,
        description=None,
        model_name=None,
        branch_name=None,
        commit_hash=None,
        training_metadata=None,
        tags=[],
    ):
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

        Returns
        -------
        rubicon.client.Experiment
            The created experiment.
        """
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        experiment = self._create_experiment_domain(
            name,
            description,
            model_name,
            branch_name,
            commit_hash,
            training_metadata,
            tags,
        )
        self.repository.create_experiment(experiment)

        return Experiment(experiment, self)

    @failsafe
    def experiment(self, id=None, name=None):
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

            experiment = experiments[-1]
        else:
            experiment = Experiment(self.repository.get_experiment(self.name, id), self)

        return experiment

    @failsafe
    def experiments(self, tags=[], qtype="or", name=None):
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
        experiments = [Experiment(e, self) for e in self.repository.get_experiments(self.name)]
        self._experiments = filter_children(experiments, tags, qtype, name)

        return self._experiments

    @failsafe
    def dataframes(self, tags=[], qtype="or", recursive=False, name=None):
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
        super().dataframes(tags=tags, qtype=qtype, name=name)

        if recursive is True:
            for experiment in self.experiments():
                self._dataframes.extend(experiment.dataframes(tags=tags, qtype=qtype, name=name))

        return self._dataframes

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
