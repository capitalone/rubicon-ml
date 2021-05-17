import asyncio

import dask.dataframe as dd
import pandas as pd

from rubicon_ml.client import Project as SyncProject
from rubicon_ml.client.asynchronous import ArtifactMixin, DataframeMixin, Experiment
from rubicon_ml.client.utils.tags import has_tag_requirements


class Project(ArtifactMixin, DataframeMixin, SyncProject):
    """An asynchronous client project.

    A `project` is a collection of `experiments`,
    `dataframes`, and `artifacts` identified by a unique name.

    Parameters
    ----------
    domain : rubicon.domain.Project
        The project domain model.
    config : rubicon.client.Config
        The config, which specifies the underlying repository.
    """

    async def _make_experiment_record(self, experiment):
        experiment_record = {
            "id": experiment.id,
            "name": experiment.name,
            "description": experiment.description,
            "model_name": experiment.model_name,
            "commit_hash": experiment.commit_hash,
            "created_at": experiment.created_at,
        }

        parameters_coroutine = experiment.parameters()
        metrics_coroutine = experiment.metrics()

        parameters, metrics = await asyncio.gather(*[parameters_coroutine, metrics_coroutine])

        # tags are being handled differently on purpose to avoid
        # an error when used in asyncio.gather
        experiment_record["tags"] = await experiment.tags

        for parameter in parameters:
            experiment_record[parameter.name] = parameter.value

        for metric in metrics:
            experiment_record[metric.name] = metric.value

        return experiment_record

    async def to_dask_df(self, group_by=None):
        """Overrides `rubicon.client.Project.to_dask_df` to asynchronously
        load the project's data into dask dataframe(s) sorted by `created_at`.

        Parameters
        ----------
        group_by : str or None, optional
            How to group the project's experiments in the returned
            dataframe(s). Valid options include ["commit_hash"].

        Returns
        -------
        dask.DataFrame or list of dask.DataFrame
            If `group_by` is `None`, a dask dataframe holding the project's
            data. Otherwise a list of dask dataframes holding the project's
            data grouped by `group_by`.
        """
        experiments = await self.experiments()
        grouped_experiments = self._group_experiments(experiments, group_by=group_by)

        experiment_dfs = {}
        for group, experiments in grouped_experiments.items():
            experiment_records = await asyncio.gather(
                *[self._make_experiment_record(experiment) for experiment in experiments]
            )

            df = pd.DataFrame.from_records(experiment_records)
            df = df.sort_values(by=["created_at"], ascending=False).reset_index(drop=True)
            experiment_dfs[group] = dd.from_pandas(df, npartitions=1)

        return experiment_dfs if group_by is not None else list(experiment_dfs.values())[0]

    async def log_experiment(
        self,
        name=None,
        description=None,
        model_name=None,
        branch_name=None,
        commit_hash=None,
        training_metadata=None,
        tags=[],
    ):
        """Overrides `rubicon.client.Project.log_experiment` to
        asynchronously log a new experiment to this project.

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
            The experiment's corresponding git commit hash.
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
        rubicon.client.asynchronous.Experiment
            The created experiment.
        """
        experiment = self._create_experiment_domain(
            name,
            description,
            model_name,
            branch_name,
            commit_hash,
            training_metadata,
            tags,
        )
        await self.repository.create_experiment(experiment)

        return Experiment(experiment, self)

    async def experiment(self, id):
        """Overrides `rubicon.client.Project.experiment` to asynchronously
        get an experiment logged to this project by id.

        Parameters
        ----------
        id : str
            The id of the experiment to get.

        Returns
        -------
        rubicon.client.asynchronous.Experiment
            The experiment logged to this project with id `id`.
        """
        experiment = Experiment(await self.repository.get_experiment(self.name, id), self)

        return experiment

    async def _filter_experiments(self, experiments, tags, qtype):
        """Filters the provided experiments by `tags` using
        query type `qtype`.
        """
        if len(tags) > 0:
            filtered_experiments = []
            [
                filtered_experiments.append(e)
                for e in experiments
                if has_tag_requirements(await e.tags, tags, qtype)
            ]
            self._experiments = filtered_experiments
        else:
            self._experiments = experiments

    async def experiments(self, tags=[], qtype="or"):
        """Overrides `rubicon.client.Project.experiments` to asynchronously
        get the experiments logged to this project.

        Parameters
        ----------
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.

        Returns
        -------
        list of rubicon.client.asynchronous.Experiment
            The experiments previously logged to this project.
        """
        experiments = [
            Experiment(e, self) for e in await self.repository.get_experiments(self.name)
        ]
        await self._filter_experiments(experiments, tags, qtype)

        return self._experiments
