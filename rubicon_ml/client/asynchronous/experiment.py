from rubicon_ml import domain
from rubicon_ml.client import Experiment as SyncExperiment
from rubicon_ml.client.asynchronous import (
    ArtifactMixin,
    DataframeMixin,
    Feature,
    Metric,
    Parameter,
    TagMixin,
)


class Experiment(ArtifactMixin, DataframeMixin, TagMixin, SyncExperiment):
    """An asynchronous client experiment.

    An `experiment` represents a model run and is identified by
    its 'created_at' time. It can have `metrics`, `parameters`,
    `features`, `dataframes`, and `artifacts` logged to it.

    An `experiment` is logged to a `project`.

    Parameters
    ----------
    domain : rubicon.domain.Experiment
        The experiment domain model.
    parent : rubicon.client.Project
        The project that the experiment is logged to.
    """

    async def log_feature(self, name, description=None, importance=None):
        """Overrides `rubicon.client.Experiment.log_feature` to
        asynchronously create a feature under the experiment.

        Parameters
        ----------
        name : str
            The features's name.
        description : str
            The feature's description. Use to provide
            additional context.
        importance : float
            The feature's importance.

        Returns
        -------
        rubicon.client.Feature
            The created feature.
        """
        feature = domain.Feature(name, description=description, importance=importance)
        await self.repository.create_feature(feature, self.project.name, self.id)

        return Feature(feature, self._config)

    async def features(self):
        """Overrides `rubicon.client.experiment.features` to
        asynchronously get the features logged to this experiment.

        Returns
        -------
        list of rubicon.client.Feature
            The features previously logged to this experiment.
        """
        self._features = [
            Feature(f, self._config)
            for f in await self.repository.get_features(self.project.name, self.id)
        ]

        return self._features

    async def log_parameter(self, name, value=None, description=None):
        """Overrides `rubicon.client.Experiment.log_parameter` to
        asynchronously create a parameter under the experiment.

        Parameters
        ----------
        name : str
            The parameter's name.
        value : object, optional
            The parameter's value. Can be an object of any JSON
            serializable (via rubicon.utils.DomainJSONEncoder)
            type.
        description : str, optional
            The parameter's description. Use to provide
            additional context.

        Returns
        -------
        rubicon.client.Parameter
            The created parameter.
        """
        parameter = domain.Parameter(name, value=value, description=description)
        await self.repository.create_parameter(parameter, self.project.name, self.id)

        return Parameter(parameter, self._config)

    async def parameters(self):
        """Overrides `rubicon.client.Experiment.parameters` to
        asynchronously get the parameters logged to this experiment.

        Returns
        -------
        list of rubicon.client.Parameter
            The parameters previously logged to this experiment.
        """
        self._parameters = [
            Parameter(p, self._config)
            for p in await self.repository.get_parameters(self.project.name, self.id)
        ]

        return self._parameters

    async def log_metric(self, name, value, directionality="score", description=None):
        """Overrides `rubicon.client.Experiment.log_metric` to
        asynchronously create a metric under the experiment.

        Parameters
        ----------
        name : str
            The metric's name.
        value : float
            The metric's value.
        directionality : str, optional
            The metric's directionality. Must be one of
            ["score", "loss"], where "score" represents
            a metric to maximize, while "loss" represents a
            metric to minimize. Defaults to "score".
        description : str, optional
            The metric's description. Use to provide additional
            context.

        Returns
        -------
        rubicon.client.asynchronous.Metric
            The created metric.
        """
        metric = domain.Metric(name, value, directionality=directionality, description=description)
        await self.repository.create_metric(metric, self.project.name, self.id)

        return Metric(metric, self._config)

    async def metrics(self):
        """Overrides `rubicon.client.Experiment.metrics` to
        asynchronously get the metrics logged to this experiment.

        Returns
        -------
        list of rubicon.client.asynchronous.Metric
            The metrics previously logged to this experiment.
        """
        self._metrics = [
            Metric(m, self._config)
            for m in await self.repository.get_metrics(self.project.name, self.id)
        ]

        return self._metrics
