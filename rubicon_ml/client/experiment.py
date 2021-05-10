from rubicon_ml import domain
from rubicon_ml.client import (
    ArtifactMixin,
    Base,
    DataframeMixin,
    Feature,
    Metric,
    Parameter,
    TagMixin,
)


class Experiment(Base, ArtifactMixin, DataframeMixin, TagMixin):
    """A client experiment.

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

    def __init__(self, domain, parent):
        super().__init__(domain, parent._config)

        self._parent = parent
        self._artifacts = []
        self._dataframes = []
        self._metrics = []
        self._features = []
        self._parameters = []

    def log_metric(self, name, value, directionality="score", description=None):
        """Create a metric under the experiment.

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
        rubicon.client.Metric
            The created metric.
        """
        metric = domain.Metric(name, value, directionality=directionality, description=description)
        self.repository.create_metric(metric, self.project.name, self.id)

        return Metric(metric, self._config)

    def metrics(self):
        """Get the metrics logged to this experiment.

        Returns
        -------
        list of rubicon.client.Metric
            The metrics previously logged to this experiment.
        """
        self._metrics = [
            Metric(m, self._config) for m in self.repository.get_metrics(self.project.name, self.id)
        ]

        return self._metrics

    def log_feature(self, name, description=None, importance=None):
        """Create a feature under the experiment.

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
        self.repository.create_feature(feature, self.project.name, self.id)

        return Feature(feature, self._config)

    def features(self):
        """Get the features logged to this experiment.

        Returns
        -------
        list of rubicon.client.Feature
            The features previously logged to this experiment.
        """
        self._features = [
            Feature(f, self._config)
            for f in self.repository.get_features(self.project.name, self.id)
        ]

        return self._features

    def log_parameter(self, name, value=None, description=None):
        """Create a parameter under the experiment.

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
        self.repository.create_parameter(parameter, self.project.name, self.id)

        return Parameter(parameter, self._config)

    def parameters(self):
        """Get the parameters logged to this experiment.

        Returns
        -------
        list of rubicon.client.Parameter
            The parameters previously logged to this experiment.
        """
        self._parameters = [
            Parameter(p, self._config)
            for p in self.repository.get_parameters(self.project.name, self.id)
        ]

        return self._parameters

    @property
    def id(self):
        """Get the experiment's id."""
        return self._domain.id

    @property
    def name(self):
        """Get the experiment's name."""
        return self._domain.name

    @property
    def description(self):
        """Get the experiment's description."""
        return self._domain.description

    @property
    def model_name(self):
        """Get the experiment's model name."""
        return self._domain.model_name

    @property
    def branch_name(self):
        """Get the experiment's branch name."""
        return self._domain.branch_name

    @property
    def commit_hash(self):
        """Get the experiment's commit hash."""
        return self._domain.commit_hash

    @property
    def training_metadata(self):
        """Get the project's training metadata."""
        training_metadata = self._domain.training_metadata.training_metadata

        if len(training_metadata) == 1:
            training_metadata = training_metadata[0]

        return training_metadata

    @property
    def created_at(self):
        """Get the time the experiment was created."""
        return self._domain.created_at

    @property
    def project(self):
        """Get the project client object that this experiment
        belongs to.
        """
        return self._parent
