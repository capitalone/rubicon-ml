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
from rubicon_ml.client.utils.exception_handling import failsafe
from rubicon_ml.client.utils.tags import filter_children


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

    def _get_identifiers(self):
        """Get the experiment's project's name and the experiment's ID."""
        return self.project.name, self.id

    @failsafe
    def log_metric(self, name, value, directionality="score", description=None, tags=[]):
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
        tags : list of str, optional
            Values to tag the experiment with. Use tags to organize and
            filter your metrics.

        Returns
        -------
        rubicon.client.Metric
            The created metric.
        """
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        metric = domain.Metric(
            name, value, directionality=directionality, description=description, tags=tags
        )
        self.repository.create_metric(metric, self.project.name, self.id)

        return Metric(metric, self)

    @failsafe
    def metrics(self, name=None, tags=[], qtype="or"):
        """Get the metrics logged to this experiment.

        Parameters
        ----------
        name : str, optional
            The name value to filter results on.
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.

        Returns
        -------
        list of rubicon.client.Metric
            The metrics previously logged to this experiment.
        """
        metrics = [Metric(m, self) for m in self.repository.get_metrics(self.project.name, self.id)]
        self._metrics = filter_children(metrics, tags, qtype, name)

        return self._metrics

    @failsafe
    def metric(self, name=None, id=None):
        """Get a metric.

        Parameters
        ----------
        name : str, optional
            The name of the metric to get.
        id : str, optional
            The id of the metric to get.

        Returns
        -------
        rubicon.client.Metric
            The metric with name `name` or id `id`.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        if name is not None:
            metric = self.repository.get_metric(self.project.name, self.id, name)
            metric = Metric(metric, self)
        else:
            metric = [m for m in self.metrics() if m.id == id][0]

        return metric

    @failsafe
    def log_feature(self, name, description=None, importance=None, tags=[]):
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
        tags : list of str, optional
            Values to tag the experiment with. Use tags to organize and
            filter your features.

        Returns
        -------
        rubicon.client.Feature
            The created feature.
        """
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        feature = domain.Feature(name, description=description, importance=importance, tags=tags)
        self.repository.create_feature(feature, self.project.name, self.id)

        return Feature(feature, self)

    @failsafe
    def features(self, name=None, tags=[], qtype="or"):
        """Get the features logged to this experiment.

         Parameters
        ----------
        name : str, optional
            The name value to filter results on.
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.

        Returns
        -------
        list of rubicon.client.Feature
            The features previously logged to this experiment.
        """

        features = [
            Feature(f, self) for f in self.repository.get_features(self.project.name, self.id)
        ]

        self._features = filter_children(features, tags, qtype, name)
        return self._features

    @failsafe
    def feature(self, name=None, id=None):
        """Get a feature.

        Parameters
        ----------
        name : str, optional
            The name of the feature to get.
        id : str, optional
            The id of the feature to get.

        Returns
        -------
        rubicon.client.Feature
            The feature with name `name` or id `id`.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        if name is not None:
            feature = self.repository.get_feature(self.project.name, self.id, name)
            feature = Feature(feature, self)
        else:
            feature = [f for f in self.features() if f.id == id][0]

        return feature

    @failsafe
    def log_parameter(self, name, value=None, description=None, tags=[]):
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
        tags : list of str, optional
            Values to tag the parameter with. Use tags to organize and
            filter your parameters.

        Returns
        -------
        rubicon.client.Parameter
            The created parameter.
        """
        if not isinstance(tags, list) or not all([isinstance(tag, str) for tag in tags]):
            raise ValueError("`tags` must be `list` of type `str`")

        parameter = domain.Parameter(name, value=value, description=description, tags=tags)
        self.repository.create_parameter(parameter, self.project.name, self.id)

        return Parameter(parameter, self)

    @failsafe
    def parameters(self, name=None, tags=[], qtype="or"):
        """Get the parameters logged to this experiment.

        Parameters
        ----------
        name : str, optional
            The name value to filter results on.
        tags : list of str, optional
            The tag values to filter results on.
        qtype : str, optional
            The query type to filter results on. Can be 'or' or
            'and'. Defaults to 'or'.

        Returns
        -------
        list of rubicon.client.Parameter
            The parameters previously logged to this experiment.
        """

        parameters = [
            Parameter(p, self) for p in self.repository.get_parameters(self.project.name, self.id)
        ]

        self._parameters = filter_children(parameters, tags, qtype, name)

        return self._parameters

    @failsafe
    def parameter(self, name=None, id=None):
        """Get a parameter.

        Parameters
        ----------
        name : str, optional
            The name of the parameter to get.
        id : str, optional
            The id of the parameter to get.

        Returns
        -------
        rubicon.client.Parameter
            The parameter with name `name` or id `id`.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("`name` OR `id` required.")

        if name is not None:
            parameter = self.repository.get_parameter(self.project.name, self.id, name)
            parameter = Parameter(parameter, self)
        else:
            parameter = [p for p in self.parameters() if p.id == id][0]

        return parameter

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
