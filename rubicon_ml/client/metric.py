from rubicon_ml.client import Base, TagMixin


class Metric(Base, TagMixin):
    """A client metric.

    A `metric` is a single-value output of an `experiment` that
    helps evaluate the quality of the model's predictions.

    It can be either a 'score' (value to maximize) or
    a 'loss' (value to minimize).

    A `metric` is logged to an `experiment`.

    Parameters
    ----------
    domain : rubicon.domain.Metric
        The metric domain model.
    parent : rubicon.client.Experiment
        The experiment that the metric is
        logged to.
    """

    def __init__(self, domain, parent):
        super().__init__(domain, parent._config)

        self._data = None
        self._parent = parent

    @property
    def id(self):
        """Get the metric's id."""
        return self._domain.id

    @property
    def name(self):
        """Get the metric's name."""
        return self._domain.name

    @property
    def value(self):
        """Get the metric's value."""
        return self._domain.value

    @property
    def directionality(self):
        """Get the metric's directionality."""
        return self._domain.directionality

    @property
    def description(self):
        """Get the metric's description."""
        return self._domain.description

    @property
    def created_at(self):
        """Get the metric's created_at."""
        return self._domain.created_at

    @property
    def parent(self):
        """Get the metric's parent client object."""
        return self._parent
