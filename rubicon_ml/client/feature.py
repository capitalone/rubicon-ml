from rubicon_ml.client import Base, TagMixin


class Feature(Base, TagMixin):
    """A client feature.

    A `feature` is an input to an `experiment` (model run)
    that's an independent, measurable property of a phenomenon
    being observed. It affects the model's predictions.

    For example, consider a model that predicts how likely a
    customer is to pay back a loan. Possible features could be
    'year', 'credit score', etc.

    A `feature` is logged to an `experiment`.

    Parameters
    ----------
    domain : rubicon.domain.Feature
        The feature domain model.
    config : rubicon.client.Config
        The config, which specifies the underlying repository.
    parent : rubicon.client.Experiment
        The experiment that the feature is
        logged to.
    """

    def __init__(self, domain, parent):
        super().__init__(domain, parent._config)

        self._data = None
        self._parent = parent

    @property
    def id(self):
        """Get the feature's id."""
        return self._domain.id

    @property
    def name(self):
        """Get the feature's name."""
        return self._domain.name

    @property
    def description(self):
        """Get the feature's description."""
        return self._domain.description

    @property
    def importance(self):
        """Get the feature's importance."""
        return self._domain.importance

    @property
    def created_at(self):
        """Get the feature's created_at."""
        return self._domain.created_at

    @property
    def parent(self):
        """Get the feature's parent client object."""
        return self._parent
