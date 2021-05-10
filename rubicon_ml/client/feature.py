from rubicon_ml.client import Base


class Feature(Base):
    """A client feature.

    A `feature` is an input to an `experiment` (model run)
    that's an independent, measurable property of a phenomenon
    being observed. It affects the model's predictions.

    For example, consider a model that predicts how likely a
    customer is to pay back a loan. Possible features could be
    'age', 'credit score', etc.

    A `feature` is logged to an `experiment`.

    Parameters
    ----------
    domain : rubicon.domain.Feature
        The feature domain model.
    config : rubicon.client.Config
        The config, which specifies the underlying repository.
    """

    def __init__(self, domain, config=None):
        super().__init__(domain, config)

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
