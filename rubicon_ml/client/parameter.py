from rubicon_ml.client import Base


class Parameter(Base):
    """A client parameter.

    A `parameter` is an input to an `experiment` (model run)
    that depends on the type of model being used. It affects
    the model's predictions.

    For example, if you were using a random forest classifier,
    'n_estimators' (the number of trees in the forest) could
    be a parameter.

    A `parameter` is logged to an `experiment`.

    Parameters
    ----------
    domain : rubicon.domain.Parameter
        The parameter domain model.
    config : rubicon.client.Config
        The config, which specifies the underlying repository.
    """

    def __init__(self, domain, config=None):
        super().__init__(domain, config)

    @property
    def id(self):
        """Get the parameter's id."""
        return self._domain.id

    @property
    def name(self):
        """Get the parameter's name."""
        return self._domain.name

    @property
    def value(self):
        """Get the parameter's value."""
        return self._domain.value

    @property
    def description(self):
        """Get the parameter's description."""
        return self._domain.description

    @property
    def created_at(self):
        """Get the time the parameter was created."""
        return self._domain.created_at
