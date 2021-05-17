from rubicon_ml.client import Parameter as SyncParameter


class Parameter(SyncParameter):
    """An asycnhronous client parameter.

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

    pass
