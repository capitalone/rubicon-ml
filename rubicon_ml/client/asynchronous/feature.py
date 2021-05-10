from rubicon_ml.client import Feature as SyncFeature


class Feature(SyncFeature):
    """An asynchronous client feature.

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

    pass
