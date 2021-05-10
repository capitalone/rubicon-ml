from rubicon_ml.client import Metric as SyncMetric


class Metric(SyncMetric):
    """An asynchronous client metric.

    A `metric` is a single-value output of an `experiment` that
    helps evaluate the quality of the model's predictions.

    It can be either a 'score' (value to maximize) or
    a 'loss' (value to minimize).

    A `metric` is logged to an `experiment`.

    Parameters
    ----------
    domain : rubicon.domain.Metric
        The metric domain model.
    config : rubicon.client.Config
        The config, which specifies the underlying repository.
    """

    pass
