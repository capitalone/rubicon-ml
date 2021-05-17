from rubicon_ml.client import Dataframe as SyncDataframe
from rubicon_ml.client.asynchronous import TagMixin


class Dataframe(TagMixin, SyncDataframe):
    """An asynchronous client dataframe.

    A `dataframe` is a two-dimensional, tabular dataset with
    labeled axes (rows and columns) that provides value to the
    model developer and/or reviewer when visualized.

    For example, confusion matrices, feature importance tables
    and marginal residuals can all be logged as a `dataframe`.

    A `dataframe` is logged to a `project` or an `experiment`.

    Parameters
    ----------
    domain : rubicon.domain.Dataframe
        The dataframe domain model.
    parent : rubicon.client.Project or rubicon.client.Experiment
        The project or experiment that the artifact is
        logged to.
    """

    pass
