deprecation_message = """`rubicon_ml.ui.dashboard.Dashboard` has been deprecated in favor
of `rubicon_ml.viz.dashboard.Dashboard`. For more information:

>>> from rubicon_ml.viz import Dashboard
>>> help(Dashboard)
"""


class Dashboard:
    """DEPRECATED. To be removed in a future release.

    A dashboard for exploring logged data. The dashboard
    relies on existing rubicon_ml data, which can be passed
    in the following ways:
        * by passing in an existing Rubicon object, which can be the
          the synchronous or asynchronous version (see param details below).
        * by passing in ``persistence`` and ``root_dir``, which will configure
          a rubicon object for you (useful from CLI or independent dashboard configuration).

    Parameters
    ----------
    rubicon : rubicon_ml.Rubicon or rubicon_ml.client.asynchronous.Rubicon, optional
        A top level Rubicon instance which holds the configuration for
        the data you wish to visualize.
    persistence : str, optional
        The persistence type. Can be one of ["filesystem", "memory"].
    root_dir : str, optional
        Absolute or relative filepath of the root directory holding Rubicon data.
        Use absolute path for best performance. Defaults to the local filesystem.
        Prefix with s3:// to use s3 instead.
    page_size : int, optional
        The number of rows that will be displayed on a page within the
        experiment table.
    dash_options: dict, optional
        Additional arguments specific to the Dash app. Visit the
        `docs <https://dash.plotly.com/reference>`_ to see what's
        available. Note, `requests_pathname_prefix` is useful for proxy
        troubles.
    storage_options : dict, optional
        Additional keyword arguments specific to the protocol being chosen. They
        are passed directly to the underlying filesystem class when ``persistence``
        and ``root_dir`` are used.
    """

    def __init__(
        self,
        rubicon=None,
        persistence=None,
        root_dir=None,
        page_size=10,
        dash_options={},
        **storage_options,
    ):
        raise NotImplementedError(deprecation_message)
