import warnings

from rubicon_ml.client import Base, TagMixin
from rubicon_ml.exceptions import RubiconException


class Dataframe(Base, TagMixin):
    """A client dataframe.

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

    def __init__(self, domain, parent):
        super().__init__(domain, parent._config)

        self._data = None
        self._parent = parent

    def get_data(self, df_type="pandas"):
        """Loads the data associated with this Dataframe
        into a `pandas` or `dask` dataframe.

        Parameters
        ----------
        df_type : str, optional
            The type of dataframe. Can be either `pandas` or `dask`.
        """
        project_name, experiment_id = self.parent._get_parent_identifiers()

        self._data = self.repository.get_dataframe_data(
            project_name,
            self.id,
            experiment_id=experiment_id,
            df_type=df_type,
        )

        return self._data

    def plot(self, df_type="pandas", **kwargs):
        """Render the dataframe using `hvplot`.

        Parameters
        ----------
        df_type : str, optional
            The type of dataframe. Can be either `pandas` or `dask`.
        kwargs : dict
            Additional keyword arguments to be passed along to the
            `hvplot` function.

        Notes
        -----
        For usage, visit: https://hvplot.holoviz.org/user_guide/Plotting.html
        For customizations, visit:
        https://hvplot.holoviz.org/user_guide/Customization.html

        Examples
        --------
        >>> # Log a line plot
        >>> dataframe.plot(kind='line', x='Year', y='Number of Subscriptions')
        """
        try:
            if df_type == "pandas":
                import hvplot.pandas  # noqa F401
            else:
                import hvplot.dask  # noqa F401
        except ImportError:
            raise RubiconException(
                "`hvplot` is required for plotting. Install with `pip install hvplot`."
            )

        return self.get_data(df_type=df_type).hvplot(**kwargs)

    @property
    def id(self):
        """Get the dataframe's id."""
        return self._domain.id

    @property
    def description(self):
        """Get the dataframe's description."""
        return self._domain.description

    @property
    def created_at(self):
        """Get the time this dataframe was created."""
        return self._domain.created_at

    @property
    def data(self):
        """Get the dataframe's data as it was logged."""
        warnings.warn(
            "`data` is deprecated, use `get_data()` instead",
            DeprecationWarning,
        )
        if self._data is None:
            self.get_data()

        return self._data

    @property
    def parent(self):
        """Get the dataframe's parent client object."""
        return self._parent
