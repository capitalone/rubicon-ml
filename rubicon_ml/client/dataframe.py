from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Literal, Optional, Union

from rubicon_ml.client import Base, CommentMixin, TagMixin
from rubicon_ml.client.utils.exception_handling import failsafe
from rubicon_ml.exceptions import RubiconException

if TYPE_CHECKING:
    from rubicon_ml.client import Experiment, Project
    from rubicon_ml.domain import Dataframe as DataframeDomain


class Dataframe(Base, TagMixin, CommentMixin):
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

    def __init__(self, domain: DataframeDomain, parent: Union[Experiment, Project]):
        super().__init__(domain, parent._config)

        self._domain: DataframeDomain

        self._data = None
        self._parent = parent

    @failsafe
    def get_data(self, df_type: Literal["pandas", "dask"] = "pandas"):
        """Loads the data associated with this Dataframe
        into a `pandas` or `dask` dataframe.

        Parameters
        ----------
        df_type : str, optional
            The type of dataframe to return. Valid options include
            ["dask", "pandas"]. Defaults to "pandas".
        """
        project_name, experiment_id = self.parent._get_identifiers()
        return_err = None
        for repo in self.repositories:
            try:
                self._data = repo.get_dataframe_data(
                    project_name,
                    self.id,
                    experiment_id=experiment_id,
                    df_type=df_type,
                )
            except Exception as err:
                return_err = err
            else:
                return self._data

        raise RubiconException(return_err)

    @failsafe
    def plot(
        self,
        df_type: Literal["pandas", "dask"] = "pandas",
        plotting_func: Optional[Callable] = None,
        **kwargs,
    ):
        """Render the dataframe using `plotly.express`.

        Parameters
        ----------
        df_type : str, optional
            The type of dataframe. Can be either `pandas` or `dask`.
            Defaults to 'pandas'.
        plotting_func : function, optional
            The `plotly.express` plotting function used to visualize the
            dataframes. Available options can be found at
            https://plotly.com/python-api-reference/plotly.express.html.
            Defaults to `plotly.express.line`.
        kwargs : dict, optional
            Keyword arguments to be passed to `plotting_func`. Available options
            can be found in the documentation of the individual functions at the
            URL above.

        Examples
        --------
        >>> # Log a line plot
        >>> dataframe.plot(x='Year', y='Number of Subscriptions')

        >>> # Log a bar plot
        >>> import plotly.express as px
        >>> dataframe.plot(plotting_func=px.bar, x='Year', y='Number of Subscriptions')
        """
        try:
            import plotly.express as px

            if plotting_func is None:
                plotting_func = px.line
        except ImportError:
            raise RubiconException(
                "`ui` extras are required for plotting. Install with `pip install rubicon-ml[ui]`."
            )

        return plotting_func(self.get_data(df_type=df_type), **kwargs)

    @property
    def id(self):
        """Get the dataframe's id."""
        return self._domain.id

    @property
    def name(self):
        """Get the dataframe's name."""
        return self._domain.name

    @property
    def description(self):
        """Get the dataframe's description."""
        return self._domain.description

    @property
    def created_at(self):
        """Get the time this dataframe was created."""
        return self._domain.created_at

    @property
    def parent(self):
        """Get the dataframe's parent client object."""
        return self._parent
