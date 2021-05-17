from intake.source import base

from rubicon_ml import __version__


class DataSourceMixin(base.DataSource):
    """The base for all `rubicon` Intake data sources.

    Parameters
    ----------
    urlpath : str
        The root directory the `rubicon` data is logged to.
    project_name : str
        The name of the `rubicon` project to load.
    """

    version = __version__

    container = "python"
    name = "rubicon_ml"

    def __init__(self, urlpath, project_name, metadata=None, storage_options=None, **kwargs):
        self._urlpath = urlpath
        self._project_name = project_name
        self._metadata = metadata or {}
        self._storage_options = storage_options or {}
        self._kwargs = kwargs or {}

        super().__init__(metadata=metadata)

    def _get_schema(self):
        """Load the specified `rubicon` object."""
        self._schema = base.Schema(
            datashape=None,
            dtype=None,
            shape=None,
            npartitions=None,
            extra_metadata=self._metadata,
        )

        return self._schema

    def read(self):
        return self._rubicon_object

    def _close(self):
        self._rubicon = None
        self._rubicon_object = None
