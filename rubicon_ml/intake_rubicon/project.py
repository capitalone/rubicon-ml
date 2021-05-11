from rubicon_ml import __version__
from rubicon_ml.client import Rubicon
from rubicon_ml.intake_rubicon.base import DataSourceMixin


class ProjectSource(DataSourceMixin):
    """An Intake data source for reading `rubicon` projects.

    Parameters
    ----------
    urlpath : str
        The root directory the `rubicon` data is logged to.
    project_name : str
        The name of the `rubicon` project to load.
    """

    version = __version__

    container = "python"
    name = "rubicon_ml_project"

    def _get_schema(self):
        """Load the project named `self._project_name`."""
        self._rubicon = Rubicon(persistence="filesystem", root_dir=self._urlpath)
        self._rubicon_object = self._rubicon.get_project(self._project_name)

        self._metadata.update(
            {
                "project": {
                    "name": self._rubicon_object.name,
                    "id": self._rubicon_object.id,
                    "description": self._rubicon_object.description,
                    "created_id": self._rubicon_object.created_at,
                }
            }
        )

        return super()._get_schema()
